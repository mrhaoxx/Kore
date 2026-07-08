package nriplugin

import (
	"context"
	"fmt"
	"sort"

	"github.com/containerd/nri/pkg/api"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/cpuset"

	"github.com/zjusct/kore/pkg/allocator"
	"github.com/zjusct/kore/pkg/metrics"
	"github.com/zjusct/kore/pkg/request"
)

func (p *Plugin) StopPodSandbox(ctx context.Context, pod *api.PodSandbox) error {
	p.releasePod(pod.Uid)
	return nil
}

func (p *Plugin) RemovePodSandbox(ctx context.Context, pod *api.PodSandbox) error {
	p.releasePod(pod.Uid)
	return nil
}

func (p *Plugin) RemoveContainer(ctx context.Context, pod *api.PodSandbox, ctr *api.Container) error {
	p.mu.Lock()
	delete(p.shared, ctr.Id)
	for _, ids := range p.poolCtrs {
		delete(ids, ctr.Id)
	}
	p.mu.Unlock()
	return nil
}

// releasePod 释放某 Pod 的全部独占分配并把共享池扩张推给运行时。
func (p *Plugin) releasePod(uid string) {
	p.mu.Lock()
	before := p.state.Used().Size()
	p.state.Release(uid)
	changed := p.state.Used().Size() != before
	updates := p.shrinkSharedLocked()
	p.mu.Unlock()
	if !changed {
		return
	}
	if p.updater != nil {
		_ = p.updater(updates) // 尽力而为；失败由下次 Synchronize 对齐
	}
	p.rep.Report(allocator.BuildStatus(p.state))
}

func (p *Plugin) Synchronize(ctx context.Context, pods []*api.PodSandbox, containers []*api.Container) ([]*api.ContainerUpdate, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	sandboxes := map[string]*api.PodSandbox{}
	for _, pod := range pods {
		sandboxes[pod.Id] = pod
	}
	var extra []*api.ContainerUpdate
	for _, c := range containers {
		if c.State == api.ContainerState_CONTAINER_STOPPED {
			continue
		}
		pod := sandboxes[c.PodSandboxId]
		if pod == nil || (pod.Annotations[request.AnnoPin] != "true" && pod.Annotations[request.AnnoPool] == "") {
			p.shared[c.Id] = true
			continue
		}
		if cs := c.Annotations[request.AnnoAllocated]; cs != "" {
			if poolName := pod.Annotations[request.AnnoPool]; poolName != "" {
				cpus, perr := cpuset.Parse(cs)
				if perr != nil {
					return nil, fmt.Errorf("kore: pool container %s bad %s annotation %q: %w", c.Id, request.AnnoAllocated, cs, perr)
				}
				if err := p.state.RestorePoolMember(poolName, cpus, pod.Uid); err != nil {
					return nil, err
				}
				if p.poolCtrs[poolName] == nil {
					p.poolCtrs[poolName] = map[string]bool{}
				}
				p.poolCtrs[poolName][c.Id] = true
				continue
			}
			if err := p.restore(pod, c, cs); err != nil {
				return nil, err
			}
			continue
		}
		// 绑核 Pod 的容器但无分配注解：可能是 sidecar，也可能是"该绑未绑"
		if u := p.remediateLocked(pod, c); u != nil {
			extra = append(extra, u)
		}
	}
	p.rep.Report(allocator.BuildStatus(p.state))
	return append(p.shrinkSharedLocked(), extra...), nil
}

func (p *Plugin) restore(pod *api.PodSandbox, c *api.Container, cs string) error {
	cpus, err := cpuset.Parse(cs)
	if err != nil {
		return fmt.Errorf("kore: container %s bad %s annotation %q: %w", c.Id, request.AnnoAllocated, cs, err)
	}
	numa := map[int]bool{}
	for _, cpu := range cpus.List() {
		numa[p.topo.ZoneOf(cpu)] = true
	}
	ids := make([]int, 0, len(numa))
	for z := range numa {
		ids = append(ids, z)
	}
	sort.Ints(ids)
	return p.state.Restore(allocator.Allocation{
		PodUID: pod.Uid, Pod: pod.Namespace + "/" + pod.Name, Container: c.Name,
		CPUs: cpus, NUMA: ids,
	})
}

// remediateLocked 处理"该绑未绑"的容器（spec §6 兜底对账）。返回 repair 模式的补绑更新。
func (p *Plugin) remediateLocked(pod *api.PodSandbox, c *api.Container) *api.ContainerUpdate {
	kpod, err := p.pods.GetPod(pod.Namespace, pod.Name)
	if err != nil {
		return nil // Pod 已不在 API server：容器即将被回收，不处理
	}
	req, err := request.ParsePod(kpod)
	if err != nil || req == nil {
		return nil
	}
	if req.Pool != "" { // 该入池未入池
		if p.cfg.Remediation == "repair" {
			adj, _, jerr := p.joinPoolLocked(kpod, req, c)
			if jerr != nil {
				p.rec.Event(kpod, corev1.EventTypeWarning, "KoreUnboundContainer",
					fmt.Sprintf("pool member %s is not in pool %q; repair failed: %v", c.Name, req.Pool, jerr))
				return nil
			}
			p.rec.Event(kpod, corev1.EventTypeWarning, "KoreRepairedBinding",
				fmt.Sprintf("pool member %s re-joined pool %q", c.Name, req.Pool))
			u := &api.ContainerUpdate{}
			u.SetContainerId(c.Id)
			u.SetLinuxCPUSetCPUs(adj.GetLinux().GetResources().GetCpu().GetCpus())
			u.SetLinuxCPUSetMems(adj.GetLinux().GetResources().GetCpu().GetMems())
			return u
		}
		p.rec.Event(kpod, corev1.EventTypeWarning, "KoreUnboundContainer",
			fmt.Sprintf("pool member %s was running outside pool %q (agent was down at creation); deleting pod", c.Name, req.Pool))
		p.rec.DeletePod(pod.Namespace, pod.Name)
		return nil
	}
	ar, pinned := BuildAllocRequest(kpod, req, p.cfg, c.Name)
	if !pinned { // sidecar → 共享池
		p.shared[c.Id] = true
		return nil
	}
	if p.cfg.Remediation == "repair" {
		metrics.Remediation("repair")
		a, aerr := p.state.Allocate(*ar)
		if aerr != nil {
			p.rec.Event(kpod, corev1.EventTypeWarning, "KoreUnboundContainer",
				fmt.Sprintf("container %s should be pinned but is not; repair failed: %v", c.Name, aerr))
			return nil
		}
		p.rec.Event(kpod, corev1.EventTypeWarning, "KoreRepairedBinding",
			fmt.Sprintf("container %s was running unpinned; re-bound to %s (memory may be non-local, consider restarting the pod)", c.Name, a.CPUs))
		p.rec.SetPodAnnotation(kpod, request.AnnoAllocated, a.CPUs.String())
		u := &api.ContainerUpdate{}
		u.SetContainerId(c.Id)
		u.SetLinuxCPUSetCPUs(a.CPUs.String())
		u.SetLinuxCPUSetMems(MemsFor(req.MemoryPolicy, a.NUMA, p.topo))
		return u
	}
	// strict：杀掉重建，绝不允许无绑定运行
	metrics.Remediation("strict")
	p.rec.Event(kpod, corev1.EventTypeWarning, "KoreUnboundContainer",
		fmt.Sprintf("container %s was running unpinned (agent was down at creation); deleting pod for rebind", c.Name))
	p.rec.DeletePod(pod.Namespace, pod.Name)
	return nil
}
