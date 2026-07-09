package nriplugin

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/containerd/nri/pkg/api"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/cpuset"

	"github.com/zjusct/kore/pkg/allocator"
	"github.com/zjusct/kore/pkg/metrics"
	"github.com/zjusct/kore/pkg/request"
	"github.com/zjusct/kore/pkg/topology"
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

// Synchronize 重建账本。铁律：单个容器的数据问题（坏注解、恢复冲突）绝不
// 返回 error——那会让 NRI 注册失败、agent 崩死循环、门闩挡掉整节点新绑核 Pod。
// 一律降级为 strict 处置（删 Pod + 事件）。
func (p *Plugin) Synchronize(ctx context.Context, pods []*api.PodSandbox, containers []*api.Container) ([]*api.ContainerUpdate, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	sandboxes := map[string]*api.PodSandbox{}
	for _, pod := range pods {
		sandboxes[pod.Id] = pod
	}
	type poolCand struct {
		c    *api.Container
		pod  *api.PodSandbox
		cpus cpuset.CPUSet
	}
	poolCands := map[string][]poolCand{}
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
		cs := c.Annotations[request.AnnoAllocated]
		poolName := pod.Annotations[request.AnnoPool]
		switch {
		case cs != "" && poolName != "":
			cpus, perr := cpuset.Parse(cs)
			if perr != nil {
				p.restoreFailedLocked(pod, c, perr)
				continue
			}
			poolCands[poolName] = append(poolCands[poolName], poolCand{c: c, pod: pod, cpus: cpus})
		case cs != "":
			if err := p.restore(pod, c, cs); err != nil {
				p.restoreFailedLocked(pod, c, err)
			}
		default:
			// 绑核 Pod 的容器但无分配注解：可能是 sidecar，也可能是"该绑未绑"
			if u := p.remediateLocked(pod, c); u != nil {
				extra = append(extra, u)
			}
		}
	}
	// 池两阶段恢复：在线扩缩容后老成员的容器注解仍是旧集合（容器注解不可变），
	// 以"注解核数 == Pod 声明 pool-size"的成员为权威，其余夹回权威集合自愈。
	for name, cands := range poolCands {
		auth := cands[0]
		if declared, err := strconv.Atoi(auth.pod.Annotations[request.AnnoPoolSize]); err == nil {
			for _, cd := range cands {
				if cd.cpus.Size() == declared {
					auth = cd
					break
				}
			}
		}
		if err := p.state.RestorePoolMember(name, auth.cpus, auth.pod.Uid); err != nil {
			p.restoreFailedLocked(auth.pod, auth.c, err)
			continue
		}
		p.trackPoolCtrLocked(name, auth.c.Id)
		mems := memsForCpus(auth.pod, auth.cpus, p.topo)
		for _, cd := range cands {
			if cd.c.Id == auth.c.Id {
				continue
			}
			if err := p.state.RestorePoolMember(name, auth.cpus, cd.pod.Uid); err != nil {
				p.restoreFailedLocked(cd.pod, cd.c, err)
				continue
			}
			p.trackPoolCtrLocked(name, cd.c.Id)
			if !cd.cpus.Equals(auth.cpus) { // 陈旧注解 → 夹回权威集合
				u := &api.ContainerUpdate{}
				u.SetContainerId(cd.c.Id)
				u.SetLinuxCPUSetCPUs(auth.cpus.String())
				u.SetLinuxCPUSetMems(mems)
				u.IgnoreFailure = true
				extra = append(extra, u)
			}
		}
	}
	p.rep.Report(allocator.BuildStatus(p.state))
	return append(p.shrinkSharedLocked(), extra...), nil
}

// restoreFailedLocked：单容器恢复失败的降级处置——删 Pod 重建 + 事件，不崩 agent。
func (p *Plugin) restoreFailedLocked(pod *api.PodSandbox, c *api.Container, err error) {
	metrics.Remediation("strict")
	if kpod, gerr := p.pods.GetPod(pod.Namespace, pod.Name); gerr == nil {
		p.rec.Event(kpod, corev1.EventTypeWarning, "KoreRestoreConflict",
			fmt.Sprintf("container %s ledger restore failed (%v); deleting pod for rebind", c.Name, err))
	}
	p.rec.DeletePod(pod.Namespace, pod.Name)
}

func (p *Plugin) trackPoolCtrLocked(name, ctrID string) {
	if p.poolCtrs[name] == nil {
		p.poolCtrs[name] = map[string]bool{}
	}
	p.poolCtrs[name][ctrID] = true
}

// memsForCpus 按 Pod 的 memory-policy 注解为一组核算 mems。
func memsForCpus(pod *api.PodSandbox, cpus cpuset.CPUSet, topo *topology.Topology) string {
	policy := request.MemoryPolicy(pod.Annotations[request.AnnoMemoryPolicy])
	numa := map[int]bool{}
	for _, c := range cpus.List() {
		numa[topo.ZoneOf(c)] = true
	}
	ids := make([]int, 0, len(numa))
	for z := range numa {
		ids = append(ids, z)
	}
	sort.Ints(ids)
	return MemsFor(policy, ids, topo)
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
