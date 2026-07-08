package nriplugin

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/containerd/nri/pkg/api"
	corev1 "k8s.io/api/core/v1"

	"github.com/zjusct/kore/pkg/agent/config"
	"github.com/zjusct/kore/pkg/allocator"
	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
	"github.com/zjusct/kore/pkg/metrics"
	"github.com/zjusct/kore/pkg/request"
	"github.com/zjusct/kore/pkg/topology"
)

type PodGetter interface {
	GetPod(namespace, name string) (*corev1.Pod, error)
}

// Recorder 的实现必须异步/尽力而为——这些调用发生在 NRI hook 关键路径上。
type Recorder interface {
	Event(pod *corev1.Pod, eventType, reason, msg string)
	SetPodAnnotation(pod *corev1.Pod, key, value string)
	DeletePod(namespace, name string)
}

type Reporter interface {
	Report(st v1alpha1.KoreNodeTopologyStatus)
}

type Plugin struct {
	mu    sync.Mutex
	topo  *topology.Topology
	cfg   *config.Config
	state *allocator.State
	pods  PodGetter
	rec   Recorder
	rep   Reporter

	shared   map[string]bool            // 共享池容器 id（围栏对象）
	poolCtrs map[string]map[string]bool // 池名 → 成员容器 id（resize 广播用）
	updater  func([]*api.ContainerUpdate) error
}

func New(topo *topology.Topology, cfg *config.Config, pods PodGetter, rec Recorder, rep Reporter) (*Plugin, error) {
	reserved, err := cfg.Reserved()
	if err != nil {
		return nil, err
	}
	return &Plugin{
		topo: topo, cfg: cfg,
		state: allocator.NewState(topo, reserved, cfg.SharedPoolMin),
		pods:  pods, rec: rec, rep: rep,
		shared: map[string]bool{}, poolCtrs: map[string]map[string]bool{},
	}, nil
}

// SetUpdater 注入 stub.UpdateContainers，用于 hook 之外主动收放共享池。
func (p *Plugin) SetUpdater(fn func([]*api.ContainerUpdate) error) { p.updater = fn }

func (p *Plugin) Configure(ctx context.Context, cfg, runtime, version string) (api.EventMask, error) {
	return api.ParseEventMask("CreateContainer,StopPodSandbox,RemovePodSandbox,RemoveContainer")
}

func (p *Plugin) CreateContainer(ctx context.Context, pod *api.PodSandbox, ctr *api.Container) (*api.ContainerAdjustment, []*api.ContainerUpdate, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if pod.Annotations[request.AnnoPin] != "true" && pod.Annotations[request.AnnoPool] == "" {
		return p.fenceLocked(ctr), nil, nil
	}
	kpod, err := p.pods.GetPod(pod.Namespace, pod.Name)
	if err != nil {
		return nil, nil, fmt.Errorf("kore: pinned pod %s/%s: pod spec unavailable: %w", pod.Namespace, pod.Name, err)
	}
	req, err := request.ParsePod(kpod)
	if err != nil {
		p.rec.Event(kpod, corev1.EventTypeWarning, "KoreInvalidAnnotations", err.Error())
		return nil, nil, fmt.Errorf("kore: %w", err)
	}
	if req == nil { // sandbox 注解说 pin 但 Pod 对象已不是 → 以 Pod 为准
		return p.fenceLocked(ctr), nil, nil
	}
	if req.Pool != "" {
		return p.joinPoolLocked(kpod, req, ctr)
	}
	ar, pinned := BuildAllocRequest(kpod, req, p.cfg, ctr.Name)
	if !pinned {
		return p.fenceLocked(ctr), nil, nil
	}
	a, err := p.state.Allocate(*ar)
	if err != nil {
		metrics.AllocFailure("pin")
		p.rec.Event(kpod, corev1.EventTypeWarning, "KoreAllocationFailed", err.Error())
		return nil, nil, fmt.Errorf("kore: %w", err)
	}
	adj := &api.ContainerAdjustment{}
	adj.SetLinuxCPUSetCPUs(a.CPUs.String())
	adj.SetLinuxCPUSetMems(MemsFor(req.MemoryPolicy, a.NUMA, p.topo))
	adj.AddAnnotation(request.AnnoAllocated, a.CPUs.String()) // Synchronize 重建的依据

	p.rec.SetPodAnnotation(kpod, request.AnnoAllocated, a.CPUs.String())
	p.rep.Report(allocator.BuildStatus(p.state))
	return adj, p.shrinkSharedLocked(), nil
}

// joinPoolLocked 让 Pod 的容器加入（必要时创建）CPU 池。调用方须持锁。
func (p *Plugin) joinPoolLocked(kpod *corev1.Pod, req *request.Request, ctr *api.Container) (*api.ContainerAdjustment, []*api.ContainerUpdate, error) {
	pr := allocator.PoolRequest{
		Name: req.Pool, Size: req.PoolSize, PodUID: string(kpod.UID),
		NUMAPolicy: req.NUMAPolicy, Placement: req.Placement,
		PodCreated: kpod.CreationTimestamp.Time,
	}
	var prev *allocator.PoolInfo
	for _, pl := range p.state.Pools() {
		if pl.Name == req.Pool {
			cp := pl
			prev = &cp
			break
		}
	}
	if pr.Placement == "" {
		pr.Placement = request.Placement(p.cfg.DefaultPlacement)
	}
	if v, ok := kpod.Annotations[request.AnnoReservedNUMA]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			pr.ReservedNUMA = &n
		}
	}
	info, err := p.state.JoinPool(pr)
	if err != nil {
		metrics.AllocFailure("pool")
		p.rec.Event(kpod, corev1.EventTypeWarning, "KorePoolFailed", err.Error())
		return nil, nil, fmt.Errorf("kore: %w", err)
	}
	adj := &api.ContainerAdjustment{}
	adj.SetLinuxCPUSetCPUs(info.CPUs.String())
	adj.SetLinuxCPUSetMems(MemsFor(req.MemoryPolicy, info.NUMA, p.topo))
	adj.AddAnnotation(request.AnnoAllocated, info.CPUs.String())
	p.rec.SetPodAnnotation(kpod, request.AnnoAllocated, info.CPUs.String())
	p.rep.Report(allocator.BuildStatus(p.state))

	updates := p.shrinkSharedLocked()
	if prev != nil && !prev.CPUs.Equals(info.CPUs) { // 在线扩缩容 → 广播全体成员
		mems := MemsFor(req.MemoryPolicy, info.NUMA, p.topo)
		for id := range p.poolCtrs[req.Pool] {
			u := &api.ContainerUpdate{}
			u.SetContainerId(id)
			u.SetLinuxCPUSetCPUs(info.CPUs.String())
			u.SetLinuxCPUSetMems(mems)
			u.IgnoreFailure = true
			updates = append(updates, u)
		}
	}
	if p.poolCtrs[req.Pool] == nil {
		p.poolCtrs[req.Pool] = map[string]bool{}
	}
	p.poolCtrs[req.Pool][ctr.Id] = true
	return adj, updates, nil
}

// fenceLocked 把非绑核容器围栏进共享池。调用方须持锁。
func (p *Plugin) fenceLocked(ctr *api.Container) *api.ContainerAdjustment {
	p.shared[ctr.Id] = true
	adj := &api.ContainerAdjustment{}
	adj.SetLinuxCPUSetCPUs(p.state.SharedPool().String())
	return adj
}

// shrinkSharedLocked 生成把全部共享容器夹到当前共享池的更新。调用方须持锁。
func (p *Plugin) shrinkSharedLocked() []*api.ContainerUpdate {
	pool := p.state.SharedPool().String()
	out := make([]*api.ContainerUpdate, 0, len(p.shared))
	for id := range p.shared {
		u := &api.ContainerUpdate{}
		u.SetContainerId(id)
		u.SetLinuxCPUSetCPUs(pool)
		u.IgnoreFailure = true // 容器可能恰好退出，围栏失败不应连坐
		out = append(out, u)
	}
	return out
}
