package scheduler

import (
	"context"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/utils/cpuset"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
	"github.com/zjusct/kore/pkg/request"
)

const (
	Name                               = "Kore"
	stateKey              fwk.StateKey = "kore.zjusct.io/state"
	leaseNamespace                     = "kore-system"
	defaultReservationTTL              = 5 * time.Minute
)

type Deps struct {
	ListTopologies     func(ctx context.Context) ([]v1alpha1.KoreNodeTopology, error)
	LeaseFresh         func(node string) bool
	PatchPodAnnotation func(ctx context.Context, ns, name, key, value string) error
}

type poolSnap struct {
	size    int
	members []string
	// pending：来自在途建池预占（Reserve 已占、CR 未体现）。跟随可以，
	// 但 size 增量不行（容量未落定）。
	pending bool
}

type nodeSnap struct {
	zones   []ZoneCap
	pools   map[string]poolSnap
	leaseOK bool
	found   bool
}

type koreState struct {
	req    *request.Request
	need   int
	byNode map[string]nodeSnap
}

func (s *koreState) Clone() fwk.StateData { return s }

type Kore struct {
	deps  Deps
	cache *Cache
}

var (
	_ fwk.PreFilterPlugin = &Kore{}
	_ fwk.FilterPlugin    = &Kore{}
	_ fwk.ScorePlugin     = &Kore{}
	_ fwk.ReservePlugin   = &Kore{}
	_ fwk.PreBindPlugin   = &Kore{}
)

func NewWithDeps(deps Deps, cache *Cache) *Kore { return &Kore{deps: deps, cache: cache} }

func (k *Kore) Name() string { return Name }

func (k *Kore) PreFilter(ctx context.Context, state fwk.CycleState, pod *corev1.Pod, _ []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	req, err := request.ParsePod(pod)
	if err != nil {
		return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "kore: "+err.Error())
	}
	if req == nil {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	need := 0
	for _, c := range req.Containers {
		need += c.CPUs
	}
	if req.Pool != "" {
		need = req.PoolSize
	}
	crs, err := k.deps.ListTopologies(ctx)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}
	st := &koreState{req: req, need: need, byNode: map[string]nodeSnap{}}
	for i := range crs {
		cr := &crs[i]
		// CR 已体现的分配/池成员 → 清对应预占，避免双重扣减
		uids := map[string]bool{}
		for _, a := range cr.Status.Allocations {
			uids[a.PodUID] = true
		}
		pools := map[string]poolSnap{}
		for _, pl := range cr.Status.Pools {
			cs, perr := cpuset.Parse(pl.Cpuset)
			if perr != nil {
				continue
			}
			pools[pl.Name] = poolSnap{size: cs.Size(), members: pl.Members}
			for _, m := range pl.Members {
				uids[m] = true
			}
		}
		k.cache.MarkAllocated(cr.Name, uids)

		zones, zerr := ZonesFromCR(cr)
		if zerr != nil {
			continue // 坏 CR 视同节点无拓扑
		}
		rs := k.cache.ByNode(cr.Name)
		zones = Deduct(zones, rs)
		for _, r := range rs { // 在途建池预占 → 视作 pending 池可跟随
			if r.Pool != "" {
				if _, exists := pools[r.Pool]; !exists {
					pools[r.Pool] = poolSnap{size: r.Count, pending: true}
				}
			}
		}
		st.byNode[cr.Name] = nodeSnap{zones: zones, pools: pools, leaseOK: k.deps.LeaseFresh(cr.Name), found: true}
	}
	state.Write(stateKey, st)
	return nil, nil
}

func (k *Kore) PreFilterExtensions() fwk.PreFilterExtensions { return nil }

func getState(state fwk.CycleState) (*koreState, bool) {
	v, err := state.Read(stateKey)
	if err != nil {
		return nil, false
	}
	st, ok := v.(*koreState)
	return st, ok
}

func (k *Kore) Filter(ctx context.Context, state fwk.CycleState, pod *corev1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	st, ok := getState(state)
	if !ok {
		return nil
	}
	node := nodeInfo.Node().Name
	ns := st.byNode[node]
	if !ns.found {
		return fwk.NewStatus(fwk.Unschedulable, "kore: no topology reported for node")
	}
	if !ns.leaseOK {
		return fwk.NewStatus(fwk.Unschedulable, "kore: agent lease expired on node")
	}
	if st.req.Pool != "" {
		if ps, ok := ns.pools[st.req.Pool]; ok {
			if ps.pending && ps.size != st.req.PoolSize {
				return fwk.NewStatus(fwk.Unschedulable, "kore: pending pool with different size")
			}
			if delta := st.req.PoolSize - ps.size; delta > 0 && TotalFree(ns.zones) < delta {
				return fwk.NewStatus(fwk.Unschedulable, "kore: insufficient free cpus to grow pool")
			}
			return nil // 跟随（含在线扩缩容：agent 侧按成员时间戳裁决）
		}
		// 建池：按 numa-policy 检查容量（池用逻辑核，跳过 SMT 对齐）
		switch st.req.NUMAPolicy {
		case request.NUMASpread:
			if !FitSpread(ns.zones, st.need) {
				return fwk.NewStatus(fwk.Unschedulable, "kore: insufficient free cpus for pool")
			}
		case request.NUMAPreferred:
			if _, ok := FitPreferred(ns.zones, st.need); !ok {
				return fwk.NewStatus(fwk.Unschedulable, "kore: insufficient free cpus for pool")
			}
		default:
			if _, ok := FitSingle(ns.zones, st.need); !ok {
				return fwk.NewStatus(fwk.Unschedulable, "kore: no NUMA zone can host the pool")
			}
		}
		return nil
	}
	if st.req.Explicit != nil {
		if !FitExplicit(ns.zones, *st.req.Explicit) {
			return fwk.NewStatus(fwk.Unschedulable, "kore: explicit cpuset not free on node")
		}
		return nil
	}
	// 调度器不知道 agent 的 ConfigMap 默认值；按注解未写 logical 即 full-core 保守判断
	if st.req.SMTPolicy != request.SMTLogical && !AlignFullCore(ns.zones, st.need) {
		return fwk.NewStatus(fwk.Unschedulable, "kore: cpu count not aligned to full cores on SMT node")
	}
	zones := effZones(st.req, ns.zones) // full-core：按整物理核容量判定
	switch st.req.NUMAPolicy {
	case request.NUMASpread:
		if !FitSpread(zones, st.need) {
			return fwk.NewStatus(fwk.Unschedulable, "kore: insufficient free cpus for spread")
		}
	case request.NUMAPreferred:
		if _, ok := FitPreferred(zones, st.need); !ok {
			return fwk.NewStatus(fwk.Unschedulable, "kore: insufficient free cpus")
		}
	default:
		if _, ok := FitSingle(zones, st.need); !ok {
			return fwk.NewStatus(fwk.Unschedulable, "kore: no NUMA zone with enough free cpus")
		}
	}
	return nil
}

func (k *Kore) Score(ctx context.Context, state fwk.CycleState, pod *corev1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	st, ok := getState(state)
	if !ok {
		return 0, nil
	}
	ns := st.byNode[nodeInfo.Node().Name]
	if !ns.found {
		return 0, nil
	}
	if st.req.Pool != "" {
		if _, ok := ns.pools[st.req.Pool]; ok {
			return 100, nil // 已有池的节点最优（成员聚合）
		}
		// 建池上限 99：保证“跟随已有池”严格胜过“新建池”（否则恰好整 zone
		// 命中的建池节点会打平 100，成员被摊到多节点裂成同名多池）
		if s := ScoreFit(ns.zones, st.req.NUMAPolicy, false, st.need); s > 99 {
			return 99, nil
		} else {
			return s, nil
		}
	}
	return ScoreFit(effZones(st.req, ns.zones), st.req.NUMAPolicy, st.req.Explicit != nil, st.need), nil
}

func (k *Kore) ScoreExtensions() fwk.ScoreExtensions { return nil }

func (k *Kore) Reserve(ctx context.Context, state fwk.CycleState, pod *corev1.Pod, nodeName string) *fwk.Status {
	st, ok := getState(state)
	if !ok {
		return nil
	}
	ns := st.byNode[nodeName]
	if st.req.Pool != "" {
		if _, ok := ns.pools[st.req.Pool]; ok {
			return nil // 跟随者不预占
		}
		r := Reservation{PodUID: string(pod.UID), Node: nodeName, Zone: -1, Count: st.need, Pool: st.req.Pool}
		switch st.req.NUMAPolicy {
		case request.NUMASpread:
		case request.NUMAPreferred:
			if z, ok := FitPreferred(ns.zones, st.need); ok {
				r.Zone = z
			}
		default:
			z, fits := FitSingle(ns.zones, st.need)
			if !fits {
				return fwk.NewStatus(fwk.Unschedulable, "kore: capacity changed during scheduling cycle")
			}
			r.Zone = z
		}
		k.cache.Add(r)
		return nil
	}
	r := Reservation{PodUID: string(pod.UID), Node: nodeName, Zone: -1, Count: st.need, Explicit: st.req.Explicit}
	if st.req.Explicit == nil {
		zones := effZones(st.req, ns.zones) // full-core：按整物理核选 zone
		switch st.req.NUMAPolicy {
		case request.NUMASpread:
			// zone 保持 -1
		case request.NUMAPreferred:
			if z, ok := FitPreferred(zones, st.need); ok {
				r.Zone = z
			}
		default:
			z, fits := FitSingle(zones, st.need)
			if !fits {
				return fwk.NewStatus(fwk.Unschedulable, "kore: capacity changed during scheduling cycle")
			}
			r.Zone = z
		}
	}
	k.cache.Add(r)
	return nil
}

func (k *Kore) Unreserve(ctx context.Context, state fwk.CycleState, pod *corev1.Pod, nodeName string) {
	k.cache.Remove(string(pod.UID))
}

func (k *Kore) PreBindPreFlight(ctx context.Context, state fwk.CycleState, pod *corev1.Pod, nodeName string) (*fwk.PreBindPreFlightResult, *fwk.Status) {
	if _, ok := getState(state); !ok {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	return nil, nil
}

func (k *Kore) PreBind(ctx context.Context, state fwk.CycleState, pod *corev1.Pod, nodeName string) *fwk.Status {
	if _, ok := getState(state); !ok {
		return nil
	}
	r, ok := k.cache.Get(string(pod.UID))
	if !ok || r.Zone < 0 {
		return nil // spread/explicit：agent 不需要 reserved-numa
	}
	if err := k.deps.PatchPodAnnotation(ctx, pod.Namespace, pod.Name, request.AnnoReservedNUMA, strconv.Itoa(r.Zone)); err != nil {
		return fwk.AsStatus(err)
	}
	return nil
}

// New 是 scheduler framework 的插件工厂。
func New(ctx context.Context, _ runtime.Object, h fwk.Handle) (fwk.Plugin, error) {
	sch := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(sch); err != nil {
		return nil, err
	}
	crc, err := ctrlclient.New(h.KubeConfig(), ctrlclient.Options{Scheme: sch})
	if err != nil {
		return nil, err
	}
	leaseLister := h.SharedInformerFactory().Coordination().V1().Leases().Lister()
	deps := Deps{
		ListTopologies: func(ctx context.Context) ([]v1alpha1.KoreNodeTopology, error) {
			var l v1alpha1.KoreNodeTopologyList
			if err := crc.List(ctx, &l); err != nil {
				return nil, err
			}
			return l.Items, nil
		},
		LeaseFresh: func(node string) bool {
			l, err := leaseLister.Leases(leaseNamespace).Get("kore-agent-" + node)
			if err != nil || l.Spec.RenewTime == nil {
				return false
			}
			d := 15 * time.Second
			if l.Spec.LeaseDurationSeconds != nil {
				d = time.Duration(*l.Spec.LeaseDurationSeconds) * time.Second
			}
			return time.Since(l.Spec.RenewTime.Time) <= d
		},
		PatchPodAnnotation: func(ctx context.Context, ns, name, key, value string) error {
			patch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, key, value))
			_, err := h.ClientSet().CoreV1().Pods(ns).Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
			return err
		},
	}
	resCache := NewCache(defaultReservationTTL)
	// Pod 删除即时清预占：堵住“Reserve+Bind 后落账前被删”的泄漏口（否则卡满 5min TTL）。
	if _, err := h.SharedInformerFactory().Core().V1().Pods().Informer().AddEventHandler(
		toolscache.ResourceEventHandlerFuncs{
			DeleteFunc: func(obj interface{}) { removeReservationOnDelete(resCache, obj) },
		}); err != nil {
		return nil, err
	}
	return NewWithDeps(deps, resCache), nil
}
