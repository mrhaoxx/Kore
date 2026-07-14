package admissioncheck

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
	"github.com/zjusct/kore/pkg/scheduler"
)

// RetryRequeue 是 Retry 作业的重评周期（等分区整核释放）。
const RetryRequeue = 30 * time.Second

// Reconciler 认领名为 CheckName 的 Kueue AdmissionCheck：对已 QuotaReserved 的 pin
// 作业，按整物理核确认目标分区放得下才放行（Ready），否则留队列（Retry）或直接拒
// （Rejected）。判定与 kore-scheduler 复用同一套整核容量函数，防止 admit-then-fail。
type Reconciler struct {
	Client     client.Client
	CheckName  string
	Cache      *ReservationCache
	LeaseFresh func(node string) bool // 可选：agent 失活节点不计入容量
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	wl := &unstructured.Unstructured{}
	wl.SetGroupVersionKind(WorkloadGVK)
	return ctrl.NewControllerManagedBy(mgr).For(wl).Complete(r)
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	lg := log.FromContext(ctx)
	wlU := &unstructured.Unstructured{}
	wlU.SetGroupVersionKind(WorkloadGVK)
	if err := r.Client.Get(ctx, req.NamespacedName, wlU); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil // 删除后预留靠 TTL 退场（UID 此处不可得）
		}
		return ctrl.Result{}, err
	}
	var wl Workload
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(wlU.Object, &wl); err != nil {
		return ctrl.Result{}, err
	}
	uid := string(wl.UID)

	idx := -1
	for i := range wl.Status.AdmissionChecks {
		if wl.Status.AdmissionChecks[i].Name == r.CheckName {
			idx = i
			break
		}
	}
	if idx < 0 {
		return ctrl.Result{}, nil // 没挂我们的 check
	}
	switch wl.Status.AdmissionChecks[idx].State {
	case CheckReady:
		return ctrl.Result{}, nil // 幂等：已放行，释放靠 pin 落账后的 TTL
	case CheckRejected:
		r.Cache.Release(uid)
		return ctrl.Result{}, nil
	}
	if wl.Status.Admission == nil {
		return ctrl.Result{}, nil // 尚未 QuotaReserved
	}

	// 取第一个 pin 的 PodSet（plat101 为单 podSet batch Job）
	var pinReq Req
	var pinPodSet string
	for i := range wl.Spec.PodSets {
		rq, err := parsePinReq(&wl.Spec.PodSets[i])
		if err != nil {
			return ctrl.Result{}, r.setState(ctx, wlU, idx, CheckRejected, "kore: 注解无效: "+err.Error())
		}
		if rq.Pin {
			pinReq, pinPodSet = rq, wl.Spec.PodSets[i].Name
			break
		}
	}
	if !pinReq.Pin {
		return ctrl.Result{}, r.setState(ctx, wlU, idx, CheckReady, "kore: 非 pin 作业，放行")
	}

	flavor := flavorForPodSet(&wl, pinPodSet)
	if flavor == "" {
		return ctrl.Result{}, nil // podSet 还没分到 flavor
	}
	nodes, err := r.partitionNodes(ctx, flavor)
	if err != nil {
		return ctrl.Result{}, err
	}
	free, total, err := r.topos(ctx, nodes)
	if err != nil {
		return ctrl.Result{}, err
	}

	decision, msg, placements := Evaluate(free, total, pinReq, r.Cache.Deducted(flavor, uid))
	switch decision {
	case Ready:
		r.Cache.Reserve(uid, flavor, placements)
		lg.Info("kore admit", "workload", req.NamespacedName.String(), "flavor", flavor)
		return ctrl.Result{}, r.setState(ctx, wlU, idx, CheckReady, msg)
	case Rejected:
		r.Cache.Release(uid)
		return ctrl.Result{}, r.setState(ctx, wlU, idx, CheckRejected, msg)
	default: // Retry
		r.Cache.Release(uid)
		if err := r.setState(ctx, wlU, idx, CheckRetry, msg); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: RetryRequeue}, nil
	}
}

// partitionNodes：flavor → ResourceFlavor.spec.nodeLabels → 匹配的节点名集合。
func (r *Reconciler) partitionNodes(ctx context.Context, flavor string) ([]string, error) {
	rfU := &unstructured.Unstructured{}
	rfU.SetGroupVersionKind(ResourceFlavorGVK)
	if err := r.Client.Get(ctx, types.NamespacedName{Name: flavor}, rfU); err != nil {
		return nil, err
	}
	var rf ResourceFlavor
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(rfU.Object, &rf); err != nil {
		return nil, err
	}
	var nl corev1.NodeList
	if err := r.Client.List(ctx, &nl,
		client.MatchingLabelsSelector{Selector: labels.SelectorFromSet(rf.Spec.NodeLabels)}); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(nl.Items))
	for i := range nl.Items {
		out = append(out, nl.Items[i].Name)
	}
	return out, nil
}

// topos：为每个节点从 KoreNodeTopology 构建「当前空闲」free 与「满容量」total 两套
// zone（total 用于判「永远放不下」）。agent 失活/无 KNT 的节点跳过。
func (r *Reconciler) topos(ctx context.Context, nodes []string) (free, total []NodeTopo, err error) {
	for _, name := range nodes {
		if r.LeaseFresh != nil && !r.LeaseFresh(name) {
			continue
		}
		var cr v1alpha1.KoreNodeTopology
		if e := r.Client.Get(ctx, types.NamespacedName{Name: name}, &cr); e != nil {
			if apierrors.IsNotFound(e) {
				continue
			}
			return nil, nil, e
		}
		fz, e := scheduler.ZonesFromCR(&cr)
		if e != nil {
			continue
		}
		tz, e := totalZones(&cr)
		if e != nil {
			continue
		}
		free = append(free, NodeTopo{Node: name, Zones: fz})
		total = append(total, NodeTopo{Node: name, Zones: tz})
	}
	return free, total, nil
}

// totalZones：把每 zone 的 Free 设为满容量（Cpus − 系统预留），siblings 不变。
func totalZones(cr *v1alpha1.KoreNodeTopology) ([]scheduler.ZoneCap, error) {
	reserved, _ := cpuset.Parse(cr.Status.ReservedSystemCpus)
	out := make([]scheduler.ZoneCap, 0, len(cr.Status.Zones))
	for _, z := range cr.Status.Zones {
		all, e := cpuset.Parse(z.Cpus)
		if e != nil {
			return nil, e
		}
		tpc := 1
		if len(z.SMTSiblings) > 0 {
			tpc = len(z.SMTSiblings[0])
		}
		out = append(out, scheduler.ZoneCap{ID: z.ID, Free: all.Difference(reserved), TPC: tpc, Siblings: z.SMTSiblings})
	}
	return out, nil
}

// setState 幂等地把 status.admissionChecks[idx] 的 state/message 写回（无变化则跳过）。
func (r *Reconciler) setState(ctx context.Context, wlU *unstructured.Unstructured, idx int, state, msg string) error {
	checks, found, err := unstructured.NestedSlice(wlU.Object, "status", "admissionChecks")
	if err != nil || !found || idx >= len(checks) {
		return err
	}
	m, ok := checks[idx].(map[string]interface{})
	if !ok {
		return nil
	}
	if m["state"] == state && m["message"] == msg {
		return nil // 无变化，避免抖动
	}
	before := wlU.DeepCopy()
	m["state"] = state
	m["message"] = msg
	m["lastTransitionTime"] = metav1.Now().UTC().Format(time.RFC3339)
	if err := unstructured.SetNestedSlice(wlU.Object, checks, "status", "admissionChecks"); err != nil {
		return err
	}
	// merge patch（不带 resourceVersion）写回：Kueue 频繁改 Workload，全量
	// Status().Update 会 409 冲突、状态永远推不动；merge patch 应用于最新对象。
	return r.Client.Status().Patch(ctx, wlU, client.MergeFrom(before))
}
