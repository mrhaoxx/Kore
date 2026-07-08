package operator

import (
	"context"
	"strings"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TaintKey 是 agent 失活污点（spec §6 三重防线第 1 层：默认调度器的 Pod 也挡住）。
const TaintKey = "kore.zjusct.io/agent-down"

const leasePrefix = "kore-agent-"

// LeaseExpired 判断 agent Lease 是否过期；字段缺失视为过期（fail-closed）。
func LeaseExpired(l *coordv1.Lease, now time.Time) bool {
	if l.Spec.RenewTime == nil || l.Spec.LeaseDurationSeconds == nil {
		return true
	}
	d := time.Duration(*l.Spec.LeaseDurationSeconds) * time.Second
	return now.Sub(l.Spec.RenewTime.Time) > d
}

// TaintReconciler watch kore-system 的 agent Lease，按存活状态增删节点污点。
type TaintReconciler struct {
	client.Client
	Now func() time.Time
}

func (r *TaintReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if !strings.HasPrefix(req.Name, leasePrefix) {
		return ctrl.Result{}, nil
	}
	nodeName := strings.TrimPrefix(req.Name, leasePrefix)

	var l coordv1.Lease
	expired := true
	if err := r.Get(ctx, req.NamespacedName, &l); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		// Lease 消失 = agent 从未起过或被清理 → 视为失活
	} else {
		expired = LeaseExpired(&l, r.Now())
	}

	var node corev1.Node
	if err := r.Get(ctx, types.NamespacedName{Name: nodeName}, &node); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil // 节点不存在：无事可做
		}
		return ctrl.Result{}, err
	}

	has := -1
	for i, t := range node.Spec.Taints {
		if t.Key == TaintKey {
			has = i
			break
		}
	}
	switch {
	case expired && has < 0:
		node.Spec.Taints = append(node.Spec.Taints, corev1.Taint{
			Key: TaintKey, Effect: corev1.TaintEffectNoSchedule,
		})
		if err := r.Update(ctx, &node); err != nil {
			return ctrl.Result{}, err
		}
	case !expired && has >= 0:
		node.Spec.Taints = append(node.Spec.Taints[:has], node.Spec.Taints[has+1:]...)
		if err := r.Update(ctx, &node); err != nil {
			return ctrl.Result{}, err
		}
	}
	// 周期重查：Lease 过期不会自己产生事件
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}
