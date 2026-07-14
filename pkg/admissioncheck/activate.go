package admissioncheck

import (
	"context"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ActivateReconciler 把本控制器认领（spec.controllerName == ControllerName）的
// AdmissionCheck 置 status.conditions[Active]=True。Kueue 要求 AdmissionCheck
// 控制器声明自己就绪，否则挂了该 check 的 ClusterQueue 上作业无法完成准入。
type ActivateReconciler struct {
	Client         client.Client
	ControllerName string
}

func (r *ActivateReconciler) SetupWithManager(mgr ctrl.Manager) error {
	ac := &unstructured.Unstructured{}
	ac.SetGroupVersionKind(AdmissionCheckGVK)
	return ctrl.NewControllerManagedBy(mgr).For(ac).Complete(r)
}

func (r *ActivateReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	ac := &unstructured.Unstructured{}
	ac.SetGroupVersionKind(AdmissionCheckGVK)
	if err := r.Client.Get(ctx, req.NamespacedName, ac); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if cn, _, _ := unstructured.NestedString(ac.Object, "spec", "controllerName"); cn != r.ControllerName {
		return ctrl.Result{}, nil // 非本控制器认领
	}

	conds, _, _ := unstructured.NestedSlice(ac.Object, "status", "conditions")
	for _, c := range conds {
		if m, ok := c.(map[string]interface{}); ok && m["type"] == "Active" && m["status"] == "True" {
			return ctrl.Result{}, nil // 已 Active，幂等
		}
	}

	before := ac.DeepCopy()
	active := map[string]interface{}{
		"type":               "Active",
		"status":             "True",
		"reason":             "Active",
		"message":            "kore admission check controller is ready",
		"lastTransitionTime": metav1.Now().UTC().Format(time.RFC3339),
		"observedGeneration": ac.GetGeneration(),
	}
	next := make([]interface{}, 0, len(conds)+1)
	replaced := false
	for _, c := range conds {
		if m, ok := c.(map[string]interface{}); ok && m["type"] == "Active" {
			next = append(next, active)
			replaced = true
		} else {
			next = append(next, c)
		}
	}
	if !replaced {
		next = append(next, active)
	}
	if err := unstructured.SetNestedSlice(ac.Object, next, "status", "conditions"); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.Client.Status().Patch(ctx, ac, client.MergeFrom(before))
}
