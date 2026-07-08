package operator

import (
	"context"
	"testing"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func lease(node string, renew time.Time, durSec int32) *coordv1.Lease {
	return &coordv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "kore-agent-" + node, Namespace: "kore-system"},
		Spec: coordv1.LeaseSpec{
			HolderIdentity:       &node,
			LeaseDurationSeconds: &durSec,
			RenewTime:            &metav1.MicroTime{Time: renew},
		},
	}
}

func TestLeaseExpired(t *testing.T) {
	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	if LeaseExpired(lease("n1", now.Add(-10*time.Second), 15), now) {
		t.Fatal("10s ago with 15s duration is fresh")
	}
	if !LeaseExpired(lease("n1", now.Add(-20*time.Second), 15), now) {
		t.Fatal("20s ago with 15s duration is expired")
	}
	if !LeaseExpired(&coordv1.Lease{}, now) {
		t.Fatal("empty lease must count as expired")
	}
}

func hasTaint(t *testing.T, n *corev1.Node) bool {
	t.Helper()
	for _, tt := range n.Spec.Taints {
		if tt.Key == TaintKey {
			return true
		}
	}
	return false
}

func TestTaintReconcile(t *testing.T) {
	sch := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}}
	l := lease("n1", now.Add(-time.Minute), 15) // 过期
	c := fake.NewClientBuilder().WithScheme(sch).WithObjects(node, l).Build()
	r := &TaintReconciler{Client: c, Now: func() time.Time { return now }}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "kore-system", Name: "kore-agent-n1"}}

	// 过期 → 打污点
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	var got corev1.Node
	if err := c.Get(context.Background(), types.NamespacedName{Name: "n1"}, &got); err != nil {
		t.Fatal(err)
	}
	if !hasTaint(t, &got) {
		t.Fatalf("taint missing: %+v", got.Spec.Taints)
	}
	// 幂等
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	c.Get(context.Background(), types.NamespacedName{Name: "n1"}, &got)
	count := 0
	for _, tt := range got.Spec.Taints {
		if tt.Key == TaintKey {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("taint duplicated: %d", count)
	}

	// 续约 → 摘污点
	fresh := lease("n1", now, 15)
	fresh.ResourceVersion = ""
	var cur coordv1.Lease
	c.Get(context.Background(), types.NamespacedName{Namespace: "kore-system", Name: "kore-agent-n1"}, &cur)
	cur.Spec.RenewTime = &metav1.MicroTime{Time: now}
	if err := c.Update(context.Background(), &cur); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	c.Get(context.Background(), types.NamespacedName{Name: "n1"}, &got)
	if hasTaint(t, &got) {
		t.Fatalf("taint not removed: %+v", got.Spec.Taints)
	}
}

func TestTaintReconcileNoNode(t *testing.T) {
	sch := runtime.NewScheme()
	clientgoscheme.AddToScheme(sch)
	now := time.Now()
	c := fake.NewClientBuilder().WithScheme(sch).WithObjects(lease("ghost", now.Add(-time.Hour), 15)).Build()
	r := &TaintReconciler{Client: c, Now: func() time.Time { return now }}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "kore-system", Name: "kore-agent-ghost"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("missing node must not error: %v", err)
	}
}
