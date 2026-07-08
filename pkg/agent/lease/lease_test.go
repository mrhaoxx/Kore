package lease

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestRenewOnceCreatesThenRenews(t *testing.T) {
	cs := fake.NewClientset()
	r := NewRenewer(cs, "m602", "kore-system", 15)
	t0 := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return t0 }

	if err := r.RenewOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	l, err := cs.CoordinationV1().Leases("kore-system").Get(context.Background(), "kore-agent-m602", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if *l.Spec.HolderIdentity != "m602" || *l.Spec.LeaseDurationSeconds != 15 {
		t.Fatalf("%+v", l.Spec)
	}
	if !l.Spec.RenewTime.Time.Equal(t0) {
		t.Fatalf("renewTime = %v", l.Spec.RenewTime)
	}

	r.now = func() time.Time { return t0.Add(5 * time.Second) }
	if err := r.RenewOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	l, _ = cs.CoordinationV1().Leases("kore-system").Get(context.Background(), "kore-agent-m602", metav1.GetOptions{})
	if !l.Spec.RenewTime.Time.Equal(t0.Add(5 * time.Second)) {
		t.Fatalf("renew not applied: %v", l.Spec.RenewTime)
	}
}
