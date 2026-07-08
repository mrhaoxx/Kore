package reporter

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
)

func TestReportCreatesThenUpdates(t *testing.T) {
	sch := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(sch).
		WithStatusSubresource(&v1alpha1.KoreNodeTopology{}).Build()
	r := New(c, "m602")

	st := v1alpha1.KoreNodeTopologyStatus{ReservedSystemCpus: "0-1"}
	if err := r.Report(context.Background(), st); err != nil {
		t.Fatal(err)
	}
	var got v1alpha1.KoreNodeTopology
	if err := c.Get(context.Background(), types.NamespacedName{Name: "m602"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.ReservedSystemCpus != "0-1" {
		t.Fatalf("status = %+v", got.Status)
	}

	st.ReservedSystemCpus = "0-3"
	if err := r.Report(context.Background(), st); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "m602"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.ReservedSystemCpus != "0-3" {
		t.Fatalf("update lost: %+v", got.Status)
	}
}
