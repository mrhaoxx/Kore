package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
)

func TestUpdateFromStatus(t *testing.T) {
	UpdateFromStatus(v1alpha1.KoreNodeTopologyStatus{
		ReservedSystemCpus: "0-1",
		Zones: []v1alpha1.Zone{
			{ID: 0, Cpus: "0-7", FreeCpus: "2-3"},
			{ID: 1, Cpus: "8-15", FreeCpus: "8-15"},
		},
		Allocations: []v1alpha1.Allocation{{PodUID: "u1", Cpuset: "4-7"}},
		Pools:       []v1alpha1.Pool{{Name: "demo", Cpuset: "8-11", Members: []string{"a", "b"}}},
	})
	if v := testutil.ToFloat64(cpusExclusive); v != 4 {
		t.Fatalf("exclusive = %v", v)
	}
	if v := testutil.ToFloat64(cpusPooled); v != 4 {
		t.Fatalf("pooled = %v", v)
	}
	if v := testutil.ToFloat64(cpusShared); v != 6 { // 16-2-4-4
		t.Fatalf("shared = %v", v)
	}
	if v := testutil.ToFloat64(poolMembers.WithLabelValues("demo")); v != 2 {
		t.Fatalf("members = %v", v)
	}
	// 池消失后 label 清理
	UpdateFromStatus(v1alpha1.KoreNodeTopologyStatus{Zones: []v1alpha1.Zone{{ID: 0, Cpus: "0-7"}}})
	if n := testutil.CollectAndCount(poolSize); n != 0 {
		t.Fatalf("stale pool labels: %d", n)
	}
}
