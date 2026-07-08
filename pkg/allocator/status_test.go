package allocator

import (
	"testing"

	"k8s.io/utils/cpuset"
)

func TestBuildStatus(t *testing.T) {
	s := NewState(x86Topo(), cpuset.New(0, 8)) // 预留物理核 (0,8)
	n := 0
	alloc(t, s, "a", 2, func(r *Request) { r.ReservedNUMA = &n }) // {1,9}
	st := BuildStatus(s)

	if st.ReservedSystemCpus != "0,8" {
		t.Fatalf("reserved = %s", st.ReservedSystemCpus)
	}
	if len(st.Zones) != 2 {
		t.Fatalf("zones = %d", len(st.Zones))
	}
	z0 := st.Zones[0]
	if z0.ID != 0 || z0.Cpus != "0-3,8-11" || z0.Allocatable != 6 {
		t.Fatalf("zone0 = %+v", z0)
	}
	if z0.FreeCpus != "2-3,10-11" {
		t.Fatalf("zone0 free = %s", z0.FreeCpus)
	}
	if len(z0.SMTSiblings) != 4 || z0.SMTSiblings[0][0] != 0 || z0.SMTSiblings[0][1] != 8 {
		t.Fatalf("siblings = %v", z0.SMTSiblings)
	}
	if z0.Devices == nil {
		t.Fatal("devices must be non-nil empty slice (v2 预留字段)")
	}
	if z0.MemoryTotal.Value() != 1<<34 {
		t.Fatalf("mem = %d", z0.MemoryTotal.Value())
	}
	if len(st.Allocations) != 1 || st.Allocations[0].Cpuset != "1,9" || st.Allocations[0].NUMA[0] != 0 {
		t.Fatalf("allocations = %+v", st.Allocations)
	}
	if st.Allocations[0].Pod != "default/a" || st.Allocations[0].Container != "app" {
		t.Fatalf("allocations = %+v", st.Allocations)
	}
}

func TestBuildStatusNoSMT(t *testing.T) {
	s := NewState(armTopo(), cpuset.New())
	st := BuildStatus(s)
	if len(st.Zones[0].SMTSiblings) != 0 {
		t.Fatalf("siblings = %v, want empty on non-SMT", st.Zones[0].SMTSiblings)
	}
}
