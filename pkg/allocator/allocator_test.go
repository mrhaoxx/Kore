package allocator

import (
	"errors"
	"testing"

	"k8s.io/utils/cpuset"

	"github.com/zjusct/kore/pkg/request"
	"github.com/zjusct/kore/pkg/topology"
)

// armTopo: 4 zone × 4 cpu，无 SMT。zone0 距离行 [10 12 20 22]（近邻顺序 1,2,3）。
func armTopo() *topology.Topology {
	var zones []topology.Zone
	dist := [][]int{{10, 12, 20, 22}, {12, 10, 22, 20}, {20, 22, 10, 12}, {22, 20, 12, 10}}
	sib := map[int]cpuset.CPUSet{}
	for z := 0; z < 4; z++ {
		cpus := cpuset.New(z*4, z*4+1, z*4+2, z*4+3)
		zones = append(zones, topology.Zone{ID: z, CPUs: cpus, MemoryTotalBytes: 1 << 34, Distances: dist[z]})
		for _, c := range cpus.List() {
			sib[c] = cpuset.New(c)
		}
	}
	return &topology.Topology{Zones: zones, Siblings: sib, ThreadsPerCore: 1}
}

func alloc(t *testing.T, s *State, name string, cpus int, mut func(*Request)) Allocation {
	t.Helper()
	r := Request{PodUID: "uid-" + name, Pod: "default/" + name, Container: "app",
		CPUs: cpus, NUMAPolicy: request.NUMASingle, Placement: request.PlacementPack, SMTPolicy: request.SMTFullCore}
	if mut != nil {
		mut(&r)
	}
	a, err := s.Allocate(r)
	if err != nil {
		t.Fatalf("alloc %s: %v", name, err)
	}
	return a
}

func TestPackSingleOnEmpty(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(), 0)
	a := alloc(t, s, "a", 2, nil)
	if a.CPUs.String() != "0-1" || len(a.NUMA) != 1 || a.NUMA[0] != 0 {
		t.Fatalf("got %s numa %v", a.CPUs, a.NUMA)
	}
}

func TestBinpackPrefersTightestZone(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(), 0)
	alloc(t, s, "a", 2, nil) // zone0 剩 2
	b := alloc(t, s, "b", 2, nil)
	if b.NUMA[0] != 0 { // 恰好填满 zone0
		t.Fatalf("b on numa %v, want 0", b.NUMA)
	}
	c := alloc(t, s, "c", 3, nil)
	if c.NUMA[0] != 1 || c.CPUs.String() != "4-6" {
		t.Fatalf("c: %s numa %v", c.CPUs, c.NUMA)
	}
}

func TestPackBestFitRun(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(), 0)
	alloc(t, s, "a", 1, nil) // {0}
	alloc(t, s, "b", 1, nil) // {1}
	s.Release("uid-a")       // zone0 free {0,2,3}
	c := alloc(t, s, "c", 2, func(r *Request) { n := 0; r.ReservedNUMA = &n })
	if c.CPUs.String() != "2-3" { // best-fit 连续段 {2,3}，不是 {0,2}
		t.Fatalf("c = %s, want 2-3", c.CPUs)
	}
}

func TestReservedNUMARespectedAndStrict(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(), 0)
	n := 2
	a := alloc(t, s, "a", 2, func(r *Request) { r.ReservedNUMA = &n })
	if a.NUMA[0] != 2 || a.CPUs.String() != "8-9" {
		t.Fatalf("%s %v", a.CPUs, a.NUMA)
	}
	// reservedNUMA 指定后不 fallback：zone2 只剩 2，要 3 必须失败
	_, err := s.Allocate(Request{PodUID: "u2", Pod: "d/p2", Container: "app", CPUs: 3,
		NUMAPolicy: request.NUMASingle, ReservedNUMA: &n})
	if !errors.Is(err, ErrInsufficient) {
		t.Fatalf("err = %v, want ErrInsufficient", err)
	}
}

func TestSingleInsufficient(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(), 0)
	_, err := s.Allocate(Request{PodUID: "u", Pod: "d/p", Container: "app", CPUs: 5, NUMAPolicy: request.NUMASingle})
	if !errors.Is(err, ErrInsufficient) {
		t.Fatalf("err = %v", err)
	}
}

func TestExplicit(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(), 0)
	cs := cpuset.New(4, 5)
	a := alloc(t, s, "a", 2, func(r *Request) { r.Explicit = &cs })
	if a.CPUs.String() != "4-5" || a.NUMA[0] != 1 {
		t.Fatalf("%s %v", a.CPUs, a.NUMA)
	}
	cs2 := cpuset.New(5, 6)
	_, err := s.Allocate(Request{PodUID: "u2", Pod: "d/p2", Container: "app", CPUs: 2, Explicit: &cs2})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
}

func TestReservedSystemCpusExcluded(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(0), 0)
	a := alloc(t, s, "a", 4, nil) // zone0 只剩 3 → 落 zone1
	if a.NUMA[0] != 1 {
		t.Fatalf("numa %v, want 1", a.NUMA)
	}
	if s.SharedPool().Contains(0) {
		t.Fatal("shared pool must exclude reserved system cpus")
	}
}

func TestSharedPool(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(0), 0)
	alloc(t, s, "a", 2, func(r *Request) { n := 1; r.ReservedNUMA = &n }) // {4,5}
	got := s.SharedPool().String()
	if got != "1-3,6-15" {
		t.Fatalf("shared = %s", got)
	}
}

func TestRestoreAndConflict(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(), 0)
	a := Allocation{PodUID: "u1", Pod: "d/p1", Container: "app", CPUs: cpuset.New(0, 1), NUMA: []int{0}}
	if err := s.Restore(a); err != nil {
		t.Fatal(err)
	}
	b := Allocation{PodUID: "u2", Pod: "d/p2", Container: "app", CPUs: cpuset.New(1, 2), NUMA: []int{0}}
	if err := s.Restore(b); !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
}

func TestDoubleAllocateSameContainer(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(), 0)
	alloc(t, s, "a", 1, nil)
	_, err := s.Allocate(Request{PodUID: "uid-a", Pod: "default/a", Container: "app", CPUs: 1, NUMAPolicy: request.NUMASingle})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v", err)
	}
}
