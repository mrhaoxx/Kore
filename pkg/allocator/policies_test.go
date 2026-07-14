package allocator

import (
	"testing"

	"k8s.io/utils/cpuset"

	"github.com/zjusct/kore/pkg/request"
	"github.com/zjusct/kore/pkg/topology"
)

// x86Topo: 2 zone、2-way SMT。zone0={0-3,8-11}（sibling (i,i+8)），zone1={4-7,12-15}。
func x86Topo() *topology.Topology {
	zones := []topology.Zone{
		{ID: 0, CPUs: cpuset.New(0, 1, 2, 3, 8, 9, 10, 11), MemoryTotalBytes: 1 << 34, Distances: []int{10, 21}},
		{ID: 1, CPUs: cpuset.New(4, 5, 6, 7, 12, 13, 14, 15), MemoryTotalBytes: 1 << 34, Distances: []int{21, 10}},
	}
	sib := map[int]cpuset.CPUSet{}
	for i := 0; i < 8; i++ {
		g := cpuset.New(i, i+8)
		sib[i], sib[i+8] = g, g
	}
	return &topology.Topology{Zones: zones, Siblings: sib, ThreadsPerCore: 2}
}

func TestPreferredSpillsByDistance(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(), 0)
	n := 0
	a := alloc(t, s, "a", 6, func(r *Request) {
		r.NUMAPolicy = request.NUMAPreferred
		r.ReservedNUMA = &n
	})
	// zone0 全部 4 个 + 距离最近的 zone1 里 2 个
	if a.CPUs.String() != "0-5" {
		t.Fatalf("cpus = %s, want 0-5", a.CPUs)
	}
	if len(a.NUMA) != 2 || a.NUMA[0] != 0 || a.NUMA[1] != 1 {
		t.Fatalf("numa = %v, want [0 1]", a.NUMA)
	}
}

func TestPreferredStaysSingleWhenFits(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(), 0)
	a := alloc(t, s, "a", 3, func(r *Request) { r.NUMAPolicy = request.NUMAPreferred })
	if len(a.NUMA) != 1 {
		t.Fatalf("numa = %v, want single zone", a.NUMA)
	}
}

func TestSpreadEven(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(), 0)
	a := alloc(t, s, "a", 8, func(r *Request) { r.NUMAPolicy = request.NUMASpread })
	if len(a.NUMA) != 4 {
		t.Fatalf("numa = %v, want 4 zones", a.NUMA)
	}
	for _, z := range a.NUMA {
		perZone := a.CPUs.Intersection(zoneCPUs(t, s, z)).Size()
		if perZone != 2 {
			t.Fatalf("zone %d got %d cpus, want 2", z, perZone)
		}
	}
}

func TestSpreadUnevenRemainder(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(), 0)
	a := alloc(t, s, "a", 6, func(r *Request) { r.NUMAPolicy = request.NUMASpread })
	sizes := map[int]int{}
	for _, z := range a.NUMA {
		sizes[z] = a.CPUs.Intersection(zoneCPUs(t, s, z)).Size()
	}
	two, one := 0, 0
	for _, n := range sizes {
		switch n {
		case 2:
			two++
		case 1:
			one++
		default:
			t.Fatalf("unexpected per-zone size %d", n)
		}
	}
	if two != 2 || one != 2 {
		t.Fatalf("sizes = %v, want two zones with 2 and two with 1", sizes)
	}
}

func TestScatterWithinZone(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(), 0)
	a := alloc(t, s, "a", 2, func(r *Request) { r.Placement = request.PlacementScatter })
	if a.CPUs.String() != "0,2" {
		t.Fatalf("cpus = %s, want 0,2", a.CPUs)
	}
}

func TestSMTFullCore(t *testing.T) {
	s := NewState(x86Topo(), cpuset.New(), 0)
	a := alloc(t, s, "a", 4, nil)     // full-core 默认
	if a.CPUs.String() != "0-1,8-9" { // 两个完整 sibling 组
		t.Fatalf("cpus = %s, want 0-1,8-9", a.CPUs)
	}
}

func TestSMTOddRoundsUpToWholeCore(t *testing.T) {
	s := NewState(x86Topo(), cpuset.New(), 0)
	a := alloc(t, s, "a", 3, nil) // full-core cpu=3 → 向上取整到 2 整核=4 逻辑核
	if a.CPUs.String() != "0-1,8-9" {
		t.Fatalf("cpus = %s, want 0-1,8-9 (2 整核)", a.CPUs)
	}
}

func TestSMTOneCoreRoundsUp(t *testing.T) {
	s := NewState(x86Topo(), cpuset.New(), 0)
	a := alloc(t, s, "a", 1, nil) // full-core cpu=1 → 1 整核 = {0,8}（学生小作业拿到独占整核）
	if a.CPUs.String() != "0,8" {
		t.Fatalf("cpus = %s, want 0,8 (1 整核)", a.CPUs)
	}
}

func TestSMTLogicalAllowsOdd(t *testing.T) {
	s := NewState(x86Topo(), cpuset.New(), 0)
	a := alloc(t, s, "a", 3, func(r *Request) { r.SMTPolicy = request.SMTLogical })
	if a.CPUs.String() != "0-2" {
		t.Fatalf("cpus = %s, want 0-2", a.CPUs)
	}
}

func TestSMTPartialCoreExcludedFromFullCore(t *testing.T) {
	s := NewState(x86Topo(), cpuset.New(), 0)
	alloc(t, s, "a", 1, func(r *Request) { r.SMTPolicy = request.SMTLogical }) // 占 {0}，物理核 (0,8) 残缺
	b := alloc(t, s, "b", 2, nil)                                              // full-core 必须避开残缺核
	if b.CPUs.Contains(8) {
		t.Fatalf("full-core alloc %s must not use partially-used core", b.CPUs)
	}
}

func zoneCPUs(t *testing.T, s *State, zone int) cpuset.CPUSet {
	t.Helper()
	for _, z := range s.topo.Zones {
		if z.ID == zone {
			return z.CPUs
		}
	}
	t.Fatalf("zone %d not found", zone)
	return cpuset.New()
}
