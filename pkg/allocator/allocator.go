// Package allocator 维护单节点的独占 CPU 分配状态并执行选核。
package allocator

import (
	"errors"
	"fmt"
	"sort"

	"k8s.io/utils/cpuset"

	"github.com/zjusct/kore/pkg/request"
	"github.com/zjusct/kore/pkg/topology"
)

var (
	ErrInsufficient = errors.New("insufficient free cpus")
	ErrConflict     = errors.New("cpuset conflict")
	ErrSMTAlignment = errors.New("cpu count not aligned to full cores")
)

type Allocation struct {
	PodUID    string
	Pod       string // namespace/name
	Container string
	CPUs      cpuset.CPUSet
	NUMA      []int
}

type Request struct {
	PodUID    string
	Pod       string
	Container string
	CPUs      int

	NUMAPolicy   request.NUMAPolicy
	Placement    request.Placement
	SMTPolicy    request.SMTPolicy
	ReservedNUMA *int
	Explicit     *cpuset.CPUSet
}

type State struct {
	topo     *topology.Topology
	reserved cpuset.CPUSet
	allocs   map[string]Allocation
}

func NewState(topo *topology.Topology, reserved cpuset.CPUSet) *State {
	return &State{topo: topo, reserved: reserved, allocs: map[string]Allocation{}}
}

func key(podUID, container string) string { return podUID + "/" + container }

func (s *State) Used() cpuset.CPUSet {
	u := cpuset.New()
	for _, a := range s.allocs {
		u = u.Union(a.CPUs)
	}
	return u
}

func (s *State) FreeInZone(zone int) cpuset.CPUSet {
	for _, z := range s.topo.Zones {
		if z.ID == zone {
			return z.CPUs.Difference(s.reserved).Difference(s.Used())
		}
	}
	return cpuset.New()
}

// SharedPool = 全部核 − 系统预留 − 已独占核（spec §3）。
func (s *State) SharedPool() cpuset.CPUSet {
	return s.topo.AllCPUs().Difference(s.reserved).Difference(s.Used())
}

func (s *State) Allocations() []Allocation {
	out := make([]Allocation, 0, len(s.allocs))
	for _, a := range s.allocs {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].PodUID != out[j].PodUID {
			return out[i].PodUID < out[j].PodUID
		}
		return out[i].Container < out[j].Container
	})
	return out
}

// Restore 用于 agent 重启后从容器注解重建状态（NRI Synchronize）。
func (s *State) Restore(a Allocation) error {
	if !a.CPUs.Intersection(s.Used()).IsEmpty() {
		return fmt.Errorf("%w: restore %s/%s overlaps existing allocation", ErrConflict, a.Pod, a.Container)
	}
	s.allocs[key(a.PodUID, a.Container)] = a
	return nil
}

func (s *State) Release(podUID string) {
	for k, a := range s.allocs {
		if a.PodUID == podUID {
			delete(s.allocs, k)
		}
	}
}

func (s *State) Allocate(req Request) (Allocation, error) {
	if _, ok := s.allocs[key(req.PodUID, req.Container)]; ok {
		return Allocation{}, fmt.Errorf("%w: %s/%s already allocated", ErrConflict, req.PodUID, req.Container)
	}
	if req.Explicit != nil {
		return s.allocateExplicit(req)
	}

	unit := 1
	if req.SMTPolicy != request.SMTLogical && s.topo.SMTEnabled() {
		unit = s.topo.ThreadsPerCore
		if req.CPUs%unit != 0 {
			return Allocation{}, fmt.Errorf("%w: %d cpus on SMT node (threads-per-core=%d); use smt-policy=logical or align the count",
				ErrSMTAlignment, req.CPUs, unit)
		}
	}
	strat := StrategyFor(req.Placement)

	var picked cpuset.CPUSet
	var numa []int
	var err error
	switch req.NUMAPolicy {
	case request.NUMASpread:
		picked, numa, err = s.pickSpread(req.CPUs, unit, strat)
	case request.NUMAPreferred:
		picked, numa, err = s.pickPreferred(req.CPUs, unit, strat, req.ReservedNUMA)
	default:
		picked, numa, err = s.pickSingle(req.CPUs, unit, strat, req.ReservedNUMA)
	}
	if err != nil {
		return Allocation{}, err
	}
	a := Allocation{PodUID: req.PodUID, Pod: req.Pod, Container: req.Container, CPUs: picked, NUMA: numa}
	s.allocs[key(req.PodUID, req.Container)] = a
	return a, nil
}

func (s *State) allocateExplicit(req Request) (Allocation, error) {
	want := *req.Explicit
	free := s.topo.AllCPUs().Difference(s.reserved).Difference(s.Used())
	if !want.Difference(free).IsEmpty() {
		return Allocation{}, fmt.Errorf("%w: explicit cpuset %s not fully free (free: %s)", ErrConflict, want, free)
	}
	zones := map[int]bool{}
	for _, c := range want.List() {
		zones[s.topo.ZoneOf(c)] = true
	}
	numa := make([]int, 0, len(zones))
	for z := range zones {
		numa = append(numa, z)
	}
	sort.Ints(numa)
	a := Allocation{PodUID: req.PodUID, Pod: req.Pod, Container: req.Container, CPUs: want, NUMA: numa}
	s.allocs[key(req.PodUID, req.Container)] = a
	return a, nil
}

// freeUnits 返回 zone 内可用于分配的空闲 cpu 集合；full-core 模式（unit>1）
// 只保留 sibling 组完全空闲的核。
func (s *State) freeUnits(zone, unit int) cpuset.CPUSet {
	free := s.FreeInZone(zone)
	if unit == 1 {
		return free
	}
	keep := cpuset.New()
	for _, c := range free.List() {
		if s.topo.Siblings[c].Difference(free).IsEmpty() {
			keep = keep.Union(s.topo.Siblings[c])
		}
	}
	return keep
}

// unitsOf 把空闲集合切成升序 Unit 列表。
func (s *State) unitsOf(free cpuset.CPUSet, unit int) []Unit {
	if unit == 1 {
		out := make([]Unit, 0, free.Size())
		for _, c := range free.List() {
			out = append(out, Unit{Min: c, CPUs: cpuset.New(c)})
		}
		return out
	}
	seen := map[int]bool{}
	var out []Unit
	for _, c := range free.List() {
		g := s.topo.Siblings[c]
		m := g.List()[0]
		if seen[m] {
			continue
		}
		seen[m] = true
		out = append(out, Unit{Min: m, CPUs: g})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Min < out[j].Min })
	return out
}

// pickInZone 在单个 zone 内选 n 个 cpu（n 必须是 unit 的整数倍）。
func (s *State) pickInZone(zone, n, unit int, strat Strategy) (cpuset.CPUSet, bool) {
	units := s.unitsOf(s.freeUnits(zone, unit), unit)
	got, ok := strat.Pick(units, n/unit)
	if !ok {
		return cpuset.New(), false
	}
	return unitsUnion(got), true
}

// zonesByFreeAsc：按空闲 unit 数升序（binpack），并列取 ID 小者。
func (s *State) zonesByFreeAsc(unit int) []int {
	ids := make([]int, 0, len(s.topo.Zones))
	freeCount := map[int]int{}
	for _, z := range s.topo.Zones {
		ids = append(ids, z.ID)
		freeCount[z.ID] = len(s.unitsOf(s.freeUnits(z.ID, unit), unit))
	}
	sort.Slice(ids, func(i, j int) bool {
		if freeCount[ids[i]] != freeCount[ids[j]] {
			return freeCount[ids[i]] < freeCount[ids[j]]
		}
		return ids[i] < ids[j]
	})
	return ids
}

func (s *State) pickSingle(n, unit int, strat Strategy, reservedNUMA *int) (cpuset.CPUSet, []int, error) {
	var candidates []int
	if reservedNUMA != nil {
		candidates = []int{*reservedNUMA} // 调度器指定后绝不 fallback（spec §6）
	} else {
		candidates = s.zonesByFreeAsc(unit)
	}
	for _, z := range candidates {
		if got, ok := s.pickInZone(z, n, unit, strat); ok {
			return got, []int{z}, nil
		}
	}
	return cpuset.New(), nil, fmt.Errorf("%w: no single NUMA zone with %d free cpus", ErrInsufficient, n)
}

func (s *State) pickPreferred(n, unit int, strat Strategy, reservedNUMA *int) (cpuset.CPUSet, []int, error) {
	return cpuset.New(), nil, fmt.Errorf("%w: preferred not implemented yet", ErrInsufficient)
}

func (s *State) pickSpread(n, unit int, strat Strategy) (cpuset.CPUSet, []int, error) {
	return cpuset.New(), nil, fmt.Errorf("%w: spread not implemented yet", ErrInsufficient)
}
