package allocator

import (
	"fmt"
	"sort"
	"time"

	"k8s.io/utils/cpuset"

	"github.com/zjusct/kore/pkg/request"
)

// PoolInfo 是一个命名 CPU 池：成员 Pod 共享 CPUs，对外（独占分配、全局共享池、
// 其他池）独占。首个成员建池，末位成员离开时释放（Release）。
type PoolInfo struct {
	Name    string
	CPUs    cpuset.CPUSet
	NUMA    []int
	Members map[string]bool // podUID 集合
	// CreatedAt：池创建时刻。晚于此创建的成员携带不同 size 时触发在线扩缩容；
	// 更早的（陈旧注解回灌）拒绝。restore 重建的池为零值（任何后来者可调）。
	CreatedAt time.Time
}

type PoolRequest struct {
	Name         string
	Size         int
	PodUID       string
	NUMAPolicy   request.NUMAPolicy
	Placement    request.Placement
	ReservedNUMA *int
	// PodCreated：joiner Pod 的 creationTimestamp（扩缩容判定用）。
	PodCreated time.Time
}

// JoinPool 加入（必要时创建）池。size 与现有不符时：joiner 晚于池创建 → 在线
// 扩缩容；否则 ErrConflict。
func (s *State) JoinPool(req PoolRequest) (PoolInfo, error) {
	if p, ok := s.pools[req.Name]; ok {
		if p.CPUs.Size() != req.Size {
			if !req.PodCreated.After(p.CreatedAt) {
				return PoolInfo{}, fmt.Errorf("%w: pool %q exists with size %d, stale member requested %d",
					ErrConflict, req.Name, p.CPUs.Size(), req.Size)
			}
			if err := s.resizePool(p, req.Size); err != nil {
				return PoolInfo{}, err
			}
		}
		p.Members[req.PodUID] = true
		return *p, nil
	}
	strat := StrategyFor(req.Placement)
	var picked cpuset.CPUSet
	var numa []int
	var err error
	switch req.NUMAPolicy { // 池内共享，无 sibling 隔离需求 → 一律逻辑核（unit=1）
	case request.NUMASpread:
		picked, numa, err = s.pickSpread(req.Size, 1, strat)
	case request.NUMAPreferred:
		picked, numa, err = s.pickPreferred(req.Size, 1, strat, req.ReservedNUMA)
	default:
		picked, numa, err = s.pickSingle(req.Size, 1, strat, req.ReservedNUMA)
	}
	if err != nil {
		return PoolInfo{}, err
	}
	if err := s.checkSharedPoolMin(picked); err != nil {
		return PoolInfo{}, err
	}
	p := &PoolInfo{Name: req.Name, CPUs: picked, NUMA: numa,
		Members: map[string]bool{req.PodUID: true}, CreatedAt: req.PodCreated}
	s.pools[req.Name] = p
	return *p, nil
}

// resizePool 在线调整池大小：缩容从高位收回；扩容按池内 NUMA 顺序就近补足，
// 不足溢出到其余 zone（距离序）。扩容受 sharedPoolMin 约束。
func (s *State) resizePool(p *PoolInfo, newSize int) error {
	cur := p.CPUs.Size()
	if newSize < cur {
		p.CPUs = cpuset.New(p.CPUs.List()[:newSize]...) // 保低位，高位归还
	} else {
		need := newSize - cur
		add := cpuset.New()
		for _, z := range p.NUMA { // 先就近：池已占 zone
			if need == 0 {
				break
			}
			avail := s.FreeInZone(z)
			take := min(need, avail.Size())
			if take == 0 {
				continue
			}
			got, ok := s.pickInZone(z, take, 1, StrategyFor(request.PlacementPack))
			if !ok {
				continue
			}
			add = add.Union(got)
			need -= take
		}
		if need > 0 { // 溢出：临时把已选并入 used 视角——用 preferred 从剩余取
			primary := p.NUMA[0]
			tmp := &State{topo: s.topo, reserved: s.reserved.Union(add), sharedPoolMin: 0,
				allocs: s.allocs, pools: s.pools}
			got, _, err := tmp.pickPreferred(need, 1, StrategyFor(request.PlacementPack), &primary)
			if err != nil {
				return err
			}
			add = add.Union(got)
		}
		if err := s.checkSharedPoolMin(add); err != nil {
			return err
		}
		p.CPUs = p.CPUs.Union(add)
	}
	numa := map[int]bool{}
	for _, c := range p.CPUs.List() {
		numa[s.topo.ZoneOf(c)] = true
	}
	ids := make([]int, 0, len(numa))
	for z := range numa {
		ids = append(ids, z)
	}
	sort.Ints(ids)
	p.NUMA = ids
	return nil
}

// RestorePoolMember 用于 agent 重启后从容器注解重建池成员（NRI Synchronize）。
func (s *State) RestorePoolMember(name string, cpus cpuset.CPUSet, podUID string) error {
	if p, ok := s.pools[name]; ok {
		if !p.CPUs.Equals(cpus) {
			return fmt.Errorf("%w: pool %q restore cpus %s != existing %s", ErrConflict, name, cpus, p.CPUs)
		}
		p.Members[podUID] = true
		return nil
	}
	if !cpus.Intersection(s.Used()).IsEmpty() {
		return fmt.Errorf("%w: pool %q restore overlaps existing usage", ErrConflict, name)
	}
	numa := map[int]bool{}
	for _, c := range cpus.List() {
		numa[s.topo.ZoneOf(c)] = true
	}
	ids := make([]int, 0, len(numa))
	for z := range numa {
		ids = append(ids, z)
	}
	sort.Ints(ids)
	s.pools[name] = &PoolInfo{Name: name, CPUs: cpus, NUMA: ids, Members: map[string]bool{podUID: true}}
	return nil
}

// Pools 返回按名字排序的池快照。
func (s *State) Pools() []PoolInfo {
	out := make([]PoolInfo, 0, len(s.pools))
	for _, p := range s.pools {
		out = append(out, *p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// checkSharedPoolMin：picked 被拿走后全局共享池不得低于保底。
func (s *State) checkSharedPoolMin(picked cpuset.CPUSet) error {
	if s.sharedPoolMin <= 0 {
		return nil
	}
	if remaining := s.SharedPool().Difference(picked).Size(); remaining < s.sharedPoolMin {
		return fmt.Errorf("%w: allocation would shrink shared pool to %d, below sharedPoolMin %d",
			ErrInsufficient, remaining, s.sharedPoolMin)
	}
	return nil
}
