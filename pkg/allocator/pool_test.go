package allocator

import (
	"errors"
	"testing"
	"time"

	"k8s.io/utils/cpuset"

	"github.com/zjusct/kore/pkg/request"
)

func joinPool(t *testing.T, s *State, name string, size int, uid string, mut func(*PoolRequest)) PoolInfo {
	t.Helper()
	r := PoolRequest{Name: name, Size: size, PodUID: uid, NUMAPolicy: request.NUMASingle, Placement: request.PlacementPack}
	if mut != nil {
		mut(&r)
	}
	p, err := s.JoinPool(r)
	if err != nil {
		t.Fatalf("join %s/%s: %v", name, uid, err)
	}
	return p
}

func TestPoolCreateAndJoin(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(), 0)
	p1 := joinPool(t, s, "demo", 4, "u1", nil)
	if p1.CPUs.String() != "0-3" || len(p1.NUMA) != 1 || p1.NUMA[0] != 0 {
		t.Fatalf("pool: %+v", p1)
	}
	if s.SharedPool().Contains(0) {
		t.Fatal("pool cpus must leave the shared pool")
	}
	// 次成员拿到相同核心，不新增占用
	p2 := joinPool(t, s, "demo", 4, "u2", nil)
	if p2.CPUs.String() != "0-3" {
		t.Fatalf("second member: %+v", p2)
	}
	if s.Used().Size() != 4 {
		t.Fatalf("used = %v", s.Used())
	}
}

func TestPoolSizeConflict(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(), 0)
	joinPool(t, s, "demo", 4, "u1", nil)
	_, err := s.JoinPool(PoolRequest{Name: "demo", Size: 8, PodUID: "u2", NUMAPolicy: request.NUMASingle})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v", err)
	}
}

func TestPoolReleaseByRefcount(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(), 0)
	joinPool(t, s, "demo", 4, "u1", nil)
	joinPool(t, s, "demo", 4, "u2", nil)
	s.Release("u1")
	if s.Used().Size() != 4 {
		t.Fatal("pool must survive while members remain")
	}
	s.Release("u2")
	if s.Used().Size() != 0 || s.SharedPool().Size() != 16 {
		t.Fatalf("pool must free after last member: used=%v shared=%v", s.Used(), s.SharedPool())
	}
}

func TestPoolExclusiveMutualExclusion(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(), 0)
	joinPool(t, s, "demo", 4, "u1", nil) // 占满 zone0
	a := alloc(t, s, "x", 4, nil)        // 独占分配必须避开池
	if !a.CPUs.Intersection(cpuset.New(0, 1, 2, 3)).IsEmpty() {
		t.Fatalf("exclusive overlaps pool: %v", a.CPUs)
	}
	// 反向：独占后建池不重叠
	p := joinPool(t, s, "demo2", 4, "u3", nil)
	if !p.CPUs.Intersection(a.CPUs).IsEmpty() {
		t.Fatalf("pool overlaps exclusive: %v vs %v", p.CPUs, a.CPUs)
	}
}

func TestPoolReservedNUMA(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(), 0)
	n := 2
	p := joinPool(t, s, "demo", 4, "u1", func(r *PoolRequest) { r.ReservedNUMA = &n })
	if p.NUMA[0] != 2 || p.CPUs.String() != "8-11" {
		t.Fatalf("%+v", p)
	}
}

func TestRestorePoolMember(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(), 0)
	if err := s.RestorePoolMember("demo", cpuset.New(4, 5), "u1"); err != nil {
		t.Fatal(err)
	}
	if s.SharedPool().Contains(4) {
		t.Fatal("restored pool must be excluded from shared pool")
	}
	// 一致成员加入
	if err := s.RestorePoolMember("demo", cpuset.New(4, 5), "u2"); err != nil {
		t.Fatal(err)
	}
	// cpus 不一致 → 冲突
	if err := s.RestorePoolMember("demo", cpuset.New(6, 7), "u3"); !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v", err)
	}
	// 与已占用重叠的新池 → 冲突
	if err := s.RestorePoolMember("other", cpuset.New(5, 6), "u4"); !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v", err)
	}
}

func TestSharedPoolMin(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(), 10) // 16 核，保底 10
	// 独占 6 核 OK（剩 10）
	alloc(t, s, "a", 4, func(r *Request) { r.NUMAPolicy = request.NUMAPreferred })
	alloc(t, s, "b", 2, nil)
	// 再要 1 核 → 触底拒绝
	_, err := s.Allocate(Request{PodUID: "u9", Pod: "d/p9", Container: "app", CPUs: 1, NUMAPolicy: request.NUMASingle})
	if !errors.Is(err, ErrInsufficient) {
		t.Fatalf("err = %v", err)
	}
	// 建池同样受限
	_, err = s.JoinPool(PoolRequest{Name: "demo", Size: 1, PodUID: "u10", NUMAPolicy: request.NUMASingle})
	if !errors.Is(err, ErrInsufficient) {
		t.Fatalf("pool err = %v", err)
	}
	// 但加入既有池不受影响（不新增占用）——先松限建池再验证
	s2 := NewState(armTopo(), cpuset.New(), 12)
	joinPool(t, s2, "demo", 4, "u1", nil)
	joinPool(t, s2, "demo", 4, "u2", nil) // 触底状态下加入仍成功
}

func TestPoolResize(t *testing.T) {
	base := time.Date(2026, 7, 9, 10, 0, 0, 0, time.UTC)
	s := NewState(armTopo(), cpuset.New(), 0)
	joinPool(t, s, "demo", 4, "u1", func(r *PoolRequest) { r.PodCreated = base })
	// 晚于池创建的成员带更大 size → 扩容（zone0 满 → 溢出邻近 zone）
	p := joinPool(t, s, "demo", 6, "u2", func(r *PoolRequest) { r.PodCreated = base.Add(time.Minute) })
	if p.CPUs.Size() != 6 || !p.CPUs.Contains(0) || !p.CPUs.Contains(4) {
		t.Fatalf("grow: %v", p.CPUs)
	}
	if len(p.NUMA) != 2 {
		t.Fatalf("numa must update: %v", p.NUMA)
	}
	// 晚创建成员带小 size → 缩容（高位收回）
	p = joinPool(t, s, "demo", 3, "u3", func(r *PoolRequest) { r.PodCreated = base.Add(2 * time.Minute) })
	if p.CPUs.String() != "0-2" {
		t.Fatalf("shrink: %v", p.CPUs)
	}
	if s.SharedPool().Size() != 13 {
		t.Fatalf("shrunk cores must return to shared: %v", s.SharedPool())
	}
	// 早于池创建的 joiner 带异 size → 拒绝（陈旧注解防回灌）
	_, err := s.JoinPool(PoolRequest{Name: "demo", Size: 8, PodUID: "u4",
		NUMAPolicy: request.NUMASingle, PodCreated: base.Add(-time.Hour)})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("stale joiner: %v", err)
	}
}

func TestPoolResizeSharedPoolMin(t *testing.T) {
	base := time.Date(2026, 7, 9, 10, 0, 0, 0, time.UTC)
	s := NewState(armTopo(), cpuset.New(), 10) // 16 核保底 10
	joinPool(t, s, "demo", 4, "u1", func(r *PoolRequest) { r.PodCreated = base })
	_, err := s.JoinPool(PoolRequest{Name: "demo", Size: 8, PodUID: "u2",
		NUMAPolicy: request.NUMASingle, PodCreated: base.Add(time.Minute)})
	if !errors.Is(err, ErrInsufficient) {
		t.Fatalf("grow past sharedPoolMin: %v", err)
	}
	if p, _ := s.JoinPool(PoolRequest{Name: "demo", Size: 4, PodUID: "u3",
		NUMAPolicy: request.NUMASingle, PodCreated: base.Add(time.Minute)}); p.CPUs.Size() != 4 {
		t.Fatalf("pool must be unchanged after failed grow: %v", p.CPUs)
	}
}

func TestBuildStatusPools(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(), 0)
	joinPool(t, s, "demo", 4, "u2", nil)
	joinPool(t, s, "demo", 4, "u1", nil)
	st := BuildStatus(s)
	if len(st.Pools) != 1 || st.Pools[0].Name != "demo" || st.Pools[0].Cpuset != "0-3" {
		t.Fatalf("pools: %+v", st.Pools)
	}
	if len(st.Pools[0].Members) != 2 || st.Pools[0].Members[0] != "u1" {
		t.Fatalf("members must be sorted: %+v", st.Pools[0].Members)
	}
	if st.Zones[0].FreeCpus != "" { // zone0 被池占满
		t.Fatalf("zone0 free = %q", st.Zones[0].FreeCpus)
	}
}
