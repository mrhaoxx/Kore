package admissioncheck

import (
	"testing"

	"k8s.io/utils/cpuset"

	"github.com/zjusct/kore/pkg/request"
	"github.com/zjusct/kore/pkg/scheduler"
)

func mustCS(t *testing.T, s string) cpuset.CPUSet {
	t.Helper()
	c, err := cpuset.Parse(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return c
}

// 非 SMT zone（TPC=1，整核==逻辑核）。
func zone(t *testing.T, id int, free string) scheduler.ZoneCap {
	return scheduler.ZoneCap{ID: id, Free: mustCS(t, free), TPC: 1}
}

// SMT zone（TPC=2 + sibling 对）。
func smtZone(t *testing.T, id int, free string, sibs [][]int) scheduler.ZoneCap {
	return scheduler.ZoneCap{ID: id, Free: mustCS(t, free), TPC: 2, Siblings: sibs}
}

func node(name string, zs ...scheduler.ZoneCap) NodeTopo { return NodeTopo{Node: name, Zones: zs} }

func TestCapacitySingleCountsPerZone(t *testing.T) {
	// single：逐 zone floor(free/need)。zone0=4→2、zone1=2→1、zone2=1→0
	nodes := []NodeTopo{node("n", zone(t, 0, "0-3"), zone(t, 1, "4-5"), zone(t, 2, "6"))}
	req := Req{Pin: true, NeedPerRep: 2, Count: 1, NUMAPolicy: request.NUMASingle}
	if r, _ := Capacity(nodes, req); r != 3 {
		t.Fatalf("single 容量 = %d, want 3", r)
	}
}

func TestCapacityPreferredSpansZones(t *testing.T) {
	// preferred/spread：单副本跨本节点 zone → floor(nodeTotal/need)。total=4+2+1=7→ floor(7/2)=3
	nodes := []NodeTopo{node("n", zone(t, 0, "0-3"), zone(t, 1, "4-5"), zone(t, 2, "6"))}
	req := Req{Pin: true, NeedPerRep: 2, Count: 1, NUMAPolicy: request.NUMAPreferred}
	if r, _ := Capacity(nodes, req); r != 3 {
		t.Fatalf("preferred 容量 = %d, want 3", r)
	}
}

func TestEvaluateExhaustedRetries(t *testing.T) {
	// 8 核 single：每 zone 只剩 4 → 放不下(0) → 但满容量放得下 → Retry(非 Rejected)
	req := Req{Pin: true, NeedPerRep: 8, Count: 1, NUMAPolicy: request.NUMASingle, SMTPolicy: request.SMTLogical}
	free := []NodeTopo{node("a", zone(t, 0, "0-3"), zone(t, 1, "4-7"))}      // 每 zone 4 空闲
	total := []NodeTopo{node("a", zone(t, 0, "0-63"), zone(t, 1, "64-127"))} // 满容量 64/zone
	d, msg, _ := Evaluate(free, total, req, nil)
	if d != Retry {
		t.Fatalf("耗尽分区应 Retry，got %s (%s)", d, msg)
	}
}

func TestEvaluateImpossibleRejected(t *testing.T) {
	// 8 核 single，但分区任何 zone 满容量也只有 4 → 永远放不下 → Rejected
	req := Req{Pin: true, NeedPerRep: 8, Count: 1, NUMAPolicy: request.NUMASingle, SMTPolicy: request.SMTLogical}
	nodes := []NodeTopo{node("a", zone(t, 0, "0-3"))}
	d, _, _ := Evaluate(nodes, nodes, req, nil)
	if d != Rejected {
		t.Fatalf("超单 zone 容量应 Rejected，got %s", d)
	}
}

func TestEvaluateReady(t *testing.T) {
	req := Req{Pin: true, NeedPerRep: 8, Count: 1, NUMAPolicy: request.NUMASingle, SMTPolicy: request.SMTLogical}
	free := []NodeTopo{node("b", zone(t, 0, "0-15"))} // 16 空闲 → 放得下
	d, _, _ := Evaluate(free, free, req, nil)
	if d != Ready {
		t.Fatalf("充足应 Ready，got %s", d)
	}
}

func TestEvaluateGang(t *testing.T) {
	// 3 副本 × 2 核 single；一 zone 8 空闲 → floor(8/2)=4 ≥ 3 → Ready
	req := Req{Pin: true, NeedPerRep: 2, Count: 3, NUMAPolicy: request.NUMASingle, SMTPolicy: request.SMTLogical}
	free := []NodeTopo{node("c", zone(t, 0, "0-7"))}   // 8 空闲 → 可放 4
	total := []NodeTopo{node("c", zone(t, 0, "0-15"))} // 满容量 16 → 可放 8
	if d, _, _ := Evaluate(free, total, req, nil); d != Ready {
		t.Fatalf("gang 3×2 于 8 空闲应 Ready，got %s", d)
	}
	// 5 副本：当前只放 4 < 5，但满容量放得下 → Retry
	req.Count = 5
	if d, _, _ := Evaluate(free, total, req, nil); d != Retry {
		t.Fatalf("gang 5×2 于 8 空闲应 Retry，got %s", d)
	}
}

func TestCapacitySMTOrphanVsLogical(t *testing.T) {
	// SMT zone cpus 0-7，对 [0,4]/[1,5]/[2,6]/[3,7]。
	// free={1,2}：兄弟 5,6 被占 → 孤儿 → full-core 整核=0；logical 则算 2
	sibs := [][]int{{0, 4}, {1, 5}, {2, 6}, {3, 7}}
	orphan := []NodeTopo{node("m", smtZone(t, 0, "1-2", sibs))}
	if r, _ := Capacity(orphan, Req{NeedPerRep: 2, Count: 1, NUMAPolicy: request.NUMASingle}); r != 0 {
		t.Fatalf("full-core 孤儿兄弟应 0 容量，got %d", r)
	}
	if r, _ := Capacity(orphan, Req{NeedPerRep: 2, Count: 1, NUMAPolicy: request.NUMASingle, SMTPolicy: request.SMTLogical}); r != 1 {
		t.Fatalf("logical 下 2 逻辑核应可放 1 个 2 核，got %d", r)
	}
	// free={0,4}（整核 [0,4] 都空）→ full-core 可放 1 个 2 核
	whole := []NodeTopo{node("m", smtZone(t, 0, "0,4", sibs))}
	if r, _ := Capacity(whole, Req{NeedPerRep: 2, Count: 1, NUMAPolicy: request.NUMASingle}); r != 1 {
		t.Fatalf("full-core 整核 [0,4] 应可放 1 个 2 核，got %d", r)
	}
}

// §13 回归：cpu=1 full-core 在 SMT2 分区向上取整到 1 整核，不再被误判 Rejected。
func TestCpu1RoundsUpOnSMT(t *testing.T) {
	sibs := [][]int{{0, 4}, {1, 5}, {2, 6}, {3, 7}}
	free := []NodeTopo{node("m", smtZone(t, 0, "0-1,4-5", sibs))} // 2 整核空闲（[0,4]、[1,5]）
	// Capacity：cpu=1→取整到1整核=2逻辑核→放2个；cpu=3→2整核=4→放1个
	if r, _ := Capacity(free, Req{NeedPerRep: 1, Count: 1, NUMAPolicy: request.NUMASingle}); r != 2 {
		t.Fatalf("cpu=1 应放 2 个整核副本，got %d", r)
	}
	if r, _ := Capacity(free, Req{NeedPerRep: 3, Count: 1, NUMAPolicy: request.NUMASingle}); r != 1 {
		t.Fatalf("cpu=3 应取整到 2 整核、放 1 个，got %d", r)
	}
	// Evaluate：cpu=1 pin 应 Ready（而非 Rejected）
	if d, _, _ := Evaluate(free, free, Req{Pin: true, NeedPerRep: 1, Count: 1, NUMAPolicy: request.NUMASingle}, nil); d != Ready {
		t.Fatalf("cpu=1 full-core 在有整核的 SMT 分区应 Ready，got %s", d)
	}
}

// 无 SMT 拓扑：cpu∈{1,2,3} 按逻辑核算、不取整。
func TestNoSMTCpu123(t *testing.T) {
	nodes := []NodeTopo{node("n", zone(t, 0, "0-5"))} // 6 逻辑核，TPC=1
	for _, c := range []struct{ cpu, want int }{{1, 6}, {2, 3}, {3, 2}} {
		if r, _ := Capacity(nodes, Req{NeedPerRep: c.cpu, Count: 1, NUMAPolicy: request.NUMASingle}); r != c.want {
			t.Errorf("无SMT cpu=%d 应放 %d 个，got %d", c.cpu, c.want, r)
		}
	}
}
