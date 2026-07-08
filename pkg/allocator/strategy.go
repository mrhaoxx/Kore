package allocator

import (
	"sort"

	"k8s.io/utils/cpuset"

	"github.com/zjusct/kore/pkg/request"
)

// Unit 是分配的最小单位：logical 模式下一个逻辑核；full-core 模式下一个完整物理核（sibling 组）。
type Unit struct {
	Min  int // 组内最小 cpu id，用于排序与连续性判断
	CPUs cpuset.CPUSet
}

// Strategy 决定在一个 NUMA zone 的空闲 unit 中挑选哪些。spec §6：v1 提供 pack/scatter。
type Strategy interface {
	Name() string
	// Pick 从升序排列的 units 中选 need 个；不可行时 ok=false。
	Pick(units []Unit, need int) ([]Unit, bool)
}

func StrategyFor(p request.Placement) Strategy {
	if p == request.PlacementScatter {
		return scatterStrategy{}
	}
	return packStrategy{}
}

type packStrategy struct{}

func (packStrategy) Name() string { return "pack" }

// Pick：连续段 best-fit——选能容纳 need 的最短连续段；无整段可容纳时，
// 从最大的段开始整段吞并直到凑够（最少碎片数）。
func (packStrategy) Pick(units []Unit, need int) ([]Unit, bool) {
	if len(units) < need {
		return nil, false
	}
	runs := runsOf(units)
	best := -1
	for i, r := range runs {
		if len(r) >= need && (best == -1 || len(r) < len(runs[best])) {
			best = i
		}
	}
	if best >= 0 {
		return runs[best][:need], true
	}
	sort.Slice(runs, func(i, j int) bool { return len(runs[i]) > len(runs[j]) })
	var out []Unit
	for _, r := range runs {
		take := min(need-len(out), len(r))
		out = append(out, r[:take]...)
		if len(out) == need {
			return out, true
		}
	}
	return nil, false
}

type scatterStrategy struct{}

func (scatterStrategy) Name() string { return "scatter" }

// Pick：等间隔取样（floor(i*len/need) 严格递增，因 len>=need）。
func (scatterStrategy) Pick(units []Unit, need int) ([]Unit, bool) {
	if len(units) < need {
		return nil, false
	}
	out := make([]Unit, 0, need)
	for i := 0; i < need; i++ {
		out = append(out, units[i*len(units)/need])
	}
	return out, true
}

// runsOf 把升序 units 按 Min 连续性切段。
func runsOf(units []Unit) [][]Unit {
	var runs [][]Unit
	for i, u := range units {
		if i == 0 || u.Min != units[i-1].Min+1 {
			runs = append(runs, []Unit{u})
			continue
		}
		runs[len(runs)-1] = append(runs[len(runs)-1], u)
	}
	return runs
}

func unitsUnion(units []Unit) cpuset.CPUSet {
	out := cpuset.New()
	for _, u := range units {
		out = out.Union(u.CPUs)
	}
	return out
}
