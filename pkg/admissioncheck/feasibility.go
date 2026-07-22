// Package admissioncheck 实现 Kore 的 Kueue AdmissionCheck 控制器：Kueue 准入
// 一个 pin 作业前，先由 Kore 按「整物理核」确认目标分区真的放得下，避免超额准入
// 后 kore-agent 在 NRI 阶段 KoreAllocationFailed、作业白占配额堵队列。
//
// 可行性判定复用 kore-scheduler 的整核容量函数（pkg/scheduler），确保「准入放行」
// 与「调度能放」用同一把尺，否则又回到 admit-then-fail。
package admissioncheck

import (
	"fmt"
	"sort"

	"github.com/zjusct/kore/pkg/request"
	"github.com/zjusct/kore/pkg/scheduler"
)

// Req 是一个 PodSet 的 pin 诉求（从 Kueue Workload 的 podSet 解析而来）。
type Req struct {
	Pin        bool
	NeedPerRep int // 每副本逻辑核数（整数 cpu request）
	Count      int // 副本数（gang 语义）
	NUMAPolicy request.NUMAPolicy
	SMTPolicy  request.SMTPolicy
}

// NodeTopo 是候选分区里一个节点的容量视角。
type NodeTopo struct {
	Node  string
	Zones []scheduler.ZoneCap
}

// Decision 是准入判定结果。
type Decision int

const (
	Ready    Decision = iota // 放得下：放行（控制器随即登记预留）
	Retry                    // 当前放不下：留在队列，稍后重评
	Rejected                 // 满容量也放不下：直接拒，别无限 Retry
)

func (d Decision) String() string {
	switch d {
	case Ready:
		return "Ready"
	case Rejected:
		return "Rejected"
	default:
		return "Retry"
	}
}

// effN 是每副本的有效逻辑核数：full-core 向上取整到整物理核（cpu=1 在 SMT2 上占
// 1 整核=2 逻辑核），与 agent 分配器/调度器同一取整语义。logical 原样。
func effN(need, tpc int, fullCore bool) int {
	if fullCore {
		return request.RoundUpToCore(need, tpc)
	}
	return need
}

// Capacity 返回分区(nodes)能放下多少个「各需 req.NeedPerRep 逻辑核、按 req 的
// SMT/NUMA 策略」的副本，以及当前可用的整核逻辑核总数（供 message）。
//
//   - full-core（默认，非 logical）：按 FullCoreZones 收窄到「同物理核兄弟全空」的
//     逻辑核，need 向上取整到整核（effN；cpu=1 → 1 整核）。
//   - single：每副本落单个 NUMA zone → 逐 zone floor(free/effN) 累加。
//   - preferred/spread：单副本可跨本节点的 zone → 逐节点 floor(nodeTotal/effN) 累加。
func Capacity(nodes []NodeTopo, req Req) (replicas, freeCoreLogical int) {
	need := req.NeedPerRep
	if need <= 0 {
		return 0, 0
	}
	fullCore := req.SMTPolicy != request.SMTLogical
	for _, n := range nodes {
		zones := n.Zones
		if fullCore {
			zones = scheduler.FullCoreZones(n.Zones)
		}
		nodeTotal, nodeTPC := 0, 1
		for _, z := range zones {
			freeCoreLogical += z.Free.Size()
			nodeTotal += z.Free.Size()
			if z.TPC > nodeTPC {
				nodeTPC = z.TPC
			}
		}
		switch req.NUMAPolicy {
		case request.NUMASpread, request.NUMAPreferred:
			replicas += nodeTotal / effN(need, nodeTPC, fullCore)
		default: // single
			for _, z := range zones {
				replicas += z.Free.Size() / effN(need, z.TPC, fullCore)
			}
		}
	}
	return replicas, freeCoreLogical
}

// zcap 是放置过程中一个 zone 的可变可用整核数。
type zcap struct{ id, free, tpc int }

// Assign 贪心地为 req.Count 个副本在 nodes 上试放，返回精确的放置原子（供预留登记）。
// reserved 是其它在途预留的 node→zone→已占核；single 落单 zone（binpack 最小可容），
// preferred/spread 在同节点跨 zone（大 zone 优先）。放不满 count 则第二返回值 false。
func Assign(nodes []NodeTopo, req Req, reserved map[string]map[int]int) ([]Placement, bool) {
	need := req.NeedPerRep
	if need <= 0 || req.Count <= 0 {
		return nil, false
	}
	fullCore := req.SMTPolicy != request.SMTLogical
	free := make([][]*zcap, len(nodes))
	names := make([]string, len(nodes))
	for i, n := range nodes {
		zs := n.Zones
		if fullCore {
			zs = scheduler.FullCoreZones(n.Zones)
		}
		list := make([]*zcap, 0, len(zs))
		for _, z := range zs {
			f := z.Free.Size() - reserved[n.Node][z.ID]
			if f < 0 {
				f = 0
			}
			list = append(list, &zcap{id: z.ID, free: f, tpc: z.TPC})
		}
		free[i], names[i] = list, n.Node
	}

	var out []Placement
	for r := 0; r < req.Count; r++ {
		if !assignOne(free, names, req, fullCore, need, &out) {
			return out, false
		}
	}
	return out, true
}

func assignOne(free [][]*zcap, names []string, req Req, fullCore bool, need int, out *[]Placement) bool {
	if req.NUMAPolicy == request.NUMASpread || req.NUMAPolicy == request.NUMAPreferred {
		for ni, zs := range free {
			if len(zs) == 0 {
				continue
			}
			en := effN(need, zs[0].tpc, fullCore) // 同节点各 zone 同 TPC
			total := 0
			for _, z := range zs {
				total += z.free
			}
			if total < en {
				continue
			}
			order := make([]int, len(zs)) // 大 zone 优先扣
			for i := range order {
				order[i] = i
			}
			sort.Slice(order, func(a, b int) bool { return zs[order[a]].free > zs[order[b]].free })
			rem := en
			for _, zi := range order {
				z := zs[zi]
				if rem == 0 {
					break
				}
				take := z.free
				if take > rem {
					take = rem
				}
				if take <= 0 {
					continue
				}
				z.free -= take
				rem -= take
				*out = append(*out, Placement{Node: names[ni], Zone: z.id, Cores: take})
			}
			return true
		}
		return false
	}
	// single：整（取整后的）need 落一个 zone，binpack 取最小可容
	var best *zcap
	bestNode, bestFree, bestEn := "", int(^uint(0)>>1), 0
	for ni, zs := range free {
		for _, z := range zs {
			en := effN(need, z.tpc, fullCore)
			if z.free >= en && z.free < bestFree {
				best, bestNode, bestFree, bestEn = z, names[ni], z.free, en
			}
		}
	}
	if best == nil {
		return false
	}
	best.free -= bestEn
	*out = append(*out, Placement{Node: bestNode, Zone: best.id, Cores: bestEn})
	return true
}

// Evaluate 判定 pin 作业能否被准入：
//   - free  ：分区各节点「当前空闲」zone。
//   - total ：分区各节点「满容量」zone（Free=allocatable，用于判「永远放不下」）。
//   - reserved：其它在途预留（node→zone→已占核）。
//
// 返回 Decision、给用户/plat101 看的 message，以及 Ready 时应登记的预留放置。
func Evaluate(free, total []NodeTopo, req Req, reserved map[string]map[int]int) (Decision, string, []Placement) {
	if req.NeedPerRep <= 0 || req.Count <= 0 {
		return Rejected, fmt.Sprintf("kore: 无效诉求 need=%d count=%d", req.NeedPerRep, req.Count), nil
	}
	if _, ok := Assign(total, req, nil); !ok {
		capTotal, _ := Capacity(total, req)
		return Rejected, fmt.Sprintf(
			"kore: 分区满容量也放不下：最多 %d 个 %d 核副本，需 %d（%s）",
			capTotal, req.NeedPerRep, req.Count, req.NUMAPolicy), nil
	}
	if pl, ok := Assign(free, req, reserved); ok {
		return Ready, fmt.Sprintf(
			"kore: 分区整核空闲充足（%d 个 %d 核副本已可放）", req.Count, req.NeedPerRep), pl
	}
	_, freeCores := Capacity(free, req)
	resv := 0
	for _, zones := range reserved {
		for _, c := range zones {
			resv += c
		}
	}
	if resv > 0 {
		// 不带预留数的话，freeCores ≥ need 时这条消息看起来自相矛盾
		return Retry, fmt.Sprintf(
			"kore: 分区整核空闲不足：当前可用整核逻辑核 %d（另有 %d 核为在途预留），需 %d 个 %d 核副本，排队等释放",
			freeCores, resv, req.Count, req.NeedPerRep), nil
	}
	return Retry, fmt.Sprintf(
		"kore: 分区整核空闲不足：当前可用整核逻辑核 %d，需 %d 个 %d 核副本，排队等释放",
		freeCores, req.Count, req.NeedPerRep), nil
}
