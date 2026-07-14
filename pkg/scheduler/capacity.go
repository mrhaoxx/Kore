// Package scheduler 实现 kore-scheduler 的 NUMA 感知调度插件。
package scheduler

import (
	"fmt"

	"k8s.io/utils/cpuset"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
	"github.com/zjusct/kore/pkg/request"
)

// ZoneCap 是调度视角下一个 NUMA zone 的可用容量。
type ZoneCap struct {
	ID       int
	Free     cpuset.CPUSet
	TPC      int     // threads-per-core；无 SMT 为 1
	Siblings [][]int // SMT 兄弟分组（每组同一物理核的逻辑核）；无 SMT 为空
}

func ZonesFromCR(cr *v1alpha1.KoreNodeTopology) ([]ZoneCap, error) {
	out := make([]ZoneCap, 0, len(cr.Status.Zones))
	for _, z := range cr.Status.Zones {
		free, err := cpuset.Parse(z.FreeCpus)
		if err != nil {
			return nil, fmt.Errorf("node %s zone %d freeCpus %q: %w", cr.Name, z.ID, z.FreeCpus, err)
		}
		tpc := 1
		if len(z.SMTSiblings) > 0 {
			tpc = len(z.SMTSiblings[0])
		}
		out = append(out, ZoneCap{ID: z.ID, Free: free, TPC: tpc, Siblings: z.SMTSiblings})
	}
	return out, nil
}

// FullCoreZones 把每个 zone 的 Free 收窄为「整物理核空闲」的逻辑核——同一物理核的
// 全部 SMT 兄弟都在 Free 里才保留。用于 full-core 独占请求：调度器据此按整核容量
// 判定/打分，杜绝把 pin Pod 绑到「逻辑核够但整核不够（只剩孤儿兄弟）」的节点导致
// kore-agent 到 NRI 阶段才失败。无 SMT（Siblings 为空）的 zone 原样返回。
func FullCoreZones(zones []ZoneCap) []ZoneCap {
	out := make([]ZoneCap, len(zones))
	for i, z := range zones {
		if len(z.Siblings) == 0 { // 非 SMT：每个逻辑核即一个整核
			out[i] = z
			continue
		}
		usable := cpuset.New()
		for _, sib := range z.Siblings {
			whole := true
			for _, c := range sib {
				if !z.Free.Contains(c) {
					whole = false
					break
				}
			}
			if whole {
				usable = usable.Union(cpuset.New(sib...))
			}
		}
		out[i] = ZoneCap{ID: z.ID, Free: usable, TPC: z.TPC, Siblings: z.Siblings}
	}
	return out
}

// effZones 返回用于容量判定/打分的 zones：full-core 独占请求（非 explicit、非
// logical）按整物理核收窄，其余（explicit / logical / 池）保持逻辑核原样。
func effZones(req *request.Request, zones []ZoneCap) []ZoneCap {
	if req.Explicit == nil && req.SMTPolicy != request.SMTLogical {
		return FullCoreZones(zones)
	}
	return zones
}

// effNeed 返回 full-core 独占请求的有效逻辑核数：向上取整到整物理核（cpu=1 在 SMT2
// 上占 1 整核=2 逻辑核），与 agent 分配器同一取整语义。explicit/logical/池 原样。
func effNeed(req *request.Request, zones []ZoneCap, need int) int {
	if req.Explicit != nil || req.SMTPolicy == request.SMTLogical {
		return need
	}
	tpc := 1
	for _, z := range zones {
		if z.TPC > tpc {
			tpc = z.TPC
		}
	}
	return request.RoundUpToCore(need, tpc)
}

// Deduct 扣除未被 CR 体现的预占。count 型从对应 zone 的高位核扣（低位段留给
// explicit 检查更常用）；Zone<0（spread）按 zone 轮转扣；explicit 型精确扣。
func Deduct(zones []ZoneCap, rs []Reservation) []ZoneCap {
	out := make([]ZoneCap, len(zones))
	copy(out, zones)
	for _, r := range rs {
		if r.Explicit != nil {
			for i := range out {
				out[i].Free = out[i].Free.Difference(*r.Explicit)
			}
			continue
		}
		if r.Zone >= 0 {
			for i := range out {
				if out[i].ID == r.Zone {
					out[i].Free = dropHigh(out[i].Free, r.Count)
				}
			}
			continue
		}
		remaining := r.Count // spread：轮转扣
		for remaining > 0 {
			progress := false
			for i := range out {
				if remaining == 0 {
					break
				}
				if out[i].Free.Size() > 0 {
					out[i].Free = dropHigh(out[i].Free, 1)
					remaining--
					progress = true
				}
			}
			if !progress {
				break
			}
		}
	}
	return out
}

func dropHigh(s cpuset.CPUSet, n int) cpuset.CPUSet {
	l := s.List()
	if n >= len(l) {
		return cpuset.New()
	}
	return cpuset.New(l[:len(l)-n]...)
}

func TotalFree(zones []ZoneCap) int {
	t := 0
	for _, z := range zones {
		t += z.Free.Size()
	}
	return t
}

// FitSingle：binpack——free 数升序中第一个能容纳 need 的 zone，并列取小 ID。
func FitSingle(zones []ZoneCap, need int) (int, bool) {
	best, bestFree := -1, int(^uint(0)>>1)
	for _, z := range zones {
		f := z.Free.Size()
		if f >= need && (f < bestFree || (f == bestFree && z.ID < best)) {
			best, bestFree = z.ID, f
		}
	}
	return best, best >= 0
}

// FitPreferred：优先单 zone；否则总量满足时以 free 最多的 zone 为 primary。
func FitPreferred(zones []ZoneCap, need int) (int, bool) {
	if z, ok := FitSingle(zones, need); ok {
		return z, true
	}
	if TotalFree(zones) < need {
		return -1, false
	}
	best, bestFree := -1, -1
	for _, z := range zones {
		if f := z.Free.Size(); f > bestFree || (f == bestFree && z.ID < best) {
			best, bestFree = z.ID, f
		}
	}
	return best, true
}

func FitSpread(zones []ZoneCap, need int) bool { return TotalFree(zones) >= need }

func FitExplicit(zones []ZoneCap, want cpuset.CPUSet) bool {
	all := cpuset.New()
	for _, z := range zones {
		all = all.Union(z.Free)
	}
	return want.Difference(all).IsEmpty()
}

// ScoreFit：越紧凑越高分（binpack 倾向），0-100。
func ScoreFit(zones []ZoneCap, policy request.NUMAPolicy, explicit bool, need int) int64 {
	denom := 0
	switch {
	case explicit, policy == request.NUMASpread, policy == request.NUMAPreferred && !fitsSingleOnly(zones, need):
		denom = TotalFree(zones)
	default:
		z, ok := FitSingle(zones, need)
		if !ok {
			return 0
		}
		for _, zc := range zones {
			if zc.ID == z {
				denom = zc.Free.Size()
			}
		}
	}
	if denom < need || denom == 0 {
		return 0
	}
	s := int64(100 * need / denom)
	if s > 100 {
		s = 100
	}
	return s
}

func fitsSingleOnly(zones []ZoneCap, need int) bool {
	_, ok := FitSingle(zones, need)
	return ok
}
