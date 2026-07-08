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
	ID   int
	Free cpuset.CPUSet
	TPC  int // threads-per-core；无 SMT 为 1
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
		out = append(out, ZoneCap{ID: z.ID, Free: free, TPC: tpc})
	}
	return out, nil
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

// AlignFullCore：full-core 语义下 need 必须能被最大 TPC 整除。
func AlignFullCore(zones []ZoneCap, need int) bool {
	tpc := 1
	for _, z := range zones {
		if z.TPC > tpc {
			tpc = z.TPC
		}
	}
	return need%tpc == 0
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
