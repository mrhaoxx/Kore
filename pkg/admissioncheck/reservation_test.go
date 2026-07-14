package admissioncheck

import (
	"testing"
	"time"

	"github.com/zjusct/kore/pkg/request"
)

// §4 核心：并发下不超发——一个 zone 只够放 1 个 8 核作业时，两个作业不能都判 Ready。
func TestReservationPreventsOverAdmit(t *testing.T) {
	req := Req{Pin: true, NeedPerRep: 8, Count: 1, NUMAPolicy: request.NUMASingle, SMTPolicy: request.SMTLogical}
	free := []NodeTopo{node("n", zone(t, 0, "0-7"))} // 8 空闲 → 只够 1 个 8 核
	total := []NodeTopo{node("n", zone(t, 0, "0-63"))}
	cache := NewReservationCache(5 * time.Minute)

	// A：无其它预留 → Ready，登记预留
	dA, _, plA := Evaluate(free, total, req, cache.Deducted("armv8", "wl-A"))
	if dA != Ready {
		t.Fatalf("A 应 Ready，got %s", dA)
	}
	cache.Reserve("wl-A", "armv8", plA)

	// B：扣掉 A 的预留后该 zone 已满 → Retry（不超发）
	if dB, _, _ := Evaluate(free, total, req, cache.Deducted("armv8", "wl-B")); dB != Retry {
		t.Fatalf("B 应因 A 的预留而 Retry，got %s", dB)
	}

	// A 释放后 B 可 Ready
	cache.Release("wl-A")
	if dB2, _, _ := Evaluate(free, total, req, cache.Deducted("armv8", "wl-B")); dB2 != Ready {
		t.Fatalf("A 释放后 B 应 Ready，got %s", dB2)
	}
}

// 过期预留被 Deducted 顺带清理，不再永久占位。
func TestReservationTTLExpiry(t *testing.T) {
	cache := NewReservationCache(time.Minute)
	t0 := time.Unix(1_700_000_000, 0)
	cache.now = func() time.Time { return t0 }
	cache.Reserve("wl", "armv8", []Placement{{Node: "n", Zone: 0, Cores: 8}})
	if got := cache.Deducted("armv8", "other")["n"][0]; got != 8 {
		t.Fatalf("预留内应扣 8，got %d", got)
	}
	cache.now = func() time.Time { return t0.Add(2 * time.Minute) } // 过期
	if d := cache.Deducted("armv8", "other"); len(d) != 0 {
		t.Fatalf("过期预留应被清，got %v", d)
	}
}
