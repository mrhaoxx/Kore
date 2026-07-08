package scheduler

import (
	"testing"
	"time"
)

func TestCacheLifecycle(t *testing.T) {
	c := NewCache(time.Minute)
	t0 := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return t0 }

	c.Add(Reservation{PodUID: "u1", Node: "n1", Zone: 0, Count: 2})
	c.Add(Reservation{PodUID: "u2", Node: "n1", Zone: 1, Count: 4})
	c.Add(Reservation{PodUID: "u3", Node: "n2", Zone: 0, Count: 1})

	if rs := c.ByNode("n1"); len(rs) != 2 {
		t.Fatalf("n1 = %+v", rs)
	}
	if r, ok := c.Get("u1"); !ok || r.Zone != 0 {
		t.Fatalf("get u1: %+v %v", r, ok)
	}
	c.Remove("u1")
	if _, ok := c.Get("u1"); ok {
		t.Fatal("u1 not removed")
	}

	// TTL 过期
	c.now = func() time.Time { return t0.Add(2 * time.Minute) }
	if rs := c.ByNode("n1"); len(rs) != 0 {
		t.Fatalf("expired not pruned: %+v", rs)
	}
	// n2 的 u3 也过期
	if rs := c.ByNode("n2"); len(rs) != 0 {
		t.Fatalf("%+v", rs)
	}
}

func TestMarkAllocated(t *testing.T) {
	c := NewCache(time.Hour)
	c.Add(Reservation{PodUID: "u1", Node: "n1", Zone: 0, Count: 2})
	c.Add(Reservation{PodUID: "u2", Node: "n1", Zone: 1, Count: 4})
	c.MarkAllocated("n1", map[string]bool{"u1": true})
	if _, ok := c.Get("u1"); ok {
		t.Fatal("u1 should be cleared (CR 已体现)")
	}
	if _, ok := c.Get("u2"); !ok {
		t.Fatal("u2 must survive")
	}
}
