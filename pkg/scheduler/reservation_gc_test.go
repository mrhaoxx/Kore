package scheduler

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	toolscache "k8s.io/client-go/tools/cache"
)

// Pod 被删（Reserve+Bind 后、agent 落账前消失）时，预占应立即清除，不等 5min TTL——
// 否则它需要整个 NUMA zone 的同类 Pod 会被这条僵尸预占挡满 TTL。
func TestReservationDroppedOnPodDelete(t *testing.T) {
	c := NewCache(5 * time.Minute)
	c.Add(Reservation{PodUID: "uid-x", Node: "n1", Zone: 3, Count: 64})
	if _, ok := c.Get("uid-x"); !ok {
		t.Fatal("setup: reservation missing")
	}
	removeReservationOnDelete(c, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "uid-x"}})
	if _, ok := c.Get("uid-x"); ok {
		t.Fatal("预占未在 Pod 删除时清除")
	}

	// tombstone：informer 漏掉删除的最终状态时给的包装对象
	c.Add(Reservation{PodUID: "uid-y", Node: "n1", Zone: 3, Count: 64})
	removeReservationOnDelete(c, toolscache.DeletedFinalStateUnknown{
		Obj: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "uid-y"}},
	})
	if _, ok := c.Get("uid-y"); ok {
		t.Fatal("预占未在 tombstone 删除时清除")
	}

	// 非 Pod 对象：不 panic、不误删
	c.Add(Reservation{PodUID: "uid-z", Node: "n1", Zone: 3, Count: 64})
	removeReservationOnDelete(c, "not-a-pod")
	if _, ok := c.Get("uid-z"); !ok {
		t.Fatal("误删了无关预占")
	}
}
