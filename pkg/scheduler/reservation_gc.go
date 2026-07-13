package scheduler

import (
	corev1 "k8s.io/api/core/v1"
	toolscache "k8s.io/client-go/tools/cache"
)

// removeReservationOnDelete 从 informer 的删除事件里取出 Pod UID（兼容 tombstone），
// 立即清掉它的在途预占。
//
// 背景：预占本来只靠 Unreserve（调度周期失败）、MarkAllocated（CR 已落账）、5min TTL
// 三条路清除。若 Pod 被 Reserve + 成功 Bind 但在 agent 落账前就被删（如绑核 Pod 快速
// 重建），前两条都不触发，预占会卡满整个 TTL——而需要一整个 NUMA zone 的同类 Pod
// （single + 满 zone 核数）会被这条僵尸预占挡到 TTL 过期。监听 Pod 删除即时清除，堵住
// 这个口子。
func removeReservationOnDelete(c *Cache, obj interface{}) {
	var pod *corev1.Pod
	switch o := obj.(type) {
	case *corev1.Pod:
		pod = o
	case toolscache.DeletedFinalStateUnknown:
		pod, _ = o.Obj.(*corev1.Pod)
	}
	if pod == nil {
		return
	}
	c.Remove(string(pod.UID))
}
