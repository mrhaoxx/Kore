package admissioncheck

import (
	"sync"
	"time"
)

// Placement 是一个副本占用的一段整核（single 落单 zone；preferred/spread 可拆成
// 同节点多个 zone 的多段）。准入判 Ready 时登记，供并发/后续判定扣减。
type Placement struct {
	Node  string
	Zone  int
	Cores int // 逻辑核数
}

// ReservationCache 记「已判 Ready 但 pod 尚未真正 pin 上」的在途预留，按 Workload
// UID 为键。并发判定时把其它 Workload 的预留算进「已占」，避免两个作业同时判 Ready
// 后其一 bind 失败（admit-then-fail）。
//
// 释放：Workload 删除 / check 不再 Ready（控制器显式 Release），或 TTL 兜底——pod
// 真正 pin 上后 KoreNodeTopology 已反映真实占用，预留到期自然退场（TTL 期内预留与
// 真实占用短暂并计，偏保守=宁可少放，不会超发）。
type ReservationCache struct {
	mu  sync.Mutex
	m   map[string]resEntry
	ttl time.Duration
	now func() time.Time
}

type resEntry struct {
	partition  string
	placements []Placement
	at         time.Time
}

func NewReservationCache(ttl time.Duration) *ReservationCache {
	return &ReservationCache{m: map[string]resEntry{}, ttl: ttl, now: time.Now}
}

// Reserve 登记/更新某 Workload 的预留。
func (c *ReservationCache) Reserve(uid, partition string, pl []Placement) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[uid] = resEntry{partition: partition, placements: pl, at: c.now()}
}

// Release 释放某 Workload 的预留（删除/驱逐/check 转非 Ready）。
func (c *ReservationCache) Release(uid string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, uid)
}

// Has 报告某 Workload 是否已有有效预留（用于幂等：已 Ready 且未过期不必重算）。
func (c *ReservationCache) Has(uid string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[uid]
	return ok && !e.at.Before(c.now().Add(-c.ttl))
}

// Deducted 返回 partition 内、除 excludeUID 外全部有效预留合计的 node→zone→cores，
// 顺带清除过期项。喂给 Assign/Evaluate 作为「已占」扣减。
func (c *ReservationCache) Deducted(partition, excludeUID string) map[string]map[int]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	cutoff := c.now().Add(-c.ttl)
	out := map[string]map[int]int{}
	for uid, e := range c.m {
		if e.at.Before(cutoff) {
			delete(c.m, uid)
			continue
		}
		if e.partition != partition || uid == excludeUID {
			continue
		}
		for _, p := range e.placements {
			if out[p.Node] == nil {
				out[p.Node] = map[int]int{}
			}
			out[p.Node][p.Zone] += p.Cores
		}
	}
	return out
}
