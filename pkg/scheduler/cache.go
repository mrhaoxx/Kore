package scheduler

import (
	"sync"
	"time"

	"k8s.io/utils/cpuset"
)

// Reservation 是调度器的在途预占：Reserve 时记录，agent 把分配写进 CR 后清除
// （MarkAllocated），TTL 兜底防泄漏（Pod 绑定失败但 Unreserve 丢失等）。
type Reservation struct {
	PodUID   string
	Node     string
	Zone     int // -1 = 无固定 zone（spread）
	Count    int
	Explicit *cpuset.CPUSet
	// Pool 非空表示这是建池预占（跟随已有池的成员不预占）。
	Pool string
	At   time.Time
}

type Cache struct {
	mu  sync.Mutex
	m   map[string]Reservation
	ttl time.Duration
	now func() time.Time
}

func NewCache(ttl time.Duration) *Cache {
	return &Cache{m: map[string]Reservation{}, ttl: ttl, now: time.Now}
}

func (c *Cache) Add(r Reservation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r.At = c.now()
	c.m[r.PodUID] = r
}

func (c *Cache) Remove(podUID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, podUID)
}

func (c *Cache) Get(podUID string) (Reservation, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.m[podUID]
	return r, ok
}

// ByNode 返回某节点的有效预占，顺带剔除全部过期项。
func (c *Cache) ByNode(node string) []Reservation {
	c.mu.Lock()
	defer c.mu.Unlock()
	cutoff := c.now().Add(-c.ttl)
	var out []Reservation
	for uid, r := range c.m {
		if r.At.Before(cutoff) {
			delete(c.m, uid)
			continue
		}
		if r.Node == node {
			out = append(out, r)
		}
	}
	return out
}

// MarkAllocated 清除 CR 已体现的预占（agent 上报的 allocations 里出现了该 podUID）。
func (c *Cache) MarkAllocated(node string, uids map[string]bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for uid, r := range c.m {
		if r.Node == node && uids[uid] {
			delete(c.m, uid)
		}
	}
}
