# kore-scheduler（Plan 3/4）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 NUMA 感知调度器插件 kore-scheduler：PreFilter 建快照、Filter 按 NUMA 容量与 agent Lease 过滤、Score 碎片最小化、Reserve 并发预占、PreBind 写 `reserved-numa` 注解；产出可作为第二调度器部署的二进制与配置。

**Architecture:** spec §3。调度器决定「节点 + NUMA」，agent 决定具体核号。数据源：KoreNodeTopology CR（每调度周期 List 一次，HPC 规模足够）+ agent Lease。预占缓存解决调度器视角与 CR 上报之间的延迟窗口；CR 体现分配后自动清预占（MarkAllocated）+ TTL 兜底。

**Tech Stack:**（全部已用 go doc/源码核实，k8s v1.36.2）
- 插件接口在 `fwk "k8s.io/kube-scheduler/framework"`（staging）：`Filter(ctx, state CycleState, pod *v1.Pod, nodeInfo NodeInfo) *Status`、`Score(...) (int64, *Status)`、`Reserve/Unreserve(... nodeName string)`、`PreBind` 需配套 `PreBindPreFlight(...) (*PreBindPreFlightResult, *Status)`、`PreFilter(ctx, state, p, nodes []NodeInfo) (*PreFilterResult, *Status)` + `PreFilterExtensions()`
- 工厂：`k8s.io/kubernetes/pkg/scheduler/framework/runtime`.PluginFactory = `func(ctx, runtime.Object, fwk.Handle) (fwk.Plugin, error)`
- 入口：`k8s.io/kubernetes/cmd/kube-scheduler/app`.NewSchedulerCommand(WithPlugin)
- 测试构造器：`kfw "k8s.io/kubernetes/pkg/scheduler/framework"` 的 NewCycleState/NewNodeInfo
- 依赖引入需 33 个 staging replace（列表来自 k8s v1.36.2 go.mod 的 `=> ./staging` 行）

## Global Constraints

- 继承 Plan 1/2 全部约束
- 调度粒度近似（记入文档）：Pod 级 need = 所有绑核容器 CPU 之和；单 zone 校验用总量（agent 在 zone 内按容器分配）
- SMT 对齐：调度器无法知道 agent 的 ConfigMap 默认值，按「注解未写 logical 即 full-core」保守判断（与 agent 默认一致）
- Lease 命名/命名空间与 Plan 2 对齐：`kore-agent-<node>` @ `kore-system`
- 非 kore Pod 必须零开销跳过（PreFilter 返回 Skip）
- Score 返回 0-100

---

### Task 1: k8s.io/kubernetes 依赖引入

**Files:**
- Modify: `go.mod`、`go.sum`

**Interfaces:**
- Produces: 可导入 `k8s.io/kube-scheduler/framework`、`k8s.io/kubernetes/pkg/scheduler/framework/runtime`、`k8s.io/kubernetes/cmd/kube-scheduler/app`

- [ ] **Step 1: 添加 staging replaces 并拉取**

```bash
curl -s https://raw.githubusercontent.com/kubernetes/kubernetes/v1.36.2/go.mod | grep "=> ./staging" | awk '{print $1}' > /tmp/staging-mods.txt
wc -l /tmp/staging-mods.txt   # 期望 33
for M in $(cat /tmp/staging-mods.txt); do go mod edit -replace $M=$M@v0.36.2; done
go get k8s.io/kubernetes@v1.36.2
```

- [ ] **Step 2: 验证关键包可构建**

```bash
go build k8s.io/kube-scheduler/framework k8s.io/kubernetes/pkg/scheduler/framework/runtime k8s.io/kubernetes/cmd/kube-scheduler/app
go build ./... && go test ./pkg/... -count=1
```

Expected: 无错误；现有测试全绿（replace 把 staging 钉在 v0.36.2，与既有依赖一致）。

- [ ] **Step 3: Commit** — `git add go.mod go.sum && git commit -m "build: 引入 k8s.io/kubernetes v1.36.2 与 staging replaces"`

---

### Task 2: 容量计算纯函数（pkg/scheduler/capacity.go）

**Files:**
- Create: `pkg/scheduler/capacity.go`
- Test: `pkg/scheduler/capacity_test.go`

**Interfaces:**
- Produces:
  - `scheduler.ZoneCap{ID int; Free cpuset.CPUSet; TPC int}`（TPC=threads-per-core，无 SMT 为 1）
  - `ZonesFromCR(cr *v1alpha1.KoreNodeTopology) ([]ZoneCap, error)`
  - `Deduct(zones []ZoneCap, rs []Reservation) []ZoneCap`（count 型预占从对应 zone 高位核扣减；explicit 型精确扣集合；Zone<0 的 count 预占按各 zone 均匀扣）
  - `TotalFree([]ZoneCap) int`
  - `FitSingle(zones, need) (zoneID int, ok bool)`（binpack：free 升序第一个 ≥need，并列取小 ID）
  - `FitPreferred(zones, need) (primaryZone int, ok bool)`（单 zone 可满足则同 FitSingle；否则总量 ≥need 且 primary=free 最多 zone）
  - `FitSpread(zones, need) bool`（总量 ≥need）
  - `FitExplicit(zones, want cpuset.CPUSet) bool`（want ⊆ 全部 Free 之并）
  - `AlignFullCore(zones, need) bool`（need 可被最大 TPC 整除）
  - `ScoreFit(zones, policy request.NUMAPolicy, explicit bool, need int) int64`（single/preferred：100*need/所选 zone free；spread/explicit：100*need/总 free；不可满足=0；上限 100）

- [ ] **Step 1: 写失败测试**

```go
package scheduler

import (
	"testing"

	"k8s.io/utils/cpuset"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
	"github.com/zjusct/kore/pkg/request"
)

func cr(node string, zones ...v1alpha1.Zone) *v1alpha1.KoreNodeTopology {
	c := &v1alpha1.KoreNodeTopology{}
	c.Name = node
	c.Status.Zones = zones
	return c
}

func TestZonesFromCR(t *testing.T) {
	c := cr("n1",
		v1alpha1.Zone{ID: 0, Cpus: "0-3,8-11", FreeCpus: "2-3,10-11", SMTSiblings: [][]int{{0, 8}, {1, 9}, {2, 10}, {3, 11}}},
		v1alpha1.Zone{ID: 1, Cpus: "4-7,12-15", FreeCpus: "4-7,12-15", SMTSiblings: [][]int{{4, 12}, {5, 13}, {6, 14}, {7, 15}}},
	)
	zones, err := ZonesFromCR(c)
	if err != nil {
		t.Fatal(err)
	}
	if len(zones) != 2 || zones[0].Free.Size() != 4 || zones[0].TPC != 2 || zones[1].Free.Size() != 8 {
		t.Fatalf("%+v", zones)
	}
}

func armZones() []ZoneCap { // 4 zone × 4 free，无 SMT
	var out []ZoneCap
	for z := 0; z < 4; z++ {
		out = append(out, ZoneCap{ID: z, Free: cpuset.New(z*4, z*4+1, z*4+2, z*4+3), TPC: 1})
	}
	return out
}

func TestDeductCountAndExplicit(t *testing.T) {
	ex := cpuset.New(0, 1)
	zones := Deduct(armZones(), []Reservation{
		{PodUID: "a", Zone: 1, Count: 3},
		{PodUID: "b", Explicit: &ex},
	})
	if zones[1].Free.Size() != 1 {
		t.Fatalf("zone1 free = %v", zones[1].Free)
	}
	if zones[0].Free.Size() != 2 || zones[0].Free.Contains(0) || zones[0].Free.Contains(1) {
		t.Fatalf("zone0 free = %v", zones[0].Free)
	}
}

func TestFitSingleBinpack(t *testing.T) {
	zones := armZones()
	zones[2].Free = cpuset.New(8, 9) // zone2 只剩 2 → 请求 2 应选 zone2
	z, ok := FitSingle(zones, 2)
	if !ok || z != 2 {
		t.Fatalf("z=%d ok=%v", z, ok)
	}
	if _, ok := FitSingle(zones, 5); ok {
		t.Fatal("5 > 任何单 zone")
	}
}

func TestFitPreferredFallsBack(t *testing.T) {
	z, ok := FitPreferred(armZones(), 6) // 单 zone 放不下，总量够
	if !ok || z != 0 { // free 全 4 并列 → 最多者取小 ID
		t.Fatalf("z=%d ok=%v", z, ok)
	}
	if _, ok := FitPreferred(armZones(), 17); ok {
		t.Fatal("17 > 总量 16")
	}
}

func TestFitSpreadAndExplicit(t *testing.T) {
	if !FitSpread(armZones(), 16) || FitSpread(armZones(), 17) {
		t.Fatal("spread total check wrong")
	}
	if !FitExplicit(armZones(), cpuset.New(3, 4, 5)) {
		t.Fatal("explicit free subset should fit")
	}
	zones := armZones()
	zones[0].Free = cpuset.New(1, 2, 3)
	if FitExplicit(zones, cpuset.New(0, 1)) {
		t.Fatal("cpu0 not free")
	}
}

func TestAlignFullCore(t *testing.T) {
	smt := []ZoneCap{{ID: 0, Free: cpuset.New(0, 1, 8, 9), TPC: 2}}
	if AlignFullCore(smt, 3) || !AlignFullCore(smt, 4) || !AlignFullCore(armZones(), 3) {
		t.Fatal("alignment check wrong")
	}
}

func TestScoreFitPrefersTightZone(t *testing.T) {
	loose := armZones()                                     // 每 zone 4 free
	tight := []ZoneCap{{ID: 0, Free: cpuset.New(0, 1), TPC: 1}} // 恰好 2
	sLoose := ScoreFit(loose, request.NUMASingle, false, 2)
	sTight := ScoreFit(tight, request.NUMASingle, false, 2)
	if sTight <= sLoose || sTight != 100 {
		t.Fatalf("tight=%d loose=%d", sTight, sLoose)
	}
	if ScoreFit(tight, request.NUMASingle, false, 3) != 0 {
		t.Fatal("unfit must score 0")
	}
}
```

- [ ] **Step 2: 确认失败** — Run: `go test ./pkg/scheduler/...` Expected: FAIL
- [ ] **Step 3: 实现 capacity.go**

```go
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
```

（`Reservation` 类型在 Task 3 定义；本 Task 先在 capacity.go 顶部临时声明，Task 3 移到 cache.go——或者两个 Task 一起提交前先写 cache.go 的类型声明。为避免编译断档：本 Task 直接在 capacity_test.go 同包引用，Task 3 才建 cache.go，因此**本 Task 需要先放一个最小 `Reservation` struct 到 capacity.go**，Task 3 把它挪进 cache.go 并扩展字段——挪动时保持字段兼容。）

```go
// Reservation 是调度器的在途预占（Task 3 扩展 TTL 语义）。
type Reservation struct {
	PodUID   string
	Node     string
	Zone     int // -1 = 无固定 zone（spread）
	Count    int
	Explicit *cpuset.CPUSet
}
```

- [ ] **Step 4: 确认通过** — Run: `go test ./pkg/scheduler/... -v` Expected: PASS ×7
- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: 调度容量计算纯函数（zone 快照/扣减/拟合/打分）"`

---

### Task 3: 预占缓存（pkg/scheduler/cache.go）

**Files:**
- Create: `pkg/scheduler/cache.go`（`Reservation` 从 capacity.go 挪入并加 `At time.Time`）
- Modify: `pkg/scheduler/capacity.go`（删临时 Reservation 声明）
- Test: `pkg/scheduler/cache_test.go`

**Interfaces:**
- Produces: `NewCache(ttl time.Duration) *Cache`（`now` 字段可注入）；`Add(r Reservation)`（自动盖 At 时间戳）；`Remove(podUID)`；`Get(podUID) (Reservation, bool)`；`ByNode(node) []Reservation`（惰性剔除过期）；`MarkAllocated(node string, uids map[string]bool)`（CR 已体现的分配 → 删预占）

- [ ] **Step 1: 写失败测试**

```go
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
```

- [ ] **Step 2: 确认失败** — Run: `go test ./pkg/scheduler/...` Expected: FAIL
- [ ] **Step 3: 实现 cache.go（并把 Reservation 挪入、加 At 字段）**

```go
package scheduler

import (
	"sync"
	"time"

	"k8s.io/utils/cpuset"
)

// Reservation 是调度器的在途预占：Reserve 时记录，agent 把分配写进 CR 后清除
//（MarkAllocated），TTL 兜底防泄漏（Pod 绑定失败但 Unreserve 丢失等）。
type Reservation struct {
	PodUID   string
	Node     string
	Zone     int // -1 = 无固定 zone（spread）
	Count    int
	Explicit *cpuset.CPUSet
	At       time.Time
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
```

- [ ] **Step 4: 确认通过** — Run: `go test ./pkg/scheduler/... -v` Expected: PASS（含 Task 2 回归）
- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: 调度器预占缓存（TTL + CR 对账清除）"`

---

### Task 4: framework 插件（pkg/scheduler/plugin.go）

**Files:**
- Create: `pkg/scheduler/plugin.go`
- Test: `pkg/scheduler/plugin_test.go`

**Interfaces:**
- Produces:
  - `scheduler.Name = "Kore"`
  - `Deps{ListTopologies func(ctx) ([]v1alpha1.KoreNodeTopology, error); LeaseFresh func(node string) bool; PatchPodAnnotation func(ctx, ns, name, key, value string) error}`
  - `NewWithDeps(deps Deps, cache *Cache) *Kore`（测试用）
  - `New(ctx, obj runtime.Object, h fwk.Handle) (fwk.Plugin, error)`（生产工厂，Task 5 接线）
  - Kore 实现：PreFilter/PreFilterExtensions、Filter、Score/ScoreExtensions、Reserve/Unreserve、PreBindPreFlight/PreBind

- [ ] **Step 1: 写失败测试**

```go
package scheduler

import (
	"context"
	"strconv"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fwk "k8s.io/kube-scheduler/framework"
	kfw "k8s.io/kubernetes/pkg/scheduler/framework"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
	"github.com/zjusct/kore/pkg/request"
)

func schedPod(cpus string, annos map[string]string) *corev1.Pod {
	a := map[string]string{request.AnnoPin: "true"}
	for k, v := range annos {
		a[k] = v
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default", UID: "uid-p", Annotations: a},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "app",
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpus)},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpus)},
			},
		}}},
	}
}

func topoCR(node string, freePerZone ...string) v1alpha1.KoreNodeTopology {
	c := v1alpha1.KoreNodeTopology{}
	c.Name = node
	for i, f := range freePerZone {
		c.Status.Zones = append(c.Status.Zones, v1alpha1.Zone{ID: i, Cpus: f, FreeCpus: f})
	}
	return c
}

type env struct {
	k       *Kore
	patched map[string]string
	stale   map[string]bool
	crs     []v1alpha1.KoreNodeTopology
}

func newEnv(t *testing.T, crs ...v1alpha1.KoreNodeTopology) *env {
	t.Helper()
	e := &env{patched: map[string]string{}, stale: map[string]bool{}, crs: crs}
	deps := Deps{
		ListTopologies: func(ctx context.Context) ([]v1alpha1.KoreNodeTopology, error) { return e.crs, nil },
		LeaseFresh:     func(node string) bool { return !e.stale[node] },
		PatchPodAnnotation: func(ctx context.Context, ns, name, key, value string) error {
			e.patched[key] = value
			return nil
		},
	}
	e.k = NewWithDeps(deps, NewCache(defaultReservationTTL))
	return e
}

func nodeInfo(name string) fwk.NodeInfo {
	ni := kfw.NewNodeInfo()
	ni.SetNode(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}})
	return ni
}

func runPreFilter(t *testing.T, k *Kore, pod *corev1.Pod) fwk.CycleState {
	t.Helper()
	state := kfw.NewCycleState()
	_, status := k.PreFilter(context.Background(), state, pod, nil)
	if !status.IsSuccess() {
		t.Fatalf("prefilter: %v", status)
	}
	return state
}

func TestPreFilterSkipsNonKorePod(t *testing.T) {
	e := newEnv(t)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "plain", Namespace: "default"}}
	state := kfw.NewCycleState()
	_, status := e.k.PreFilter(context.Background(), state, pod, nil)
	if status.Code() != fwk.Skip {
		t.Fatalf("status = %v, want Skip", status)
	}
}

func TestFilterByCapacityAndLease(t *testing.T) {
	e := newEnv(t,
		topoCR("small", "0-1"),      // 单 zone 2 free
		topoCR("big", "0-3", "4-7"), // 2 zone 各 4 free
		topoCR("dead", "0-7"),
	)
	e.stale["dead"] = true
	pod := schedPod("4", nil) // single 策略默认
	state := runPreFilter(t, e.k, pod)

	if st := e.k.Filter(context.Background(), state, pod, nodeInfo("small")); st.IsSuccess() {
		t.Fatal("small 放不下 4")
	}
	if st := e.k.Filter(context.Background(), state, pod, nodeInfo("big")); !st.IsSuccess() {
		t.Fatalf("big 应通过: %v", st)
	}
	if st := e.k.Filter(context.Background(), state, pod, nodeInfo("dead")); st.IsSuccess() {
		t.Fatal("lease 过期节点必须被拒")
	}
	if st := e.k.Filter(context.Background(), state, pod, nodeInfo("unknown")); st.IsSuccess() {
		t.Fatal("无 CR 节点必须被拒")
	}
}

func TestScorePrefersTighterNode(t *testing.T) {
	e := newEnv(t, topoCR("tight", "0-3"), topoCR("loose", "0-15"))
	pod := schedPod("4", nil)
	state := runPreFilter(t, e.k, pod)
	sTight, st1 := e.k.Score(context.Background(), state, pod, nodeInfo("tight"))
	sLoose, st2 := e.k.Score(context.Background(), state, pod, nodeInfo("loose"))
	if !st1.IsSuccess() || !st2.IsSuccess() || sTight <= sLoose {
		t.Fatalf("tight=%d loose=%d", sTight, sLoose)
	}
}

func TestReserveDeductsForNextPod(t *testing.T) {
	e := newEnv(t, topoCR("n1", "0-3")) // 4 free
	p1 := schedPod("3", nil)
	state1 := runPreFilter(t, e.k, p1)
	if st := e.k.Reserve(context.Background(), state1, p1, "n1"); !st.IsSuccess() {
		t.Fatalf("reserve: %v", st)
	}
	// 第二个 Pod（不同 UID）在预占生效后只剩 1 核
	p2 := schedPod("2", nil)
	p2.UID = "uid-p2"
	state2 := runPreFilter(t, e.k, p2)
	if st := e.k.Filter(context.Background(), state2, p2, nodeInfo("n1")); st.IsSuccess() {
		t.Fatal("预占后 n1 只剩 1 核，2 核请求必须被拒")
	}
	// Unreserve 归还
	e.k.Unreserve(context.Background(), state1, p1, "n1")
	state3 := runPreFilter(t, e.k, p2)
	if st := e.k.Filter(context.Background(), state3, p2, nodeInfo("n1")); !st.IsSuccess() {
		t.Fatalf("unreserve 后应通过: %v", st)
	}
}

func TestPreBindWritesReservedNUMA(t *testing.T) {
	e := newEnv(t, topoCR("n1", "0-3", "4-7"))
	pod := schedPod("2", map[string]string{request.AnnoNUMAPolicy: "single"})
	state := runPreFilter(t, e.k, pod)
	if st := e.k.Reserve(context.Background(), state, pod, "n1"); !st.IsSuccess() {
		t.Fatal(st)
	}
	if st := e.k.PreBind(context.Background(), state, pod, "n1"); !st.IsSuccess() {
		t.Fatal(st)
	}
	if z, err := strconv.Atoi(e.patched[request.AnnoReservedNUMA]); err != nil || z < 0 || z > 1 {
		t.Fatalf("patched = %v", e.patched)
	}
}

func TestMarkAllocatedClearsReservationViaPreFilter(t *testing.T) {
	e := newEnv(t, topoCR("n1", "0-3"))
	p1 := schedPod("3", nil)
	state1 := runPreFilter(t, e.k, p1)
	if st := e.k.Reserve(context.Background(), state1, p1, "n1"); !st.IsSuccess() {
		t.Fatal(st)
	}
	// agent 上报：CR 现在体现了 uid-p 的分配（freeCpus 已扣、allocations 有记录）
	cr := topoCR("n1", "3") // 只剩 1 free
	cr.Status.Allocations = []v1alpha1.Allocation{{PodUID: "uid-p", Pod: "default/p", Container: "app", Cpuset: "0-2", NUMA: []int{0}}}
	e.crs = []v1alpha1.KoreNodeTopology{cr}

	p2 := schedPod("1", nil)
	p2.UID = "uid-p2"
	state2 := runPreFilter(t, e.k, p2)
	// 若预占未被 MarkAllocated 清除会双重扣减（1 free − 3 预占 = 负），Filter 必失败；
	// 正确行为：预占已清，1 free 容纳 1 核请求
	if st := e.k.Filter(context.Background(), state2, p2, nodeInfo("n1")); !st.IsSuccess() {
		t.Fatalf("double counting detected: %v", st)
	}
}
```

- [ ] **Step 2: 确认失败** — Run: `go test ./pkg/scheduler/...` Expected: FAIL
- [ ] **Step 3: 实现 plugin.go**

```go
package scheduler

import (
	"context"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	fwk "k8s.io/kube-scheduler/framework"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
	"github.com/zjusct/kore/pkg/request"
)

const (
	Name                  = "Kore"
	stateKey fwk.StateKey = "kore.zjusct.io/state"
	leaseNamespace        = "kore-system"
	defaultReservationTTL = 5 * time.Minute
)

type Deps struct {
	ListTopologies     func(ctx context.Context) ([]v1alpha1.KoreNodeTopology, error)
	LeaseFresh         func(node string) bool
	PatchPodAnnotation func(ctx context.Context, ns, name, key, value string) error
}

type nodeSnap struct {
	zones   []ZoneCap
	leaseOK bool
	found   bool
}

type koreState struct {
	req    *request.Request
	need   int
	byNode map[string]nodeSnap
}

func (s *koreState) Clone() fwk.StateData { return s }

type Kore struct {
	deps  Deps
	cache *Cache
}

var (
	_ fwk.PreFilterPlugin = &Kore{}
	_ fwk.FilterPlugin    = &Kore{}
	_ fwk.ScorePlugin     = &Kore{}
	_ fwk.ReservePlugin   = &Kore{}
	_ fwk.PreBindPlugin   = &Kore{}
)

func NewWithDeps(deps Deps, cache *Cache) *Kore { return &Kore{deps: deps, cache: cache} }

func (k *Kore) Name() string { return Name }

func (k *Kore) PreFilter(ctx context.Context, state fwk.CycleState, pod *corev1.Pod, _ []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	req, err := request.ParsePod(pod)
	if err != nil {
		return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "kore: "+err.Error())
	}
	if req == nil {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	need := 0
	for _, c := range req.Containers {
		need += c.CPUs
	}
	crs, err := k.deps.ListTopologies(ctx)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}
	st := &koreState{req: req, need: need, byNode: map[string]nodeSnap{}}
	for i := range crs {
		cr := &crs[i]
		// CR 已体现的分配 → 清对应预占，避免双重扣减
		uids := map[string]bool{}
		for _, a := range cr.Status.Allocations {
			uids[a.PodUID] = true
		}
		k.cache.MarkAllocated(cr.Name, uids)

		zones, zerr := ZonesFromCR(cr)
		if zerr != nil {
			continue // 坏 CR 视同节点无拓扑
		}
		zones = Deduct(zones, k.cache.ByNode(cr.Name))
		st.byNode[cr.Name] = nodeSnap{zones: zones, leaseOK: k.deps.LeaseFresh(cr.Name), found: true}
	}
	state.Write(stateKey, st)
	return nil, nil
}

func (k *Kore) PreFilterExtensions() fwk.PreFilterExtensions { return nil }

func getState(state fwk.CycleState) (*koreState, bool) {
	v, err := state.Read(stateKey)
	if err != nil {
		return nil, false
	}
	st, ok := v.(*koreState)
	return st, ok
}

func (k *Kore) Filter(ctx context.Context, state fwk.CycleState, pod *corev1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	st, ok := getState(state)
	if !ok {
		return nil
	}
	node := nodeInfo.Node().Name
	ns := st.byNode[node]
	if !ns.found {
		return fwk.NewStatus(fwk.Unschedulable, "kore: no topology reported for node")
	}
	if !ns.leaseOK {
		return fwk.NewStatus(fwk.Unschedulable, "kore: agent lease expired on node")
	}
	if st.req.Explicit != nil {
		if !FitExplicit(ns.zones, *st.req.Explicit) {
			return fwk.NewStatus(fwk.Unschedulable, "kore: explicit cpuset not free on node")
		}
		return nil
	}
	// 调度器不知道 agent 的 ConfigMap 默认值；按注解未写 logical 即 full-core 保守判断
	if st.req.SMTPolicy != request.SMTLogical && !AlignFullCore(ns.zones, st.need) {
		return fwk.NewStatus(fwk.Unschedulable, "kore: cpu count not aligned to full cores on SMT node")
	}
	switch st.req.NUMAPolicy {
	case request.NUMASpread:
		if !FitSpread(ns.zones, st.need) {
			return fwk.NewStatus(fwk.Unschedulable, "kore: insufficient free cpus for spread")
		}
	case request.NUMAPreferred:
		if _, ok := FitPreferred(ns.zones, st.need); !ok {
			return fwk.NewStatus(fwk.Unschedulable, "kore: insufficient free cpus")
		}
	default:
		if _, ok := FitSingle(ns.zones, st.need); !ok {
			return fwk.NewStatus(fwk.Unschedulable, "kore: no NUMA zone with enough free cpus")
		}
	}
	return nil
}

func (k *Kore) Score(ctx context.Context, state fwk.CycleState, pod *corev1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	st, ok := getState(state)
	if !ok {
		return 0, nil
	}
	ns := st.byNode[nodeInfo.Node().Name]
	if !ns.found {
		return 0, nil
	}
	return ScoreFit(ns.zones, st.req.NUMAPolicy, st.req.Explicit != nil, st.need), nil
}

func (k *Kore) ScoreExtensions() fwk.ScoreExtensions { return nil }

func (k *Kore) Reserve(ctx context.Context, state fwk.CycleState, pod *corev1.Pod, nodeName string) *fwk.Status {
	st, ok := getState(state)
	if !ok {
		return nil
	}
	ns := st.byNode[nodeName]
	r := Reservation{PodUID: string(pod.UID), Node: nodeName, Zone: -1, Count: st.need, Explicit: st.req.Explicit}
	if st.req.Explicit == nil {
		switch st.req.NUMAPolicy {
		case request.NUMASpread:
			// zone 保持 -1
		case request.NUMAPreferred:
			if z, ok := FitPreferred(ns.zones, st.need); ok {
				r.Zone = z
			}
		default:
			z, fits := FitSingle(ns.zones, st.need)
			if !fits {
				return fwk.NewStatus(fwk.Unschedulable, "kore: capacity changed during scheduling cycle")
			}
			r.Zone = z
		}
	}
	k.cache.Add(r)
	return nil
}

func (k *Kore) Unreserve(ctx context.Context, state fwk.CycleState, pod *corev1.Pod, nodeName string) {
	k.cache.Remove(string(pod.UID))
}

func (k *Kore) PreBindPreFlight(ctx context.Context, state fwk.CycleState, pod *corev1.Pod, nodeName string) (*fwk.PreBindPreFlightResult, *fwk.Status) {
	if _, ok := getState(state); !ok {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	return nil, nil
}

func (k *Kore) PreBind(ctx context.Context, state fwk.CycleState, pod *corev1.Pod, nodeName string) *fwk.Status {
	if _, ok := getState(state); !ok {
		return nil
	}
	r, ok := k.cache.Get(string(pod.UID))
	if !ok || r.Zone < 0 {
		return nil // spread/explicit：agent 不需要 reserved-numa
	}
	if err := k.deps.PatchPodAnnotation(ctx, pod.Namespace, pod.Name, request.AnnoReservedNUMA, strconv.Itoa(r.Zone)); err != nil {
		return fwk.AsStatus(err)
	}
	return nil
}
```

生产工厂（同文件；Task 5 接线到 main）：

```go
// New 是 scheduler framework 的插件工厂。
func New(ctx context.Context, _ runtime.Object, h fwk.Handle) (fwk.Plugin, error) {
	sch := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(sch); err != nil {
		return nil, err
	}
	crc, err := ctrlclient.New(h.KubeConfig(), ctrlclient.Options{Scheme: sch})
	if err != nil {
		return nil, err
	}
	leaseLister := h.SharedInformerFactory().Coordination().V1().Leases().Lister()
	deps := Deps{
		ListTopologies: func(ctx context.Context) ([]v1alpha1.KoreNodeTopology, error) {
			var l v1alpha1.KoreNodeTopologyList
			if err := crc.List(ctx, &l); err != nil {
				return nil, err
			}
			return l.Items, nil
		},
		LeaseFresh: func(node string) bool {
			l, err := leaseLister.Leases(leaseNamespace).Get("kore-agent-" + node)
			if err != nil || l.Spec.RenewTime == nil {
				return false
			}
			d := 15 * time.Second
			if l.Spec.LeaseDurationSeconds != nil {
				d = time.Duration(*l.Spec.LeaseDurationSeconds) * time.Second
			}
			return time.Since(l.Spec.RenewTime.Time) <= d
		},
		PatchPodAnnotation: func(ctx context.Context, ns, name, key, value string) error {
			patch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, key, value))
			_, err := h.ClientSet().CoreV1().Pods(ns).Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
			return err
		},
	}
	return NewWithDeps(deps, NewCache(defaultReservationTTL)), nil
}
```

- [ ] **Step 4: 确认通过** — Run: `go test ./pkg/scheduler/... -v` Expected: PASS（全部）
- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: Kore 调度插件（PreFilter/Filter/Score/Reserve/PreBind）"`

---

### Task 5: cmd/kore-scheduler 与调度器配置

**Files:**
- Create: `cmd/kore-scheduler/main.go`
- Create: `deploy/scheduler/kube-scheduler-config.yaml`

**Interfaces:**
- Produces: `kore-scheduler` 二进制（完整 kube-scheduler + Kore 插件）；第二调度器配置样例（`schedulerName: kore-scheduler`）

- [ ] **Step 1: 写 main.go**

```go
// kore-scheduler：内嵌 Kore 插件的 kube-scheduler（第二调度器部署）。
package main

import (
	"os"

	"k8s.io/component-base/cli"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"

	"github.com/zjusct/kore/pkg/scheduler"
)

func main() {
	cmd := app.NewSchedulerCommand(app.WithPlugin(scheduler.Name, scheduler.New))
	os.Exit(cli.Run(cmd))
}
```

- [ ] **Step 2: 写调度器配置样例**

```yaml
# deploy/scheduler/kube-scheduler-config.yaml
# 第二调度器：kore-webhook 会给绑核 Pod 注入 schedulerName: kore-scheduler
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
leaderElection:
  leaderElect: false
profiles:
- schedulerName: kore-scheduler
  plugins:
    multiPoint:
      enabled:
      - name: Kore
```

- [ ] **Step 3: 全量验证与交叉编译**

Run: `make test && make build && go vet ./... && GOOS=linux GOARCH=amd64 go build ./... && GOOS=linux GOARCH=arm64 go build ./... && ./bin-check`
（`./bin-check` 指代：`go build -o /tmp/kore-scheduler ./cmd/kore-scheduler && /tmp/kore-scheduler --help | head -5`，确认二进制能打印 kube-scheduler 帮助）
Expected: 全绿；帮助输出含 kube-scheduler 用法。

- [ ] **Step 4: Commit** — `git add -A && git commit -m "feat: kore-scheduler 二进制与第二调度器配置"`

---

## Plan 3 完成标准

- `make test` 全绿；双架构交叉编译干净；`kore-scheduler --help` 可运行
- 全链路语义闭环（单测层面）：PreFilter 快照 → Filter 容量/Lease → Score binpack → Reserve 预占对后续 Pod 立即生效 → PreBind 写 `reserved-numa` → CR 体现分配后预占自动清除（防双重扣减）
- 部署与真实集群端到端验证留给 Plan 4（operator/webhook 注入 schedulerName + manifests + E2E）
