# kore-agent（Plan 2/4）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现节点侧 kore-agent：NRI 插件（容器启动前下发 cpuset/mems、共享池围栏、Synchronize 对账）、device plugin 准入门闩、KoreNodeTopology CR 上报、Lease 报活，以及可在真机冒烟的 `--inspect` 模式。

**Architecture:** 见 spec §3/§6。agent 是节点上唯一分配权威；本计划全部逻辑通过接口注入（PodGetter/Recorder/Reporter/updater）实现纯单测，不需要集群；与真实 containerd/kubelet 的集成测试在 Plan 4。

**Tech Stack:** github.com/containerd/nri v0.12（已核实 API：`CreateContainer(ctx,pod,ctr) (*api.ContainerAdjustment, []*api.ContainerUpdate, error)`、`adj.SetLinuxCPUSetCPUs/Mems`、`u.SetContainerId`）、k8s.io/kubelet v0.36.2（device plugin v1beta1）、sigs.k8s.io/controller-runtime（CR 客户端 + fake）、client-go fake clientset

## Global Constraints

- 继承 Plan 1 全部约束（module、Go ≥1.24、纯 Go、k8s v0.36.x、可判别错误）
- **fail-closed**：绑核容器任何环节失败必须返回 error 让容器创建失败，绝不静默降级（spec §6 三重防线）
- NRI hook 内不做同步 API 调用（除 pinned pod 的 GetPod，informer 缓存兜底直连）；事件/注解回写/CR 上报全部异步
- 共享池围栏只动 `cpuset.cpus`，不动共享容器的 `cpuset.mems`
- agent 内所有共享状态用单一 mutex 保护（NRI 回调可能并发）
- 新依赖：`go get github.com/containerd/nri@latest k8s.io/kubelet@v0.36.2 k8s.io/client-go@v0.36.2 sigs.k8s.io/controller-runtime@latest sigs.k8s.io/yaml@latest google.golang.org/grpc@latest`（部分已在 go.mod）

---

### Task 1: agent 配置（pkg/agent/config）

**Files:**
- Create: `pkg/agent/config/config.go`
- Test: `pkg/agent/config/config_test.go`

**Interfaces:**
- Produces: `config.Config{ReservedSystemCpus, DefaultPlacement, DefaultSMTPolicy, Remediation string}`、`Load(path string) (*Config, error)`（path=="" 返回默认值）、`(c *Config) Reserved() (cpuset.CPUSet, error)`
- 默认值：Placement=`pack`、SMTPolicy=`full-core`、Remediation=`strict`、Reserved=空

- [ ] **Step 1: 写失败测试**

```go
package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if c.DefaultPlacement != "pack" || c.DefaultSMTPolicy != "full-core" || c.Remediation != "strict" {
		t.Fatalf("defaults: %+v", c)
	}
	r, err := c.Reserved()
	if err != nil || r.Size() != 0 {
		t.Fatalf("reserved: %v %v", r, err)
	}
}

func TestLoadFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "cfg.yaml")
	os.WriteFile(p, []byte("reservedSystemCpus: \"0-1\"\ndefaultPlacement: scatter\nremediation: repair\n"), 0o644)
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.DefaultPlacement != "scatter" || c.Remediation != "repair" || c.DefaultSMTPolicy != "full-core" {
		t.Fatalf("%+v", c)
	}
	if r, _ := c.Reserved(); r.String() != "0-1" {
		t.Fatalf("reserved = %s", r)
	}
}

func TestLoadInvalidEnum(t *testing.T) {
	p := filepath.Join(t.TempDir(), "cfg.yaml")
	os.WriteFile(p, []byte("defaultPlacement: diagonal\n"), 0o644)
	if _, err := Load(p); err == nil {
		t.Fatal("expected error")
	}
}
```

- [ ] **Step 2: 确认失败** — Run: `go test ./pkg/agent/config/...` Expected: FAIL（编译错误）

- [ ] **Step 3: 实现 config.go**

```go
// Package config 是 kore-agent 的节点配置（由 ConfigMap 挂载为文件）。
package config

import (
	"fmt"
	"os"

	"k8s.io/utils/cpuset"
	"sigs.k8s.io/yaml"
)

type Config struct {
	// ReservedSystemCpus 是系统预留核（cpulist 语法），不参与独占分配与共享池。
	ReservedSystemCpus string `json:"reservedSystemCpus"`
	DefaultPlacement   string `json:"defaultPlacement"` // pack | scatter
	DefaultSMTPolicy   string `json:"defaultSMTPolicy"` // full-core | logical
	Remediation        string `json:"remediation"`      // strict | repair（spec §6 对账兜底）
}

func Load(path string) (*Config, error) {
	c := &Config{DefaultPlacement: "pack", DefaultSMTPolicy: "full-core", Remediation: "strict"}
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if err := yaml.Unmarshal(b, c); err != nil {
			return nil, err
		}
	}
	if err := oneOf("defaultPlacement", c.DefaultPlacement, "pack", "scatter"); err != nil {
		return nil, err
	}
	if err := oneOf("defaultSMTPolicy", c.DefaultSMTPolicy, "full-core", "logical"); err != nil {
		return nil, err
	}
	if err := oneOf("remediation", c.Remediation, "strict", "repair"); err != nil {
		return nil, err
	}
	if _, err := c.Reserved(); err != nil {
		return nil, fmt.Errorf("reservedSystemCpus: %w", err)
	}
	return c, nil
}

func (c *Config) Reserved() (cpuset.CPUSet, error) {
	if c.ReservedSystemCpus == "" {
		return cpuset.New(), nil
	}
	return cpuset.Parse(c.ReservedSystemCpus)
}

func oneOf(field, v string, allowed ...string) error {
	for _, a := range allowed {
		if v == a {
			return nil
		}
	}
	return fmt.Errorf("%s: invalid value %q (allowed: %v)", field, v, allowed)
}
```

注意：yaml 默认值语义——Unmarshal 到已填默认值的 struct 上，缺失字段保留默认（sigs.k8s.io/yaml 行为）。

- [ ] **Step 4: 确认通过** — Run: `go test ./pkg/agent/config/... -v` Expected: PASS ×3
- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: agent 节点配置加载与校验"`

---

### Task 2: 策略解析纯函数（pkg/nriplugin/policy.go）

**Files:**
- Create: `pkg/nriplugin/policy.go`
- Test: `pkg/nriplugin/policy_test.go`

**Interfaces:**
- Consumes: `request`、`allocator`、`topology`、`config`（Plan 1 / Task 1）
- Produces:
  - `func MemsFor(p request.MemoryPolicy, numa []int, topo *topology.Topology) string` — strict→仅分配 NUMA；preferred→全部 zone
  - `func BuildAllocRequest(kpod *corev1.Pod, req *request.Request, cfg *config.Config, containerName string) (*allocator.Request, bool)` — 返回 (分配请求, 是否绑核容器)；解析 reserved-numa 注解、用 cfg 补全 placement/smt 空值

- [ ] **Step 1: 写失败测试**

```go
package nriplugin

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/cpuset"

	"github.com/zjusct/kore/pkg/agent/config"
	"github.com/zjusct/kore/pkg/request"
	"github.com/zjusct/kore/pkg/topology"
)

// twoZoneTopo: zone0={0-3} zone1={4-7}，无 SMT。
func twoZoneTopo() *topology.Topology {
	sib := map[int]cpuset.CPUSet{}
	for i := 0; i < 8; i++ {
		sib[i] = cpuset.New(i)
	}
	return &topology.Topology{
		Zones: []topology.Zone{
			{ID: 0, CPUs: cpuset.New(0, 1, 2, 3), MemoryTotalBytes: 1 << 30, Distances: []int{10, 20}},
			{ID: 1, CPUs: cpuset.New(4, 5, 6, 7), MemoryTotalBytes: 1 << 30, Distances: []int{20, 10}},
		},
		Siblings: sib, ThreadsPerCore: 1,
	}
}

func pinnedPod(annos map[string]string) *corev1.Pod {
	a := map[string]string{request.AnnoPin: "true"}
	for k, v := range annos {
		a[k] = v
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default", UID: "uid-p", Annotations: a},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "app",
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
			},
		}}},
	}
}

func TestMemsFor(t *testing.T) {
	topo := twoZoneTopo()
	if got := MemsFor(request.MemStrict, []int{1}, topo); got != "1" {
		t.Fatalf("strict = %q", got)
	}
	if got := MemsFor(request.MemPreferred, []int{1}, topo); got != "0-1" {
		t.Fatalf("preferred = %q", got)
	}
}

func TestBuildAllocRequest(t *testing.T) {
	cfg, _ := config.Load("")
	kpod := pinnedPod(map[string]string{request.AnnoReservedNUMA: "1"})
	req, err := request.ParsePod(kpod)
	if err != nil {
		t.Fatal(err)
	}
	ar, pinned := BuildAllocRequest(kpod, req, cfg, "app")
	if !pinned {
		t.Fatal("app should be pinned")
	}
	if ar.CPUs != 2 || ar.PodUID != "uid-p" || ar.Pod != "default/p" || ar.Container != "app" {
		t.Fatalf("%+v", ar)
	}
	if ar.ReservedNUMA == nil || *ar.ReservedNUMA != 1 {
		t.Fatalf("reservedNUMA = %v", ar.ReservedNUMA)
	}
	// 注解未设 placement/smt → 用 cfg 默认
	if ar.Placement != request.PlacementPack || ar.SMTPolicy != request.SMTFullCore {
		t.Fatalf("effective policies: %+v", ar)
	}
	if _, pinned := BuildAllocRequest(kpod, req, cfg, "sidecar"); pinned {
		t.Fatal("unknown container must not be pinned")
	}
}

func TestBuildAllocRequestAnnotationOverridesConfig(t *testing.T) {
	cfg, _ := config.Load("")
	cfg.DefaultPlacement = "scatter"
	kpod := pinnedPod(map[string]string{request.AnnoPlacement: "pack"})
	req, _ := request.ParsePod(kpod)
	ar, _ := BuildAllocRequest(kpod, req, cfg, "app")
	if ar.Placement != request.PlacementPack {
		t.Fatalf("annotation must override config, got %v", ar.Placement)
	}
}
```

- [ ] **Step 2: 确认失败** — Run: `go test ./pkg/nriplugin/...` Expected: FAIL
- [ ] **Step 3: 实现 policy.go**

```go
// Package nriplugin 实现 kore-agent 的 NRI 插件逻辑。
package nriplugin

import (
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/cpuset"

	"github.com/zjusct/kore/pkg/agent/config"
	"github.com/zjusct/kore/pkg/allocator"
	"github.com/zjusct/kore/pkg/request"
	"github.com/zjusct/kore/pkg/topology"
)

// MemsFor 计算容器的 cpuset.mems：strict 仅本地 NUMA（numactl --membind 等价），
// preferred 全部 zone（first-touch 本地化）。
func MemsFor(p request.MemoryPolicy, numa []int, topo *topology.Topology) string {
	if p == request.MemPreferred {
		ids := make([]int, 0, len(topo.Zones))
		for _, z := range topo.Zones {
			ids = append(ids, z.ID)
		}
		return cpuset.New(ids...).String()
	}
	return cpuset.New(numa...).String()
}

// BuildAllocRequest 把 (Pod, 解析后的注解, 集群默认值, 容器名) 变成分配请求。
// 第二返回值为 false 表示该容器不绑核（sidecar/init → 共享池）。
func BuildAllocRequest(kpod *corev1.Pod, req *request.Request, cfg *config.Config, containerName string) (*allocator.Request, bool) {
	var cr *request.ContainerRequest
	for i := range req.Containers {
		if req.Containers[i].Name == containerName {
			cr = &req.Containers[i]
			break
		}
	}
	if cr == nil {
		return nil, false
	}
	ar := &allocator.Request{
		PodUID:     string(kpod.UID),
		Pod:        fmt.Sprintf("%s/%s", kpod.Namespace, kpod.Name),
		Container:  containerName,
		CPUs:       cr.CPUs,
		NUMAPolicy: req.NUMAPolicy,
		Placement:  req.Placement,
		SMTPolicy:  req.SMTPolicy,
		Explicit:   req.Explicit,
	}
	if ar.Placement == "" {
		ar.Placement = request.Placement(cfg.DefaultPlacement)
	}
	if ar.SMTPolicy == "" {
		ar.SMTPolicy = request.SMTPolicy(cfg.DefaultSMTPolicy)
	}
	if v, ok := kpod.Annotations[request.AnnoReservedNUMA]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			ar.ReservedNUMA = &n
		}
	}
	return ar, true
}
```

- [ ] **Step 4: 确认通过** — Run: `go test ./pkg/nriplugin/... -v` Expected: PASS ×3
- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: NRI 插件策略解析纯函数"`

---

### Task 3: NRI CreateContainer 与共享池围栏（pkg/nriplugin/plugin.go）

**Files:**
- Create: `pkg/nriplugin/plugin.go`
- Test: `pkg/nriplugin/plugin_test.go`

**Interfaces:**
- Produces:
  - `nriplugin.PodGetter interface { GetPod(namespace, name string) (*corev1.Pod, error) }`
  - `nriplugin.Recorder interface { Event(pod *corev1.Pod, eventType, reason, msg string); SetPodAnnotation(pod *corev1.Pod, key, value string); DeletePod(namespace, name string) }`（实现均须异步/尽力而为，接口本身同步签名）
  - `nriplugin.Reporter interface { Report(st v1alpha1.KoreNodeTopologyStatus) }`
  - `nriplugin.New(topo, cfg, pods PodGetter, rec Recorder, rep Reporter) (*Plugin, error)`
  - `(p *Plugin) Configure / CreateContainer / StopPodSandbox / RemovePodSandbox / RemoveContainer / Synchronize`（NRI 接口，Task 4 补后三个）
  - `(p *Plugin) SetUpdater(fn func([]*api.ContainerUpdate) error)` — main 里接 stub.UpdateContainers

- [ ] **Step 1: 写失败测试**

```go
package nriplugin

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/containerd/nri/pkg/api"
	corev1 "k8s.io/api/core/v1"

	"github.com/zjusct/kore/pkg/agent/config"
	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
	"github.com/zjusct/kore/pkg/request"
)

type fakePods struct{ pods map[string]*corev1.Pod }

func (f *fakePods) GetPod(ns, name string) (*corev1.Pod, error) {
	if p, ok := f.pods[ns+"/"+name]; ok {
		return p, nil
	}
	return nil, errors.New("not found")
}

type fakeRec struct {
	events      []string
	annotations map[string]string
	deleted     []string
}

func (f *fakeRec) Event(pod *corev1.Pod, et, reason, msg string) {
	f.events = append(f.events, reason)
}
func (f *fakeRec) SetPodAnnotation(pod *corev1.Pod, k, v string) {
	if f.annotations == nil {
		f.annotations = map[string]string{}
	}
	f.annotations[k] = v
}
func (f *fakeRec) DeletePod(ns, name string) { f.deleted = append(f.deleted, ns+"/"+name) }

type fakeRep struct{ last *v1alpha1.KoreNodeTopologyStatus }

func (f *fakeRep) Report(st v1alpha1.KoreNodeTopologyStatus) { f.last = &st }

func newTestPlugin(t *testing.T, pods ...*corev1.Pod) (*Plugin, *fakeRec, *fakeRep) {
	t.Helper()
	cfg, _ := config.Load("")
	cfg.ReservedSystemCpus = "0"
	fp := &fakePods{pods: map[string]*corev1.Pod{}}
	for _, p := range pods {
		fp.pods[p.Namespace+"/"+p.Name] = p
	}
	rec, rep := &fakeRec{}, &fakeRep{}
	p, err := New(twoZoneTopo(), cfg, fp, rec, rep)
	if err != nil {
		t.Fatal(err)
	}
	return p, rec, rep
}

func sandbox(uid string, annos map[string]string) *api.PodSandbox {
	return &api.PodSandbox{Id: "sb-" + uid, Name: "p", Uid: uid, Namespace: "default", Annotations: annos}
}

func ctr(id, sandboxID, name string) *api.Container {
	return &api.Container{Id: id, PodSandboxId: sandboxID, Name: name, State: api.ContainerState_CONTAINER_RUNNING}
}

func TestCreateContainerFencesNonKorePod(t *testing.T) {
	p, _, _ := newTestPlugin(t)
	adj, updates, err := p.CreateContainer(context.Background(), sandbox("u9", nil), ctr("c9", "sb-u9", "web"))
	if err != nil {
		t.Fatal(err)
	}
	if got := adj.GetLinux().GetCpusetCpus(); got != "1-7" { // 全部核 − 预留 {0}
		t.Fatalf("shared cpus = %q", got)
	}
	if adj.GetLinux().GetCpusetMems() != "" {
		t.Fatal("shared containers must not get mems pinning")
	}
	if len(updates) != 0 {
		t.Fatalf("no prior shared containers to update, got %d", len(updates))
	}
}

func TestCreateContainerPinsAndShrinksShared(t *testing.T) {
	kpod := pinnedPod(map[string]string{request.AnnoReservedNUMA: "1"})
	p, rec, rep := newTestPlugin(t, kpod)
	// 先来一个共享容器
	_, _, err := p.CreateContainer(context.Background(), sandbox("u9", nil), ctr("c9", "sb-u9", "web"))
	if err != nil {
		t.Fatal(err)
	}
	// 绑核容器：2 cpu、NUMA 1
	adj, updates, err := p.CreateContainer(context.Background(),
		sandbox("uid-p", map[string]string{request.AnnoPin: "true"}), ctr("c1", "sb-uid-p", "app"))
	if err != nil {
		t.Fatal(err)
	}
	if got := adj.GetLinux().GetCpusetCpus(); got != "4-5" {
		t.Fatalf("pinned cpus = %q", got)
	}
	if got := adj.GetLinux().GetCpusetMems(); got != "1" {
		t.Fatalf("pinned mems = %q", got)
	}
	if adj.GetAnnotations()[request.AnnoAllocated] != "4-5" {
		t.Fatalf("container annotation missing: %v", adj.GetAnnotations())
	}
	// 共享容器被夹回 1-3,6-7
	if len(updates) != 1 || updates[0].GetContainerId() != "c9" ||
		updates[0].GetLinux().GetCpusetCpus() != "1-3,6-7" {
		t.Fatalf("updates = %+v", updates)
	}
	if rec.annotations[request.AnnoAllocated] != "4-5" {
		t.Fatalf("pod annotation writeback missing: %v", rec.annotations)
	}
	if rep.last == nil || len(rep.last.Allocations) != 1 {
		t.Fatalf("CR report missing: %+v", rep.last)
	}
}

func TestCreateContainerSidecarFenced(t *testing.T) {
	kpod := pinnedPod(nil)
	p, _, _ := newTestPlugin(t, kpod)
	adj, _, err := p.CreateContainer(context.Background(),
		sandbox("uid-p", map[string]string{request.AnnoPin: "true"}), ctr("c2", "sb-uid-p", "sidecar"))
	if err != nil {
		t.Fatal(err)
	}
	if got := adj.GetLinux().GetCpusetCpus(); got != "1-7" {
		t.Fatalf("sidecar must be fenced to shared pool, got %q", got)
	}
}

func TestCreateContainerInsufficientFailsClosed(t *testing.T) {
	kpod := pinnedPod(nil)
	kpod.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = mustQ("5")
	kpod.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU] = mustQ("5")
	p, rec, _ := newTestPlugin(t, kpod)
	_, _, err := p.CreateContainer(context.Background(),
		sandbox("uid-p", map[string]string{request.AnnoPin: "true"}), ctr("c1", "sb-uid-p", "app"))
	if err == nil {
		t.Fatal("must fail closed") // 5 > 单 zone 可分配（zone0 只有 3 个非预留）
	}
	if len(rec.events) == 0 || !strings.Contains(rec.events[0], "AllocationFailed") {
		t.Fatalf("event missing: %v", rec.events)
	}
}

func TestCreateContainerPodSpecUnavailableFailsClosed(t *testing.T) {
	p, _, _ := newTestPlugin(t) // 不注入 Pod
	_, _, err := p.CreateContainer(context.Background(),
		sandbox("uid-p", map[string]string{request.AnnoPin: "true"}), ctr("c1", "sb-uid-p", "app"))
	if err == nil {
		t.Fatal("pinned pod without pod spec must fail closed")
	}
}
```

（`mustQ` 助手：`func mustQ(s string) resource.Quantity { return resource.MustParse(s) }`，加进本测试文件并补 import。）

- [ ] **Step 2: 确认失败** — Run: `go test ./pkg/nriplugin/...` Expected: FAIL（Plugin 未定义）
- [ ] **Step 3: 实现 plugin.go**

```go
package nriplugin

import (
	"context"
	"fmt"
	"sync"

	"github.com/containerd/nri/pkg/api"
	corev1 "k8s.io/api/core/v1"

	"github.com/zjusct/kore/pkg/agent/config"
	"github.com/zjusct/kore/pkg/allocator"
	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
	"github.com/zjusct/kore/pkg/request"
	"github.com/zjusct/kore/pkg/topology"
)

type PodGetter interface {
	GetPod(namespace, name string) (*corev1.Pod, error)
}

// Recorder 的实现必须异步/尽力而为——这些调用发生在 NRI hook 关键路径上。
type Recorder interface {
	Event(pod *corev1.Pod, eventType, reason, msg string)
	SetPodAnnotation(pod *corev1.Pod, key, value string)
	DeletePod(namespace, name string)
}

type Reporter interface {
	Report(st v1alpha1.KoreNodeTopologyStatus)
}

type Plugin struct {
	mu    sync.Mutex
	topo  *topology.Topology
	cfg   *config.Config
	state *allocator.State
	pods  PodGetter
	rec   Recorder
	rep   Reporter

	shared  map[string]bool // 共享池容器 id（围栏对象）
	updater func([]*api.ContainerUpdate) error
}

func New(topo *topology.Topology, cfg *config.Config, pods PodGetter, rec Recorder, rep Reporter) (*Plugin, error) {
	reserved, err := cfg.Reserved()
	if err != nil {
		return nil, err
	}
	return &Plugin{
		topo: topo, cfg: cfg,
		state:  allocator.NewState(topo, reserved),
		pods:   pods, rec: rec, rep: rep,
		shared: map[string]bool{},
	}, nil
}

// SetUpdater 注入 stub.UpdateContainers，用于 hook 之外主动收放共享池。
func (p *Plugin) SetUpdater(fn func([]*api.ContainerUpdate) error) { p.updater = fn }

func (p *Plugin) Configure(ctx context.Context, cfg, runtime, version string) (api.EventMask, error) {
	return api.ParseEventMask("CreateContainer,StopPodSandbox,RemovePodSandbox,RemoveContainer")
}

func (p *Plugin) CreateContainer(ctx context.Context, pod *api.PodSandbox, ctr *api.Container) (*api.ContainerAdjustment, []*api.ContainerUpdate, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if pod.Annotations[request.AnnoPin] != "true" {
		return p.fenceLocked(ctr), nil, nil
	}
	kpod, err := p.pods.GetPod(pod.Namespace, pod.Name)
	if err != nil {
		return nil, nil, fmt.Errorf("kore: pinned pod %s/%s: pod spec unavailable: %w", pod.Namespace, pod.Name, err)
	}
	req, err := request.ParsePod(kpod)
	if err != nil {
		p.rec.Event(kpod, corev1.EventTypeWarning, "KoreInvalidAnnotations", err.Error())
		return nil, nil, fmt.Errorf("kore: %w", err)
	}
	if req == nil { // sandbox 注解说 pin 但 Pod 对象已不是 → 以 Pod 为准
		return p.fenceLocked(ctr), nil, nil
	}
	ar, pinned := BuildAllocRequest(kpod, req, p.cfg, ctr.Name)
	if !pinned {
		return p.fenceLocked(ctr), nil, nil
	}
	a, err := p.state.Allocate(*ar)
	if err != nil {
		p.rec.Event(kpod, corev1.EventTypeWarning, "KoreAllocationFailed", err.Error())
		return nil, nil, fmt.Errorf("kore: %w", err)
	}
	adj := &api.ContainerAdjustment{}
	adj.SetLinuxCPUSetCPUs(a.CPUs.String())
	adj.SetLinuxCPUSetMems(MemsFor(req.MemoryPolicy, a.NUMA, p.topo))
	adj.AddAnnotation(request.AnnoAllocated, a.CPUs.String()) // Synchronize 重建的依据

	p.rec.SetPodAnnotation(kpod, request.AnnoAllocated, a.CPUs.String())
	p.rep.Report(allocator.BuildStatus(p.state))
	return adj, p.shrinkSharedLocked(), nil
}

// fenceLocked 把非绑核容器围栏进共享池。调用方须持锁。
func (p *Plugin) fenceLocked(ctr *api.Container) *api.ContainerAdjustment {
	p.shared[ctr.Id] = true
	adj := &api.ContainerAdjustment{}
	adj.SetLinuxCPUSetCPUs(p.state.SharedPool().String())
	return adj
}

// shrinkSharedLocked 生成把全部共享容器夹到当前共享池的更新。调用方须持锁。
func (p *Plugin) shrinkSharedLocked() []*api.ContainerUpdate {
	pool := p.state.SharedPool().String()
	out := make([]*api.ContainerUpdate, 0, len(p.shared))
	for id := range p.shared {
		u := &api.ContainerUpdate{}
		u.SetContainerId(id)
		u.SetLinuxCPUSetCPUs(pool)
		u.IgnoreFailure = true // 容器可能恰好退出，围栏失败不应连坐
		out = append(out, u)
	}
	return out
}
```

（`u.IgnoreFailure` 为导出字段；若编译失败改用 setter 或直接字段赋值——执行时以 go doc 为准。）

- [ ] **Step 4: 确认通过** — Run: `go test ./pkg/nriplugin/... -v` Expected: PASS（Task 2+3 全部用例）
- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: NRI CreateContainer 绑核与共享池围栏"`

---

### Task 4: 生命周期与 Synchronize 对账（pkg/nriplugin/lifecycle.go）

**Files:**
- Create: `pkg/nriplugin/lifecycle.go`
- Test: `pkg/nriplugin/lifecycle_test.go`

**Interfaces:**
- Produces: `StopPodSandbox`/`RemovePodSandbox`（释放分配 + 经 updater 扩张共享池）、`RemoveContainer`（清共享跟踪）、`Synchronize`（重建状态；"该绑未绑"按 cfg.Remediation：strict→DeletePod+event，repair→补绑 update+event）

- [ ] **Step 1: 写失败测试**

```go
package nriplugin

import (
	"context"
	"testing"

	"github.com/containerd/nri/pkg/api"

	"github.com/zjusct/kore/pkg/request"
)

func TestStopPodReleasesAndGrowsShared(t *testing.T) {
	kpod := pinnedPod(map[string]string{request.AnnoReservedNUMA: "1"})
	p, _, _ := newTestPlugin(t, kpod)
	var pushed []*api.ContainerUpdate
	p.SetUpdater(func(us []*api.ContainerUpdate) error { pushed = us; return nil })

	sb := sandbox("uid-p", map[string]string{request.AnnoPin: "true"})
	if _, _, err := p.CreateContainer(context.Background(), sb, ctr("c1", sb.Id, "app")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.CreateContainer(context.Background(), sandbox("u9", nil), ctr("c9", "sb-u9", "web")); err != nil {
		t.Fatal(err)
	}
	if err := p.StopPodSandbox(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	// 释放后共享池恢复 1-7，c9 被推回
	if len(pushed) != 1 || pushed[0].GetContainerId() != "c9" ||
		pushed[0].GetLinux().GetCpusetCpus() != "1-7" {
		t.Fatalf("pushed = %+v", pushed)
	}
}

func TestSynchronizeRestoresState(t *testing.T) {
	kpod := pinnedPod(map[string]string{request.AnnoReservedNUMA: "1"})
	p, _, _ := newTestPlugin(t, kpod)
	sb := sandbox("uid-p", map[string]string{request.AnnoPin: "true"})
	pinned := ctr("c1", sb.Id, "app")
	pinned.Annotations = map[string]string{request.AnnoAllocated: "4-5"}
	shared := ctr("c9", "sb-u9", "web")

	updates, err := p.Synchronize(context.Background(),
		[]*api.PodSandbox{sb, sandbox("u9", nil)},
		[]*api.Container{pinned, shared})
	if err != nil {
		t.Fatal(err)
	}
	// 状态重建：4-5 已占用 → 共享池 1-3,6-7；c9 被夹回
	if len(updates) != 1 || updates[0].GetContainerId() != "c9" ||
		updates[0].GetLinux().GetCpusetCpus() != "1-3,6-7" {
		t.Fatalf("updates = %+v", updates)
	}
	// 再次分配不得与恢复的 4-5 冲突
	kpod2 := pinnedPod(map[string]string{request.AnnoReservedNUMA: "1"})
	kpod2.Name, kpod2.UID = "p2", "uid-p2"
	p.pods.(*fakePods).pods["default/p2"] = kpod2
	sb2 := sandbox("uid-p2", map[string]string{request.AnnoPin: "true"})
	sb2.Name = "p2"
	adj, _, err := p.CreateContainer(context.Background(), sb2, ctr("c2", sb2.Id, "app"))
	if err != nil {
		t.Fatal(err)
	}
	if got := adj.GetLinux().GetCpusetCpus(); got != "6-7" {
		t.Fatalf("second alloc = %q, want 6-7", got)
	}
}

func TestSynchronizeUnboundStrictDeletesPod(t *testing.T) {
	kpod := pinnedPod(nil)
	p, rec, _ := newTestPlugin(t, kpod) // Remediation 默认 strict
	sb := sandbox("uid-p", map[string]string{request.AnnoPin: "true"})
	unbound := ctr("c1", sb.Id, "app") // 无 AnnoAllocated 注解 = 该绑未绑

	if _, err := p.Synchronize(context.Background(), []*api.PodSandbox{sb}, []*api.Container{unbound}); err != nil {
		t.Fatal(err)
	}
	if len(rec.deleted) != 1 || rec.deleted[0] != "default/p" {
		t.Fatalf("deleted = %v", rec.deleted)
	}
	if len(rec.events) == 0 {
		t.Fatal("expected warning event")
	}
}

func TestSynchronizeUnboundRepairRebinds(t *testing.T) {
	kpod := pinnedPod(nil)
	p, rec, _ := newTestPlugin(t, kpod)
	p.cfg.Remediation = "repair"
	sb := sandbox("uid-p", map[string]string{request.AnnoPin: "true"})
	unbound := ctr("c1", sb.Id, "app")

	updates, err := p.Synchronize(context.Background(), []*api.PodSandbox{sb}, []*api.Container{unbound})
	if err != nil {
		t.Fatal(err)
	}
	var rebind *api.ContainerUpdate
	for _, u := range updates {
		if u.GetContainerId() == "c1" {
			rebind = u
		}
	}
	if rebind == nil || rebind.GetLinux().GetCpusetCpus() == "" {
		t.Fatalf("repair rebind missing: %+v", updates)
	}
	if len(rec.deleted) != 0 {
		t.Fatal("repair must not delete pod")
	}
	if len(rec.events) == 0 {
		t.Fatal("repair must emit warning event")
	}
}
```

- [ ] **Step 2: 确认失败** — Run: `go test ./pkg/nriplugin/...` Expected: FAIL
- [ ] **Step 3: 实现 lifecycle.go**

```go
package nriplugin

import (
	"context"
	"fmt"

	"github.com/containerd/nri/pkg/api"
	corev1 "k8s.io/api/core/v1"

	"github.com/zjusct/kore/pkg/allocator"
	"github.com/zjusct/kore/pkg/request"
)

func (p *Plugin) StopPodSandbox(ctx context.Context, pod *api.PodSandbox) error {
	p.releasePod(pod.Uid)
	return nil
}

func (p *Plugin) RemovePodSandbox(ctx context.Context, pod *api.PodSandbox) error {
	p.releasePod(pod.Uid)
	return nil
}

func (p *Plugin) RemoveContainer(ctx context.Context, pod *api.PodSandbox, ctr *api.Container) error {
	p.mu.Lock()
	delete(p.shared, ctr.Id)
	p.mu.Unlock()
	return nil
}

// releasePod 释放某 Pod 的全部独占分配并把共享池扩张推给运行时。
func (p *Plugin) releasePod(uid string) {
	p.mu.Lock()
	before := p.state.Used().Size()
	p.state.Release(uid)
	changed := p.state.Used().Size() != before
	updates := p.shrinkSharedLocked()
	p.mu.Unlock()
	if !changed {
		return
	}
	if p.updater != nil {
		_ = p.updater(updates) // 尽力而为；失败由下次 Synchronize 对齐
	}
	p.rep.Report(allocator.BuildStatus(p.state))
}

func (p *Plugin) Synchronize(ctx context.Context, pods []*api.PodSandbox, containers []*api.Container) ([]*api.ContainerUpdate, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	sandboxes := map[string]*api.PodSandbox{}
	for _, pod := range pods {
		sandboxes[pod.Id] = pod
	}
	var extra []*api.ContainerUpdate
	for _, c := range containers {
		if c.State == api.ContainerState_CONTAINER_STOPPED {
			continue
		}
		pod := sandboxes[c.PodSandboxId]
		if pod == nil || pod.Annotations[request.AnnoPin] != "true" {
			p.shared[c.Id] = true
			continue
		}
		if cs := c.Annotations[request.AnnoAllocated]; cs != "" {
			if err := p.restore(pod, c, cs); err != nil {
				return nil, err
			}
			continue
		}
		// 绑核 Pod 的容器但无分配注解：可能是 sidecar，也可能是"该绑未绑"
		if u := p.remediateLocked(pod, c); u != nil {
			extra = append(extra, u)
		}
	}
	p.rep.Report(allocator.BuildStatus(p.state))
	return append(p.shrinkSharedLocked(), extra...), nil
}

func (p *Plugin) restore(pod *api.PodSandbox, c *api.Container, cs string) error {
	cpus, err := cpusetParse(cs)
	if err != nil {
		return fmt.Errorf("kore: container %s bad %s annotation %q: %w", c.Id, request.AnnoAllocated, cs, err)
	}
	numa := map[int]bool{}
	for _, cpu := range cpus.List() {
		numa[p.topo.ZoneOf(cpu)] = true
	}
	ids := make([]int, 0, len(numa))
	for z := range numa {
		ids = append(ids, z)
	}
	return p.state.Restore(allocator.Allocation{
		PodUID: pod.Uid, Pod: pod.Namespace + "/" + pod.Name, Container: c.Name,
		CPUs: cpus, NUMA: sortedInts(ids),
	})
}

// remediateLocked 处理"该绑未绑"的容器（spec §6 兜底对账）。返回 repair 模式的补绑更新。
func (p *Plugin) remediateLocked(pod *api.PodSandbox, c *api.Container) *api.ContainerUpdate {
	kpod, err := p.pods.GetPod(pod.Namespace, pod.Name)
	if err != nil {
		return nil // Pod 已不在 API server：容器即将被回收，不处理
	}
	req, err := request.ParsePod(kpod)
	if err != nil || req == nil {
		return nil
	}
	ar, pinned := BuildAllocRequest(kpod, req, p.cfg, c.Name)
	if !pinned { // sidecar → 共享池
		p.shared[c.Id] = true
		return nil
	}
	if p.cfg.Remediation == "repair" {
		a, aerr := p.state.Allocate(*ar)
		if aerr != nil {
			p.rec.Event(kpod, corev1.EventTypeWarning, "KoreUnboundContainer",
				fmt.Sprintf("container %s should be pinned but is not; repair failed: %v", c.Name, aerr))
			return nil
		}
		p.rec.Event(kpod, corev1.EventTypeWarning, "KoreRepairedBinding",
			fmt.Sprintf("container %s was running unpinned; re-bound to %s (memory may be non-local, consider restarting the pod)", c.Name, a.CPUs))
		p.rec.SetPodAnnotation(kpod, request.AnnoAllocated, a.CPUs.String())
		u := &api.ContainerUpdate{}
		u.SetContainerId(c.Id)
		u.SetLinuxCPUSetCPUs(a.CPUs.String())
		u.SetLinuxCPUSetMems(MemsFor(req.MemoryPolicy, a.NUMA, p.topo))
		return u
	}
	// strict：杀掉重建，绝不允许无绑定运行
	p.rec.Event(kpod, corev1.EventTypeWarning, "KoreUnboundContainer",
		fmt.Sprintf("container %s was running unpinned (agent was down at creation); deleting pod for rebind", c.Name))
	p.rec.DeletePod(pod.Namespace, pod.Name)
	return nil
}
```

辅助（同文件底部）：

```go
func cpusetParse(s string) (cs cpuset.CPUSet, err error) { return cpuset.Parse(s) }

func sortedInts(v []int) []int { sort.Ints(v); return v }
```

（import 需补 `sort` 与 `k8s.io/utils/cpuset`。）

- [ ] **Step 4: 确认通过** — Run: `go test ./pkg/nriplugin/... -v` Expected: PASS（含 Task 2/3 回归）
- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: NRI 生命周期释放与 Synchronize 对账（strict/repair 兜底）"`

---

### Task 5: CR 上报与 Lease 报活（pkg/agent/reporter、pkg/agent/lease）

**Files:**
- Create: `pkg/agent/reporter/reporter.go`、`pkg/agent/lease/lease.go`
- Test: `pkg/agent/reporter/reporter_test.go`、`pkg/agent/lease/lease_test.go`

**Interfaces:**
- Produces:
  - `reporter.New(c client.Client, node string) *Reporter`；`(r *Reporter) Report(ctx, st v1alpha1.KoreNodeTopologyStatus) error` — upsert CR（Get→无则 Create→Status().Update）
  - `lease.NewRenewer(cs kubernetes.Interface, node, namespace string, durationSeconds int32) *Renewer`；`(r *Renewer) RenewOnce(ctx) error`；`(r *Renewer) Run(ctx, interval time.Duration)`；Lease 名 `kore-agent-<node>`；`r.now` 字段可注入时钟

- [ ] **Step 1: 写失败测试（reporter）**

```go
package reporter

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
)

func TestReportCreatesThenUpdates(t *testing.T) {
	sch := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(sch).
		WithStatusSubresource(&v1alpha1.KoreNodeTopology{}).Build()
	r := New(c, "m602")

	st := v1alpha1.KoreNodeTopologyStatus{ReservedSystemCpus: "0-1"}
	if err := r.Report(context.Background(), st); err != nil {
		t.Fatal(err)
	}
	var got v1alpha1.KoreNodeTopology
	if err := c.Get(context.Background(), types.NamespacedName{Name: "m602"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.ReservedSystemCpus != "0-1" {
		t.Fatalf("status = %+v", got.Status)
	}

	st.ReservedSystemCpus = "0-3"
	if err := r.Report(context.Background(), st); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "m602"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.ReservedSystemCpus != "0-3" {
		t.Fatalf("update lost: %+v", got.Status)
	}
}
```

- [ ] **Step 2: 写失败测试（lease）**

```go
package lease

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestRenewOnceCreatesThenRenews(t *testing.T) {
	cs := fake.NewClientset()
	r := NewRenewer(cs, "m602", "kore-system", 15)
	t0 := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return t0 }

	if err := r.RenewOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	l, err := cs.CoordinationV1().Leases("kore-system").Get(context.Background(), "kore-agent-m602", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if *l.Spec.HolderIdentity != "m602" || *l.Spec.LeaseDurationSeconds != 15 {
		t.Fatalf("%+v", l.Spec)
	}
	if !l.Spec.RenewTime.Time.Equal(t0) {
		t.Fatalf("renewTime = %v", l.Spec.RenewTime)
	}

	r.now = func() time.Time { return t0.Add(5 * time.Second) }
	if err := r.RenewOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	l, _ = cs.CoordinationV1().Leases("kore-system").Get(context.Background(), "kore-agent-m602", metav1.GetOptions{})
	if !l.Spec.RenewTime.Time.Equal(t0.Add(5 * time.Second)) {
		t.Fatalf("renew not applied: %v", l.Spec.RenewTime)
	}
}
```

- [ ] **Step 3: 确认失败** — Run: `go test ./pkg/agent/...` Expected: FAIL ×2
- [ ] **Step 4: 实现 reporter.go**

```go
// Package reporter 把 agent 的分配状态写入 KoreNodeTopology CR。
package reporter

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
)

type Reporter struct {
	c    client.Client
	node string
}

func New(c client.Client, node string) *Reporter { return &Reporter{c: c, node: node} }

func (r *Reporter) Report(ctx context.Context, st v1alpha1.KoreNodeTopologyStatus) error {
	var cr v1alpha1.KoreNodeTopology
	err := r.c.Get(ctx, types.NamespacedName{Name: r.node}, &cr)
	if apierrors.IsNotFound(err) {
		cr = v1alpha1.KoreNodeTopology{ObjectMeta: metav1.ObjectMeta{Name: r.node}}
		if err := r.c.Create(ctx, &cr); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	cr.Status = st
	return r.c.Status().Update(ctx, &cr)
}
```

- [ ] **Step 5: 实现 lease.go**

```go
// Package lease 维护 agent 报活 Lease（kore-scheduler Filter 与 operator 污点控制消费）。
package lease

import (
	"context"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Renewer struct {
	cs        kubernetes.Interface
	node, ns  string
	duration  int32
	now       func() time.Time
}

func NewRenewer(cs kubernetes.Interface, node, namespace string, durationSeconds int32) *Renewer {
	return &Renewer{cs: cs, node: node, ns: namespace, duration: durationSeconds, now: time.Now}
}

func (r *Renewer) name() string { return "kore-agent-" + r.node }

func (r *Renewer) RenewOnce(ctx context.Context) error {
	leases := r.cs.CoordinationV1().Leases(r.ns)
	renew := metav1.NewMicroTime(r.now())
	l, err := leases.Get(ctx, r.name(), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = leases.Create(ctx, &coordv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: r.name(), Namespace: r.ns},
			Spec: coordv1.LeaseSpec{
				HolderIdentity:       &r.node,
				LeaseDurationSeconds: &r.duration,
				RenewTime:            &renew,
			},
		}, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	l.Spec.RenewTime = &renew
	_, err = leases.Update(ctx, l, metav1.UpdateOptions{})
	return err
}

func (r *Renewer) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = r.RenewOnce(ctx) // 失败下轮重试；持续失败 → Lease 过期 → 调度侧自动排除
		}
	}
}
```

- [ ] **Step 6: 确认通过** — Run: `go test ./pkg/agent/... -v` Expected: PASS（config+reporter+lease）
- [ ] **Step 7: Commit** — `git add -A && git commit -m "feat: KoreNodeTopology CR 上报与 Lease 报活"`

---

### Task 6: device plugin 准入门闩（pkg/deviceplugin）

**Files:**
- Create: `pkg/deviceplugin/server.go`
- Test: `pkg/deviceplugin/server_test.go`

**Interfaces:**
- Produces: `deviceplugin.New(count int, pluginDir string) *Server`；`(s *Server) Start() error`（unix socket `kore.sock` 上服务）；`(s *Server) Register(kubeletSocket string) error`；`(s *Server) Stop()`；资源名 `request.ExtendedResource`；设备为不透明 token `kore-token-<n>`（spec §3：kubelet 选哪个无意义，仅计数）

- [ ] **Step 1: 写失败测试**

```go
package deviceplugin

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"

	"github.com/zjusct/kore/pkg/request"
)

func dial(t *testing.T, socket string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("unix://"+socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestListAndWatchAndAllocate(t *testing.T) {
	dir := t.TempDir()
	s := New(6, dir)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	conn := dial(t, filepath.Join(dir, "kore.sock"))
	defer conn.Close()
	c := pluginapi.NewDevicePluginClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := c.ListAndWatch(ctx, &pluginapi.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Devices) != 6 || resp.Devices[0].Health != pluginapi.Healthy {
		t.Fatalf("devices = %+v", resp.Devices)
	}

	ar, err := c.Allocate(ctx, &pluginapi.AllocateRequest{
		ContainerRequests: []*pluginapi.ContainerAllocateRequest{{DevicesIDs: []string{"kore-token-0", "kore-token-1"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ar.ContainerResponses) != 1 {
		t.Fatalf("responses = %+v", ar)
	}
}

// fakeRegistration 记录 kubelet 注册请求。
type fakeRegistration struct {
	pluginapi.UnimplementedRegistrationServer
	got chan *pluginapi.RegisterRequest
}

func (f *fakeRegistration) Register(ctx context.Context, r *pluginapi.RegisterRequest) (*pluginapi.Empty, error) {
	f.got <- r
	return &pluginapi.Empty{}, nil
}

func TestRegister(t *testing.T) {
	dir := t.TempDir()
	kubeletSock := filepath.Join(dir, "kubelet.sock")
	lis, err := net.Listen("unix", kubeletSock)
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	fr := &fakeRegistration{got: make(chan *pluginapi.RegisterRequest, 1)}
	pluginapi.RegisterRegistrationServer(gs, fr)
	go gs.Serve(lis)
	defer gs.Stop()

	s := New(4, dir)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	if err := s.Register(kubeletSock); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-fr.got:
		if r.ResourceName != request.ExtendedResource || r.Endpoint != "kore.sock" || r.Version != pluginapi.Version {
			t.Fatalf("register = %+v", r)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("kubelet did not receive registration")
	}
}
```

- [ ] **Step 2: 确认失败** — Run: `go test ./pkg/deviceplugin/...` Expected: FAIL
- [ ] **Step 3: 实现 server.go**

```go
// Package deviceplugin 实现 kubelet 准入门闩（spec §6 三重防线第 2 层）：
// agent 死 → 端点消失 → kubelet 拒绝启动请求 kore.zjusct.io/cpu 的 Pod。
// 设备是不透明计数 token，真正选核由 NRI 路径完成。
package deviceplugin

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"

	"github.com/zjusct/kore/pkg/request"
)

const socketName = "kore.sock"

type Server struct {
	pluginapi.UnimplementedDevicePluginServer
	count  int
	dir    string
	grpc   *grpc.Server
	stopCh chan struct{}
}

func New(count int, pluginDir string) *Server {
	return &Server{count: count, dir: pluginDir, stopCh: make(chan struct{})}
}

func (s *Server) SocketPath() string { return filepath.Join(s.dir, socketName) }

func (s *Server) Start() error {
	lis, err := net.Listen("unix", s.SocketPath())
	if err != nil {
		return err
	}
	s.grpc = grpc.NewServer()
	pluginapi.RegisterDevicePluginServer(s.grpc, s)
	go func() { _ = s.grpc.Serve(lis) }()
	return nil
}

func (s *Server) Stop() {
	close(s.stopCh)
	if s.grpc != nil {
		s.grpc.Stop()
	}
}

func (s *Server) Register(kubeletSocket string) error {
	conn, err := grpc.NewClient("unix://"+kubeletSocket,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = pluginapi.NewRegistrationClient(conn).Register(ctx, &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     socketName,
		ResourceName: request.ExtendedResource,
	})
	return err
}

func (s *Server) GetDevicePluginOptions(context.Context, *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{}, nil
}

func (s *Server) ListAndWatch(_ *pluginapi.Empty, stream pluginapi.DevicePlugin_ListAndWatchServer) error {
	devs := make([]*pluginapi.Device, s.count)
	for i := range devs {
		devs[i] = &pluginapi.Device{ID: fmt.Sprintf("kore-token-%d", i), Health: pluginapi.Healthy}
	}
	if err := stream.Send(&pluginapi.ListAndWatchResponse{Devices: devs}); err != nil {
		return err
	}
	select { // 设备集是静态的；阻塞直到停止
	case <-s.stopCh:
	case <-stream.Context().Done():
	}
	return nil
}

func (s *Server) Allocate(ctx context.Context, req *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	out := &pluginapi.AllocateResponse{}
	for range req.ContainerRequests {
		out.ContainerResponses = append(out.ContainerResponses, &pluginapi.ContainerAllocateResponse{})
	}
	return out, nil
}

func (s *Server) GetPreferredAllocation(context.Context, *pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error) {
	return &pluginapi.PreferredAllocationResponse{}, nil
}

func (s *Server) PreStartContainer(context.Context, *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	return &pluginapi.PreStartContainerResponse{}, nil
}
```

（若 pluginapi 无 `UnimplementedDevicePluginServer`/`UnimplementedRegistrationServer`，删掉内嵌行、直接实现全部方法——执行时以 go doc 为准。）

- [ ] **Step 4: 确认通过** — Run: `go test ./pkg/deviceplugin/... -v` Expected: PASS ×2
- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: device plugin 计数门闩（kubelet 准入防线）"`

---

### Task 7: cmd/kore-agent 接线与 --inspect 冒烟

**Files:**
- Create: `pkg/agent/inspect.go`、`cmd/kore-agent/main.go`
- Test: `pkg/agent/inspect_test.go`

**Interfaces:**
- Produces: `agent.Inspect(sysfsRoot, reservedCpus string) (string, error)`（拓扑发现→空状态 BuildStatus→缩进 JSON）；`kore-agent` 二进制：`--inspect [--sysfs --reserved]` 本地冒烟；正常模式 `--node-name/$NODE_NAME --config --namespace --kubelet-dir` 接线 NRI stub + device plugin + informer + reporter + lease

- [ ] **Step 1: 写失败测试（inspect）**

```go
package agent

import (
	"encoding/json"
	"testing"

	"github.com/zjusct/kore/pkg/topology/topotest"
	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
)

func TestInspect(t *testing.T) {
	root := topotest.Write(t, []topotest.Zone{
		{ID: 0, CPUList: "0-3", MemTotalKB: 1024, Distances: "10 20"},
		{ID: 1, CPUList: "4-7", MemTotalKB: 1024, Distances: "20 10"},
	}, map[int]string{0: "0", 1: "1", 2: "2", 3: "3", 4: "4", 5: "5", 6: "6", 7: "7"})

	out, err := Inspect(root, "0-1")
	if err != nil {
		t.Fatal(err)
	}
	var st v1alpha1.KoreNodeTopologyStatus
	if err := json.Unmarshal([]byte(out), &st); err != nil {
		t.Fatal(err)
	}
	if len(st.Zones) != 2 || st.ReservedSystemCpus != "0-1" || st.Zones[0].FreeCpus != "2-3" {
		t.Fatalf("%s", out)
	}
}
```

- [ ] **Step 2: 确认失败** — Run: `go test ./pkg/agent/...` Expected: FAIL
- [ ] **Step 3: 实现 inspect.go**

```go
// Package agent 是 kore-agent 的顶层接线。
package agent

import (
	"encoding/json"

	"k8s.io/utils/cpuset"

	"github.com/zjusct/kore/pkg/allocator"
	"github.com/zjusct/kore/pkg/topology"
)

// Inspect 发现本机拓扑并输出空状态的 KoreNodeTopology status JSON（真机冒烟用）。
func Inspect(sysfsRoot, reservedCpus string) (string, error) {
	topo, err := topology.Discover(sysfsRoot)
	if err != nil {
		return "", err
	}
	reserved := cpuset.New()
	if reservedCpus != "" {
		if reserved, err = cpuset.Parse(reservedCpus); err != nil {
			return "", err
		}
	}
	st := allocator.BuildStatus(allocator.NewState(topo, reserved))
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}
```

- [ ] **Step 4: 实现 main.go**

```go
// kore-agent：节点侧绑核执行者（NRI 插件 + device plugin 门闩 + CR 上报 + Lease）。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/containerd/nri/pkg/stub"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/zjusct/kore/pkg/agent"
	"github.com/zjusct/kore/pkg/agent/config"
	"github.com/zjusct/kore/pkg/agent/lease"
	"github.com/zjusct/kore/pkg/agent/reporter"
	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
	"github.com/zjusct/kore/pkg/allocator"
	"github.com/zjusct/kore/pkg/deviceplugin"
	"github.com/zjusct/kore/pkg/nriplugin"
	"github.com/zjusct/kore/pkg/topology"
)

func main() {
	var (
		inspect    = flag.Bool("inspect", false, "发现本机拓扑并打印 status JSON 后退出")
		sysfs      = flag.String("sysfs", "/sys", "sysfs 根路径")
		reserved   = flag.String("reserved", "", "--inspect 模式的系统预留核")
		nodeName   = flag.String("node-name", os.Getenv("NODE_NAME"), "节点名")
		cfgPath    = flag.String("config", "", "agent 配置文件路径")
		namespace  = flag.String("namespace", "kore-system", "Lease 命名空间")
		kubeletDir = flag.String("kubelet-dir", "/var/lib/kubelet/device-plugins", "kubelet device plugin 目录")
	)
	flag.Parse()

	if *inspect {
		out, err := agent.Inspect(*sysfs, *reserved)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(out)
		return
	}
	if err := run(*sysfs, *nodeName, *cfgPath, *namespace, *kubeletDir); err != nil {
		log.Fatal(err)
	}
}

func run(sysfs, nodeName, cfgPath, namespace, kubeletDir string) error {
	if nodeName == "" {
		return fmt.Errorf("--node-name or $NODE_NAME required")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}
	topo, err := topology.Discover(sysfs)
	if err != nil {
		return err
	}

	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	cs, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return err
	}
	sch := runtime.NewScheme()
	if err := scheme.AddToScheme(sch); err != nil {
		return err
	}
	if err := v1alpha1.AddToScheme(sch); err != nil {
		return err
	}
	crc, err := ctrlclient.New(restCfg, ctrlclient.Options{Scheme: sch})
	if err != nil {
		return err
	}

	// 本节点 Pod informer：NRI hook 里查 Pod spec 用（缓存优先，miss 直连）
	factory := informers.NewSharedInformerFactoryWithOptions(cs, 30*time.Second,
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", nodeName).String()
		}))
	podLister := factory.Core().V1().Pods().Lister()
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: cs.CoreV1().Events("")})
	rec := broadcaster.NewRecorder(sch, corev1.EventSource{Component: "kore-agent", Host: nodeName})

	adapters := &k8sAdapters{cs: cs, rec: rec, lister: podLister, ctx: ctx}
	rep := &asyncReporter{r: reporter.New(crc, nodeName), ctx: ctx}

	plugin, err := nriplugin.New(topo, cfg, adapters, adapters, rep)
	if err != nil {
		return err
	}
	st, err := stub.New(plugin, stub.WithPluginName("kore"), stub.WithPluginIdx("10"))
	if err != nil {
		return err
	}
	plugin.SetUpdater(func(us []*api.ContainerUpdate) error {
		_, err := st.UpdateContainers(us)
		return err
	})

	reservedSet, _ := cfg.Reserved()
	dp := deviceplugin.New(topo.AllCPUs().Difference(reservedSet).Size(), kubeletDir)
	if err := dp.Start(); err != nil {
		return err
	}
	defer dp.Stop()
	if err := dp.Register(kubeletDir + "/kubelet.sock"); err != nil {
		return err
	}

	renewer := lease.NewRenewer(cs, nodeName, namespace, 15)
	if err := renewer.RenewOnce(ctx); err != nil {
		return err
	}
	go renewer.Run(ctx, 5*time.Second)

	rep.Report(allocator.BuildStatus(allocator.NewState(topo, reservedSet)))
	log.Printf("kore-agent up on %s: %d zones, %d cpus", nodeName, len(topo.Zones), topo.AllCPUs().Size())
	return st.Run(ctx) // 阻塞直到 NRI 连接结束/ctx 取消
}
```

同文件的适配器（PodGetter/Recorder 接 k8s、异步化）：

```go
type k8sAdapters struct {
	cs     kubernetes.Interface
	rec    record.EventRecorder
	lister listerscorev1.PodLister
	ctx    context.Context
}

func (a *k8sAdapters) GetPod(ns, name string) (*corev1.Pod, error) {
	if p, err := a.lister.Pods(ns).Get(name); err == nil {
		return p, nil
	}
	ctx, cancel := context.WithTimeout(a.ctx, 2*time.Second)
	defer cancel()
	return a.cs.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
}

func (a *k8sAdapters) Event(pod *corev1.Pod, et, reason, msg string) {
	a.rec.Event(pod, et, reason, msg)
}

func (a *k8sAdapters) SetPodAnnotation(pod *corev1.Pod, key, value string) {
	go func() {
		ctx, cancel := context.WithTimeout(a.ctx, 5*time.Second)
		defer cancel()
		patch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, key, value))
		_, _ = a.cs.CoreV1().Pods(pod.Namespace).Patch(ctx, pod.Name, types.MergePatchType, patch, metav1.PatchOptions{})
	}()
}

func (a *k8sAdapters) DeletePod(ns, name string) {
	go func() {
		ctx, cancel := context.WithTimeout(a.ctx, 10*time.Second)
		defer cancel()
		_ = a.cs.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{})
	}()
}

type asyncReporter struct {
	r   *reporter.Reporter
	ctx context.Context
}

func (ar *asyncReporter) Report(st v1alpha1.KoreNodeTopologyStatus) {
	go func() {
		ctx, cancel := context.WithTimeout(ar.ctx, 10*time.Second)
		defer cancel()
		_ = ar.r.Report(ctx, st)
	}()
}
```

（补 import：`k8s.io/apimachinery/pkg/types`、`listerscorev1 "k8s.io/client-go/listers/core/v1"`。）

- [ ] **Step 5: 全量验证与交叉编译**

Run: `make test && make build && go vet ./... && GOOS=linux GOARCH=amd64 go build ./... && GOOS=linux GOARCH=arm64 go build ./...`
Expected: 全绿。

Run（真机冒烟示例，可选，在任一 Linux 节点上）: `./kore-agent --inspect --reserved 0-1`
Expected: 打印真实 NUMA 拓扑的 status JSON。

- [ ] **Step 6: Commit** — `git add -A && git commit -m "feat: kore-agent 主程序接线与 --inspect 冒烟"`

---

## Plan 2 完成标准

- `make test` 全绿，两架构交叉编译干净
- `kore-agent --inspect` 在 macOS 上用 fixture 测试通过（真机验证留待部署）
- NRI 全链路（CreateContainer 绑核/围栏、Stop/Remove 释放、Synchronize 重建+strict/repair 兜底）单测覆盖
- Plan 3 的消费接口就绪：Lease 命名 `kore-agent-<node>`（namespace kore-system）、CR 由 agent upsert

## 已知留到 Plan 4 的事项

- kind/真机集成测试（真实 containerd NRI socket、真实 kubelet device plugin 注册）
- DaemonSet/RBAC/ConfigMap manifests 与镜像构建
- webhook 注入扩展资源（operator 侧）——在此之前 device plugin 门闩虽在运行但 Pod 不请求该资源，不阻断（防线 1/3 层已可用）
