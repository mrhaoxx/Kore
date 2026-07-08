# Kore 基础层（Plan 1/4）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 搭建 Kore 仓库基础层：CRD 类型、NUMA/SMT 拓扑发现、Pod 注解解析、CPU 分配器（pack/scatter、single/preferred/spread、SMT 整核）——全部纯逻辑、全部单测覆盖，不需要集群。

**Architecture:** 见 `docs/superpowers/specs/2026-07-08-kore-numa-design.md`。本计划是 4 个计划中的第 1 个（后续：Plan 2 kore-agent、Plan 3 kore-scheduler、Plan 4 kore-operator+部署+E2E）。本计划产出的包是后续三个二进制共享的核心库。

**Tech Stack:** Go ≥1.24、k8s.io/apimachinery+api v0.36.x、k8s.io/utils/cpuset、controller-gen（CRD/deepcopy 生成）

## Global Constraints

- module 路径：`github.com/zjusct/kore`
- Go ≥ 1.24，纯 Go 禁 cgo（需交叉编译 linux/amd64 + linux/arm64）
- k8s.io/api、k8s.io/apimachinery 用 v0.36.x（对齐集群 k8s 1.36）；若精确 patch 版不存在用最近的 v0.36.x
- 注解域 `kore.zjusct.io`；CRD 组 `kore.zjusct.io`，版本 `v1alpha1`
- 注解生效默认值（spec §4）：numa-policy=`single`、memory-policy=`strict`、smt-policy=`full-core`、placement=`pack`。其中 placement/smt-policy 属于 ConfigMap 可调的集群默认（spec §6）：ParsePod 在用户未设置时**留空 `""`**，由 agent 结合 ConfigMap 解析；allocator 对 `""` 的行为等价 pack/full-core（安全默认）
- 可判别错误：`ErrInsufficient`/`ErrConflict`/`ErrSMTAlignment` 必须支持 `errors.Is`
- TDD：每个功能先写失败测试再实现；每个 Task 结束提交一次
- CPU 集合一律用 `k8s.io/utils/cpuset.CPUSet`（kubelet 同款），不自造轮子

---

### Task 1: 仓库脚手架

**Files:**
- Create: `go.mod`、`.gitignore`、`Makefile`、`README.md`

**Interfaces:**
- Produces: module `github.com/zjusct/kore`；`make build`/`make test`/`make generate`/`make manifests` 目标

- [ ] **Step 1: 初始化 module 与依赖**

```bash
cd /Users/star/Kore
go mod init github.com/zjusct/kore
go get k8s.io/apimachinery@v0.36.2 k8s.io/api@v0.36.2 k8s.io/utils@latest
```

Expected: `go: added k8s.io/apimachinery v0.36.x` 等（patch 版本以实际可用为准）。

- [ ] **Step 2: 写 .gitignore**

```gitignore
bin/
*.out
*.test
```

- [ ] **Step 3: 写 Makefile**

```make
GOBIN := $(shell pwd)/bin
CONTROLLER_GEN := $(GOBIN)/controller-gen

.PHONY: build test fmt vet generate manifests

build:
	go build ./...

test:
	go test ./... -count=1

fmt:
	gofmt -l -w .

vet:
	go vet ./...

$(CONTROLLER_GEN):
	GOBIN=$(GOBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@v0.19.0

generate: $(CONTROLLER_GEN)
	$(CONTROLLER_GEN) object paths=./pkg/apis/...

manifests: $(CONTROLLER_GEN)
	$(CONTROLLER_GEN) crd paths=./pkg/apis/... output:crd:dir=deploy/crd
```

（若 controller-gen v0.19.0 不存在，改用最新 release 并把版本号写死在 Makefile。）

- [ ] **Step 4: 写 README.md**

```markdown
# Kore

Kubernetes 绑核与 NUMA 绑定系统（NRI 插件 + NUMA 感知调度器插件 + operator）。

设计文档：`docs/superpowers/specs/2026-07-08-kore-numa-design.md`

## 组件
- `kore-agent`：节点 DaemonSet——NRI 插件（容器启动前下发 cpuset）、device plugin 准入门闩、拓扑上报
- `kore-scheduler`：NUMA 感知调度器插件
- `kore-operator`：webhook 校验/注入 + agent 健康污点控制

## 开发
make build / make test / make generate / make manifests
```

- [ ] **Step 5: 验证构建**

Run: `go build ./... && go vet ./...`
Expected: 无输出，退出码 0。

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "chore: 仓库脚手架（go module、Makefile）"
```

---

### Task 2: KoreNodeTopology CRD 类型

**Files:**
- Create: `pkg/apis/kore/v1alpha1/types.go`
- Create: `pkg/apis/kore/v1alpha1/groupversion_info.go`
- Create: `pkg/apis/kore/v1alpha1/zz_generated.deepcopy.go`（controller-gen 生成）
- Create: `deploy/crd/kore.zjusct.io_korenodetopologies.yaml`（controller-gen 生成）
- Test: `pkg/apis/kore/v1alpha1/types_test.go`

**Interfaces:**
- Produces: `v1alpha1.KoreNodeTopology`、`v1alpha1.KoreNodeTopologyStatus{ReservedSystemCpus string; Zones []Zone; Allocations []Allocation}`、`v1alpha1.Zone{ID int; Cpus string; Allocatable int; FreeCpus string; MemoryTotal resource.Quantity; SMTSiblings [][]int; Devices []Device}`、`v1alpha1.Allocation{PodUID, Pod, Container, Cpuset string; NUMA []int}`、`AddToScheme`

- [ ] **Step 1: 写失败测试**

```go
package v1alpha1

import (
	"encoding/json"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestSchemeRegistration(t *testing.T) {
	s := runtime.NewScheme()
	if err := AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	gvks, _, err := s.ObjectKinds(&KoreNodeTopology{})
	if err != nil {
		t.Fatal(err)
	}
	if gvks[0].Group != "kore.zjusct.io" || gvks[0].Version != "v1alpha1" || gvks[0].Kind != "KoreNodeTopology" {
		t.Fatalf("unexpected GVK: %v", gvks[0])
	}
}

func TestStatusRoundtrip(t *testing.T) {
	in := KoreNodeTopologyStatus{
		ReservedSystemCpus: "0-1",
		Zones: []Zone{{
			ID: 0, Cpus: "0-15,32-47", Allocatable: 28, FreeCpus: "4-15,36-47",
			MemoryTotal: resource.MustParse("256Gi"),
			SMTSiblings: [][]int{{2, 34}, {3, 35}},
			Devices:     []Device{},
		}},
		Allocations: []Allocation{{PodUID: "u1", Pod: "default/hpc-0", Container: "app", Cpuset: "8-15", NUMA: []int{0}}},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out KoreNodeTopologyStatus
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Zones[0].FreeCpus != "4-15,36-47" || out.Allocations[0].Cpuset != "8-15" {
		t.Fatalf("roundtrip mismatch: %+v", out)
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./pkg/apis/... -run 'TestScheme|TestStatus' -v`
Expected: FAIL（编译错误：类型未定义）。

- [ ] **Step 3: 写 types.go**

```go
// Package v1alpha1 contains the Kore API types.
// +kubebuilder:object:generate=true
// +groupName=kore.zjusct.io
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// KoreNodeTopology 由 kore-agent 维护（与节点同名），kore-scheduler 消费。
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=knt
// +kubebuilder:subresource:status
type KoreNodeTopology struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Status KoreNodeTopologyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type KoreNodeTopologyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KoreNodeTopology `json:"items"`
}

type KoreNodeTopologyStatus struct {
	// ReservedSystemCpus 是系统预留核（cpulist 语法，如 "0-1"）。
	ReservedSystemCpus string `json:"reservedSystemCpus,omitempty"`
	Zones              []Zone `json:"zones,omitempty"`
	Allocations        []Allocation `json:"allocations,omitempty"`
}

type Zone struct {
	ID          int    `json:"id"`
	Cpus        string `json:"cpus"`
	Allocatable int    `json:"allocatable"`
	FreeCpus    string `json:"freeCpus"`
	MemoryTotal resource.Quantity `json:"memoryTotal,omitempty"`
	// SMTSiblings 是本 zone 内的 sibling 组（无 SMT 则空）。
	SMTSiblings [][]int  `json:"smtSiblings,omitempty"`
	// Devices 为 v2 预留（NPU/网卡 NUMA 归属）。
	Devices     []Device `json:"devices,omitempty"`
}

type Device struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Allocation struct {
	PodUID    string `json:"podUID"`
	// Pod 格式为 namespace/name。
	Pod       string `json:"pod"`
	Container string `json:"container"`
	Cpuset    string `json:"cpuset"`
	NUMA      []int  `json:"numa"`
}
```

- [ ] **Step 4: 写 groupversion_info.go**

```go
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	GroupVersion  = schema.GroupVersion{Group: "kore.zjusct.io", Version: "v1alpha1"}
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion, &KoreNodeTopology{}, &KoreNodeTopologyList{})
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}
```

- [ ] **Step 5: 生成 deepcopy 与 CRD**

Run: `make generate manifests`
Expected: 生成 `pkg/apis/kore/v1alpha1/zz_generated.deepcopy.go` 与 `deploy/crd/kore.zjusct.io_korenodetopologies.yaml`；后者含 `scope: Cluster`。

- [ ] **Step 6: 运行测试确认通过**

Run: `go test ./pkg/apis/... -v`
Expected: PASS ×2。

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat: KoreNodeTopology CRD 类型与 manifest 生成"
```

---

### Task 3: 拓扑发现（pkg/topology）

**Files:**
- Create: `pkg/topology/topology.go`
- Create: `pkg/topology/topotest/topotest.go`（fake sysfs 测试助手）
- Test: `pkg/topology/topology_test.go`

**Interfaces:**
- Produces:
  - `topology.Topology{Zones []Zone; Siblings map[int]cpuset.CPUSet; ThreadsPerCore int}`
  - `topology.Zone{ID int; CPUs cpuset.CPUSet; MemoryTotalBytes int64; Distances []int}`
  - `func Discover(sysfsRoot string) (*Topology, error)`
  - `func (t *Topology) SMTEnabled() bool` / `ZoneOf(cpu int) int` / `AllCPUs() cpuset.CPUSet`
  - `topotest.Write(t, zones []topotest.Zone, siblings map[int]string) string`（返回 fake sysfs 根目录）

- [ ] **Step 1: 写 topotest 助手**

```go
// Package topotest 在 t.TempDir() 下构造 fake sysfs 树供拓扑发现测试。
package topotest

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

type Zone struct {
	ID         int
	CPUList    string // 如 "0-3,8-11"
	MemTotalKB int64
	Distances  string // 如 "10 21"
}

// Write 构造 fake sysfs；siblings 是 cpu id -> thread_siblings_list 内容（如 0 -> "0,8"）。
func Write(t *testing.T, zones []Zone, siblings map[int]string) string {
	t.Helper()
	root := t.TempDir()
	for _, z := range zones {
		dir := filepath.Join(root, "devices/system/node", fmt.Sprintf("node%d", z.ID))
		mustMkdir(t, dir)
		mustWrite(t, filepath.Join(dir, "cpulist"), z.CPUList+"\n")
		mustWrite(t, filepath.Join(dir, "meminfo"),
			fmt.Sprintf("Node %d MemTotal:       %d kB\n", z.ID, z.MemTotalKB))
		mustWrite(t, filepath.Join(dir, "distance"), z.Distances+"\n")
	}
	for cpu, sib := range siblings {
		dir := filepath.Join(root, "devices/system/cpu", fmt.Sprintf("cpu%d", cpu), "topology")
		mustMkdir(t, dir)
		mustWrite(t, filepath.Join(dir, "thread_siblings_list"), sib+"\n")
	}
	return root
}

func mustMkdir(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
```

- [ ] **Step 2: 写失败测试**

```go
package topology

import (
	"fmt"
	"testing"

	"github.com/zjusct/kore/pkg/topology/topotest"
)

func x86SMTSysfs(t *testing.T) string {
	// 2 NUMA、每 zone 8 逻辑核、2-way SMT：sibling 对 (i, i+8)
	zones := []topotest.Zone{
		{ID: 0, CPUList: "0-3,8-11", MemTotalKB: 32 * 1024 * 1024, Distances: "10 21"},
		{ID: 1, CPUList: "4-7,12-15", MemTotalKB: 32 * 1024 * 1024, Distances: "21 10"},
	}
	sib := map[int]string{}
	for i := 0; i < 8; i++ {
		s := fmt.Sprintf("%d,%d", i, i+8)
		sib[i], sib[i+8] = s, s
	}
	return topotest.Write(t, zones, sib)
}

func armSysfs(t *testing.T) string {
	zones := []topotest.Zone{
		{ID: 0, CPUList: "0-3", MemTotalKB: 16 * 1024 * 1024, Distances: "10 12 20 22"},
		{ID: 1, CPUList: "4-7", MemTotalKB: 16 * 1024 * 1024, Distances: "12 10 22 20"},
		{ID: 2, CPUList: "8-11", MemTotalKB: 16 * 1024 * 1024, Distances: "20 22 10 12"},
		{ID: 3, CPUList: "12-15", MemTotalKB: 16 * 1024 * 1024, Distances: "22 20 12 10"},
	}
	sib := map[int]string{}
	for i := 0; i < 16; i++ {
		sib[i] = fmt.Sprintf("%d", i)
	}
	return topotest.Write(t, zones, sib)
}

func TestDiscoverX86SMT(t *testing.T) {
	topo, err := Discover(x86SMTSysfs(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(topo.Zones) != 2 {
		t.Fatalf("zones = %d, want 2", len(topo.Zones))
	}
	if !topo.SMTEnabled() || topo.ThreadsPerCore != 2 {
		t.Fatalf("SMT: enabled=%v threads=%d", topo.SMTEnabled(), topo.ThreadsPerCore)
	}
	if got := topo.Zones[0].MemoryTotalBytes; got != 32*1024*1024*1024 {
		t.Fatalf("zone0 mem = %d", got)
	}
	if got := topo.Zones[1].Distances; len(got) != 2 || got[0] != 21 || got[1] != 10 {
		t.Fatalf("zone1 distances = %v", got)
	}
	if topo.ZoneOf(9) != 0 || topo.ZoneOf(13) != 1 {
		t.Fatalf("ZoneOf wrong: cpu9->%d cpu13->%d", topo.ZoneOf(9), topo.ZoneOf(13))
	}
	if sib := topo.Siblings[2].List(); len(sib) != 2 || sib[0] != 2 || sib[1] != 10 {
		t.Fatalf("siblings of 2 = %v", sib)
	}
	if topo.AllCPUs().Size() != 16 {
		t.Fatalf("AllCPUs = %v", topo.AllCPUs())
	}
}

func TestDiscoverARMNoSMT(t *testing.T) {
	topo, err := Discover(armSysfs(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(topo.Zones) != 4 || topo.SMTEnabled() {
		t.Fatalf("zones=%d smt=%v", len(topo.Zones), topo.SMTEnabled())
	}
}

func TestDiscoverEmptyRootFails(t *testing.T) {
	if _, err := Discover(t.TempDir()); err == nil {
		t.Fatal("expected error on empty sysfs")
	}
}
```

（`fmt_Sprintf` 为示意——实现时用 `fmt.Sprintf` 并加 import。）

- [ ] **Step 3: 运行测试确认失败**

Run: `go test ./pkg/topology/... -v`
Expected: FAIL（`Discover` 未定义）。

- [ ] **Step 4: 实现 topology.go**

```go
// Package topology 从 sysfs 发现 NUMA/SMT 拓扑。
package topology

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"k8s.io/utils/cpuset"
)

type Zone struct {
	ID               int
	CPUs             cpuset.CPUSet
	MemoryTotalBytes int64
	// Distances[i] 是到 zone i 的 NUMA 距离（sysfs distance 行）。
	Distances []int
}

type Topology struct {
	Zones []Zone // 按 ID 升序
	// Siblings[cpu] 是该逻辑核所在物理核的全部逻辑核（含自身）。
	Siblings       map[int]cpuset.CPUSet
	ThreadsPerCore int
}

func (t *Topology) SMTEnabled() bool { return t.ThreadsPerCore > 1 }

func (t *Topology) ZoneOf(cpu int) int {
	for _, z := range t.Zones {
		if z.CPUs.Contains(cpu) {
			return z.ID
		}
	}
	return -1
}

func (t *Topology) AllCPUs() cpuset.CPUSet {
	all := cpuset.New()
	for _, z := range t.Zones {
		all = all.Union(z.CPUs)
	}
	return all
}

func Discover(sysfsRoot string) (*Topology, error) {
	nodeDirs, err := filepath.Glob(filepath.Join(sysfsRoot, "devices/system/node/node[0-9]*"))
	if err != nil {
		return nil, err
	}
	if len(nodeDirs) == 0 {
		return nil, fmt.Errorf("no NUMA nodes under %s", sysfsRoot)
	}
	sort.Slice(nodeDirs, func(i, j int) bool { return dirNodeID(nodeDirs[i]) < dirNodeID(nodeDirs[j]) })

	topo := &Topology{Siblings: map[int]cpuset.CPUSet{}, ThreadsPerCore: 1}
	for _, dir := range nodeDirs {
		id := dirNodeID(dir)
		cpus, err := readCPUList(filepath.Join(dir, "cpulist"))
		if err != nil {
			return nil, fmt.Errorf("node%d cpulist: %w", id, err)
		}
		mem, err := readMemTotalBytes(filepath.Join(dir, "meminfo"))
		if err != nil {
			return nil, fmt.Errorf("node%d meminfo: %w", id, err)
		}
		dist, err := readInts(filepath.Join(dir, "distance"))
		if err != nil {
			return nil, fmt.Errorf("node%d distance: %w", id, err)
		}
		topo.Zones = append(topo.Zones, Zone{ID: id, CPUs: cpus, MemoryTotalBytes: mem, Distances: dist})

		for _, cpu := range cpus.List() {
			p := filepath.Join(sysfsRoot, fmt.Sprintf("devices/system/cpu/cpu%d/topology/thread_siblings_list", cpu))
			sib, err := readCPUList(p)
			if err != nil {
				return nil, fmt.Errorf("cpu%d siblings: %w", cpu, err)
			}
			topo.Siblings[cpu] = sib
			if sib.Size() > topo.ThreadsPerCore {
				topo.ThreadsPerCore = sib.Size()
			}
		}
	}
	return topo, nil
}

func dirNodeID(dir string) int {
	n, _ := strconv.Atoi(strings.TrimPrefix(filepath.Base(dir), "node"))
	return n
}

func readCPUList(path string) (cpuset.CPUSet, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return cpuset.New(), err
	}
	return cpuset.Parse(strings.TrimSpace(string(b)))
}

// readMemTotalBytes 解析 "Node 0 MemTotal:  32768 kB" 行。
func readMemTotalBytes(path string) (int64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.Contains(line, "MemTotal:") {
			continue
		}
		f := strings.Fields(line)
		if len(f) < 4 {
			return 0, fmt.Errorf("malformed meminfo line: %q", line)
		}
		kb, err := strconv.ParseInt(f[3], 10, 64)
		if err != nil {
			return 0, err
		}
		return kb * 1024, nil
	}
	return 0, fmt.Errorf("MemTotal not found in %s", path)
}

func readInts(path string) ([]int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []int
	for _, f := range strings.Fields(string(b)) {
		n, err := strconv.Atoi(f)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}
```

- [ ] **Step 5: 运行测试确认通过**

Run: `go test ./pkg/topology/... -v`
Expected: PASS ×3。

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: sysfs NUMA/SMT 拓扑发现与 topotest 助手"
```

---

### Task 4: 注解解析与校验（pkg/request）

**Files:**
- Create: `pkg/request/request.go`
- Test: `pkg/request/request_test.go`

**Interfaces:**
- Produces:
  - 注解常量：`AnnoPin`/`AnnoNUMAPolicy`/`AnnoMemoryPolicy`/`AnnoPlacement`/`AnnoSMTPolicy`/`AnnoCPUSet`/`AnnoReservedNUMA`/`AnnoAllocated`/`ExtendedResource`
  - 枚举：`NUMAPolicy`(`NUMASingle|NUMAPreferred|NUMASpread`)、`MemoryPolicy`(`MemStrict|MemPreferred`)、`Placement`(`PlacementPack|PlacementScatter`)、`SMTPolicy`(`SMTFullCore|SMTLogical`)
  - `request.Request{NUMAPolicy; MemoryPolicy; Placement; SMTPolicy; Explicit *cpuset.CPUSet; Containers []ContainerRequest}`、`ContainerRequest{Name string; CPUs int}`
  - `func ParsePod(pod *corev1.Pod) (*Request, error)` — 未启用 pin 返回 `(nil, nil)`；非法配置返回 error

- [ ] **Step 1: 写失败测试（表驱动）**

```go
package request

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func pod(annos map[string]string, mutate func(*corev1.Pod)) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default", Annotations: annos},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "app",
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")},
			},
		}}},
	}
	if mutate != nil {
		mutate(p)
	}
	return p
}

func TestParsePod(t *testing.T) {
	cases := []struct {
		name    string
		pod     *corev1.Pod
		wantNil bool
		wantErr string // 空串=期望成功；否则 error 需含该子串
		check   func(t *testing.T, r *Request)
	}{
		{name: "无注解返回 nil", pod: pod(nil, nil), wantNil: true},
		{name: "pin=false 返回 nil", pod: pod(map[string]string{AnnoPin: "false"}, nil), wantNil: true},
		{name: "pin 非法值报错", pod: pod(map[string]string{AnnoPin: "yes"}, nil), wantErr: "pin"},
		{
			name: "默认值", pod: pod(map[string]string{AnnoPin: "true"}, nil),
			check: func(t *testing.T, r *Request) {
				// placement/smt-policy 未设置时留空，由 agent 结合 ConfigMap 集群默认值解析
				if r.NUMAPolicy != NUMASingle || r.MemoryPolicy != MemStrict ||
					r.SMTPolicy != "" || r.Placement != "" {
					t.Fatalf("defaults wrong: %+v", r)
				}
				if len(r.Containers) != 1 || r.Containers[0].CPUs != 8 {
					t.Fatalf("containers: %+v", r.Containers)
				}
			},
		},
		{
			name: "显式策略", pod: pod(map[string]string{
				AnnoPin: "true", AnnoNUMAPolicy: "spread", AnnoMemoryPolicy: "preferred",
				AnnoPlacement: "scatter", AnnoSMTPolicy: "logical"}, nil),
			check: func(t *testing.T, r *Request) {
				if r.NUMAPolicy != NUMASpread || r.MemoryPolicy != MemPreferred ||
					r.Placement != PlacementScatter || r.SMTPolicy != SMTLogical {
					t.Fatalf("%+v", r)
				}
			},
		},
		{name: "非法 numa-policy", pod: pod(map[string]string{AnnoPin: "true", AnnoNUMAPolicy: "both"}, nil), wantErr: "numa-policy"},
		{
			name: "非整数 CPU 容器落共享池且报错(无可绑容器)",
			pod: pod(map[string]string{AnnoPin: "true"}, func(p *corev1.Pod) {
				p.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("500m")
				p.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU] = resource.MustParse("500m")
			}),
			wantErr: "no container",
		},
		{
			name: "整数但 requests!=limits 报错",
			pod: pod(map[string]string{AnnoPin: "true"}, func(p *corev1.Pod) {
				p.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU] = resource.MustParse("9")
			}),
			wantErr: "requests must equal limits",
		},
		{
			name: "sidecar 非整数被跳过",
			pod: pod(map[string]string{AnnoPin: "true"}, func(p *corev1.Pod) {
				p.Spec.Containers = append(p.Spec.Containers, corev1.Container{
					Name: "sidecar",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
						Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
					},
				})
			}),
			check: func(t *testing.T, r *Request) {
				if len(r.Containers) != 1 || r.Containers[0].Name != "app" {
					t.Fatalf("containers: %+v", r.Containers)
				}
			},
		},
		{
			name: "显式 cpuset 需指定节点",
			pod:  pod(map[string]string{AnnoPin: "true", AnnoCPUSet: "0-7"}, nil),
			wantErr: "nodeName or nodeSelector",
		},
		{
			name: "显式 cpuset OK",
			pod: pod(map[string]string{AnnoPin: "true", AnnoCPUSet: "0-7"}, func(p *corev1.Pod) {
				p.Spec.NodeName = "m602"
			}),
			check: func(t *testing.T, r *Request) {
				if r.Explicit == nil || r.Explicit.Size() != 8 {
					t.Fatalf("explicit: %v", r.Explicit)
				}
			},
		},
		{
			name: "显式 cpuset 与 numa-policy 互斥",
			pod: pod(map[string]string{AnnoPin: "true", AnnoCPUSet: "0-7", AnnoNUMAPolicy: "spread"}, func(p *corev1.Pod) {
				p.Spec.NodeName = "m602"
			}),
			wantErr: "mutually exclusive",
		},
		{
			name: "显式 cpuset 大小必须等于 CPU 数",
			pod: pod(map[string]string{AnnoPin: "true", AnnoCPUSet: "0-3"}, func(p *corev1.Pod) {
				p.Spec.NodeName = "m602"
			}),
			wantErr: "size",
		},
		{
			name: "显式 cpuset 多容器不允许",
			pod: pod(map[string]string{AnnoPin: "true", AnnoCPUSet: "0-7"}, func(p *corev1.Pod) {
				p.Spec.NodeName = "m602"
				p.Spec.Containers = append(p.Spec.Containers, corev1.Container{
					Name: "app2",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
						Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
					},
				})
			}),
			wantErr: "exactly one",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := ParsePod(tc.pod)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want contains %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantNil {
				if r != nil {
					t.Fatalf("want nil, got %+v", r)
				}
				return
			}
			tc.check(t, r)
		})
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./pkg/request/... -v`
Expected: FAIL（编译错误）。

- [ ] **Step 3: 实现 request.go**

```go
// Package request 解析并校验 Pod 上的 Kore 注解。
package request

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/cpuset"
)

const (
	AnnoPin          = "kore.zjusct.io/pin"
	AnnoNUMAPolicy   = "kore.zjusct.io/numa-policy"
	AnnoMemoryPolicy = "kore.zjusct.io/memory-policy"
	AnnoPlacement    = "kore.zjusct.io/placement"
	AnnoSMTPolicy    = "kore.zjusct.io/smt-policy"
	AnnoCPUSet       = "kore.zjusct.io/cpuset"
	// AnnoReservedNUMA 由 kore-scheduler PreBind 写入。
	AnnoReservedNUMA = "kore.zjusct.io/reserved-numa"
	// AnnoAllocated 由 kore-agent 分配后写入（只读，供观测）。
	AnnoAllocated = "kore.zjusct.io/allocated-cpuset"
	// ExtendedResource 由 webhook 自动注入，作为 kubelet 准入门闩。
	ExtendedResource = "kore.zjusct.io/cpu"
)

type NUMAPolicy string
type MemoryPolicy string
type Placement string
type SMTPolicy string

const (
	NUMASingle    NUMAPolicy   = "single"
	NUMAPreferred NUMAPolicy   = "preferred"
	NUMASpread    NUMAPolicy   = "spread"
	MemStrict     MemoryPolicy = "strict"
	MemPreferred  MemoryPolicy = "preferred"
	PlacementPack    Placement = "pack"
	PlacementScatter Placement = "scatter"
	SMTFullCore   SMTPolicy    = "full-core"
	SMTLogical    SMTPolicy    = "logical"
)

type ContainerRequest struct {
	Name string
	CPUs int
}

type Request struct {
	NUMAPolicy   NUMAPolicy
	MemoryPolicy MemoryPolicy
	Placement    Placement
	SMTPolicy    SMTPolicy
	// Explicit 非 nil 表示用户显式指定核号（逃生舱）。
	Explicit   *cpuset.CPUSet
	Containers []ContainerRequest
}

// ParsePod 解析 Kore 注解。未启用 pin 返回 (nil, nil)；配置非法返回 error。
func ParsePod(pod *corev1.Pod) (*Request, error) {
	switch pod.Annotations[AnnoPin] {
	case "", "false":
		return nil, nil
	case "true":
	default:
		return nil, fmt.Errorf("%s must be \"true\" or \"false\", got %q", AnnoPin, pod.Annotations[AnnoPin])
	}

	// placement/smt-policy 未设置时留空（""）：集群默认值来自 ConfigMap（spec §6），
	// 由 agent 在分配时解析；allocator 对 "" 的行为等价 pack/full-core。
	r := &Request{
		NUMAPolicy:   NUMASingle,
		MemoryPolicy: MemStrict,
	}
	var err error
	if r.NUMAPolicy, err = parseEnum(pod, AnnoNUMAPolicy, r.NUMAPolicy, NUMASingle, NUMAPreferred, NUMASpread); err != nil {
		return nil, err
	}
	if r.MemoryPolicy, err = parseEnum(pod, AnnoMemoryPolicy, r.MemoryPolicy, MemStrict, MemPreferred); err != nil {
		return nil, err
	}
	if r.Placement, err = parseEnum(pod, AnnoPlacement, "", PlacementPack, PlacementScatter); err != nil {
		return nil, err
	}
	if r.SMTPolicy, err = parseEnum(pod, AnnoSMTPolicy, "", SMTFullCore, SMTLogical); err != nil {
		return nil, err
	}

	for _, c := range pod.Spec.Containers {
		req := c.Resources.Requests[corev1.ResourceCPU]
		lim := c.Resources.Limits[corev1.ResourceCPU]
		if req.IsZero() || req.MilliValue()%1000 != 0 {
			continue // 非整数 CPU → 共享池
		}
		if req.Cmp(lim) != 0 {
			return nil, fmt.Errorf("container %s: pinned container cpu requests must equal limits", c.Name)
		}
		r.Containers = append(r.Containers, ContainerRequest{Name: c.Name, CPUs: int(req.Value())})
	}
	if len(r.Containers) == 0 {
		return nil, fmt.Errorf("%s enabled but no container has integer cpu requests", AnnoPin)
	}

	if v, ok := pod.Annotations[AnnoCPUSet]; ok {
		cs, err := cpuset.Parse(v)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", AnnoCPUSet, err)
		}
		if pod.Spec.NodeName == "" && len(pod.Spec.NodeSelector) == 0 {
			return nil, fmt.Errorf("%s requires nodeName or nodeSelector", AnnoCPUSet)
		}
		if _, set := pod.Annotations[AnnoNUMAPolicy]; set {
			return nil, fmt.Errorf("%s and %s are mutually exclusive", AnnoCPUSet, AnnoNUMAPolicy)
		}
		if _, set := pod.Annotations[AnnoPlacement]; set {
			return nil, fmt.Errorf("%s and %s are mutually exclusive", AnnoCPUSet, AnnoPlacement)
		}
		if len(r.Containers) != 1 {
			return nil, fmt.Errorf("%s requires exactly one pinned container, got %d", AnnoCPUSet, len(r.Containers))
		}
		if cs.Size() != r.Containers[0].CPUs {
			return nil, fmt.Errorf("%s size %d != cpu request %d", AnnoCPUSet, cs.Size(), r.Containers[0].CPUs)
		}
		r.Explicit = &cs
	}
	return r, nil
}

func parseEnum[T ~string](pod *corev1.Pod, anno string, def T, allowed ...T) (T, error) {
	v, ok := pod.Annotations[anno]
	if !ok {
		return def, nil
	}
	for _, a := range allowed {
		if T(v) == a {
			return a, nil
		}
	}
	return def, fmt.Errorf("%s: invalid value %q (allowed: %v)", anno, v, allowed)
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `go test ./pkg/request/... -v`
Expected: PASS，全部子用例通过。

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: Pod 注解解析与校验（pkg/request）"
```

---

### Task 5: 分配器核心（pkg/allocator：State、pack、single、explicit）

**Files:**
- Create: `pkg/allocator/allocator.go`（State 与 Allocate 入口）
- Create: `pkg/allocator/strategy.go`（Strategy 接口 + pack）
- Test: `pkg/allocator/allocator_test.go`

**Interfaces:**
- Consumes: `topology.Topology`（Task 3）、`request` 枚举（Task 4）
- Produces:
  - `allocator.State`：`NewState(topo *topology.Topology, reserved cpuset.CPUSet) *State`、`(s *State) Allocate(req Request) (Allocation, error)`、`Release(podUID string)`、`Restore(a Allocation) error`、`Used() cpuset.CPUSet`、`FreeInZone(zone int) cpuset.CPUSet`、`SharedPool() cpuset.CPUSet`、`Allocations() []Allocation`
  - `allocator.Request{PodUID, Pod, Container string; CPUs int; NUMAPolicy request.NUMAPolicy; Placement request.Placement; SMTPolicy request.SMTPolicy; ReservedNUMA *int; Explicit *cpuset.CPUSet}`
  - `allocator.Allocation{PodUID, Pod, Container string; CPUs cpuset.CPUSet; NUMA []int}`
  - `ErrInsufficient`、`ErrConflict`、`ErrSMTAlignment`
  - `strategy.go`：`Unit{Min int; CPUs cpuset.CPUSet}`、`Strategy interface{ Name() string; Pick(units []Unit, need int) ([]Unit, bool) }`、`StrategyFor(p request.Placement) Strategy`

- [ ] **Step 1: 写失败测试（本 Task 覆盖 pack/single/explicit/release/restore/shared-pool/reserved）**

```go
package allocator

import (
	"errors"
	"testing"

	"k8s.io/utils/cpuset"

	"github.com/zjusct/kore/pkg/request"
	"github.com/zjusct/kore/pkg/topology"
)

// armTopo: 4 zone × 4 cpu，无 SMT。zone0 距离行 [10 12 20 22]（近邻顺序 1,2,3）。
func armTopo() *topology.Topology {
	var zones []topology.Zone
	dist := [][]int{{10, 12, 20, 22}, {12, 10, 22, 20}, {20, 22, 10, 12}, {22, 20, 12, 10}}
	sib := map[int]cpuset.CPUSet{}
	for z := 0; z < 4; z++ {
		cpus := cpuset.New(z*4, z*4+1, z*4+2, z*4+3)
		zones = append(zones, topology.Zone{ID: z, CPUs: cpus, MemoryTotalBytes: 1 << 34, Distances: dist[z]})
		for _, c := range cpus.List() {
			sib[c] = cpuset.New(c)
		}
	}
	return &topology.Topology{Zones: zones, Siblings: sib, ThreadsPerCore: 1}
}

func alloc(t *testing.T, s *State, name string, cpus int, mut func(*Request)) Allocation {
	t.Helper()
	r := Request{PodUID: "uid-" + name, Pod: "default/" + name, Container: "app",
		CPUs: cpus, NUMAPolicy: request.NUMASingle, Placement: request.PlacementPack, SMTPolicy: request.SMTFullCore}
	if mut != nil {
		mut(&r)
	}
	a, err := s.Allocate(r)
	if err != nil {
		t.Fatalf("alloc %s: %v", name, err)
	}
	return a
}

func TestPackSingleOnEmpty(t *testing.T) {
	s := NewState(armTopo(), cpuset.New())
	a := alloc(t, s, "a", 2, nil)
	if a.CPUs.String() != "0-1" || len(a.NUMA) != 1 || a.NUMA[0] != 0 {
		t.Fatalf("got %s numa %v", a.CPUs, a.NUMA)
	}
}

func TestBinpackPrefersTightestZone(t *testing.T) {
	s := NewState(armTopo(), cpuset.New())
	alloc(t, s, "a", 2, nil) // zone0 剩 2
	b := alloc(t, s, "b", 2, nil)
	if b.NUMA[0] != 0 { // 恰好填满 zone0
		t.Fatalf("b on numa %v, want 0", b.NUMA)
	}
	c := alloc(t, s, "c", 3, nil)
	if c.NUMA[0] != 1 || c.CPUs.String() != "4-6" {
		t.Fatalf("c: %s numa %v", c.CPUs, c.NUMA)
	}
}

func TestPackBestFitRun(t *testing.T) {
	s := NewState(armTopo(), cpuset.New())
	alloc(t, s, "a", 1, nil) // {0}
	alloc(t, s, "b", 1, nil) // {1}
	s.Release("uid-a")       // zone0 free {0,2,3}
	c := alloc(t, s, "c", 2, func(r *Request) { n := 0; r.ReservedNUMA = &n })
	if c.CPUs.String() != "2-3" { // best-fit 连续段 {2,3}，不是 {0,2}
		t.Fatalf("c = %s, want 2-3", c.CPUs)
	}
}

func TestReservedNUMARespectedAndStrict(t *testing.T) {
	s := NewState(armTopo(), cpuset.New())
	n := 2
	a := alloc(t, s, "a", 2, func(r *Request) { r.ReservedNUMA = &n })
	if a.NUMA[0] != 2 || a.CPUs.String() != "8-9" {
		t.Fatalf("%s %v", a.CPUs, a.NUMA)
	}
	// reservedNUMA 指定后不 fallback：zone2 只剩 2，要 3 必须失败
	_, err := s.Allocate(Request{PodUID: "u2", Pod: "d/p2", Container: "app", CPUs: 3,
		NUMAPolicy: request.NUMASingle, ReservedNUMA: &n})
	if !errors.Is(err, ErrInsufficient) {
		t.Fatalf("err = %v, want ErrInsufficient", err)
	}
}

func TestSingleInsufficient(t *testing.T) {
	s := NewState(armTopo(), cpuset.New())
	_, err := s.Allocate(Request{PodUID: "u", Pod: "d/p", Container: "app", CPUs: 5, NUMAPolicy: request.NUMASingle})
	if !errors.Is(err, ErrInsufficient) {
		t.Fatalf("err = %v", err)
	}
}

func TestExplicit(t *testing.T) {
	s := NewState(armTopo(), cpuset.New())
	cs := cpuset.New(4, 5)
	a := alloc(t, s, "a", 2, func(r *Request) { r.Explicit = &cs })
	if a.CPUs.String() != "4-5" || a.NUMA[0] != 1 {
		t.Fatalf("%s %v", a.CPUs, a.NUMA)
	}
	cs2 := cpuset.New(5, 6)
	_, err := s.Allocate(Request{PodUID: "u2", Pod: "d/p2", Container: "app", CPUs: 2, Explicit: &cs2})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
}

func TestReservedSystemCpusExcluded(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(0))
	a := alloc(t, s, "a", 4, nil) // zone0 只剩 3 → 落 zone1
	if a.NUMA[0] != 1 {
		t.Fatalf("numa %v, want 1", a.NUMA)
	}
	if s.SharedPool().Contains(0) {
		t.Fatal("shared pool must exclude reserved system cpus")
	}
}

func TestSharedPool(t *testing.T) {
	s := NewState(armTopo(), cpuset.New(0))
	alloc(t, s, "a", 2, func(r *Request) { n := 1; r.ReservedNUMA = &n }) // {4,5}
	got := s.SharedPool().String()
	if got != "1-3,6-15" {
		t.Fatalf("shared = %s", got)
	}
}

func TestRestoreAndConflict(t *testing.T) {
	s := NewState(armTopo(), cpuset.New())
	a := Allocation{PodUID: "u1", Pod: "d/p1", Container: "app", CPUs: cpuset.New(0, 1), NUMA: []int{0}}
	if err := s.Restore(a); err != nil {
		t.Fatal(err)
	}
	b := Allocation{PodUID: "u2", Pod: "d/p2", Container: "app", CPUs: cpuset.New(1, 2), NUMA: []int{0}}
	if err := s.Restore(b); !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
}

func TestDoubleAllocateSameContainer(t *testing.T) {
	s := NewState(armTopo(), cpuset.New())
	alloc(t, s, "a", 1, nil)
	_, err := s.Allocate(Request{PodUID: "uid-a", Pod: "default/a", Container: "app", CPUs: 1, NUMAPolicy: request.NUMASingle})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v", err)
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./pkg/allocator/... -v`
Expected: FAIL（编译错误）。

- [ ] **Step 3: 实现 strategy.go（接口 + pack）**

```go
package allocator

import (
	"sort"

	"k8s.io/utils/cpuset"

	"github.com/zjusct/kore/pkg/request"
)

// Unit 是分配的最小单位：logical 模式下一个逻辑核；full-core 模式下一个完整物理核（sibling 组）。
type Unit struct {
	Min  int // 组内最小 cpu id，用于排序与连续性判断
	CPUs cpuset.CPUSet
}

// Strategy 决定在一个 NUMA zone 的空闲 unit 中挑选哪些。spec §6：v1 提供 pack/scatter。
type Strategy interface {
	Name() string
	// Pick 从升序排列的 units 中选 need 个；不可行时 ok=false。
	Pick(units []Unit, need int) ([]Unit, bool)
}

func StrategyFor(p request.Placement) Strategy {
	if p == request.PlacementScatter {
		return scatterStrategy{}
	}
	return packStrategy{}
}

type packStrategy struct{}

func (packStrategy) Name() string { return "pack" }

// Pick：连续段 best-fit——选能容纳 need 的最短连续段；无整段可容纳时，
// 从最大的段开始整段吞并直到凑够（最少碎片数）。
func (packStrategy) Pick(units []Unit, need int) ([]Unit, bool) {
	if len(units) < need {
		return nil, false
	}
	runs := runsOf(units)
	best := -1
	for i, r := range runs {
		if len(r) >= need && (best == -1 || len(r) < len(runs[best])) {
			best = i
		}
	}
	if best >= 0 {
		return runs[best][:need], true
	}
	sort.Slice(runs, func(i, j int) bool { return len(runs[i]) > len(runs[j]) })
	var out []Unit
	for _, r := range runs {
		take := min(need-len(out), len(r))
		out = append(out, r[:take]...)
		if len(out) == need {
			return out, true
		}
	}
	return nil, false
}

type scatterStrategy struct{}

func (scatterStrategy) Name() string { return "scatter" }

// Pick：等间隔取样（floor(i*len/need) 严格递增，因 len>=need）。
func (scatterStrategy) Pick(units []Unit, need int) ([]Unit, bool) {
	if len(units) < need {
		return nil, false
	}
	out := make([]Unit, 0, need)
	for i := 0; i < need; i++ {
		out = append(out, units[i*len(units)/need])
	}
	return out, true
}

// runsOf 把升序 units 按 Min 连续性切段。
func runsOf(units []Unit) [][]Unit {
	var runs [][]Unit
	for i, u := range units {
		if i == 0 || u.Min != units[i-1].Min+1 {
			runs = append(runs, []Unit{u})
			continue
		}
		runs[len(runs)-1] = append(runs[len(runs)-1], u)
	}
	return runs
}

func unitsUnion(units []Unit) cpuset.CPUSet {
	out := cpuset.New()
	for _, u := range units {
		out = out.Union(u.CPUs)
	}
	return out
}
```

（scatter 在本 Task 一并落地——两策略共享全部脚手架，分开写反而要重复测试基建；scatter 的行为测试在 Task 6。）

- [ ] **Step 4: 实现 allocator.go**

```go
// Package allocator 维护单节点的独占 CPU 分配状态并执行选核。
package allocator

import (
	"errors"
	"fmt"
	"sort"

	"k8s.io/utils/cpuset"

	"github.com/zjusct/kore/pkg/request"
	"github.com/zjusct/kore/pkg/topology"
)

var (
	ErrInsufficient = errors.New("insufficient free cpus")
	ErrConflict     = errors.New("cpuset conflict")
	ErrSMTAlignment = errors.New("cpu count not aligned to full cores")
)

type Allocation struct {
	PodUID    string
	Pod       string // namespace/name
	Container string
	CPUs      cpuset.CPUSet
	NUMA      []int
}

type Request struct {
	PodUID    string
	Pod       string
	Container string
	CPUs      int
	NUMAPolicy   request.NUMAPolicy
	Placement    request.Placement
	SMTPolicy    request.SMTPolicy
	ReservedNUMA *int
	Explicit     *cpuset.CPUSet
}

type State struct {
	topo     *topology.Topology
	reserved cpuset.CPUSet
	allocs   map[string]Allocation
}

func NewState(topo *topology.Topology, reserved cpuset.CPUSet) *State {
	return &State{topo: topo, reserved: reserved, allocs: map[string]Allocation{}}
}

func key(podUID, container string) string { return podUID + "/" + container }

func (s *State) Used() cpuset.CPUSet {
	u := cpuset.New()
	for _, a := range s.allocs {
		u = u.Union(a.CPUs)
	}
	return u
}

func (s *State) FreeInZone(zone int) cpuset.CPUSet {
	for _, z := range s.topo.Zones {
		if z.ID == zone {
			return z.CPUs.Difference(s.reserved).Difference(s.Used())
		}
	}
	return cpuset.New()
}

// SharedPool = 全部核 − 系统预留 − 已独占核（spec §3）。
func (s *State) SharedPool() cpuset.CPUSet {
	return s.topo.AllCPUs().Difference(s.reserved).Difference(s.Used())
}

func (s *State) Allocations() []Allocation {
	out := make([]Allocation, 0, len(s.allocs))
	for _, a := range s.allocs {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].PodUID != out[j].PodUID {
			return out[i].PodUID < out[j].PodUID
		}
		return out[i].Container < out[j].Container
	})
	return out
}

// Restore 用于 agent 重启后从容器注解重建状态（NRI Synchronize）。
func (s *State) Restore(a Allocation) error {
	if !a.CPUs.Intersection(s.Used()).IsEmpty() {
		return fmt.Errorf("%w: restore %s/%s overlaps existing allocation", ErrConflict, a.Pod, a.Container)
	}
	s.allocs[key(a.PodUID, a.Container)] = a
	return nil
}

func (s *State) Release(podUID string) {
	for k, a := range s.allocs {
		if a.PodUID == podUID {
			delete(s.allocs, k)
		}
	}
}

func (s *State) Allocate(req Request) (Allocation, error) {
	if _, ok := s.allocs[key(req.PodUID, req.Container)]; ok {
		return Allocation{}, fmt.Errorf("%w: %s/%s already allocated", ErrConflict, req.PodUID, req.Container)
	}
	if req.Explicit != nil {
		return s.allocateExplicit(req)
	}

	unit := 1
	if req.SMTPolicy != request.SMTLogical && s.topo.SMTEnabled() {
		unit = s.topo.ThreadsPerCore
		if req.CPUs%unit != 0 {
			return Allocation{}, fmt.Errorf("%w: %d cpus on SMT node (threads-per-core=%d); use smt-policy=logical or align the count",
				ErrSMTAlignment, req.CPUs, unit)
		}
	}
	strat := StrategyFor(req.Placement)

	var picked cpuset.CPUSet
	var numa []int
	var err error
	switch req.NUMAPolicy {
	case request.NUMASpread:
		picked, numa, err = s.pickSpread(req.CPUs, unit, strat)
	case request.NUMAPreferred:
		picked, numa, err = s.pickPreferred(req.CPUs, unit, strat, req.ReservedNUMA)
	default:
		picked, numa, err = s.pickSingle(req.CPUs, unit, strat, req.ReservedNUMA)
	}
	if err != nil {
		return Allocation{}, err
	}
	a := Allocation{PodUID: req.PodUID, Pod: req.Pod, Container: req.Container, CPUs: picked, NUMA: numa}
	s.allocs[key(req.PodUID, req.Container)] = a
	return a, nil
}

func (s *State) allocateExplicit(req Request) (Allocation, error) {
	want := *req.Explicit
	free := s.topo.AllCPUs().Difference(s.reserved).Difference(s.Used())
	if !want.Difference(free).IsEmpty() {
		return Allocation{}, fmt.Errorf("%w: explicit cpuset %s not fully free (free: %s)", ErrConflict, want, free)
	}
	zones := map[int]bool{}
	for _, c := range want.List() {
		zones[s.topo.ZoneOf(c)] = true
	}
	numa := make([]int, 0, len(zones))
	for z := range zones {
		numa = append(numa, z)
	}
	sort.Ints(numa)
	a := Allocation{PodUID: req.PodUID, Pod: req.Pod, Container: req.Container, CPUs: want, NUMA: numa}
	s.allocs[key(req.PodUID, req.Container)] = a
	return a, nil
}

// freeUnits 返回 zone 内可用于分配的空闲 cpu 集合；full-core 模式（unit>1）
// 只保留 sibling 组完全空闲的核。
func (s *State) freeUnits(zone, unit int) cpuset.CPUSet {
	free := s.FreeInZone(zone)
	if unit == 1 {
		return free
	}
	keep := cpuset.New()
	for _, c := range free.List() {
		if s.topo.Siblings[c].Difference(free).IsEmpty() {
			keep = keep.Union(s.topo.Siblings[c])
		}
	}
	return keep
}

// unitsOf 把空闲集合切成升序 Unit 列表。
func (s *State) unitsOf(free cpuset.CPUSet, unit int) []Unit {
	if unit == 1 {
		out := make([]Unit, 0, free.Size())
		for _, c := range free.List() {
			out = append(out, Unit{Min: c, CPUs: cpuset.New(c)})
		}
		return out
	}
	seen := map[int]bool{}
	var out []Unit
	for _, c := range free.List() {
		g := s.topo.Siblings[c]
		m := g.List()[0]
		if seen[m] {
			continue
		}
		seen[m] = true
		out = append(out, Unit{Min: m, CPUs: g})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Min < out[j].Min })
	return out
}

// pickInZone 在单个 zone 内选 n 个 cpu（n 必须是 unit 的整数倍）。
func (s *State) pickInZone(zone, n, unit int, strat Strategy) (cpuset.CPUSet, bool) {
	units := s.unitsOf(s.freeUnits(zone, unit), unit)
	got, ok := strat.Pick(units, n/unit)
	if !ok {
		return cpuset.New(), false
	}
	return unitsUnion(got), true
}

// zonesByFreeAsc：按空闲 unit 数升序（binpack），并列取 ID 小者。
func (s *State) zonesByFreeAsc(unit int) []int {
	ids := make([]int, 0, len(s.topo.Zones))
	freeCount := map[int]int{}
	for _, z := range s.topo.Zones {
		ids = append(ids, z.ID)
		freeCount[z.ID] = len(s.unitsOf(s.freeUnits(z.ID, unit), unit))
	}
	sort.Slice(ids, func(i, j int) bool {
		if freeCount[ids[i]] != freeCount[ids[j]] {
			return freeCount[ids[i]] < freeCount[ids[j]]
		}
		return ids[i] < ids[j]
	})
	return ids
}

func (s *State) pickSingle(n, unit int, strat Strategy, reservedNUMA *int) (cpuset.CPUSet, []int, error) {
	var candidates []int
	if reservedNUMA != nil {
		candidates = []int{*reservedNUMA} // 调度器指定后绝不 fallback（spec §6）
	} else {
		candidates = s.zonesByFreeAsc(unit)
	}
	for _, z := range candidates {
		if got, ok := s.pickInZone(z, n, unit, strat); ok {
			return got, []int{z}, nil
		}
	}
	return cpuset.New(), nil, fmt.Errorf("%w: no single NUMA zone with %d free cpus", ErrInsufficient, n)
}
```

（`pickPreferred`/`pickSpread` 在 Task 6 实现；本 Task 先放占位实现让编译通过并让未覆盖路径显式失败：）

```go
func (s *State) pickPreferred(n, unit int, strat Strategy, reservedNUMA *int) (cpuset.CPUSet, []int, error) {
	return cpuset.New(), nil, fmt.Errorf("%w: preferred not implemented yet", ErrInsufficient)
}

func (s *State) pickSpread(n, unit int, strat Strategy) (cpuset.CPUSet, []int, error) {
	return cpuset.New(), nil, fmt.Errorf("%w: spread not implemented yet", ErrInsufficient)
}
```

- [ ] **Step 5: 运行测试确认通过**

Run: `go test ./pkg/allocator/... -v`
Expected: PASS，全部用例通过。

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: 分配器核心（State、pack 策略、single、explicit）"
```

---

### Task 6: 分配策略完善（scatter、preferred、spread、SMT）

**Files:**
- Modify: `pkg/allocator/allocator.go`（替换 pickPreferred/pickSpread 占位实现）
- Test: `pkg/allocator/policies_test.go`

**Interfaces:**
- Consumes: Task 5 的全部导出符号
- Produces: `pickPreferred`/`pickSpread` 真实实现（包内私有；行为由测试锁定）

- [ ] **Step 1: 写失败测试**

```go
package allocator

import (
	"errors"
	"testing"

	"k8s.io/utils/cpuset"

	"github.com/zjusct/kore/pkg/request"
	"github.com/zjusct/kore/pkg/topology"
)

// x86Topo: 2 zone、2-way SMT。zone0={0-3,8-11}（sibling (i,i+8)），zone1={4-7,12-15}。
func x86Topo() *topology.Topology {
	zones := []topology.Zone{
		{ID: 0, CPUs: cpuset.New(0, 1, 2, 3, 8, 9, 10, 11), MemoryTotalBytes: 1 << 34, Distances: []int{10, 21}},
		{ID: 1, CPUs: cpuset.New(4, 5, 6, 7, 12, 13, 14, 15), MemoryTotalBytes: 1 << 34, Distances: []int{21, 10}},
	}
	sib := map[int]cpuset.CPUSet{}
	for i := 0; i < 8; i++ {
		g := cpuset.New(i, i+8)
		sib[i], sib[i+8] = g, g
	}
	return &topology.Topology{Zones: zones, Siblings: sib, ThreadsPerCore: 2}
}

func TestPreferredSpillsByDistance(t *testing.T) {
	s := NewState(armTopo(), cpuset.New())
	n := 0
	a := alloc(t, s, "a", 6, func(r *Request) {
		r.NUMAPolicy = request.NUMAPreferred
		r.ReservedNUMA = &n
	})
	// zone0 全部 4 个 + 距离最近的 zone1 里 2 个
	if a.CPUs.String() != "0-5" {
		t.Fatalf("cpus = %s, want 0-5", a.CPUs)
	}
	if len(a.NUMA) != 2 || a.NUMA[0] != 0 || a.NUMA[1] != 1 {
		t.Fatalf("numa = %v, want [0 1]", a.NUMA)
	}
}

func TestPreferredStaysSingleWhenFits(t *testing.T) {
	s := NewState(armTopo(), cpuset.New())
	a := alloc(t, s, "a", 3, func(r *Request) { r.NUMAPolicy = request.NUMAPreferred })
	if len(a.NUMA) != 1 {
		t.Fatalf("numa = %v, want single zone", a.NUMA)
	}
}

func TestSpreadEven(t *testing.T) {
	s := NewState(armTopo(), cpuset.New())
	a := alloc(t, s, "a", 8, func(r *Request) { r.NUMAPolicy = request.NUMASpread })
	if len(a.NUMA) != 4 {
		t.Fatalf("numa = %v, want 4 zones", a.NUMA)
	}
	for _, z := range a.NUMA {
		perZone := a.CPUs.Intersection(zoneCPUs(t, s, z)).Size()
		if perZone != 2 {
			t.Fatalf("zone %d got %d cpus, want 2", z, perZone)
		}
	}
}

func TestSpreadUnevenRemainder(t *testing.T) {
	s := NewState(armTopo(), cpuset.New())
	a := alloc(t, s, "a", 6, func(r *Request) { r.NUMAPolicy = request.NUMASpread })
	sizes := map[int]int{}
	for _, z := range a.NUMA {
		sizes[z] = a.CPUs.Intersection(zoneCPUs(t, s, z)).Size()
	}
	two, one := 0, 0
	for _, n := range sizes {
		switch n {
		case 2:
			two++
		case 1:
			one++
		default:
			t.Fatalf("unexpected per-zone size %d", n)
		}
	}
	if two != 2 || one != 2 {
		t.Fatalf("sizes = %v, want two zones with 2 and two with 1", sizes)
	}
}

func TestScatterWithinZone(t *testing.T) {
	s := NewState(armTopo(), cpuset.New())
	a := alloc(t, s, "a", 2, func(r *Request) { r.Placement = request.PlacementScatter })
	if a.CPUs.String() != "0,2" {
		t.Fatalf("cpus = %s, want 0,2", a.CPUs)
	}
}

func TestSMTFullCore(t *testing.T) {
	s := NewState(x86Topo(), cpuset.New())
	a := alloc(t, s, "a", 4, nil) // full-core 默认
	if a.CPUs.String() != "0-1,8-9" { // 两个完整 sibling 组
		t.Fatalf("cpus = %s, want 0-1,8-9", a.CPUs)
	}
}

func TestSMTMisalignmentFails(t *testing.T) {
	s := NewState(x86Topo(), cpuset.New())
	_, err := s.Allocate(Request{PodUID: "u", Pod: "d/p", Container: "app", CPUs: 3,
		NUMAPolicy: request.NUMASingle, SMTPolicy: request.SMTFullCore})
	if !errors.Is(err, ErrSMTAlignment) {
		t.Fatalf("err = %v", err)
	}
}

func TestSMTLogicalAllowsOdd(t *testing.T) {
	s := NewState(x86Topo(), cpuset.New())
	a := alloc(t, s, "a", 3, func(r *Request) { r.SMTPolicy = request.SMTLogical })
	if a.CPUs.String() != "0-2" {
		t.Fatalf("cpus = %s, want 0-2", a.CPUs)
	}
}

func TestSMTPartialCoreExcludedFromFullCore(t *testing.T) {
	s := NewState(x86Topo(), cpuset.New())
	alloc(t, s, "a", 1, func(r *Request) { r.SMTPolicy = request.SMTLogical }) // 占 {0}，物理核 (0,8) 残缺
	b := alloc(t, s, "b", 2, nil)                                             // full-core 必须避开残缺核
	if b.CPUs.Contains(8) {
		t.Fatalf("full-core alloc %s must not use partially-used core", b.CPUs)
	}
}

func zoneCPUs(t *testing.T, s *State, zone int) cpuset.CPUSet {
	t.Helper()
	for _, z := range s.topo.Zones {
		if z.ID == zone {
			return z.CPUs
		}
	}
	t.Fatalf("zone %d not found", zone)
	return cpuset.New()
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./pkg/allocator/... -v`
Expected: 新增用例 FAIL（preferred/spread 占位返回 ErrInsufficient；scatter/SMT 部分可能已过——关注失败集合合理）。

- [ ] **Step 3: 实现 pickPreferred 与 pickSpread（替换占位）**

```go
// pickPreferred：先尝试单 zone；不够则以 primary（reservedNUMA 或空闲最多的 zone）
// 为起点，按 NUMA 距离升序溢出（spec §4 preferred 语义）。
func (s *State) pickPreferred(n, unit int, strat Strategy, reservedNUMA *int) (cpuset.CPUSet, []int, error) {
	if got, numa, err := s.pickSingle(n, unit, strat, nil); err == nil {
		return got, numa, nil
	}
	primary := 0
	if reservedNUMA != nil {
		primary = *reservedNUMA
	} else {
		bestFree := -1
		for _, z := range s.topo.Zones {
			free := len(s.unitsOf(s.freeUnits(z.ID, unit), unit))
			if free > bestFree {
				bestFree, primary = free, z.ID
			}
		}
	}
	var primaryZone *topology.Zone
	for i := range s.topo.Zones {
		if s.topo.Zones[i].ID == primary {
			primaryZone = &s.topo.Zones[i]
		}
	}
	if primaryZone == nil {
		return cpuset.New(), nil, fmt.Errorf("%w: unknown NUMA zone %d", ErrInsufficient, primary)
	}
	order := make([]int, 0, len(s.topo.Zones))
	for _, z := range s.topo.Zones {
		order = append(order, z.ID)
	}
	sort.Slice(order, func(i, j int) bool {
		di, dj := primaryZone.Distances[order[i]], primaryZone.Distances[order[j]]
		if di != dj {
			return di < dj
		}
		return order[i] < order[j]
	})

	remaining := n
	result := cpuset.New()
	var numa []int
	for _, z := range order {
		avail := s.unitsOf(s.freeUnits(z, unit), unit)
		take := min(remaining, len(avail)*unit)
		take -= take % unit
		if take == 0 {
			continue
		}
		got, ok := s.pickInZone(z, take, unit, strat)
		if !ok {
			continue
		}
		result = result.Union(got)
		numa = append(numa, z)
		remaining -= take
		if remaining == 0 {
			sort.Ints(numa)
			return result, numa, nil
		}
	}
	return cpuset.New(), nil, fmt.Errorf("%w: %d cpus not available across all zones", ErrInsufficient, n)
}

// pickSpread：把 needUnits 均分到空闲最多的 zcount 个 zone（余数分给前几个）。
// 任一 zone 配额无法满足即失败（spread 要求均匀，不做倾斜兜底）。
func (s *State) pickSpread(n, unit int, strat Strategy) (cpuset.CPUSet, []int, error) {
	needUnits := n / unit
	if n%unit != 0 {
		return cpuset.New(), nil, fmt.Errorf("%w: %d cpus not a multiple of unit %d", ErrSMTAlignment, n, unit)
	}
	type zf struct{ id, free int }
	var zones []zf
	for _, z := range s.topo.Zones {
		if free := len(s.unitsOf(s.freeUnits(z.ID, unit), unit)); free > 0 {
			zones = append(zones, zf{z.ID, free})
		}
	}
	sort.Slice(zones, func(i, j int) bool {
		if zones[i].free != zones[j].free {
			return zones[i].free > zones[j].free
		}
		return zones[i].id < zones[j].id
	})
	zcount := min(len(zones), needUnits)
	if zcount == 0 {
		return cpuset.New(), nil, fmt.Errorf("%w: no free zones", ErrInsufficient)
	}
	base, extra := needUnits/zcount, needUnits%zcount

	result := cpuset.New()
	var numa []int
	for i := 0; i < zcount; i++ {
		quota := base
		if i < extra {
			quota++
		}
		got, ok := s.pickInZone(zones[i].id, quota*unit, unit, strat)
		if !ok {
			return cpuset.New(), nil, fmt.Errorf("%w: zone %d cannot satisfy spread quota %d", ErrInsufficient, zones[i].id, quota*unit)
		}
		result = result.Union(got)
		numa = append(numa, zones[i].id)
	}
	sort.Ints(numa)
	return result, numa, nil
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `go test ./pkg/allocator/... -v`
Expected: PASS，含 Task 5 全部旧用例（回归）。

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: preferred/spread NUMA 策略与 scatter/SMT 行为"
```

---

### Task 7: CR status 构建（pkg/allocator/status.go）

**Files:**
- Create: `pkg/allocator/status.go`
- Test: `pkg/allocator/status_test.go`

**Interfaces:**
- Consumes: `allocator.State`（Task 5/6）、`v1alpha1` 类型（Task 2）
- Produces: `func BuildStatus(s *State) v1alpha1.KoreNodeTopologyStatus` — Plan 2 的 agent 用它写 CR

- [ ] **Step 1: 写失败测试**

```go
package allocator

import (
	"testing"

	"k8s.io/utils/cpuset"

	"github.com/zjusct/kore/pkg/request"
)

func TestBuildStatus(t *testing.T) {
	s := NewState(x86Topo(), cpuset.New(0, 8)) // 预留物理核 (0,8)
	n := 0
	alloc(t, s, "a", 2, func(r *Request) { r.ReservedNUMA = &n }) // {1,9}
	st := BuildStatus(s)

	if st.ReservedSystemCpus != "0,8" {
		t.Fatalf("reserved = %s", st.ReservedSystemCpus)
	}
	if len(st.Zones) != 2 {
		t.Fatalf("zones = %d", len(st.Zones))
	}
	z0 := st.Zones[0]
	if z0.ID != 0 || z0.Cpus != "0-3,8-11" || z0.Allocatable != 6 {
		t.Fatalf("zone0 = %+v", z0)
	}
	if z0.FreeCpus != "2-3,10-11" {
		t.Fatalf("zone0 free = %s", z0.FreeCpus)
	}
	if len(z0.SMTSiblings) != 4 || z0.SMTSiblings[0][0] != 0 || z0.SMTSiblings[0][1] != 8 {
		t.Fatalf("siblings = %v", z0.SMTSiblings)
	}
	if z0.Devices == nil {
		t.Fatal("devices must be non-nil empty slice (v2 预留字段)")
	}
	if z0.MemoryTotal.Value() != 1<<34 {
		t.Fatalf("mem = %d", z0.MemoryTotal.Value())
	}
	if len(st.Allocations) != 1 || st.Allocations[0].Cpuset != "1,9" || st.Allocations[0].NUMA[0] != 0 {
		t.Fatalf("allocations = %+v", st.Allocations)
	}
	if st.Allocations[0].Pod != "default/a" || st.Allocations[0].Container != "app" {
		t.Fatalf("allocations = %+v", st.Allocations)
	}
}

func TestBuildStatusNoSMT(t *testing.T) {
	s := NewState(armTopo(), cpuset.New())
	st := BuildStatus(s)
	if len(st.Zones[0].SMTSiblings) != 0 {
		t.Fatalf("siblings = %v, want empty on non-SMT", st.Zones[0].SMTSiblings)
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./pkg/allocator/... -run TestBuildStatus -v`
Expected: FAIL（`BuildStatus` 未定义）。

- [ ] **Step 3: 实现 status.go**

```go
package allocator

import (
	"k8s.io/apimachinery/pkg/api/resource"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
)

// BuildStatus 把当前分配状态渲染为 KoreNodeTopology 的 status（agent 定期写 CR）。
func BuildStatus(s *State) v1alpha1.KoreNodeTopologyStatus {
	st := v1alpha1.KoreNodeTopologyStatus{ReservedSystemCpus: s.reserved.String()}
	for _, z := range s.topo.Zones {
		zone := v1alpha1.Zone{
			ID:          z.ID,
			Cpus:        z.CPUs.String(),
			Allocatable: z.CPUs.Difference(s.reserved).Size(),
			FreeCpus:    s.FreeInZone(z.ID).String(),
			MemoryTotal: *resource.NewQuantity(z.MemoryTotalBytes, resource.BinarySI),
			Devices:     []v1alpha1.Device{},
		}
		if s.topo.SMTEnabled() {
			seen := map[int]bool{}
			for _, c := range z.CPUs.List() {
				g := s.topo.Siblings[c]
				m := g.List()[0]
				if seen[m] {
					continue
				}
				seen[m] = true
				zone.SMTSiblings = append(zone.SMTSiblings, g.List())
			}
		}
		st.Zones = append(st.Zones, zone)
	}
	for _, a := range s.Allocations() {
		st.Allocations = append(st.Allocations, v1alpha1.Allocation{
			PodUID: a.PodUID, Pod: a.Pod, Container: a.Container,
			Cpuset: a.CPUs.String(), NUMA: a.NUMA,
		})
	}
	return st
}
```

- [ ] **Step 4: 运行全部测试与构建**

Run: `make test && make build && go vet ./...`
Expected: 全部 PASS、构建干净。

- [ ] **Step 5: 交叉编译验证（Global Constraints）**

Run: `GOOS=linux GOARCH=amd64 go build ./... && GOOS=linux GOARCH=arm64 go build ./...`
Expected: 无输出，退出码 0（纯 Go 无 cgo）。

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: KoreNodeTopology status 构建（BuildStatus）"
```

---

## Plan 1 完成标准

- `make test` 全绿；`make build`、两架构交叉编译干净
- `deploy/crd/` 有可 `kubectl apply` 的 CRD manifest
- 后续计划的消费接口就绪：Plan 2（agent）用 `topology.Discover` + `allocator.State/BuildStatus` + `request.ParsePod`；Plan 3（scheduler）用 `v1alpha1` 类型 + `request` 注解常量

## 后续计划（占位，落地后另写）

- Plan 2：kore-agent —— NRI 插件（CreateContainer/UpdateContainers/Synchronize）、device plugin 门闩、CR 上报、Lease、共享池围栏
- Plan 3：kore-scheduler —— Filter/Score/Reserve/PreBind
- Plan 4：kore-operator + deploy manifests + kind 集成测试 + 真机 E2E
