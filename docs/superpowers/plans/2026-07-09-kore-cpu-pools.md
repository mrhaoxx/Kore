# Kore v0.2.0：CPU 池与共享池保底（Plan 5）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增命名 CPU 池（balloon 语义：一组 Pod 共享固定大小的专属核心区，对外独占、池内共享）与 `sharedPoolMin`（全局共享池保底），发布 v0.2.0 并部署验收。

**Architecture:** 池 = 一次"大号独占分配"+ 引用计数。首个成员到达节点时按 numa-policy 划出 size 个核建池，后续成员直接引用，末位成员离开时释放。调度器"跟随已有池，否则按建池容量过滤"。所有介入点复用 v0.1 的四组件管线。

**Tech Stack:** 不变（Go、NRI、scheduler framework、controller-runtime）。

## Global Constraints

- 继承 v0.1 全部约束（fail-closed、异步回写、单 mutex、可判别错误、双架构）
- 新注解：`kore.zjusct.io/pool`（池名，DNS label 格式）、`kore.zjusct.io/pool-size`（正整数）
- 互斥：pool ⟂ pin、pool ⟂ cpuset；pool 成员**不要求**整数 CPU / requests==limits（池内共享）
- 池创建用 numa-policy（默认 single）+ placement；SMT 一律按逻辑核分配（unit=1，池内共享无 sibling 隔离需求）
- 池成员**不注入**门闩扩展资源（成员共享，per-Pod 计数会超卖 token；agent-down 窗口由 Synchronize 对账兜底——记入 README 取舍）
- `sharedPoolMin`（agent ConfigMap，默认 0）：独占分配与建池后若共享池 < min → ErrInsufficient 拒绝；仅 agent 侧强制（调度器不感知，极端情况下节点上拒绝后由 kubelet 重试，记入文档）
- 镜像与 manifests 版本 v0.2.0

---

### Task 1: 注解解析（pkg/request）

**Files:** Modify `pkg/request/request.go`、`pkg/request/request_test.go`

**Interfaces:**
- 新常量：`AnnoPool = "kore.zjusct.io/pool"`、`AnnoPoolSize = "kore.zjusct.io/pool-size"`
- `Request` 新字段：`Pool string; PoolSize int`
- `ParsePod` 语义变化：pin=true **或** pool 非空 都视为 kore Pod（否则仍返回 nil,nil）
- 校验：pool 必须配 pool-size（正整数）；pool-size 无 pool → error；pool+pin → error；pool+cpuset → error；池名符合 DNS label（`[a-z0-9]([-a-z0-9]*[a-z0-9])?`，≤63）；池模式跳过"整数 CPU/requests==limits/至少一个绑核容器"三项检查（Containers 留空）

**Steps（TDD）：**
- [ ] 失败测试（补进现有表驱动）：纯 pool 合法（Pool/PoolSize 解析、Containers 为空、numa-policy 默认 single 仍生效）；缺 pool-size 报错；pool-size 非法值（"0"、"-1"、"abc"）报错；pool+pin 互斥；pool+cpuset 互斥；非法池名（"Ab_c"）报错；pool-size 无 pool 报错
- [ ] 实现（要点：pin/pool 双开关入口；pool 分支先于容器遍历返回校验）：

```go
// 入口改为：
pin := pod.Annotations[AnnoPin] == "true"
poolName, hasPool := pod.Annotations[AnnoPool]
switch {
case pod.Annotations[AnnoPin] != "" && pod.Annotations[AnnoPin] != "true" && pod.Annotations[AnnoPin] != "false":
    return nil, fmt.Errorf(...)
case !pin && !hasPool:
    if _, ok := pod.Annotations[AnnoPoolSize]; ok {
        return nil, fmt.Errorf("%s requires %s", AnnoPoolSize, AnnoPool)
    }
    return nil, nil
case pin && hasPool:
    return nil, fmt.Errorf("%s and %s are mutually exclusive", AnnoPin, AnnoPool)
}
// pool 分支：校验名字正则+size 正整数，置 r.Pool/r.PoolSize；
// 与 AnnoCPUSet 互斥；跳过容器整数校验直接 return
```

- [ ] 全包回归 → Commit `feat: 池注解解析与校验`

---

### Task 2: CRD 池状态（pkg/apis + deploy/crd）

**Files:** Modify `pkg/apis/kore/v1alpha1/types.go`；regen deepcopy + CRD

**Interfaces:**
- `KoreNodeTopologyStatus` 新增 `Pools []Pool`
- `type Pool struct { Name string; Cpuset string; NUMA []int; Members []string }`（Members = podUID 列表，调度器预占清理用）

**Steps：**
- [ ] types 增加字段 + `make generate manifests` + roundtrip 测试补一个 Pools 字段断言 → Commit `feat: KNT 状态上报 CPU 池`

---

### Task 3: 分配器池语义与 sharedPoolMin（pkg/allocator）

**Files:** Modify `pkg/allocator/allocator.go`、`status.go`；Create `pkg/allocator/pool.go`、`pool_test.go`

**Interfaces:**
- `NewState(topo, reserved, sharedPoolMin int) *State`（签名变更，v0.1 调用点同步改：nriplugin.New、Inspect、各测试传 0）
- `PoolInfo struct { Name string; CPUs cpuset.CPUSet; NUMA []int; Members map[string]bool }`
- `(s *State) JoinPool(req PoolRequest) (PoolInfo, error)`；`PoolRequest{Name string; Size int; PodUID string; NUMAPolicy request.NUMAPolicy; Placement request.Placement; ReservedNUMA *int}`
  - 池已存在：Size 与现有 cpus 数不符 → ErrConflict；否则加成员返回现有
  - 不存在：选核（unit=1，复用 pickSingle/pickPreferred/pickSpread）→ sharedPoolMin 检查 → 建池
- `(s *State) Release(podUID)` 扩展：同时处理独占分配与池成员退出（末位退出释放池核心）
- `(s *State) RestorePoolMember(name string, cpus cpuset.CPUSet, podUID string) error`（Synchronize 重建：池不存在则按 cpus 重建并做 overlap 检查；存在则校验 cpus 一致后加成员）
- `Used()`/`SharedPool()` 计入池核心；独占 `Allocate` 与建池共同受 sharedPoolMin 约束
- `BuildStatus` 输出 `Pools`（Members 排序）

**Steps（TDD，测试用例清单——每条都要有）：**
- [ ] 失败测试：首成员建池（single，4 核在 zone0，SharedPool 相应缩小）；次成员加入返回**相同** cpus；size 不符 ErrConflict；成员1退出池仍在、成员2退出池释放（SharedPool 恢复）；池核心与独占分配互斥（建池后 Allocate 不得重叠，反向同理）；ReservedNUMA 引导建池 zone；RestorePoolMember 重建/一致性校验/overlap 冲突；sharedPoolMin：预留后 Allocate 触底拒绝、建池触底拒绝、池内加成员不受影响；BuildStatus 含 Pools
- [ ] 实现 pool.go + Release/Used/SharedPool/BuildStatus/NewState 改造；全部 v0.1 测试回归（NewState 调用点补 0）
- [ ] Commit `feat: 分配器 CPU 池语义与 sharedPoolMin`

---

### Task 4: agent 池路径（pkg/nriplugin + pkg/agent/config）

**Files:** Modify `pkg/agent/config/config.go`（+SharedPoolMin int，校验 ≥0）、`pkg/nriplugin/plugin.go`、`lifecycle.go`、各测试；`cmd/kore-agent/main.go`（NewState 传参不变——在 nriplugin.New 内取 cfg.SharedPoolMin）

**Interfaces:**
- CreateContainer：`req.Pool != ""` → `JoinPool`（ReservedNUMA 取自 kpod 注解）→ adjustment cpus=池核心、mems=MemsFor(memory-policy, pool.NUMA)、容器注解 AnnoAllocated=池 cpus → report + shrinkShared。Pod 的**每个**容器都走此路径（JoinPool 幂等加成员）
- StopPod/RemovePod：`releasePod` 经 `State.Release` 自动处理池退出 → report + updater
- Synchronize：sandbox 注解有 pool 且容器有 AnnoAllocated → `RestorePoolMember`；有 pool 无 AnnoAllocated → remediate（repair=JoinPool+update；strict=DeletePod），事件文案区分池
- 失败路径 fail-closed 不变（JoinPool 错误 → 拒绝创建 + 事件 KorePoolFailed）

**Steps（TDD）：**
- [ ] 失败测试：两个池成员容器拿到相同 cpuset 且共享池收缩一次；池+独占混布互斥；末位成员 StopPod 后 updater 推送共享池扩张；Synchronize 恢复池（两成员）后新独占分配不与池重叠；unbound 池成员 strict 删 Pod / repair 补入池；config 解析 sharedPoolMin
- [ ] 实现 + 回归 → Commit `feat: agent 池成员绑定与对账`

---

### Task 5: 调度器跟随/建池（pkg/scheduler）

**Files:** Modify `pkg/scheduler/plugin.go`、`capacity.go`、`plugin_test.go`

**Interfaces:**
- `nodeSnap` 增 `pools map[string]poolSnap`；`poolSnap{cpus int; memberUIDs []string}`（自 CR status.pools）
- PreFilter：pool 模式 need=PoolSize；MarkAllocated 的 uids 集合并入 pools[].Members（清理建池预占）
- Filter（pool 模式）：节点已有同名池 → size 不符（`len(pool cpus) != PoolSize`）Unschedulable，否则 Success（跟随，零新增容量）；无池 → 按 numa-policy 用现有 Fit* 检查 PoolSize
- Score：已有同名池的节点 → 100（聚合优先）；否则 ScoreFit(PoolSize)
- Reserve：跟随 → 不预占；建池 → 预占 Count=PoolSize（Zone 按 FitSingle/FitPreferred）
- PreBind：建池才写 reserved-numa（跟随跳过——判断依据：Reserve 时是否加了预占，记录在 cache entry：`Reservation.Pool string` 非空表示建池预占）

**Steps（TDD）：**
- [ ] 失败测试：无池节点按 size 过滤（8 核请求 4 核节点拒）；有池节点跟随通过且 Score=100 高于建池节点；size 不符拒绝；建池 Reserve 对下一 Pod 生效（容量扣减）、跟随 Reserve 不扣减；CR pools.members 触发 MarkAllocated 清理
- [ ] 实现 + 回归 → Commit `feat: 调度器池跟随与建池过滤`

---

### Task 6: webhook、E2E、发布 v0.2.0

**Files:** Modify `pkg/operator/webhook.go`（池成员：注入 schedulerName，**不**注入门闩资源）、`webhook_test.go`；`test/e2e/kind-e2e.sh`（新增池场景步骤）；`deploy/*/{daemonset,deployment}.yaml` 镜像 v0.2.0；README 池用法章节

**E2E 新增步骤（插在原 6/8 之后）：**
- [ ] 提交两个池成员 Pod（pool=demo、size=2、各 requests cpu 500m）→ 断言两者 cgroup cpuset **完全相同**且核数=2、与绑核 Pod 的核不重叠 → 删除一个成员池仍在 → 删除另一个后新建非池 Pod 的共享池恢复包含原池核心
- [ ] webhook 测试：池 Pod 注入 schedulerName、无 kore.zjusct.io/cpu 资源；池+pin 被 Denied
- [ ] `make test` 全绿 + 双架构交叉编译 + `make e2e-kind` 全通过
- [ ] 构建推送 `docker.io/mrhaoxx/kore-{agent,scheduler,operator}:v0.2.0`（串行）→ manifests 指向 v0.2.0 → 部署 zjusct（CRD 先行，agent 滚动注意绑核 Pod 创建窗口）→ 920B 验收：两个池 Pod 共享同一 64 核区 + 与既有独占 Pod 无重叠
- [ ] Commit `feat: v0.2.0 CPU 池发布` + 更新 README

## 完成标准

- 全部单测/回归/kind E2E 绿；v0.2.0 部署 zjusct 且真机验收池语义
- 池与独占、全局共享池三者互斥性在 KNT 账本可见（zones.freeCpus 不含池核心、status.pools 列出成员）
