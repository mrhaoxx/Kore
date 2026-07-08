# Kore：Kubernetes 绑核与 NUMA 绑定系统 — 设计文档

日期：2026-07-08
状态：已确认（待实现计划）

## 1. 背景与目标

ZJUSCT 自建 k8s 集群上运行低延迟/性能敏感的 HPC 业务，需要 numactl 级别的
CPU 独占绑核与 NUMA 本地内存绑定能力。k8s 原生 CPU Manager / Topology Manager
是节点级策略：无法按 Pod 定制、无法指定核号、调度器对 NUMA 不感知。

**目标**：
- Pod 级声明式绑核：用户声明"N 个独占核、NUMA 约束、内存策略"，系统选核并绑定
- 逃生舱：允许显式指定核号（配合 isolcpus/IRQ 调优场景）
- 全链路 NUMA 感知：调度器知道每节点各 NUMA 剩余核心/内存，Pod 保证落到能满足约束的地方
- 严格绑定保证：绑核容器绝不在无绑定状态下运行
- 绑定发生在容器启动之前（进程首条指令即在正确核心上，内存首次分配即本地）

**目标环境**：
- k8s v1.35/v1.36，containerd 2.2.x（NRI 默认启用）
- 异构节点：x86_64（m6xx/m7xx，Debian 13）+ arm64（鲲鹏 920B，多 NUMA）+ Atlas 800（NPU）
- 完全自建自运维，kubelet/containerd 配置可控
- kubelet 保持 `cpu-manager-policy=none`（默认值），cpuset 管理权完全归 Kore

**v1 明确不做**（防止范围蔓延）：
- 设备（NPU/GPU/网卡）NUMA 亲和对齐 → v2（拓扑 CRD 已预留 devices 字段）
- DRA（ResourceClaim）前端 → v2 候选（CPU-over-DRA 上游未趟平双重记账问题）
- IRQ 亲和性迁移、isolcpus 联动
- 在线重绑（运行中改核数）
- 与 kubelet CPU Manager static 策略共存

## 2. 方案选型

| 方案 | 结论 |
|---|---|
| **A. NRI 插件 + 调度器插件（选定）** | 容器启动前绑定，无竞态；全官方扩展点 |
| B. DaemonSet 直改 cgroupfs | 启动竞态 + 已分配内存页不迁移，NUMA 内存绑定失效，对低延迟场景不可接受 |
| C. 编排原生 CPU Manager/Topology Manager | 无法指定核号、无 Pod 级策略、调度器不感知，满足不了需求 |

用户 API 载体：**纯注解**（用户明确选定）。曾讨论并否决：
- DRA ResourceClaim 嵌入 Pod spec：CPU 双重记账上游未解决，节点侧仍需 NRI，v2 再议
- nvidia 式扩展资源作为用户接口：resources 表达不了 NUMA 策略，用户不想写两遍
- 注：扩展资源 `kore.zjusct.io/cpu` 仍存在，但仅作为 webhook 自动注入的**内部安全机制**
  （kubelet 准入门闩 + quota），不是用户接口

## 3. 总体架构

```
                     ┌──────────────────────────────────────────┐
                     │                控制平面                    │
 Pod (kore 注解)      │ ┌──────────────┐    ┌─────────────────┐  │
     │ 创建           │ │ kore-operator │    │ kore-scheduler  │  │
     ├───────────────►│ │ webhook 校验  │    │ Filter/Score/   │  │
     │                │ │ 注入调度器名/  │    │ Reserve/PreBind │  │
     │                │ │ 扩展资源      │    └────────┬────────┘  │
     │                │ │ Lease→污点   │             │ 读        │
     │                │ └──────────────┘   ┌─────────▼─────────┐ │
     │                │                    │ KoreNodeTopology  │ │
     │                │                    │ CR（每节点一个）    │ │
     └────────────────┴────────────────────┴─────────▲─────────┴─┘
                                                     │ 写（拓扑/分配/心跳）
                     ┌───────────────────────────────┼──────────┐
                     │ 计算节点                       │          │
                     │ ┌─────────────────────────────┴───────┐  │
                     │ │ kore-agent (DaemonSet)              │  │
                     │ │  · sysfs 拓扑发现（NUMA/SMT）         │  │
                     │ │  · NRI 插件 ◄─socket─► containerd    │  │
                     │ │  · device plugin（计数门闩）          │  │
                     │ │  · Allocator（可插拔选核策略）         │  │
                     │ │  · 共享池围栏                         │  │
                     │ │  · Lease 续约                        │  │
                     │ └─────────────────────────────────────┘  │
                     └──────────────────────────────────────────┘
```

### 组件职责

**kore-agent**（DaemonSet，节点权威）：
- 以 NRI 插件注册到 containerd；`CreateContainer` hook 中按注解与本地分配表选核，
  在 OCI spec 下发 `cpuset.cpus`/`cpuset.mems`（启动前生效）
- 内嵌 device plugin，advertise `kore.zjusct.io/cpu` 不透明计数 token
  （无真实核号映射、无拓扑信息；kubelet 选哪个 token 无意义）
- sysfs 发现 NUMA/SMT 拓扑，写 KoreNodeTopology CR（拓扑 + 实时分配表）
- 维护共享池围栏：非绑核容器限制在 `全部核 − 系统预留 − 已独占核`；
  独占分配变化时经 NRI `UpdateContainers` 收缩/扩张共享池（含 pause 容器）
- 每节点一个 Lease，秒级续约报活
- `Synchronize`（重连/重启）时从容器注解重建分配表并对账

**kore-scheduler**（scheduler framework 插件，第二调度器 `kore-scheduler`）：
- Filter：排除「无任何 NUMA 满足请求」与「Lease 过期」的节点；
  显式 cpuset 则校验指定核未被占用
- Score：默认碎片最小化（binpack），大块连续空间留给后续大请求
- Reserve/Unreserve：调度器本地缓存预占，防并发调度冲突
- PreBind：写 `kore.zjusct.io/reserved-numa` 注解到 Pod

**kore-operator**：
- validating webhook：校验注解合法性（CPU 整数且 requests==limits、cpuset 语法、
  互斥项、显式 cpuset 必须配 nodeName/nodeSelector）
- mutating webhook：注入 `schedulerName: kore-scheduler`；
  给绑核容器注入 `resources.limits["kore.zjusct.io/cpu"]`（= cpu 请求数）
- Lease watcher：agent Lease 过期 → 给节点打 `kore.zjusct.io/agent-down:NoSchedule`
  污点（存量 Pod 不驱逐），恢复后摘除

### 分工原则

调度器决定「哪个节点 + 哪个 NUMA」（全局视角），agent 决定「具体哪些核」
（节点权威）。agent 是最终权威：预占信息过期导致节点上实际无法满足时，
agent 拒绝创建容器并打事件，Pod 走 kubelet 退避重试/重调度。

普通 Pod（无注解）走默认调度器，不感知 Kore 存在，仅在节点上被围栏限制在共享池。

## 4. 用户 API

设计原则：核数从原生 `resources.requests.cpu` 派生（quota/监控诚实），
注解只表达"怎么绑"。

```yaml
apiVersion: v1
kind: Pod
metadata:
  annotations:
    kore.zjusct.io/pin: "true"              # 开关：启用独占绑核
    kore.zjusct.io/numa-policy: "single"    # single | preferred | spread（默认 single）
    kore.zjusct.io/memory-policy: "strict"  # strict | preferred（默认 strict）
    kore.zjusct.io/placement: "pack"        # pack | scatter（默认 pack，可覆盖集群默认）
    kore.zjusct.io/smt-policy: "full-core"  # full-core | logical（默认 full-core）
    kore.zjusct.io/cpuset: "8-15"           # 逃生舱：显式核号（与系统选核互斥）
spec:
  containers:
  - name: hpc-app
    resources:
      requests: { cpu: "8", memory: "16Gi" }
      limits:   { cpu: "8", memory: "16Gi" }   # 必须整数核且 requests==limits
```

| 注解 | 取值 | 语义 |
|---|---|---|
| `pin` | `"true"` | 启用绑核；要求至少一个业务容器 CPU 整数且 requests==limits（webhook 校验）。整数核容器被绑定，非整数容器落共享池 |
| `numa-policy` | `single` | 所有核同一 NUMA，调度硬过滤，节点上不够则拒绝（绝不降级） |
| | `preferred` | 尽量单 NUMA，不够允许溢出（按 NUMA 距离升序） |
| | `spread` | 核均分到多个 NUMA（带宽敏感型负载） |
| `memory-policy` | `strict` | `cpuset.mems` 仅含分配到的 NUMA（等价 `numactl --membind`） |
| | `preferred` | `cpuset.mems` 全开，靠 first-touch 本地化 |
| `placement` | `pack`/`scatter` | NUMA 内紧凑或打散（缓存竞争敏感负载用 scatter） |
| `smt-policy` | `full-core` | SMT 机器上独占整物理核（sibling 一起拿，真独占） |
| | `logical` | 允许按逻辑核分配（接受 sibling 干扰换装箱率） |
| `cpuset` | 如 `"8-15,40-47"` | 显式核号；要求 Pod 用 nodeName/nodeSelector 指定节点；冲突先到先得 |

**回写注解**（组件写，用户只读）：
- `kore.zjusct.io/reserved-numa`：调度器 PreBind 写入
- `kore.zjusct.io/allocated-cpuset`：agent 分配后写入（可观测性）

**多容器规则（v1）**：业务容器 = 非 init 容器。注解作用于所有整数 CPU 请求的
业务容器，各容器独立分配、优先同 NUMA；非整数 CPU 的 sidecar 落共享池；
init 容器不绑。

**集群级配置**（ConfigMap，agent 启动加载）：默认 placement 策略、SMT 默认行为、
系统预留核（如 `0-1`）、共享池最小保留、对账 remediation 模式（见 §6）。

## 5. KoreNodeTopology CRD

cluster-scoped，与节点同名，agent 写、调度器读：

```yaml
apiVersion: kore.zjusct.io/v1alpha1
kind: KoreNodeTopology
metadata:
  name: m602.clusters.zjusct.io
status:
  reservedSystemCpus: "0-1"
  zones:
  - id: 0
    cpus: "0-15,32-47"        # 本 NUMA 全部逻辑核（示例：HT 机器，32-47 为 sibling）
    allocatable: 28           # 除去系统预留可独占的核数
    freeCpus: "4-15,36-47"
    memoryTotal: "256Gi"
    smtSiblings: [[2,34],[3,35]]   # SMT sibling 对（无 SMT 则空）
    devices: []               # v2 预留：NPU/网卡 NUMA 归属
  allocations:
  - podUID: "…"
    pod: "default/hpc-app-0"
    container: "hpc-app"
    cpuset: "8-15"
    numa: [0]
```

agent 报活用独立的 Lease（coordination.k8s.io，每节点一个，秒级续约），
不用 CR 里的时间戳字段（避免高频 status 更新）。

## 6. 分配算法与严格性保证

### Allocator（可插拔）

`Allocator` 为 Go interface，v1 实现 `pack`/`scatter` 两策略。选核规则：

1. **SMT 感知**：`full-core` 模式下整物理核分配（sibling 对一起给）
2. **连续性优先**：优先连续核号区间（L3/cluster 局部性）
3. **NUMA 内 binpack**（pack）：紧挨已用区域分配，保留大块连续空间；
   scatter 则在 NUMA 内打散

`numa-policy` 落地：`single` → 仅在 reserved-numa 内选，不够即拒绝；
`preferred` → 先 reserved-numa 再溢出；`spread` → 均分多 NUMA。
`memory-policy` 落地：`strict` → mems 仅含分配 NUMA；`preferred` → mems 全开。

### 严格绑定保证：三重防线

前提（诚实说明）：NRI 上游对插件干净断开是 fail-open 的（无 required-plugin
机制），因此严格性靠 NRI 之外的层保证：

1. **调度层**：Lease 过期 → kore-scheduler Filter 排除节点；
   operator 打 `NoSchedule` 污点（默认调度器的 Pod 也挡住）
2. **kubelet 准入层（关键门闩）**：agent 死 = device plugin 端点消失 →
   kubelet 拒绝启动请求 `kore.zjusct.io/cpu` 的 Pod（UnexpectedAdmissionError）。
   **没有活着的 agent，绑核容器根本起不来**
3. **运行时层**：NRI CreateContainer 下发 cpuset（主路径）；
   agent 假死/hang → containerd `plugin_request_timeout` → 创建失败（fail-closed）

残余理论窗口：device plugin Allocate 成功后毫秒级内 agent 干净退出。
兜底：agent 恢复后 `Synchronize` 对账发现"该绑未绑"容器，按 ConfigMap
`remediation` 处置——`strict`（默认）：杀容器重建 + event；
`repair`：NRI UpdateContainer 事后补绑 + warning event（内存可能非本地）。
绝不静默放行。

### 状态与恢复

- 分配表在 agent 内存，双恢复路径：NRI `Synchronize` 全量容器列表（从容器注解重建）
  + KoreNodeTopology CR 对账
- 节点重启：容器重建时 NRI hook 重新触发，绑定自动恢复
- agent 挂：存量 Pod 不受影响（cpuset 已在 cgroup，不依赖 agent 存活）

### 错误处理表

| 故障 | 行为 |
|---|---|
| 调度预占过期/冲突 | agent 拒绝 CreateContainer + Pod event，kubelet 退避重试；single 绝不降级 |
| agent 挂 | 三重防线（见上）；存量不受影响 |
| webhook 不可用 | `failurePolicy: Ignore`；agent 二次校验注解，非法则拒绝创建 + event |
| 显式 cpuset 冲突 | Filter 拒绝；竞态漏过则 agent 拒绝，先到先得 |
| 未经 kore-scheduler 的绑核 Pod | webhook 已注入扩展资源，默认调度器整数计数兜底防超卖；agent 按注解正常绑定 |

## 7. 技术栈与仓库结构

- Go 单 module、multi-binary；交叉编译 amd64 + arm64
- `github.com/containerd/nri`（插件 stub）、`sigs.k8s.io/controller-runtime` +
  kubebuilder（CRD/operator/webhook）、scheduler framework（out-of-tree，
  锁 k8s v1.36 依赖）、`k8s.io/kubelet` device plugin API

```
Kore/
├── cmd/{kore-agent, kore-scheduler, kore-operator}/
├── pkg/
│   ├── apis/v1alpha1/       # KoreNodeTopology 类型
│   ├── topology/            # sysfs 拓扑发现（纯函数，fixture 可测）
│   ├── allocator/           # Allocator 接口 + pack/scatter
│   ├── nriplugin/           # NRI hook 处理
│   ├── deviceplugin/        # 计数 token 门闩
│   └── scheduler/           # Filter/Score/Reserve/PreBind
├── deploy/                  # CRD、DaemonSet、RBAC、webhook manifests
├── docs/superpowers/specs/
└── test/e2e/
```

## 8. 测试策略（TDD）

| 层 | 方法 |
|---|---|
| allocator/topology | 表驱动单测；sysfs fixture 取自真机（m602 x86 + 920B arm64）；覆盖 SMT 整核、pack/scatter、single/preferred/spread、显式 cpuset 冲突、碎片场景 |
| scheduler 插件 | framework fake client 单测：Filter（NUMA 不足/Lease 过期）、Score、Reserve/Unreserve 幂等 |
| NRI 插件 | kind（containerd 开 NRI）集成测试，断言 cgroup `cpuset.cpus`/`cpuset.mems` |
| E2E | 真机（x86 + 920B 各一）：`taskset -pc`、`numastat -p`、cgroupfs 验证绑定与内存本地性；杀 agent 验证三重防线逐层行为 |

## 9. 验收标准

1. 带 `pin: "true"` 的 Pod，容器内进程 affinity 恰为分配核，`numastat` 显示内存本地
2. `single` 策略在 NUMA 放不下时 Pod 不调度（而非降级）
3. 显式 cpuset 与已有分配冲突时调度失败且事件可读
4. 杀死 agent 后：新绑核 Pod 不调度到该节点；已运行 Pod 绑定不变
5. agent/节点重启后分配表与实际 cgroup 状态一致（对账无 drift）
6. 普通 Pod 始终运行在共享池内，独占核上无干扰进程
