# Kore

Kubernetes 的 CPU 绑核、NUMA 绑定与 CPU 池系统——给 Pod 提供 `numactl` 级别的
核心独占与内存本地化能力，绑定发生在**容器启动之前**（进程第一条指令就在正确的
核心上，内存首次分配即本地）。为 ZJUSCT 集群构建，通用于任何 containerd ≥ 2.0
的集群。

```
用户 Pod（注解）─► kore-scheduler（NUMA 感知调度：容量过滤/binpack/预占）
                └► kore-webhook（注入调度器名 + 准入门闩资源）
落节点后 ─► containerd ─► NRI ─► kore-agent（选核，启动前写 cpuset.cpus/mems）
                                    └► KoreNodeTopology CR（账本回报调度器）
```

## 特性

- **独占绑核**：整数 CPU 的容器独占核心，SMT 机器默认整物理核分配（sibling 成对）
- **NUMA 绑定**：`single`（同 NUMA + 严格本地内存）/ `preferred` / `spread`
- **CPU 池**：一组 Pod 共享一块固定大小的专属核心区（对外独占、池内共享），
  支持**在线扩缩容**（成员零重启）与原生 CFS 组合超售
- **全链路 NUMA 感知调度**：调度器知道每节点每 NUMA 的剩余核心，Pod 保证落到
  放得下的地方
- **严格绑定保证（三重防线）**：agent 失活时调度排除 + 节点污点 + kubelet 准入
  门闩，绑核 Pod 绝不在无绑定状态下运行
- 共享池围栏：非 Kore Pod 自动限制在剩余核心，永远踩不进独占区
- 观测性：每节点 Prometheus 指标 + `kubectl kore` CLI

## 预编译镜像

多架构（linux/amd64 + linux/arm64，含鲲鹏/ARM 服务器）：

| 镜像 | 说明 |
|---|---|
| `docker.io/mrhaoxx/kore-agent:v0.3.0` | 节点 DaemonSet（NRI 插件 + device plugin + 拓扑上报） |
| `docker.io/mrhaoxx/kore-scheduler:v0.3.0` | 内嵌 Kore 插件的完整 kube-scheduler |
| `docker.io/mrhaoxx/kore-operator:v0.3.0` | webhook 注入/校验 + agent 失活污点控制 |

deploy/ 里的 manifests 已指向这些镜像，可直接部署；自建镜像见文末「开发」。

## 快速开始

前置条件：

- containerd ≥ 2.0（NRI 默认开启；1.7 需手动开 `io.containerd.nri.v1.nri`）
- kubelet 保持默认 `cpu-manager-policy=none`（cpuset 管理权交给 Kore）
- cert-manager（webhook 证书；没有的话用 `deploy/operator/gen-certs.sh` 自签）

```bash
kubectl apply -f deploy/crd -f deploy/namespace.yaml
kubectl apply -f deploy/agent -f deploy/scheduler -f deploy/operator
# 无 cert-manager 则删掉 operator/certificate.yaml 那次 apply，改跑：
#   bash deploy/operator/gen-certs.sh
kubectl -n kore-system rollout status ds/kore-agent
```

按需修改：`deploy/agent/configmap.yaml`（系统预留核等，见下文配置）、DaemonSet 的
tolerations/nodeSelector（默认容忍 `kore.zjusct.io/agent-down` 与
`node.zjusct.io/remote`）。

## 使用

### 独占绑核

整数 CPU（requests == limits）+ 一个注解：

```yaml
apiVersion: v1
kind: Pod
metadata:
  annotations:
    kore.zjusct.io/pin: "true"
spec:
  containers:
  - name: app
    resources:
      requests: { cpu: "8", memory: "16Gi" }
      limits:   { cpu: "8", memory: "16Gi" }   # CPU 必须整数且两者相等
```

其余全自动：webhook 注入 `schedulerName: kore-scheduler` 和门闩资源
`kore.zjusct.io/cpu`，调度器选 NUMA，agent 在容器启动前绑定。实际绑到的核回写在
Pod 注解 `kore.zjusct.io/allocated-cpuset`。

多容器 Pod：所有整数 CPU 的容器各自独占绑核（优先同 NUMA）；非整数 CPU 的
sidecar 落共享池；init 容器不绑。

### CPU 池

一组 Pod 共享固定大小的专属核心区（对外独占、池内自由竞争）：

```yaml
metadata:
  annotations:
    kore.zjusct.io/pool: "team-hpl"     # 池名（DNS label 格式）
    kore.zjusct.io/pool-size: "64"      # 池大小
spec:
  containers:
  - resources:
      requests: { cpu: "2" }             # 池成员不要求整数 CPU
      limits:   { cpu: "32" }
```

- 首个成员到达节点时按 `numa-policy` 建池；后续同名成员自动调度到同一节点、
  进同一批核；末位成员退出时池自动释放
- **在线扩缩容**：改注解里的 `pool-size` 后重建任意一个成员 Pod（更晚的
  creationTimestamp 是扩缩容的授权依据，防陈旧注解回灌）——其余成员的 cpuset
  被 NRI 原地更新，**零重启**
- **池内超售**：池边界（cpuset）与原生 CFS 正交叠加，令
  `Σlimits > pool-size ≥ Σrequests` 即可——空闲时成员冲到各自 limits，争抢时按
  requests 权重回落；因池对外独占，权重竞争天然只发生在成员之间

### 显式核号（逃生舱）

配合 isolcpus/IRQ 调优等需要精确核号的场景：

```yaml
metadata:
  annotations:
    kore.zjusct.io/pin: "true"
    kore.zjusct.io/cpuset: "8-15"        # 显式核号
spec:
  nodeName: m602.clusters.zjusct.io      # 必须指定节点（核号只在具体机器上有意义）
```

冲突时先到先得，后来者调度失败并出事件。

## 注解参考

### 用户可写（Pod 上）

| 注解 | 取值 | 默认 | 说明 |
|---|---|---|---|
| `kore.zjusct.io/pin` | `"true"` | — | 启用独占绑核。要求 ≥1 个容器 CPU 整数且 requests==limits |
| `kore.zjusct.io/numa-policy` | `single` \| `preferred` \| `spread` | `single` | `single`：全部核同一 NUMA，放不下不调度（绝不降级）；`preferred`：尽量单 NUMA，不够按距离溢出；`spread`：均分多 NUMA（带宽敏感型） |
| `kore.zjusct.io/memory-policy` | `strict` \| `preferred` | `strict` | `strict`：`cpuset.mems` 仅含分配 NUMA（≈`numactl --membind`）；`preferred`：mems 全开靠 first-touch |
| `kore.zjusct.io/placement` | `pack` \| `scatter` | 集群默认（`pack`） | NUMA 内紧凑分配 or 打散（缓存竞争敏感用 scatter） |
| `kore.zjusct.io/smt-policy` | `full-core` \| `logical` | 集群默认（`full-core`） | SMT 机器上整物理核（sibling 成对，真独占；核数需被每核线程数整除）or 按逻辑核（接受 sibling 干扰换装箱率） |
| `kore.zjusct.io/cpuset` | 如 `"8-15,40-47"` | — | 显式核号。需 nodeName/nodeSelector；与 numa-policy/placement/pool 互斥；仅限单绑核容器且核数等于 CPU 请求 |
| `kore.zjusct.io/pool` | 池名（DNS label） | — | 加入命名 CPU 池。与 pin/cpuset 互斥 |
| `kore.zjusct.io/pool-size` | 正整数 | — | 池大小，pool 必配。变更 + 重建一个成员 = 在线扩缩容 |

### 系统回写（只读）

| 注解 | 写入者 | 说明 |
|---|---|---|
| `kore.zjusct.io/reserved-numa` | kore-scheduler | 调度时选中的 NUMA zone |
| `kore.zjusct.io/allocated-cpuset` | kore-agent | 实际绑定的核号（`kubectl get pod -o yaml` 即可见） |

### 内部机制（用户不用管）

| 资源/注解 | 说明 |
|---|---|
| `kore.zjusct.io/cpu`（扩展资源） | webhook 自动注入绑核容器的 requests/limits。kubelet 准入门闩：agent 失活时该资源不可分配，绑核 Pod 拒绝启动而非无绑定运行。可用 ResourceQuota 按 namespace 限制绑核总量。池成员不注入（成员共享，逐 Pod 计数会超卖 token） |

## agent 配置（ConfigMap `kore-agent-config`）

```yaml
reservedSystemCpus: ""        # 系统预留核（cpulist），不参与独占分配与共享池；空 = 不预留
defaultPlacement: pack        # pack | scatter（注解未写时的集群默认）
defaultSMTPolicy: full-core   # full-core | logical
remediation: strict           # strict | repair：对账发现"该绑未绑"的容器时
                              #   strict = 删 Pod 重建（默认，绝不静默放行）
                              #   repair = NRI 事后补绑 + 警告事件（内存可能非本地）
sharedPoolMin: 0              # 全局共享池保底核数：独占分配/建池会把共享池压到
                              #   低于此值时直接拒绝（保护节点上的普通负载）
```

改配置后 `kubectl -n kore-system rollout restart ds/kore-agent` 生效（存量绑定经
Synchronize 无损恢复）。

## 观测

```bash
# CLI（编译一次装进 PATH，kubectl 自动识别为插件）
make kubectl-kore && sudo cp bin/kubectl-kore /usr/local/bin/
kubectl kore nodes                # 全集群账本：每节点独占/池/共享/各 zone 剩余
kubectl kore pools                # 所有 CPU 池及成员数
kubectl kore pod <ns> <name>      # 单 Pod 绑定详情（注解 + 节点账本交叉核对）
kubectl kore top                  # 实时 TUI：每个核心一个格子（SMT 成列堆叠），
                                  #   颜色+字母标注占用者（独占 Pod/池），2s 刷新
kubectl kore top --once           # 单帧快照（脚本/截图用）

# Prometheus（agent :9100，DaemonSet 已带 prometheus.io/scrape 注解）
kore_cpus_exclusive / kore_cpus_pooled / kore_cpus_shared
kore_pool_size{pool} / kore_pool_members{pool}
kore_allocation_failures_total{kind=pin|pool} / kore_remediations_total{mode}

# 原始账本
kubectl get knt                   # KoreNodeTopology，每节点一个
kubectl exec <pod> -- cat /sys/fs/cgroup/cpuset.cpus.effective   # 容器内自查
```

## 故障语义（三重防线）

agent 失活时，绑核/池 Pod 的处置顺序：

1. **调度层**：agent Lease 过期 → kore-scheduler 过滤该节点 + operator 打
   `kore.zjusct.io/agent-down:NoSchedule` 污点（存量 Pod 不驱逐、绑定不受影响）
2. **kubelet 层**：device plugin 端点消失 → 已绑定到该节点的绑核 Pod 拒绝启动
   （`UnexpectedAdmissionError`），fail-fast 交给上层 controller 重试
3. **对账兜底**：agent 恢复后 Synchronize 重建账本；发现无绑定运行的容器按
   `remediation` 处置

设计取舍与完整架构见 `docs/superpowers/specs/2026-07-08-kore-numa-design.md`。

## 开发

```bash
make test                # 全部单测（~150 用例，含 manifests 解析校验）
make build               # 全部二进制
make generate manifests  # CRD 代码/清单再生成
make e2e-kind            # kind 端到端（需本机 docker）：绑核/池/扩缩容/杀 agent 防线
# 自建多架构镜像
docker buildx build --platform linux/amd64,linux/arm64 --target agent \
  -t <registry>/kore-agent:TAG --push .   # target: agent | scheduler | operator
```

真机冒烟（无需集群）：`./kore-agent --inspect --reserved 0-1` 打印本机 NUMA 拓扑。
