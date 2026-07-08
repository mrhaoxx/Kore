# kore-operator 与部署（Plan 4/4）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 补齐最后一个组件 kore-operator（webhook 校验/注入 + agent-down 污点控制器），产出全套可部署物（Dockerfile、manifests、部署文档）与 kind E2E 脚本，项目代码侧完结。

**Architecture:** spec §3 operator 职责。webhook 逻辑做成纯函数（MutatePod/ValidatePod）+ controller-runtime admission Handler 薄包装；污点控制用 controller-runtime Reconciler watch kore-system 的 agent Lease。

**Tech Stack:** controller-runtime（webhook admission + manager + fake client）、cert-manager（webhook 证书，附无 cert-manager 的 gen-certs.sh 备选）

## Global Constraints

- 继承前三个计划全部约束
- webhook `failurePolicy: Ignore`（spec §6：webhook 挂了不阻塞集群，agent 侧有二次校验）
- 污点：`kore.zjusct.io/agent-down:NoSchedule`（不驱逐存量）
- 本机无 docker daemon：kind E2E 交付为**可执行脚本 + Make target**，语法校验（bash -n）+ 文档，实际运行留给有 docker 的环境
- manifests 离线校验：`kubectl create --dry-run=client --validate=false -f <dir> -R`

---

### Task 1: webhook 逻辑（pkg/operator/webhook.go）

**Files:** Create `pkg/operator/webhook.go`、Test `pkg/operator/webhook_test.go`

**Interfaces:**
- `operator.MutatePod(pod *corev1.Pod) (*corev1.Pod, error)`：非 kore Pod 原样返回；kore Pod 深拷贝后 (a) `schedulerName` 为空或 `default-scheduler` 时设为 `kore-scheduler`；(b) 每个绑核容器 requests+limits 注入 `kore.zjusct.io/cpu = <整数核数>`；注解非法返回 error
- `operator.ValidatePod(pod *corev1.Pod) error`：ParsePod 的校验结果
- `operator.NewMutateHandler(scheme) admission.Handler`、`NewValidateHandler(scheme) admission.Handler`（controller-runtime admission：Handle 里 decode Pod → 调纯函数 → PatchResponseFromRaw / Allowed / Denied）

**Steps（TDD）:**
- [ ] 失败测试：表驱动覆盖——非 kore Pod 不变；pin Pod 注入 schedulerName + 扩展资源（数量=cpu 请求）；已显式写 schedulerName 的不覆盖（非 default）；sidecar 容器不注入；非法注解 Mutate/Validate 均报错；Handler 层：mutate 请求返回 patch、validate 非法返回 Denied
- [ ] 实现 → 测试通过 → Commit `feat: operator webhook 校验与注入`

### Task 2: agent-down 污点控制器（pkg/operator/taint.go）

**Files:** Create `pkg/operator/taint.go`、Test `pkg/operator/taint_test.go`

**Interfaces:**
- `operator.LeaseExpired(l *coordv1.Lease, now time.Time) bool`（RenewTime+Duration 判断；缺字段视为过期）
- `operator.TaintReconciler{Client, now func() time.Time}`：Reconcile(lease) → 解析节点名（`kore-agent-` 前缀）→ 过期加污点/新鲜摘污点（幂等）→ `RequeueAfter: 5s`
- 污点常量 `TaintKey = "kore.zjusct.io/agent-down"`

**Steps（TDD）:**
- [ ] 失败测试（controller-runtime fake client + 注入时钟）：过期 Lease → 节点获得污点；续约后 Reconcile → 污点摘除；重复 Reconcile 幂等；无对应节点不报错
- [ ] 实现 → 测试通过 → Commit `feat: agent Lease 污点控制器`

### Task 3: cmd/kore-operator 接线

**Files:** Create `cmd/kore-operator/main.go`

- [ ] manager（webhook server :9443，证书目录 /tmp/k8s-webhook-server/serving-certs 默认）+ 注册 `/mutate-pod`、`/validate-pod` 路由 + TaintReconciler（For coordv1.Lease，namespace kore-system 过滤）
- [ ] 验证：`make build` + 双架构交叉编译
- [ ] Commit `feat: kore-operator 主程序接线`

### Task 4: Dockerfile 与部署 manifests

**Files:** Create `Dockerfile`、`deploy/namespace.yaml`、`deploy/agent/{daemonset,rbac,configmap}.yaml`、`deploy/scheduler/{deployment,rbac,configmap}.yaml`、`deploy/operator/{deployment,rbac,webhook,certificate}.yaml`、`deploy/operator/gen-certs.sh`、README 部署章节

**精确要求（每项都必须落实）：**
- Dockerfile：多阶段（golang:1.26 build，`CGO_ENABLED=0`，`TARGETARCH` 支持 buildx 多架构）→ 三个 final stage（distroless/static），target 名 `agent`/`scheduler`/`operator`
- agent DaemonSet（ns kore-system）：hostPath 挂载 `/var/run/nri/nri.sock`、`/var/lib/kubelet/device-plugins`、`/sys`(ro)、ConfigMap 挂 `/etc/kore/config.yaml`；env `NODE_NAME` from fieldRef；`priorityClassName: system-node-critical`；仅 compute 节点可用 nodeSelector 留注释
- agent RBAC：pods get/list/watch/patch/delete；events create/patch；leases get/create/update；korenodetopologies + status 全 CRUD；serviceaccount kore-agent
- scheduler Deployment：`--config` 挂 ConfigMap（内容=deploy/scheduler/kube-scheduler-config.yaml）+ `--authentication-skip-lookup=true`；RBAC：绑 `system:kube-scheduler`/`system:volume-scheduler` ClusterRole + extension-apiserver-authentication configmap 读 + korenodetopologies 读 + leases 读 + pods patch
- operator Deployment：webhook 证书 secret 挂载；Service 9443；Mutating/ValidatingWebhookConfiguration：`failurePolicy: Ignore`、`objectSelector` 不限、rules 仅 pods CREATE、`sideEffects: None`、cert-manager `cert-manager.io/inject-ca-from` 注解 + Certificate 资源；gen-certs.sh：openssl 自签 + kubectl 建 secret + caBundle patch（无 cert-manager 的备选，脚本可执行、set -euo pipefail）
- operator RBAC：nodes get/patch；leases get/list/watch
- [ ] 验证：`kubectl create --dry-run=client --validate=false -f deploy -R` 全部通过；`bash -n deploy/operator/gen-certs.sh`
- [ ] Commit `feat: Dockerfile 与全套部署 manifests`

### Task 5: kind E2E 脚本与收尾

**Files:** Create `test/e2e/kind-e2e.sh`、`test/e2e/kind-config.yaml`、`test/e2e/testdata/pinned-pod.yaml`、Makefile 加 `e2e-kind` target、README E2E 章节

**脚本必须实现的步骤（每步带失败即退出与清晰输出）：**
1. kind create cluster（kind-config.yaml 用 containerdConfigPatches 开 NRI：`[plugins."io.containerd.nri.v1.nri"] disable = false`）
2. `docker buildx build`（或 build）三镜像 + `kind load docker-image`
3. apply CRD → namespace → gen-certs.sh（kind 无 cert-manager）→ 全部 manifests
4. 等 agent/scheduler/operator Ready（kubectl wait，超时 120s）
5. 提交 testdata/pinned-pod.yaml（pin=true、cpu 2、numa-policy single——kind 单 NUMA 也可验证绑核）
6. 断言：pod Running；`kubectl exec` 读 `/sys/fs/cgroup/cpuset.cpus.effective` 非空且核数=2；pod 注解 `kore.zjusct.io/allocated-cpuset` 存在且与 cgroup 一致
7. 负路径：kill agent（delete DaemonSet）→ 再建绑核 Pod → 必须 Pending（三重防线）；恢复 DaemonSet → Pod 跑起来
8. kind delete cluster（trap 清理）
- [ ] 验证：`bash -n` 两个脚本/配置 YAML dry-run；Makefile target 存在
- [ ] `make test`、双架构交叉编译最后全量回归
- [ ] Commit `feat: kind E2E 脚本与部署文档`

## Plan 4 完成标准

- operator 单测全绿；全仓 `make test` 全绿；双架构编译干净
- `kubectl create --dry-run=client` 校验 deploy/ 全部通过
- kind E2E 脚本语法正确、步骤完整（实际执行需 docker daemon，文档写明 `make e2e-kind`）
- README 含完整部署指南（含真机验证步骤：`--inspect`、taskset、numastat）
