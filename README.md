# Kore

Kubernetes 绑核与 NUMA 绑定系统（NRI 插件 + NUMA 感知调度器插件 + operator）。

设计文档：`docs/superpowers/specs/2026-07-08-kore-numa-design.md`

## 组件

- `kore-agent`：节点 DaemonSet——NRI 插件（容器启动前下发 cpuset）、device plugin 准入门闩、拓扑上报
- `kore-scheduler`：NUMA 感知调度器插件
- `kore-operator`：webhook 校验/注入 + agent 健康污点控制

## 开发

```
make build / make test / make generate / make manifests
```

## 使用

给 Pod 加注解 + 整数 CPU（requests==limits）即可：

```yaml
metadata:
  annotations:
    kore.zjusct.io/pin: "true"            # 独占绑核
    kore.zjusct.io/numa-policy: "single"  # single | preferred | spread
    kore.zjusct.io/memory-policy: "strict"
spec:
  containers:
  - resources:
      requests: { cpu: "8", memory: "16Gi" }
      limits:   { cpu: "8", memory: "16Gi" }
```

高级：`kore.zjusct.io/cpuset: "8-15"`（显式核号，需 nodeName/nodeSelector）、
`placement: pack|scatter`、`smt-policy: full-core|logical`。
实际绑到的核回写在 `kore.zjusct.io/allocated-cpuset` 注解。

## 部署

前置：containerd ≥2.0（NRI 默认开启）；kubelet 保持 `cpu-manager-policy=none`。

```bash
# 1. 镜像（多架构）
docker buildx build --platform linux/amd64,linux/arm64 --target agent    -t <registry>/kore-agent:TAG --push .
docker buildx build --platform linux/amd64,linux/arm64 --target scheduler -t <registry>/kore-scheduler:TAG --push .
docker buildx build --platform linux/amd64,linux/arm64 --target operator  -t <registry>/kore-operator:TAG --push .
# 2. 改 deploy/*/{daemonset,deployment}.yaml 里的镜像地址
# 3. 部署
kubectl apply -f deploy/crd -f deploy/namespace.yaml
kubectl apply -f deploy/agent -f deploy/scheduler -f deploy/operator
#    有 cert-manager：apply deploy/operator/certificate.yaml
#    没有：bash deploy/operator/gen-certs.sh
```

真机验证：

```bash
# 节点上看拓扑（无需集群）
./kore-agent --inspect --reserved 0-1
# 绑定验证
kubectl exec <pod> -- cat /sys/fs/cgroup/cpuset.cpus.effective
taskset -pc <pid>; numastat -p <pid>
```

## E2E

`make e2e-kind`（需要本机 docker）：kind 集群开 NRI → 部署三组件 → 绑核 Pod 断言
cgroup/注解一致 → 杀 agent 验证三重防线（新 Pod Pending）→ 恢复后自动跑起。
