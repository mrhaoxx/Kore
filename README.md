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
