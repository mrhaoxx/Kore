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
