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

	SMTFullCore SMTPolicy = "full-core"
	SMTLogical  SMTPolicy = "logical"
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
