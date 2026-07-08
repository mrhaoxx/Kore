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
	ReservedSystemCpus string       `json:"reservedSystemCpus,omitempty"`
	Zones              []Zone       `json:"zones,omitempty"`
	Allocations        []Allocation `json:"allocations,omitempty"`
	// Pools 是节点上的命名 CPU 池（成员共享、对外独占）。
	Pools []Pool `json:"pools,omitempty"`
}

type Pool struct {
	Name   string `json:"name"`
	Cpuset string `json:"cpuset"`
	NUMA   []int  `json:"numa"`
	// Members 是成员 Pod 的 UID（调度器预占清理用）。
	Members []string `json:"members"`
}

type Zone struct {
	ID          int               `json:"id"`
	Cpus        string            `json:"cpus"`
	Allocatable int               `json:"allocatable"`
	FreeCpus    string            `json:"freeCpus"`
	MemoryTotal resource.Quantity `json:"memoryTotal,omitempty"`
	// SMTSiblings 是本 zone 内的 sibling 组（无 SMT 则空）。
	SMTSiblings [][]int `json:"smtSiblings,omitempty"`
	// Devices 为 v2 预留（NPU/网卡 NUMA 归属）。
	Devices []Device `json:"devices,omitempty"`
}

type Device struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Allocation struct {
	PodUID string `json:"podUID"`
	// Pod 格式为 namespace/name。
	Pod       string `json:"pod"`
	Container string `json:"container"`
	Cpuset    string `json:"cpuset"`
	NUMA      []int  `json:"numa"`
}
