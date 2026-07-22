package admissioncheck

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/zjusct/kore/pkg/request"
)

// 最小 Kueue 类型：只声明本控制器读写的字段，用 unstructured watch/patch，靠
// json tag 与 runtime.DefaultUnstructuredConverter 互转，无需引官方 kueue 模块
// 或手写 DeepCopy（本仓库为调度器已有 33 个 staging replace，避免依赖冲突）。

// 用 v1beta2（集群 storage 版本；v1beta1 已弃用为 served-only）。字段名两版一致。
var (
	WorkloadGVK       = schema.GroupVersionKind{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "Workload"}
	ResourceFlavorGVK = schema.GroupVersionKind{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "ResourceFlavor"}
	AdmissionCheckGVK = schema.GroupVersionKind{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "AdmissionCheck"}
)

// ControllerName 是本控制器在 AdmissionCheck.spec.controllerName 上认领的名字。
const ControllerName = "kore.zjusct.io/admissioncheck"

type Workload struct {
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              WorkloadSpec   `json:"spec,omitempty"`
	Status            WorkloadStatus `json:"status,omitempty"`
}

type WorkloadSpec struct {
	PodSets   []PodSet `json:"podSets,omitempty"`
	QueueName string   `json:"queueName,omitempty"`
}

type PodSet struct {
	Name     string                 `json:"name"`
	Count    int32                  `json:"count"`
	Template corev1.PodTemplateSpec `json:"template,omitempty"`
}

type WorkloadStatus struct {
	Admission       *Admission            `json:"admission,omitempty"`
	AdmissionChecks []AdmissionCheckState `json:"admissionChecks,omitempty"`
	Conditions      []metav1.Condition    `json:"conditions,omitempty"`
}

// Finished 报告作业是否已结束（Kueue 置 Finished=True）。
func (w *Workload) Finished() bool {
	for i := range w.Status.Conditions {
		c := &w.Status.Conditions[i]
		if c.Type == "Finished" && c.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

type Admission struct {
	ClusterQueue      string             `json:"clusterQueue,omitempty"`
	PodSetAssignments []PodSetAssignment `json:"podSetAssignments,omitempty"`
}

type PodSetAssignment struct {
	Name    string                         `json:"name"`
	Flavors map[corev1.ResourceName]string `json:"flavors,omitempty"`
}

type AdmissionCheckState struct {
	Name               string      `json:"name"`
	State              string      `json:"state"`
	Message            string      `json:"message,omitempty"`
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`
}

type ResourceFlavor struct {
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ResourceFlavorSpec `json:"spec,omitempty"`
}

type ResourceFlavorSpec struct {
	NodeLabels map[string]string `json:"nodeLabels,omitempty"`
}

// Kueue AdmissionCheck 状态字面量。
const (
	CheckPending  = "Pending"
	CheckReady    = "Ready"
	CheckRetry    = "Retry"
	CheckRejected = "Rejected"
)

// parsePinReq 从一个 PodSet 解析 pin 诉求。复用 request.ParsePod：把 PodSet 模板
// 当成一个 Pod 解析（pin / numa-policy / smt-policy / 各容器 cpu）。need = Σ 容器核。
func parsePinReq(ps *PodSet) (Req, error) {
	pod := &corev1.Pod{ObjectMeta: ps.Template.ObjectMeta, Spec: ps.Template.Spec}
	r, err := request.ParsePod(pod)
	if err != nil {
		return Req{}, err
	}
	if r == nil || r.Pool != "" { // 非 pin（普通/池共享）→ 本 check 放行
		return Req{Pin: false, Count: int(ps.Count)}, nil
	}
	need := 0
	for _, c := range r.Containers {
		need += c.CPUs
	}
	return Req{
		Pin:        true,
		NeedPerRep: need,
		Count:      int(ps.Count),
		NUMAPolicy: r.NUMAPolicy,
		SMTPolicy:  r.SMTPolicy,
	}, nil
}

// flavorForPodSet 返回某 PodSet 分到的 cpu ResourceFlavor 名（QuotaReserved 后由
// status.admission 给出）。
func flavorForPodSet(wl *Workload, podSet string) string {
	if wl.Status.Admission == nil {
		return ""
	}
	for _, a := range wl.Status.Admission.PodSetAssignments {
		if a.Name == podSet {
			return a.Flavors[corev1.ResourceCPU]
		}
	}
	return ""
}
