// Package operator 实现 kore-operator：webhook 校验/注入与 agent 健康污点控制。
package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/zjusct/kore/pkg/request"
)

const schedulerName = "kore-scheduler"

// MutatePod 返回注入后的 Pod 深拷贝：设置 schedulerName、给绑核容器注入扩展资源
//（kubelet 准入门闩，spec §6 防线 2）。非 kore Pod 原样返回。
func MutatePod(pod *corev1.Pod) (*corev1.Pod, error) {
	req, err := request.ParsePod(pod)
	if err != nil {
		return nil, err
	}
	if req == nil {
		return pod, nil
	}
	out := pod.DeepCopy()
	if out.Spec.SchedulerName == "" || out.Spec.SchedulerName == "default-scheduler" {
		out.Spec.SchedulerName = schedulerName
	}
	pinned := map[string]int{}
	for _, c := range req.Containers {
		pinned[c.Name] = c.CPUs
	}
	for i := range out.Spec.Containers {
		cpus, ok := pinned[out.Spec.Containers[i].Name]
		if !ok {
			continue
		}
		q := *resource.NewQuantity(int64(cpus), resource.DecimalSI)
		res := &out.Spec.Containers[i].Resources
		if res.Requests == nil {
			res.Requests = corev1.ResourceList{}
		}
		if res.Limits == nil {
			res.Limits = corev1.ResourceList{}
		}
		res.Requests[corev1.ResourceName(request.ExtendedResource)] = q
		res.Limits[corev1.ResourceName(request.ExtendedResource)] = q
	}
	return out, nil
}

// ValidatePod 校验 kore 注解；非 kore Pod 恒通过。
func ValidatePod(pod *corev1.Pod) error {
	_, err := request.ParsePod(pod)
	return err
}

type mutateHandler struct{ decoder admission.Decoder }

func NewMutateHandler(scheme *runtime.Scheme) admission.Handler {
	return &mutateHandler{decoder: admission.NewDecoder(scheme)}
}

func (h *mutateHandler) Handle(ctx context.Context, req admission.Request) admission.Response {
	pod := &corev1.Pod{}
	if err := h.decoder.Decode(req, pod); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	out, err := MutatePod(pod)
	if err != nil {
		return admission.Denied(fmt.Sprintf("kore: %v", err))
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, raw)
}

type validateHandler struct{ decoder admission.Decoder }

func NewValidateHandler(scheme *runtime.Scheme) admission.Handler {
	return &validateHandler{decoder: admission.NewDecoder(scheme)}
}

func (h *validateHandler) Handle(ctx context.Context, req admission.Request) admission.Response {
	pod := &corev1.Pod{}
	if err := h.decoder.Decode(req, pod); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	if err := ValidatePod(pod); err != nil {
		return admission.Denied(fmt.Sprintf("kore: %v", err))
	}
	return admission.Allowed("")
}
