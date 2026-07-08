package operator

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/zjusct/kore/pkg/request"
)

func webhookPod(annos map[string]string, mutate func(*corev1.Pod)) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default", Annotations: annos},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "app",
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
			},
		}}},
	}
	if mutate != nil {
		mutate(p)
	}
	return p
}

func TestMutatePod(t *testing.T) {
	t.Run("非 kore Pod 不变", func(t *testing.T) {
		in := webhookPod(nil, nil)
		out, err := MutatePod(in)
		if err != nil {
			t.Fatal(err)
		}
		if out.Spec.SchedulerName != "" {
			t.Fatalf("must not touch: %+v", out.Spec.SchedulerName)
		}
	})
	t.Run("pin Pod 注入调度器与扩展资源", func(t *testing.T) {
		in := webhookPod(map[string]string{request.AnnoPin: "true"}, nil)
		out, err := MutatePod(in)
		if err != nil {
			t.Fatal(err)
		}
		if out.Spec.SchedulerName != "kore-scheduler" {
			t.Fatalf("schedulerName = %q", out.Spec.SchedulerName)
		}
		q := out.Spec.Containers[0].Resources.Limits[corev1.ResourceName(request.ExtendedResource)]
		if q.Value() != 4 {
			t.Fatalf("extended resource = %v", q)
		}
		rq := out.Spec.Containers[0].Resources.Requests[corev1.ResourceName(request.ExtendedResource)]
		if rq.Value() != 4 {
			t.Fatalf("requests extended resource = %v", rq)
		}
		if in.Spec.SchedulerName != "" {
			t.Fatal("input must not be mutated in place")
		}
	})
	t.Run("显式 schedulerName 不覆盖", func(t *testing.T) {
		in := webhookPod(map[string]string{request.AnnoPin: "true"}, func(p *corev1.Pod) {
			p.Spec.SchedulerName = "my-sched"
		})
		out, err := MutatePod(in)
		if err != nil {
			t.Fatal(err)
		}
		if out.Spec.SchedulerName != "my-sched" {
			t.Fatalf("overwrote user schedulerName: %q", out.Spec.SchedulerName)
		}
	})
	t.Run("default-scheduler 被替换", func(t *testing.T) {
		in := webhookPod(map[string]string{request.AnnoPin: "true"}, func(p *corev1.Pod) {
			p.Spec.SchedulerName = "default-scheduler"
		})
		out, _ := MutatePod(in)
		if out.Spec.SchedulerName != "kore-scheduler" {
			t.Fatalf("%q", out.Spec.SchedulerName)
		}
	})
	t.Run("sidecar 不注入", func(t *testing.T) {
		in := webhookPod(map[string]string{request.AnnoPin: "true"}, func(p *corev1.Pod) {
			p.Spec.Containers = append(p.Spec.Containers, corev1.Container{
				Name: "sidecar",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
					Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
				},
			})
		})
		out, err := MutatePod(in)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := out.Spec.Containers[1].Resources.Limits[corev1.ResourceName(request.ExtendedResource)]; ok {
			t.Fatal("sidecar must not get extended resource")
		}
	})
	t.Run("非法注解报错", func(t *testing.T) {
		in := webhookPod(map[string]string{request.AnnoPin: "true", request.AnnoNUMAPolicy: "bogus"}, nil)
		if _, err := MutatePod(in); err == nil {
			t.Fatal("expected error")
		}
		if err := ValidatePod(in); err == nil || !strings.Contains(err.Error(), "numa-policy") {
			t.Fatalf("validate err = %v", err)
		}
	})
}

func admissionReq(t *testing.T, pod *corev1.Pod) admission.Request {
	t.Helper()
	raw, err := json.Marshal(pod)
	if err != nil {
		t.Fatal(err)
	}
	return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Object: runtime.RawExtension{Raw: raw},
	}}
}

func TestHandlers(t *testing.T) {
	sch := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	mut := NewMutateHandler(sch)
	val := NewValidateHandler(sch)

	// mutate：pin Pod 应产生 patch
	resp := mut.Handle(context.Background(), admissionReq(t, webhookPod(map[string]string{request.AnnoPin: "true"}, nil)))
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("mutate resp: allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
	}
	// validate：非法注解 → Denied
	resp = val.Handle(context.Background(), admissionReq(t, webhookPod(map[string]string{request.AnnoPin: "maybe"}, nil)))
	if resp.Allowed {
		t.Fatal("invalid pod must be denied")
	}
	// validate：普通 Pod → Allowed
	resp = val.Handle(context.Background(), admissionReq(t, webhookPod(nil, nil)))
	if !resp.Allowed {
		t.Fatalf("plain pod must pass: %v", resp.Result)
	}
}
