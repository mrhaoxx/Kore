package nriplugin

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/cpuset"

	"github.com/zjusct/kore/pkg/agent/config"
	"github.com/zjusct/kore/pkg/request"
	"github.com/zjusct/kore/pkg/topology"
)

// twoZoneTopo: zone0={0-3} zone1={4-7}，无 SMT。
func twoZoneTopo() *topology.Topology {
	sib := map[int]cpuset.CPUSet{}
	for i := 0; i < 8; i++ {
		sib[i] = cpuset.New(i)
	}
	return &topology.Topology{
		Zones: []topology.Zone{
			{ID: 0, CPUs: cpuset.New(0, 1, 2, 3), MemoryTotalBytes: 1 << 30, Distances: []int{10, 20}},
			{ID: 1, CPUs: cpuset.New(4, 5, 6, 7), MemoryTotalBytes: 1 << 30, Distances: []int{20, 10}},
		},
		Siblings: sib, ThreadsPerCore: 1,
	}
}

func pinnedPod(annos map[string]string) *corev1.Pod {
	a := map[string]string{request.AnnoPin: "true"}
	for k, v := range annos {
		a[k] = v
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default", UID: "uid-p", Annotations: a},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "app",
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
			},
		}}},
	}
}

func TestMemsFor(t *testing.T) {
	topo := twoZoneTopo()
	if got := MemsFor(request.MemStrict, []int{1}, topo); got != "1" {
		t.Fatalf("strict = %q", got)
	}
	if got := MemsFor(request.MemPreferred, []int{1}, topo); got != "0-1" {
		t.Fatalf("preferred = %q", got)
	}
}

func TestBuildAllocRequest(t *testing.T) {
	cfg, _ := config.Load("")
	kpod := pinnedPod(map[string]string{request.AnnoReservedNUMA: "1"})
	req, err := request.ParsePod(kpod)
	if err != nil {
		t.Fatal(err)
	}
	ar, pinned := BuildAllocRequest(kpod, req, cfg, "app")
	if !pinned {
		t.Fatal("app should be pinned")
	}
	if ar.CPUs != 2 || ar.PodUID != "uid-p" || ar.Pod != "default/p" || ar.Container != "app" {
		t.Fatalf("%+v", ar)
	}
	if ar.ReservedNUMA == nil || *ar.ReservedNUMA != 1 {
		t.Fatalf("reservedNUMA = %v", ar.ReservedNUMA)
	}
	// 注解未设 placement/smt → 用 cfg 默认
	if ar.Placement != request.PlacementPack || ar.SMTPolicy != request.SMTFullCore {
		t.Fatalf("effective policies: %+v", ar)
	}
	if _, pinned := BuildAllocRequest(kpod, req, cfg, "sidecar"); pinned {
		t.Fatal("unknown container must not be pinned")
	}
}

func TestBuildAllocRequestAnnotationOverridesConfig(t *testing.T) {
	cfg, _ := config.Load("")
	cfg.DefaultPlacement = "scatter"
	kpod := pinnedPod(map[string]string{request.AnnoPlacement: "pack"})
	req, _ := request.ParsePod(kpod)
	ar, _ := BuildAllocRequest(kpod, req, cfg, "app")
	if ar.Placement != request.PlacementPack {
		t.Fatalf("annotation must override config, got %v", ar.Placement)
	}
}
