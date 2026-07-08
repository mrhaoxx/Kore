package nriplugin

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/containerd/nri/pkg/api"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/zjusct/kore/pkg/agent/config"
	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
	"github.com/zjusct/kore/pkg/request"
)

func mustQ(s string) resource.Quantity { return resource.MustParse(s) }

func adjCpus(a *api.ContainerAdjustment) string {
	return a.GetLinux().GetResources().GetCpu().GetCpus()
}
func adjMems(a *api.ContainerAdjustment) string {
	return a.GetLinux().GetResources().GetCpu().GetMems()
}
func updCpus(u *api.ContainerUpdate) string { return u.GetLinux().GetResources().GetCpu().GetCpus() }

type fakePods struct{ pods map[string]*corev1.Pod }

func (f *fakePods) GetPod(ns, name string) (*corev1.Pod, error) {
	if p, ok := f.pods[ns+"/"+name]; ok {
		return p, nil
	}
	return nil, errors.New("not found")
}

type fakeRec struct {
	events      []string
	annotations map[string]string
	deleted     []string
}

func (f *fakeRec) Event(pod *corev1.Pod, et, reason, msg string) {
	f.events = append(f.events, reason)
}
func (f *fakeRec) SetPodAnnotation(pod *corev1.Pod, k, v string) {
	if f.annotations == nil {
		f.annotations = map[string]string{}
	}
	f.annotations[k] = v
}
func (f *fakeRec) DeletePod(ns, name string) { f.deleted = append(f.deleted, ns+"/"+name) }

type fakeRep struct {
	last *v1alpha1.KoreNodeTopologyStatus
}

func (f *fakeRep) Report(st v1alpha1.KoreNodeTopologyStatus) { f.last = &st }

func newTestPlugin(t *testing.T, pods ...*corev1.Pod) (*Plugin, *fakeRec, *fakeRep) {
	t.Helper()
	cfg, _ := config.Load("")
	cfg.ReservedSystemCpus = "0"
	fp := &fakePods{pods: map[string]*corev1.Pod{}}
	for _, p := range pods {
		fp.pods[p.Namespace+"/"+p.Name] = p
	}
	rec, rep := &fakeRec{}, &fakeRep{}
	p, err := New(twoZoneTopo(), cfg, fp, rec, rep)
	if err != nil {
		t.Fatal(err)
	}
	return p, rec, rep
}

func sandbox(uid string, annos map[string]string) *api.PodSandbox {
	return &api.PodSandbox{Id: "sb-" + uid, Name: "p", Uid: uid, Namespace: "default", Annotations: annos}
}

func ctr(id, sandboxID, name string) *api.Container {
	return &api.Container{Id: id, PodSandboxId: sandboxID, Name: name, State: api.ContainerState_CONTAINER_RUNNING}
}

func TestCreateContainerFencesNonKorePod(t *testing.T) {
	p, _, _ := newTestPlugin(t)
	adj, updates, err := p.CreateContainer(context.Background(), sandbox("u9", nil), ctr("c9", "sb-u9", "web"))
	if err != nil {
		t.Fatal(err)
	}
	if got := adjCpus(adj); got != "1-7" { // 全部核 − 预留 {0}
		t.Fatalf("shared cpus = %q", got)
	}
	if adjMems(adj) != "" {
		t.Fatal("shared containers must not get mems pinning")
	}
	if len(updates) != 0 {
		t.Fatalf("no prior shared containers to update, got %d", len(updates))
	}
}

func TestCreateContainerPinsAndShrinksShared(t *testing.T) {
	kpod := pinnedPod(map[string]string{request.AnnoReservedNUMA: "1"})
	p, rec, rep := newTestPlugin(t, kpod)
	// 先来一个共享容器
	_, _, err := p.CreateContainer(context.Background(), sandbox("u9", nil), ctr("c9", "sb-u9", "web"))
	if err != nil {
		t.Fatal(err)
	}
	// 绑核容器：2 cpu、NUMA 1
	adj, updates, err := p.CreateContainer(context.Background(),
		sandbox("uid-p", map[string]string{request.AnnoPin: "true"}), ctr("c1", "sb-uid-p", "app"))
	if err != nil {
		t.Fatal(err)
	}
	if got := adjCpus(adj); got != "4-5" {
		t.Fatalf("pinned cpus = %q", got)
	}
	if got := adjMems(adj); got != "1" {
		t.Fatalf("pinned mems = %q", got)
	}
	if adj.GetAnnotations()[request.AnnoAllocated] != "4-5" {
		t.Fatalf("container annotation missing: %v", adj.GetAnnotations())
	}
	// 共享容器被夹回 1-3,6-7
	if len(updates) != 1 || updates[0].GetContainerId() != "c9" ||
		updCpus(updates[0]) != "1-3,6-7" {
		t.Fatalf("updates = %+v", updates)
	}
	if rec.annotations[request.AnnoAllocated] != "4-5" {
		t.Fatalf("pod annotation writeback missing: %v", rec.annotations)
	}
	if rep.last == nil || len(rep.last.Allocations) != 1 {
		t.Fatalf("CR report missing: %+v", rep.last)
	}
}

func TestCreateContainerSidecarFenced(t *testing.T) {
	kpod := pinnedPod(nil)
	p, _, _ := newTestPlugin(t, kpod)
	adj, _, err := p.CreateContainer(context.Background(),
		sandbox("uid-p", map[string]string{request.AnnoPin: "true"}), ctr("c2", "sb-uid-p", "sidecar"))
	if err != nil {
		t.Fatal(err)
	}
	if got := adjCpus(adj); got != "1-7" {
		t.Fatalf("sidecar must be fenced to shared pool, got %q", got)
	}
}

func TestCreateContainerInsufficientFailsClosed(t *testing.T) {
	kpod := pinnedPod(nil)
	kpod.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = mustQ("5")
	kpod.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU] = mustQ("5")
	p, rec, _ := newTestPlugin(t, kpod)
	_, _, err := p.CreateContainer(context.Background(),
		sandbox("uid-p", map[string]string{request.AnnoPin: "true"}), ctr("c1", "sb-uid-p", "app"))
	if err == nil {
		t.Fatal("must fail closed") // 5 > 单 zone 可分配（zone0 只有 3 个非预留）
	}
	if len(rec.events) == 0 || !strings.Contains(rec.events[0], "AllocationFailed") {
		t.Fatalf("event missing: %v", rec.events)
	}
}

func TestCreateContainerPodSpecUnavailableFailsClosed(t *testing.T) {
	p, _, _ := newTestPlugin(t) // 不注入 Pod
	_, _, err := p.CreateContainer(context.Background(),
		sandbox("uid-p", map[string]string{request.AnnoPin: "true"}), ctr("c1", "sb-uid-p", "app"))
	if err == nil {
		t.Fatal("pinned pod without pod spec must fail closed")
	}
}
