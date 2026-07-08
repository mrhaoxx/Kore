package nriplugin

import (
	"context"
	"testing"

	"github.com/containerd/nri/pkg/api"

	"github.com/zjusct/kore/pkg/request"
)

func TestStopPodReleasesAndGrowsShared(t *testing.T) {
	kpod := pinnedPod(map[string]string{request.AnnoReservedNUMA: "1"})
	p, _, _ := newTestPlugin(t, kpod)
	var pushed []*api.ContainerUpdate
	p.SetUpdater(func(us []*api.ContainerUpdate) error { pushed = us; return nil })

	sb := sandbox("uid-p", map[string]string{request.AnnoPin: "true"})
	if _, _, err := p.CreateContainer(context.Background(), sb, ctr("c1", sb.Id, "app")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.CreateContainer(context.Background(), sandbox("u9", nil), ctr("c9", "sb-u9", "web")); err != nil {
		t.Fatal(err)
	}
	if err := p.StopPodSandbox(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	// 释放后共享池恢复 1-7，c9 被推回
	if len(pushed) != 1 || pushed[0].GetContainerId() != "c9" ||
		updCpus(pushed[0]) != "1-7" {
		t.Fatalf("pushed = %+v", pushed)
	}
}

func TestSynchronizeRestoresState(t *testing.T) {
	kpod := pinnedPod(map[string]string{request.AnnoReservedNUMA: "1"})
	p, _, _ := newTestPlugin(t, kpod)
	sb := sandbox("uid-p", map[string]string{request.AnnoPin: "true"})
	pinned := ctr("c1", sb.Id, "app")
	pinned.Annotations = map[string]string{request.AnnoAllocated: "4-5"}
	shared := ctr("c9", "sb-u9", "web")

	updates, err := p.Synchronize(context.Background(),
		[]*api.PodSandbox{sb, sandbox("u9", nil)},
		[]*api.Container{pinned, shared})
	if err != nil {
		t.Fatal(err)
	}
	// 状态重建：4-5 已占用 → 共享池 1-3,6-7；c9 被夹回
	if len(updates) != 1 || updates[0].GetContainerId() != "c9" ||
		updCpus(updates[0]) != "1-3,6-7" {
		t.Fatalf("updates = %+v", updates)
	}
	// 再次分配不得与恢复的 4-5 冲突
	kpod2 := pinnedPod(map[string]string{request.AnnoReservedNUMA: "1"})
	kpod2.Name, kpod2.UID = "p2", "uid-p2"
	p.pods.(*fakePods).pods["default/p2"] = kpod2
	sb2 := sandbox("uid-p2", map[string]string{request.AnnoPin: "true"})
	sb2.Name = "p2"
	adj, _, err := p.CreateContainer(context.Background(), sb2, ctr("c2", sb2.Id, "app"))
	if err != nil {
		t.Fatal(err)
	}
	if got := adjCpus(adj); got != "6-7" {
		t.Fatalf("second alloc = %q, want 6-7", got)
	}
}

func TestSynchronizeUnboundStrictDeletesPod(t *testing.T) {
	kpod := pinnedPod(nil)
	p, rec, _ := newTestPlugin(t, kpod) // Remediation 默认 strict
	sb := sandbox("uid-p", map[string]string{request.AnnoPin: "true"})
	unbound := ctr("c1", sb.Id, "app") // 无 AnnoAllocated 注解 = 该绑未绑

	if _, err := p.Synchronize(context.Background(), []*api.PodSandbox{sb}, []*api.Container{unbound}); err != nil {
		t.Fatal(err)
	}
	if len(rec.deleted) != 1 || rec.deleted[0] != "default/p" {
		t.Fatalf("deleted = %v", rec.deleted)
	}
	if len(rec.events) == 0 {
		t.Fatal("expected warning event")
	}
}

func TestSynchronizeUnboundRepairRebinds(t *testing.T) {
	kpod := pinnedPod(nil)
	p, rec, _ := newTestPlugin(t, kpod)
	p.cfg.Remediation = "repair"
	sb := sandbox("uid-p", map[string]string{request.AnnoPin: "true"})
	unbound := ctr("c1", sb.Id, "app")

	updates, err := p.Synchronize(context.Background(), []*api.PodSandbox{sb}, []*api.Container{unbound})
	if err != nil {
		t.Fatal(err)
	}
	var rebind *api.ContainerUpdate
	for _, u := range updates {
		if u.GetContainerId() == "c1" {
			rebind = u
		}
	}
	if rebind == nil || updCpus(rebind) == "" {
		t.Fatalf("repair rebind missing: %+v", updates)
	}
	if len(rec.deleted) != 0 {
		t.Fatal("repair must not delete pod")
	}
	if len(rec.events) == 0 {
		t.Fatal("repair must emit warning event")
	}
}
