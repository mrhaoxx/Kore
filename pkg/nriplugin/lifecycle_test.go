package nriplugin

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/nri/pkg/api"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
	"github.com/zjusct/kore/pkg/request"
)

type blockingReporter struct {
	reported chan v1alpha1.KoreNodeTopologyStatus
	unblock  <-chan struct{}
}

func (r *blockingReporter) Report(st v1alpha1.KoreNodeTopologyStatus) {
	r.reported <- st
	<-r.unblock
}

func TestStopPodReleasesAndGrowsShared(t *testing.T) {
	kpod := pinnedPod(map[string]string{request.AnnoReservedNUMA: "1"})
	p, _, _ := newTestPlugin(t, kpod)
	pushed := make(chan []*api.ContainerUpdate, 1)
	p.SetUpdater(func(us []*api.ContainerUpdate) error {
		pushed <- us
		return nil
	})

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
	var updates []*api.ContainerUpdate
	select {
	case updates = <-pushed:
	case <-time.After(time.Second):
		t.Fatal("shared-container update was not delivered")
	}
	// 释放后共享池恢复 1-7，c9 被推回
	if len(updates) != 1 || updates[0].GetContainerId() != "c9" ||
		updCpus(updates[0]) != "1-7" {
		t.Fatalf("pushed = %+v", updates)
	}
}

func TestPodSandboxReleaseReportsBeforeNonBlockingUpdate(t *testing.T) {
	hooks := map[string]func(*Plugin, context.Context, *api.PodSandbox) error{
		"stop":   (*Plugin).StopPodSandbox,
		"remove": (*Plugin).RemovePodSandbox,
	}
	for name, hook := range hooks {
		t.Run(name, func(t *testing.T) {
			kpod := pinnedPod(map[string]string{request.AnnoReservedNUMA: "1"})
			p, _, _ := newTestPlugin(t, kpod)
			sb := sandbox("uid-p", map[string]string{request.AnnoPin: "true"})
			if _, _, err := p.CreateContainer(context.Background(), sb, ctr("c1", sb.Id, "app")); err != nil {
				t.Fatal(err)
			}
			if _, _, err := p.CreateContainer(context.Background(), sandbox("u9", nil), ctr("c9", "sb-u9", "web")); err != nil {
				t.Fatal(err)
			}

			reportUnblock := make(chan struct{})
			reportCompleted := false
			defer func() {
				if !reportCompleted {
					close(reportUnblock)
				}
			}()
			reported := make(chan v1alpha1.KoreNodeTopologyStatus, 1)
			p.rep = &blockingReporter{reported: reported, unblock: reportUnblock}
			updaterStarted := make(chan []*api.ContainerUpdate, 1)
			updaterUnblock := make(chan struct{})
			defer close(updaterUnblock)
			p.SetUpdater(func(us []*api.ContainerUpdate) error {
				updaterStarted <- us
				<-updaterUnblock
				return nil
			})

			done := make(chan error, 1)
			go func() { done <- hook(p, context.Background(), sb) }()

			var status v1alpha1.KoreNodeTopologyStatus
			select {
			case status = <-reported:
			case <-time.After(time.Second):
				t.Fatal("released state was not reported")
			}
			if len(status.Allocations) != 0 {
				t.Fatalf("reported allocations = %+v, want none", status.Allocations)
			}
			if len(status.Zones) != 2 || status.Zones[0].FreeCpus != "1-3" || status.Zones[1].FreeCpus != "4-7" {
				t.Fatalf("reported zones = %+v, want free CPUs 1-3 and 4-7", status.Zones)
			}
			select {
			case updates := <-updaterStarted:
				t.Fatalf("updater started before release report completed: %+v", updates)
			case <-time.After(100 * time.Millisecond):
			}
			select {
			case err := <-done:
				t.Fatalf("hook returned before release report completed: %v", err)
			case <-time.After(100 * time.Millisecond):
			}

			close(reportUnblock)
			reportCompleted = true
			var updates []*api.ContainerUpdate
			select {
			case updates = <-updaterStarted:
			case <-time.After(time.Second):
				t.Fatal("shared-container updater was not called")
			}
			if len(updates) != 1 || updates[0].GetContainerId() != "c9" || updCpus(updates[0]) != "1-7" {
				t.Fatalf("updates = %+v, want c9 -> 1-7", updates)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("hook waited for the shared-container updater")
			}
		})
	}
}

func TestPodSandboxReleaseUpdateConvergesAfterConcurrentAllocation(t *testing.T) {
	p1 := pinnedPod(map[string]string{request.AnnoReservedNUMA: "1"})
	p, _, _ := newTestPlugin(t, p1)
	sb1 := sandbox("uid-p", map[string]string{request.AnnoPin: "true"})
	if _, _, err := p.CreateContainer(context.Background(), sb1, ctr("c1", sb1.Id, "app")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.CreateContainer(context.Background(), sandbox("u9", nil), ctr("c9", "sb-u9", "web")); err != nil {
		t.Fatal(err)
	}

	updaterCalls := make(chan []*api.ContainerUpdate, 2)
	unblockFirst := make(chan struct{})
	firstUnblocked := false
	defer func() {
		if !firstUnblocked {
			close(unblockFirst)
		}
	}()
	var call atomic.Int32
	p.SetUpdater(func(us []*api.ContainerUpdate) error {
		n := call.Add(1)
		updaterCalls <- us
		if n == 1 {
			<-unblockFirst
			return nil
		}
		if n == 2 {
			return errors.New("injected updater failure")
		}
		return nil
	})

	if err := p.StopPodSandbox(context.Background(), sb1); err != nil {
		t.Fatal(err)
	}
	var first []*api.ContainerUpdate
	select {
	case first = <-updaterCalls:
	case <-time.After(time.Second):
		t.Fatal("first release update was not delivered")
	}
	if len(first) != 1 || first[0].GetContainerId() != "c9" || updCpus(first[0]) != "1-7" {
		t.Fatalf("first release update = %+v, want c9 -> 1-7", first)
	}
	if first[0].IgnoreFailure {
		t.Fatal("unsolicited release update must report container failures")
	}

	p2 := pinnedPod(map[string]string{request.AnnoReservedNUMA: "1"})
	p2.Name, p2.UID = "p2", "uid-p2"
	p.pods.(*fakePods).pods["default/p2"] = p2
	sb2 := sandbox("uid-p2", map[string]string{request.AnnoPin: "true"})
	sb2.Name = "p2"
	adj, returned, err := p.CreateContainer(context.Background(), sb2, ctr("c2", sb2.Id, "app"))
	if err != nil {
		t.Fatal(err)
	}
	if adjCpus(adj) != "4-5" || len(returned) != 1 || updCpus(returned[0]) != "1-3,6-7" {
		t.Fatalf("concurrent allocation adjustment=%q updates=%+v", adjCpus(adj), returned)
	}

	close(unblockFirst)
	firstUnblocked = true
	for attempt := 2; attempt <= 3; attempt++ {
		select {
		case latest := <-updaterCalls:
			if len(latest) != 1 || latest[0].GetContainerId() != "c9" || updCpus(latest[0]) != "1-3,6-7" {
				t.Fatalf("converged update %d = %+v, want c9 -> 1-3,6-7", attempt, latest)
			}
			if latest[0].IgnoreFailure {
				t.Fatalf("converged update %d ignored container failures", attempt)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("release updater did not complete convergence attempt %d", attempt)
		}
	}
}

func TestPodSandboxReleaseUpdatesAreSingleFlight(t *testing.T) {
	p1 := pinnedPod(map[string]string{request.AnnoReservedNUMA: "1"})
	p2 := pinnedPod(map[string]string{request.AnnoReservedNUMA: "1"})
	p2.Name, p2.UID = "p2", "uid-p2"
	p, _, _ := newTestPlugin(t, p1, p2)
	sb1 := sandbox("uid-p", map[string]string{request.AnnoPin: "true"})
	sb2 := sandbox("uid-p2", map[string]string{request.AnnoPin: "true"})
	sb2.Name = "p2"
	if _, _, err := p.CreateContainer(context.Background(), sb1, ctr("c1", sb1.Id, "app")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.CreateContainer(context.Background(), sb2, ctr("c2", sb2.Id, "app")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.CreateContainer(context.Background(), sandbox("u9", nil), ctr("c9", "sb-u9", "web")); err != nil {
		t.Fatal(err)
	}

	updaterCalls := make(chan []*api.ContainerUpdate, 2)
	unblockFirst := make(chan struct{})
	firstUnblocked := false
	defer func() {
		if !firstUnblocked {
			close(unblockFirst)
		}
	}()
	var call atomic.Int32
	p.SetUpdater(func(us []*api.ContainerUpdate) error {
		n := call.Add(1)
		updaterCalls <- us
		if n == 1 {
			<-unblockFirst
			return errors.New("injected updater failure")
		}
		return nil
	})

	if err := p.StopPodSandbox(context.Background(), sb1); err != nil {
		t.Fatal(err)
	}
	select {
	case first := <-updaterCalls:
		if len(first) != 1 || updCpus(first[0]) != "1-5" {
			t.Fatalf("first update = %+v, want c9 -> 1-5", first)
		}
	case <-time.After(time.Second):
		t.Fatal("first release update was not delivered")
	}
	if err := p.StopPodSandbox(context.Background(), sb2); err != nil {
		t.Fatal(err)
	}
	select {
	case concurrent := <-updaterCalls:
		t.Fatalf("second updater ran concurrently with the blocked first call: %+v", concurrent)
	case <-time.After(100 * time.Millisecond):
	}

	close(unblockFirst)
	firstUnblocked = true
	select {
	case latest := <-updaterCalls:
		if len(latest) != 1 || latest[0].GetContainerId() != "c9" || updCpus(latest[0]) != "1-7" {
			t.Fatalf("latest update = %+v, want c9 -> 1-7", latest)
		}
	case <-time.After(time.Second):
		t.Fatal("queued release update was not delivered")
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
