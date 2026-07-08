package nriplugin

import (
	"context"
	"testing"
	"time"

	"github.com/containerd/nri/pkg/api"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/zjusct/kore/pkg/request"
)

func poolAnnos() map[string]string {
	return map[string]string{request.AnnoPool: "demo", request.AnnoPoolSize: "2"}
}

func poolPod(name, uid string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(uid), Annotations: poolAnnos()},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}},
	}
}

func TestPoolMembersShareCpuset(t *testing.T) {
	p1, p2 := poolPod("m1", "uid-m1"), poolPod("m2", "uid-m2")
	p, _, rep := newTestPlugin(t, p1, p2)

	sb1 := &api.PodSandbox{Id: "sb-m1", Name: "m1", Uid: "uid-m1", Namespace: "default", Annotations: poolAnnos()}
	adj1, _, err := p.CreateContainer(context.Background(), sb1, ctr("c1", sb1.Id, "app"))
	if err != nil {
		t.Fatal(err)
	}
	sb2 := &api.PodSandbox{Id: "sb-m2", Name: "m2", Uid: "uid-m2", Namespace: "default", Annotations: poolAnnos()}
	adj2, _, err := p.CreateContainer(context.Background(), sb2, ctr("c2", sb2.Id, "app"))
	if err != nil {
		t.Fatal(err)
	}
	if adjCpus(adj1) == "" || adjCpus(adj1) != adjCpus(adj2) {
		t.Fatalf("members must share: %q vs %q", adjCpus(adj1), adjCpus(adj2))
	}
	if adjMems(adj1) != "0" { // reserved {0} → zone0 建池，strict 单 NUMA
		t.Fatalf("mems = %q", adjMems(adj1))
	}
	if len(rep.last.Pools) != 1 || len(rep.last.Pools[0].Members) != 2 {
		t.Fatalf("CR pools: %+v", rep.last.Pools)
	}
}

func TestPoolReleaseGrowsSharedAfterLastMember(t *testing.T) {
	p1, p2 := poolPod("m1", "uid-m1"), poolPod("m2", "uid-m2")
	p, _, _ := newTestPlugin(t, p1, p2)
	var pushed []*api.ContainerUpdate
	p.SetUpdater(func(us []*api.ContainerUpdate) error { pushed = us; return nil })

	sb1 := &api.PodSandbox{Id: "sb-m1", Name: "m1", Uid: "uid-m1", Namespace: "default", Annotations: poolAnnos()}
	sb2 := &api.PodSandbox{Id: "sb-m2", Name: "m2", Uid: "uid-m2", Namespace: "default", Annotations: poolAnnos()}
	p.CreateContainer(context.Background(), sb1, ctr("c1", sb1.Id, "app"))
	p.CreateContainer(context.Background(), sb2, ctr("c2", sb2.Id, "app"))
	p.CreateContainer(context.Background(), sandbox("u9", nil), ctr("c9", "sb-u9", "web")) // 共享池观察者

	if err := p.StopPodSandbox(context.Background(), sb1); err != nil {
		t.Fatal(err)
	}
	if len(pushed) != 0 { // 池未释放（还有成员）→ Used 未变 → 不推送
		t.Fatalf("pool must survive first member exit, pushed=%+v", pushed)
	}
	if err := p.StopPodSandbox(context.Background(), sb2); err != nil {
		t.Fatal(err)
	}
	if len(pushed) != 1 || updCpus(pushed[0]) != "1-7" {
		t.Fatalf("shared pool must grow after pool freed: %+v", pushed)
	}
}

func TestSynchronizeRestoresPool(t *testing.T) {
	p1, p2 := poolPod("m1", "uid-m1"), poolPod("m2", "uid-m2")
	p, _, _ := newTestPlugin(t, p1, p2)
	sb1 := &api.PodSandbox{Id: "sb-m1", Name: "m1", Uid: "uid-m1", Namespace: "default", Annotations: poolAnnos()}
	sb2 := &api.PodSandbox{Id: "sb-m2", Name: "m2", Uid: "uid-m2", Namespace: "default", Annotations: poolAnnos()}
	c1, c2 := ctr("c1", sb1.Id, "app"), ctr("c2", sb2.Id, "app")
	c1.Annotations = map[string]string{request.AnnoAllocated: "1-2"}
	c2.Annotations = map[string]string{request.AnnoAllocated: "1-2"}

	if _, err := p.Synchronize(context.Background(), []*api.PodSandbox{sb1, sb2}, []*api.Container{c1, c2}); err != nil {
		t.Fatal(err)
	}
	// 恢复后独占分配必须避开池核心 1-2
	kpod := pinnedPod(nil)
	kpod.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = mustQ("2")
	kpod.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU] = mustQ("2")
	p.pods.(*fakePods).pods["default/p"] = kpod
	adj, _, err := p.CreateContainer(context.Background(),
		sandbox("uid-p", map[string]string{request.AnnoPin: "true"}), ctr("c3", "sb-uid-p", "app"))
	if err != nil {
		t.Fatal(err)
	}
	if got := adjCpus(adj); got != "4-5" { // zone0 仅剩 {3} 放不下 → zone1
		t.Fatalf("exclusive must avoid pool: %q", got)
	}
}

func TestPoolResizeBroadcast(t *testing.T) {
	p1, p2 := poolPod("m1", "uid-m1"), poolPod("m2", "uid-m2")
	p3 := poolPod("m3", "uid-m3")
	p3.Annotations[request.AnnoPoolSize] = "3"
	p3.CreationTimestamp = metav1.Time{Time: time.Now()} // 晚于池创建（其余为零值）
	p, _, _ := newTestPlugin(t, p1, p2, p3)

	sb1 := &api.PodSandbox{Id: "sb-m1", Name: "m1", Uid: "uid-m1", Namespace: "default", Annotations: poolAnnos()}
	sb2 := &api.PodSandbox{Id: "sb-m2", Name: "m2", Uid: "uid-m2", Namespace: "default", Annotations: poolAnnos()}
	if _, _, err := p.CreateContainer(context.Background(), sb1, ctr("c1", sb1.Id, "app")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.CreateContainer(context.Background(), sb2, ctr("c2", sb2.Id, "app")); err != nil {
		t.Fatal(err)
	}
	sb3annos := poolAnnos()
	sb3annos[request.AnnoPoolSize] = "3"
	sb3 := &api.PodSandbox{Id: "sb-m3", Name: "m3", Uid: "uid-m3", Namespace: "default", Annotations: sb3annos}
	adj3, updates, err := p.CreateContainer(context.Background(), sb3, ctr("c3", sb3.Id, "app"))
	if err != nil {
		t.Fatal(err)
	}
	if got := adjCpus(adj3); got != "1-3" { // 2 核池就近扩到 3
		t.Fatalf("resized pool = %q", got)
	}
	got := map[string]string{}
	for _, u := range updates {
		got[u.GetContainerId()] = updCpus(u)
	}
	if got["c1"] != "1-3" || got["c2"] != "1-3" {
		t.Fatalf("resize must broadcast to existing members: %v", got)
	}
}

func TestPoolUnboundRemediation(t *testing.T) {
	p1 := poolPod("m1", "uid-m1")
	p, rec, _ := newTestPlugin(t, p1) // strict 默认
	sb1 := &api.PodSandbox{Id: "sb-m1", Name: "m1", Uid: "uid-m1", Namespace: "default", Annotations: poolAnnos()}
	unbound := ctr("c1", sb1.Id, "app")

	if _, err := p.Synchronize(context.Background(), []*api.PodSandbox{sb1}, []*api.Container{unbound}); err != nil {
		t.Fatal(err)
	}
	if len(rec.deleted) != 1 {
		t.Fatalf("strict must delete unbound pool pod: %v", rec.deleted)
	}

	// repair 模式：补入池
	p2, rec2, _ := newTestPlugin(t, p1)
	p2.cfg.Remediation = "repair"
	updates, err := p2.Synchronize(context.Background(), []*api.PodSandbox{sb1}, []*api.Container{unbound})
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
		t.Fatalf("repair must rebind pool member: %+v", updates)
	}
	if len(rec2.deleted) != 0 {
		t.Fatal("repair must not delete")
	}
}
