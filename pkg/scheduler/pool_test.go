package scheduler

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
	"github.com/zjusct/kore/pkg/request"
)

func poolSchedPod(pool string, size string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pp", Namespace: "default", UID: "uid-pp", Annotations: map[string]string{
			request.AnnoPool: pool, request.AnnoPoolSize: size,
		}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "app",
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
			},
		}}},
	}
}

func withPool(cr v1alpha1.KoreNodeTopology, name, cpus string, members ...string) v1alpha1.KoreNodeTopology {
	cr.Status.Pools = append(cr.Status.Pools, v1alpha1.Pool{Name: name, Cpuset: cpus, NUMA: []int{0}, Members: members})
	return cr
}

func TestPoolFollowOrCreate(t *testing.T) {
	// hasPool：已有 demo 池（4 核），剩余 free 很少；empty：无池但容量充足；tiny：建不了池
	e := newEnv(t,
		withPool(topoCR("haspool", "4-5"), "demo", "0-3", "uid-old"),
		topoCR("empty", "0-7"),
		topoCR("tiny", "0-1"),
	)
	pod := poolSchedPod("demo", "4")
	state := runPreFilter(t, e.k, pod)

	if st := e.k.Filter(context.Background(), state, pod, nodeInfo("haspool")); !st.IsSuccess() {
		t.Fatalf("follow must pass regardless of free: %v", st)
	}
	if st := e.k.Filter(context.Background(), state, pod, nodeInfo("empty")); !st.IsSuccess() {
		t.Fatalf("create must pass with capacity: %v", st)
	}
	if st := e.k.Filter(context.Background(), state, pod, nodeInfo("tiny")); st.IsSuccess() {
		t.Fatal("tiny cannot host a 4-cpu pool")
	}
	sFollow, _ := e.k.Score(context.Background(), state, pod, nodeInfo("haspool"))
	sCreate, _ := e.k.Score(context.Background(), state, pod, nodeInfo("empty"))
	if sFollow != 100 || sFollow <= sCreate {
		t.Fatalf("follow=%d create=%d", sFollow, sCreate)
	}
	// 恰好整 zone 命中的建池节点也不得打平跟随（上限 99）
	e2 := newEnv(t, topoCR("exact", "0-3"))
	pod4 := poolSchedPod("demo", "4")
	st2 := runPreFilter(t, e2.k, pod4)
	if s, _ := e2.k.Score(context.Background(), st2, pod4, nodeInfo("exact")); s != 99 {
		t.Fatalf("create score must cap at 99, got %d", s)
	}
}

func TestPoolSizeMismatchResizeSemantics(t *testing.T) {
	// CR 池 4 核，节点剩 4 free：扩到 6（差量 2 ≤ free）→ 通过；扩到 12（差量 8 > free）→ 拒；缩到 2 → 通过
	e := newEnv(t, withPool(topoCR("n1", "4-7"), "demo", "0-3", "uid-old"))
	for _, tc := range []struct {
		size string
		ok   bool
	}{{"6", true}, {"12", false}, {"2", true}} {
		pod := poolSchedPod("demo", tc.size)
		state := runPreFilter(t, e.k, pod)
		st := e.k.Filter(context.Background(), state, pod, nodeInfo("n1"))
		if st.IsSuccess() != tc.ok {
			t.Fatalf("size %s: got %v want ok=%v", tc.size, st, tc.ok)
		}
	}
}

func TestFollowPendingPoolReservation(t *testing.T) {
	e := newEnv(t, topoCR("n1", "0-7"), topoCR("n2", "0-7"))
	creator := poolSchedPod("demo", "6")
	st1 := runPreFilter(t, e.k, creator)
	if s := e.k.Reserve(context.Background(), st1, creator, "n1"); !s.IsSuccess() {
		t.Fatal(s)
	}
	// 同一突发里的第二个成员：CR 还没有池，但 n1 有在途建池预占
	member := poolSchedPod("demo", "6")
	member.UID = "uid-m2"
	st2 := runPreFilter(t, e.k, member)
	if s := e.k.Filter(context.Background(), st2, member, nodeInfo("n1")); !s.IsSuccess() {
		t.Fatalf("must follow pending pool despite deducted capacity: %v", s)
	}
	sN1, _ := e.k.Score(context.Background(), st2, member, nodeInfo("n1"))
	sN2, _ := e.k.Score(context.Background(), st2, member, nodeInfo("n2"))
	if sN1 != 100 || sN1 <= sN2 {
		t.Fatalf("pending follow must win: n1=%d n2=%d", sN1, sN2)
	}
	if s := e.k.Reserve(context.Background(), st2, member, "n1"); !s.IsSuccess() {
		t.Fatal(s)
	}
	// follower 未新增预占：小独占 Pod 仍能用剩余 2 核
	small := schedPod("2", nil)
	small.UID = "uid-s"
	st3 := runPreFilter(t, e.k, small)
	if s := e.k.Filter(context.Background(), st3, small, nodeInfo("n1")); !s.IsSuccess() {
		t.Fatalf("follower must not double-reserve: %v", s)
	}
	// pending 池不支持 size 增量（容量未落定）
	bigger := poolSchedPod("demo", "8")
	bigger.UID = "uid-b"
	st4 := runPreFilter(t, e.k, bigger)
	if s := e.k.Filter(context.Background(), st4, bigger, nodeInfo("n1")); s.IsSuccess() {
		t.Fatal("pending pool with different size must be rejected")
	}
}

func TestPoolReserveOnlyWhenCreating(t *testing.T) {
	e := newEnv(t, topoCR("n1", "0-7")) // 8 free
	creator := poolSchedPod("demo", "6")
	st1 := runPreFilter(t, e.k, creator)
	if s := e.k.Reserve(context.Background(), st1, creator, "n1"); !s.IsSuccess() {
		t.Fatal(s)
	}
	// 建池预占后：另一个独占 Pod 只剩 2 核可用
	pinned := schedPod("4", nil)
	pinned.UID = "uid-x"
	st2 := runPreFilter(t, e.k, pinned)
	if s := e.k.Filter(context.Background(), st2, pinned, nodeInfo("n1")); s.IsSuccess() {
		t.Fatal("creator reservation must deduct capacity")
	}
	// 跟随者不再预占：CR 出现池后，成员 Pod Reserve 不新增扣减
	e.crs = []v1alpha1.KoreNodeTopology{withPool(topoCR("n1", "6-7"), "demo", "0-5", "uid-pp")}
	member := poolSchedPod("demo", "6")
	member.UID = "uid-m2"
	st3 := runPreFilter(t, e.k, member) // MarkAllocated(uid-pp) 顺带清了 creator 预占
	if s := e.k.Reserve(context.Background(), st3, member, "n1"); !s.IsSuccess() {
		t.Fatal(s)
	}
	small := schedPod("2", nil)
	small.UID = "uid-y"
	st4 := runPreFilter(t, e.k, small)
	if s := e.k.Filter(context.Background(), st4, small, nodeInfo("n1")); !s.IsSuccess() {
		t.Fatalf("follower must not deduct: %v", s)
	}
}
