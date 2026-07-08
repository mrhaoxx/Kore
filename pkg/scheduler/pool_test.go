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
}

func TestPoolSizeMismatchRejected(t *testing.T) {
	e := newEnv(t, withPool(topoCR("n1", "4-7"), "demo", "0-3", "uid-old"))
	pod := poolSchedPod("demo", "8")
	state := runPreFilter(t, e.k, pod)
	if st := e.k.Filter(context.Background(), state, pod, nodeInfo("n1")); st.IsSuccess() {
		t.Fatal("size mismatch must be rejected")
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
