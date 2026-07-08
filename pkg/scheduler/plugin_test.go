package scheduler

import (
	"context"
	"strconv"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fwk "k8s.io/kube-scheduler/framework"
	kfw "k8s.io/kubernetes/pkg/scheduler/framework"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
	"github.com/zjusct/kore/pkg/request"
)

func schedPod(cpus string, annos map[string]string) *corev1.Pod {
	a := map[string]string{request.AnnoPin: "true"}
	for k, v := range annos {
		a[k] = v
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default", UID: "uid-p", Annotations: a},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "app",
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpus)},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpus)},
			},
		}}},
	}
}

func topoCR(node string, freePerZone ...string) v1alpha1.KoreNodeTopology {
	c := v1alpha1.KoreNodeTopology{}
	c.Name = node
	for i, f := range freePerZone {
		c.Status.Zones = append(c.Status.Zones, v1alpha1.Zone{ID: i, Cpus: f, FreeCpus: f})
	}
	return c
}

type env struct {
	k       *Kore
	patched map[string]string
	stale   map[string]bool
	crs     []v1alpha1.KoreNodeTopology
}

func newEnv(t *testing.T, crs ...v1alpha1.KoreNodeTopology) *env {
	t.Helper()
	e := &env{patched: map[string]string{}, stale: map[string]bool{}, crs: crs}
	deps := Deps{
		ListTopologies: func(ctx context.Context) ([]v1alpha1.KoreNodeTopology, error) { return e.crs, nil },
		LeaseFresh:     func(node string) bool { return !e.stale[node] },
		PatchPodAnnotation: func(ctx context.Context, ns, name, key, value string) error {
			e.patched[key] = value
			return nil
		},
	}
	e.k = NewWithDeps(deps, NewCache(defaultReservationTTL))
	return e
}

func nodeInfo(name string) fwk.NodeInfo {
	ni := kfw.NewNodeInfo()
	ni.SetNode(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}})
	return ni
}

func runPreFilter(t *testing.T, k *Kore, pod *corev1.Pod) fwk.CycleState {
	t.Helper()
	state := kfw.NewCycleState()
	_, status := k.PreFilter(context.Background(), state, pod, nil)
	if !status.IsSuccess() {
		t.Fatalf("prefilter: %v", status)
	}
	return state
}

func TestPreFilterSkipsNonKorePod(t *testing.T) {
	e := newEnv(t)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "plain", Namespace: "default"}}
	state := kfw.NewCycleState()
	_, status := e.k.PreFilter(context.Background(), state, pod, nil)
	if status.Code() != fwk.Skip {
		t.Fatalf("status = %v, want Skip", status)
	}
}

func TestFilterByCapacityAndLease(t *testing.T) {
	e := newEnv(t,
		topoCR("small", "0-1"),      // 单 zone 2 free
		topoCR("big", "0-3", "4-7"), // 2 zone 各 4 free
		topoCR("dead", "0-7"),
	)
	e.stale["dead"] = true
	pod := schedPod("4", nil) // single 策略默认
	state := runPreFilter(t, e.k, pod)

	if st := e.k.Filter(context.Background(), state, pod, nodeInfo("small")); st.IsSuccess() {
		t.Fatal("small 放不下 4")
	}
	if st := e.k.Filter(context.Background(), state, pod, nodeInfo("big")); !st.IsSuccess() {
		t.Fatalf("big 应通过: %v", st)
	}
	if st := e.k.Filter(context.Background(), state, pod, nodeInfo("dead")); st.IsSuccess() {
		t.Fatal("lease 过期节点必须被拒")
	}
	if st := e.k.Filter(context.Background(), state, pod, nodeInfo("unknown")); st.IsSuccess() {
		t.Fatal("无 CR 节点必须被拒")
	}
}

func TestScorePrefersTighterNode(t *testing.T) {
	e := newEnv(t, topoCR("tight", "0-3"), topoCR("loose", "0-15"))
	pod := schedPod("4", nil)
	state := runPreFilter(t, e.k, pod)
	sTight, st1 := e.k.Score(context.Background(), state, pod, nodeInfo("tight"))
	sLoose, st2 := e.k.Score(context.Background(), state, pod, nodeInfo("loose"))
	if !st1.IsSuccess() || !st2.IsSuccess() || sTight <= sLoose {
		t.Fatalf("tight=%d loose=%d", sTight, sLoose)
	}
}

func TestReserveDeductsForNextPod(t *testing.T) {
	e := newEnv(t, topoCR("n1", "0-3")) // 4 free
	p1 := schedPod("3", nil)
	state1 := runPreFilter(t, e.k, p1)
	if st := e.k.Reserve(context.Background(), state1, p1, "n1"); !st.IsSuccess() {
		t.Fatalf("reserve: %v", st)
	}
	// 第二个 Pod（不同 UID）在预占生效后只剩 1 核
	p2 := schedPod("2", nil)
	p2.UID = "uid-p2"
	state2 := runPreFilter(t, e.k, p2)
	if st := e.k.Filter(context.Background(), state2, p2, nodeInfo("n1")); st.IsSuccess() {
		t.Fatal("预占后 n1 只剩 1 核，2 核请求必须被拒")
	}
	// Unreserve 归还
	e.k.Unreserve(context.Background(), state1, p1, "n1")
	state3 := runPreFilter(t, e.k, p2)
	if st := e.k.Filter(context.Background(), state3, p2, nodeInfo("n1")); !st.IsSuccess() {
		t.Fatalf("unreserve 后应通过: %v", st)
	}
}

func TestPreBindWritesReservedNUMA(t *testing.T) {
	e := newEnv(t, topoCR("n1", "0-3", "4-7"))
	pod := schedPod("2", map[string]string{request.AnnoNUMAPolicy: "single"})
	state := runPreFilter(t, e.k, pod)
	if st := e.k.Reserve(context.Background(), state, pod, "n1"); !st.IsSuccess() {
		t.Fatal(st)
	}
	if st := e.k.PreBind(context.Background(), state, pod, "n1"); !st.IsSuccess() {
		t.Fatal(st)
	}
	if z, err := strconv.Atoi(e.patched[request.AnnoReservedNUMA]); err != nil || z < 0 || z > 1 {
		t.Fatalf("patched = %v", e.patched)
	}
}

func TestMarkAllocatedClearsReservationViaPreFilter(t *testing.T) {
	e := newEnv(t, topoCR("n1", "0-3"))
	p1 := schedPod("3", nil)
	state1 := runPreFilter(t, e.k, p1)
	if st := e.k.Reserve(context.Background(), state1, p1, "n1"); !st.IsSuccess() {
		t.Fatal(st)
	}
	// agent 上报：CR 现在体现了 uid-p 的分配（freeCpus 已扣、allocations 有记录）
	cr := topoCR("n1", "3") // 只剩 1 free
	cr.Status.Allocations = []v1alpha1.Allocation{{PodUID: "uid-p", Pod: "default/p", Container: "app", Cpuset: "0-2", NUMA: []int{0}}}
	e.crs = []v1alpha1.KoreNodeTopology{cr}

	p2 := schedPod("1", nil)
	p2.UID = "uid-p2"
	state2 := runPreFilter(t, e.k, p2)
	// 若预占未被 MarkAllocated 清除会双重扣减（1 free − 3 预占 = 负），Filter 必失败；
	// 正确行为：预占已清，1 free 容纳 1 核请求
	if st := e.k.Filter(context.Background(), state2, p2, nodeInfo("n1")); !st.IsSuccess() {
		t.Fatalf("double counting detected: %v", st)
	}
}
