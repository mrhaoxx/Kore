package scheduler

import (
	"context"
	"testing"

	"k8s.io/utils/cpuset"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
)

func mustCPUSet(t *testing.T, s string) cpuset.CPUSet {
	t.Helper()
	cs, err := cpuset.Parse(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return cs
}

// smtZoneCR：单 NUMA zone、带 SMT 兄弟对的节点拓扑。
func smtZoneCR(node, cpus, free string, sibs [][]int) v1alpha1.KoreNodeTopology {
	c := v1alpha1.KoreNodeTopology{}
	c.Name = node
	c.Status.Zones = []v1alpha1.Zone{{ID: 0, Cpus: cpus, FreeCpus: free, SMTSiblings: sibs}}
	return c
}

// 复现 m700 事故：numa0 只剩孤儿 SMT 兄弟（同核另一半被占，整物理核空闲=0），
// full-core pin 应被判「放不下」而非绑上去后由 kore-agent 在 NRI 阶段失败；
// 同候选集合里全空的 m701 应可调度，且打分不劣于/高于碎片节点。
func TestFilterFullCoreRejectsOrphanSiblings(t *testing.T) {
	// cpus 0-3，SMT 对 [0,2]/[1,3]。
	// m700 free={2,3}：2 的兄弟 0、3 的兄弟 1 都被占 → 整核=0（复刻 70↔22 / 71↔23）。
	// m701 free=0-3：全空 → 2 个整核。
	m700 := smtZoneCR("m700", "0-3", "2-3", [][]int{{0, 2}, {1, 3}})
	m701 := smtZoneCR("m701", "0-3", "0-3", [][]int{{0, 2}, {1, 3}})
	e := newEnv(t, m700, m701)
	pod := schedPod("2", nil) // pin, numa=single(默认), smt=full-core(默认)
	state := runPreFilter(t, e.k, pod)

	if st := e.k.Filter(context.Background(), state, pod, nodeInfo("m700")); st.IsSuccess() {
		t.Fatal("m700 只剩孤儿兄弟（整核=0），full-core pin 应判 Unschedulable")
	}
	if st := e.k.Filter(context.Background(), state, pod, nodeInfo("m701")); !st.IsSuccess() {
		t.Fatalf("m701 有整核，应可调度：%v", st)
	}

	// 打分：m700 放不下应 0，m701 正分 → 不再把 pin 抢到碎片节点
	s700, _ := e.k.Score(context.Background(), state, pod, nodeInfo("m700"))
	s701, _ := e.k.Score(context.Background(), state, pod, nodeInfo("m701"))
	if s700 >= s701 {
		t.Fatalf("full-core 打分不应偏向碎片节点：m700=%d m701=%d", s700, s701)
	}
}

// 单元级：FullCoreZones 只保留“同核所有兄弟都空”的逻辑核；非 SMT（无 sibling）原样。
func TestFullCoreZones(t *testing.T) {
	// 孤儿：free={2,3}，对 [0,2]/[1,3]，0/1 占用 → 整核 0
	orphan := FullCoreZones([]ZoneCap{{ID: 0, Free: mustCPUSet(t, "2-3"), TPC: 2,
		Siblings: [][]int{{0, 2}, {1, 3}}}})
	if orphan[0].Free.Size() != 0 {
		t.Fatalf("孤儿兄弟应得 0 可用整核核，got %v", orphan[0].Free)
	}
	// 一个整核空：free={0,2}（对 [0,2] 都空）+ 孤儿 3 → 只保留 0,2
	whole := FullCoreZones([]ZoneCap{{ID: 0, Free: mustCPUSet(t, "0,2-3"), TPC: 2,
		Siblings: [][]int{{0, 2}, {1, 3}}}})
	if whole[0].Free.String() != "0,2" {
		t.Fatalf("应只保留整核 {0,2}，got %v", whole[0].Free)
	}
	// 非 SMT（无 sibling）：原样返回
	nosmt := FullCoreZones([]ZoneCap{{ID: 0, Free: mustCPUSet(t, "0-3"), TPC: 1}})
	if nosmt[0].Free.String() != "0-3" {
		t.Fatalf("非 SMT 应原样，got %v", nosmt[0].Free)
	}
}
