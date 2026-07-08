package main

import (
	"testing"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
)

func TestBuildNodeGridSMT(t *testing.T) {
	cr := &v1alpha1.KoreNodeTopology{}
	cr.Name = "n1"
	cr.Status = v1alpha1.KoreNodeTopologyStatus{
		ReservedSystemCpus: "0,8",
		Zones: []v1alpha1.Zone{{
			ID: 0, Cpus: "0-3,8-11", FreeCpus: "2-3,10-11",
			SMTSiblings: [][]int{{0, 8}, {1, 9}, {2, 10}, {3, 11}},
		}},
		Allocations: []v1alpha1.Allocation{{PodUID: "u1", Pod: "d/pinned", Cpuset: "1,9", NUMA: []int{0}}},
		Pools:       []v1alpha1.Pool{{Name: "demo", Cpuset: "2,10", NUMA: []int{0}, Members: []string{"u2"}}},
	}
	g := BuildNodeGrid(cr)
	if len(g.Legend) != 2 || g.Legend[0].Key != 'A' || g.Legend[0].Owner != "d/pinned" ||
		g.Legend[1].Key != 'B' || g.Legend[1].Owner != "pool:demo" {
		t.Fatalf("legend: %+v", g.Legend)
	}
	z := g.Zones[0]
	if len(z.Rows) != 2 || len(z.Rows[0]) != 4 {
		t.Fatalf("rows: %+v", z.Rows)
	}
	// 列=物理核：col0=(0,8) 预留、col1=(1,9) 独占A、col2=(2,10) 池B、col3=(3,11) 空闲
	top, bot := z.Rows[0], z.Rows[1]
	if top[0].Kind != CellReserved || bot[0].Kind != CellReserved {
		t.Fatalf("col0 must be reserved: %+v %+v", top[0], bot[0])
	}
	if top[1].Kind != CellExclusive || top[1].Key != 'A' || bot[1].CPU != 9 || bot[1].Key != 'A' {
		t.Fatalf("col1: %+v %+v", top[1], bot[1])
	}
	if top[2].Kind != CellPool || top[2].Key != 'B' {
		t.Fatalf("col2: %+v", top[2])
	}
	if top[3].Kind != CellFree || bot[3].Kind != CellFree {
		t.Fatalf("col3: %+v %+v", top[3], bot[3])
	}
}

func TestBuildNodeGridNoSMT(t *testing.T) {
	cr := &v1alpha1.KoreNodeTopology{}
	cr.Name = "arm"
	cr.Status = v1alpha1.KoreNodeTopologyStatus{
		Zones: []v1alpha1.Zone{{ID: 0, Cpus: "0-3", FreeCpus: "2-3"}},
		Pools: []v1alpha1.Pool{{Name: "p", Cpuset: "0-1", NUMA: []int{0}, Members: []string{"u"}}},
	}
	g := BuildNodeGrid(cr)
	if len(g.Zones[0].Rows) != 1 || len(g.Zones[0].Rows[0]) != 4 {
		t.Fatalf("%+v", g.Zones[0].Rows)
	}
	if g.Zones[0].Rows[0][0].Kind != CellPool || g.Zones[0].Rows[0][3].Kind != CellFree {
		t.Fatalf("%+v", g.Zones[0].Rows[0])
	}
}
