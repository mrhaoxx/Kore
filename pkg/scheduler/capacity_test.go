package scheduler

import (
	"testing"

	"k8s.io/utils/cpuset"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
	"github.com/zjusct/kore/pkg/request"
)

func cr(node string, zones ...v1alpha1.Zone) *v1alpha1.KoreNodeTopology {
	c := &v1alpha1.KoreNodeTopology{}
	c.Name = node
	c.Status.Zones = zones
	return c
}

func TestZonesFromCR(t *testing.T) {
	c := cr("n1",
		v1alpha1.Zone{ID: 0, Cpus: "0-3,8-11", FreeCpus: "2-3,10-11", SMTSiblings: [][]int{{0, 8}, {1, 9}, {2, 10}, {3, 11}}},
		v1alpha1.Zone{ID: 1, Cpus: "4-7,12-15", FreeCpus: "4-7,12-15", SMTSiblings: [][]int{{4, 12}, {5, 13}, {6, 14}, {7, 15}}},
	)
	zones, err := ZonesFromCR(c)
	if err != nil {
		t.Fatal(err)
	}
	if len(zones) != 2 || zones[0].Free.Size() != 4 || zones[0].TPC != 2 || zones[1].Free.Size() != 8 {
		t.Fatalf("%+v", zones)
	}
}

func armZones() []ZoneCap { // 4 zone × 4 free，无 SMT
	var out []ZoneCap
	for z := 0; z < 4; z++ {
		out = append(out, ZoneCap{ID: z, Free: cpuset.New(z*4, z*4+1, z*4+2, z*4+3), TPC: 1})
	}
	return out
}

func TestDeductCountAndExplicit(t *testing.T) {
	ex := cpuset.New(0, 1)
	zones := Deduct(armZones(), []Reservation{
		{PodUID: "a", Zone: 1, Count: 3},
		{PodUID: "b", Explicit: &ex},
	})
	if zones[1].Free.Size() != 1 {
		t.Fatalf("zone1 free = %v", zones[1].Free)
	}
	if zones[0].Free.Size() != 2 || zones[0].Free.Contains(0) || zones[0].Free.Contains(1) {
		t.Fatalf("zone0 free = %v", zones[0].Free)
	}
}

func TestFitSingleBinpack(t *testing.T) {
	zones := armZones()
	zones[2].Free = cpuset.New(8, 9) // zone2 只剩 2 → 请求 2 应选 zone2
	z, ok := FitSingle(zones, 2)
	if !ok || z != 2 {
		t.Fatalf("z=%d ok=%v", z, ok)
	}
	if _, ok := FitSingle(zones, 5); ok {
		t.Fatal("5 > 任何单 zone")
	}
}

func TestFitPreferredFallsBack(t *testing.T) {
	z, ok := FitPreferred(armZones(), 6) // 单 zone 放不下，总量够
	if !ok || z != 0 {                   // free 全 4 并列 → 最多者取小 ID
		t.Fatalf("z=%d ok=%v", z, ok)
	}
	if _, ok := FitPreferred(armZones(), 17); ok {
		t.Fatal("17 > 总量 16")
	}
}

func TestFitSpreadAndExplicit(t *testing.T) {
	if !FitSpread(armZones(), 16) || FitSpread(armZones(), 17) {
		t.Fatal("spread total check wrong")
	}
	if !FitExplicit(armZones(), cpuset.New(3, 4, 5)) {
		t.Fatal("explicit free subset should fit")
	}
	zones := armZones()
	zones[0].Free = cpuset.New(1, 2, 3)
	if FitExplicit(zones, cpuset.New(0, 1)) {
		t.Fatal("cpu0 not free")
	}
}

func TestScoreFitPrefersTightZone(t *testing.T) {
	loose := armZones()                                         // 每 zone 4 free
	tight := []ZoneCap{{ID: 0, Free: cpuset.New(0, 1), TPC: 1}} // 恰好 2
	sLoose := ScoreFit(loose, request.NUMASingle, false, 2)
	sTight := ScoreFit(tight, request.NUMASingle, false, 2)
	if sTight <= sLoose || sTight != 100 {
		t.Fatalf("tight=%d loose=%d", sTight, sLoose)
	}
	if ScoreFit(tight, request.NUMASingle, false, 3) != 0 {
		t.Fatal("unfit must score 0")
	}
}
