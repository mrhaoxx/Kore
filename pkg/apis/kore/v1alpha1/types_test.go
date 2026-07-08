package v1alpha1

import (
	"encoding/json"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestSchemeRegistration(t *testing.T) {
	s := runtime.NewScheme()
	if err := AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	gvks, _, err := s.ObjectKinds(&KoreNodeTopology{})
	if err != nil {
		t.Fatal(err)
	}
	if gvks[0].Group != "kore.zjusct.io" || gvks[0].Version != "v1alpha1" || gvks[0].Kind != "KoreNodeTopology" {
		t.Fatalf("unexpected GVK: %v", gvks[0])
	}
}

func TestStatusRoundtrip(t *testing.T) {
	in := KoreNodeTopologyStatus{
		ReservedSystemCpus: "0-1",
		Zones: []Zone{{
			ID: 0, Cpus: "0-15,32-47", Allocatable: 28, FreeCpus: "4-15,36-47",
			MemoryTotal: resource.MustParse("256Gi"),
			SMTSiblings: [][]int{{2, 34}, {3, 35}},
			Devices:     []Device{},
		}},
		Allocations: []Allocation{{PodUID: "u1", Pod: "default/hpc-0", Container: "app", Cpuset: "8-15", NUMA: []int{0}}},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out KoreNodeTopologyStatus
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Zones[0].FreeCpus != "4-15,36-47" || out.Allocations[0].Cpuset != "8-15" {
		t.Fatalf("roundtrip mismatch: %+v", out)
	}
}
