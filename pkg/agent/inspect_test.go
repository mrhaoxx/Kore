package agent

import (
	"encoding/json"
	"testing"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
	"github.com/zjusct/kore/pkg/topology/topotest"
)

func TestInspect(t *testing.T) {
	root := topotest.Write(t, []topotest.Zone{
		{ID: 0, CPUList: "0-3", MemTotalKB: 1024, Distances: "10 20"},
		{ID: 1, CPUList: "4-7", MemTotalKB: 1024, Distances: "20 10"},
	}, map[int]string{0: "0", 1: "1", 2: "2", 3: "3", 4: "4", 5: "5", 6: "6", 7: "7"})

	out, err := Inspect(root, "0-1")
	if err != nil {
		t.Fatal(err)
	}
	var st v1alpha1.KoreNodeTopologyStatus
	if err := json.Unmarshal([]byte(out), &st); err != nil {
		t.Fatal(err)
	}
	if len(st.Zones) != 2 || st.ReservedSystemCpus != "0-1" || st.Zones[0].FreeCpus != "2-3" {
		t.Fatalf("%s", out)
	}
}
