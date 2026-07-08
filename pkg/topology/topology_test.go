package topology

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/zjusct/kore/pkg/topology/topotest"
)

func x86SMTSysfs(t *testing.T) string {
	// 2 NUMA、每 zone 8 逻辑核、2-way SMT：sibling 对 (i, i+8)
	zones := []topotest.Zone{
		{ID: 0, CPUList: "0-3,8-11", MemTotalKB: 32 * 1024 * 1024, Distances: "10 21"},
		{ID: 1, CPUList: "4-7,12-15", MemTotalKB: 32 * 1024 * 1024, Distances: "21 10"},
	}
	sib := map[int]string{}
	for i := 0; i < 8; i++ {
		s := fmt.Sprintf("%d,%d", i, i+8)
		sib[i], sib[i+8] = s, s
	}
	return topotest.Write(t, zones, sib)
}

func armSysfs(t *testing.T) string {
	zones := []topotest.Zone{
		{ID: 0, CPUList: "0-3", MemTotalKB: 16 * 1024 * 1024, Distances: "10 12 20 22"},
		{ID: 1, CPUList: "4-7", MemTotalKB: 16 * 1024 * 1024, Distances: "12 10 22 20"},
		{ID: 2, CPUList: "8-11", MemTotalKB: 16 * 1024 * 1024, Distances: "20 22 10 12"},
		{ID: 3, CPUList: "12-15", MemTotalKB: 16 * 1024 * 1024, Distances: "22 20 12 10"},
	}
	sib := map[int]string{}
	for i := 0; i < 16; i++ {
		sib[i] = fmt.Sprintf("%d", i)
	}
	return topotest.Write(t, zones, sib)
}

func TestDiscoverX86SMT(t *testing.T) {
	topo, err := Discover(x86SMTSysfs(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(topo.Zones) != 2 {
		t.Fatalf("zones = %d, want 2", len(topo.Zones))
	}
	if !topo.SMTEnabled() || topo.ThreadsPerCore != 2 {
		t.Fatalf("SMT: enabled=%v threads=%d", topo.SMTEnabled(), topo.ThreadsPerCore)
	}
	if got := topo.Zones[0].MemoryTotalBytes; got != 32*1024*1024*1024 {
		t.Fatalf("zone0 mem = %d", got)
	}
	if got := topo.Zones[1].Distances; len(got) != 2 || got[0] != 21 || got[1] != 10 {
		t.Fatalf("zone1 distances = %v", got)
	}
	if topo.ZoneOf(9) != 0 || topo.ZoneOf(13) != 1 {
		t.Fatalf("ZoneOf wrong: cpu9->%d cpu13->%d", topo.ZoneOf(9), topo.ZoneOf(13))
	}
	if sib := topo.Siblings[2].List(); len(sib) != 2 || sib[0] != 2 || sib[1] != 10 {
		t.Fatalf("siblings of 2 = %v", sib)
	}
	if topo.AllCPUs().Size() != 16 {
		t.Fatalf("AllCPUs = %v", topo.AllCPUs())
	}
}

func TestDiscoverARMNoSMT(t *testing.T) {
	topo, err := Discover(armSysfs(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(topo.Zones) != 4 || topo.SMTEnabled() {
		t.Fatalf("zones=%d smt=%v", len(topo.Zones), topo.SMTEnabled())
	}
}

func TestDiscoverEmptyRootFails(t *testing.T) {
	if _, err := Discover(t.TempDir()); err == nil {
		t.Fatal("expected error on empty sysfs")
	}
}

// 无 NUMA sysfs 的内核（VM 常见）→ 降级为单 zone 0 含全部在线 CPU。
func TestDiscoverNonNUMAFallback(t *testing.T) {
	root := t.TempDir()
	cpuDir := filepath.Join(root, "devices/system/cpu")
	if err := os.MkdirAll(cpuDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cpuDir, "online"), []byte("0-3\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// cpu0/1 有 topology（单线程），cpu2/3 连 topology 都没有 → 按单核处理
	for i := 0; i < 2; i++ {
		d := filepath.Join(cpuDir, fmt.Sprintf("cpu%d/topology", i))
		os.MkdirAll(d, 0o755)
		os.WriteFile(filepath.Join(d, "thread_siblings_list"), []byte(fmt.Sprintf("%d\n", i)), 0o644)
	}
	topo, err := Discover(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(topo.Zones) != 1 || topo.Zones[0].ID != 0 || topo.Zones[0].CPUs.Size() != 4 {
		t.Fatalf("zones = %+v", topo.Zones)
	}
	if topo.SMTEnabled() || topo.ZoneOf(2) != 0 || topo.AllCPUs().Size() != 4 {
		t.Fatalf("topo = %+v", topo)
	}
}
