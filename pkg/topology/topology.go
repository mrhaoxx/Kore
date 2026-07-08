// Package topology 从 sysfs 发现 NUMA/SMT 拓扑。
package topology

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"k8s.io/utils/cpuset"
)

type Zone struct {
	ID               int
	CPUs             cpuset.CPUSet
	MemoryTotalBytes int64
	// Distances[i] 是到 zone i 的 NUMA 距离（sysfs distance 行）。
	Distances []int
}

type Topology struct {
	Zones []Zone // 按 ID 升序
	// Siblings[cpu] 是该逻辑核所在物理核的全部逻辑核（含自身）。
	Siblings       map[int]cpuset.CPUSet
	ThreadsPerCore int
}

func (t *Topology) SMTEnabled() bool { return t.ThreadsPerCore > 1 }

func (t *Topology) ZoneOf(cpu int) int {
	for _, z := range t.Zones {
		if z.CPUs.Contains(cpu) {
			return z.ID
		}
	}
	return -1
}

func (t *Topology) AllCPUs() cpuset.CPUSet {
	all := cpuset.New()
	for _, z := range t.Zones {
		all = all.Union(z.CPUs)
	}
	return all
}

func Discover(sysfsRoot string) (*Topology, error) {
	nodeDirs, err := filepath.Glob(filepath.Join(sysfsRoot, "devices/system/node/node[0-9]*"))
	if err != nil {
		return nil, err
	}
	if len(nodeDirs) == 0 {
		return nil, fmt.Errorf("no NUMA nodes under %s", sysfsRoot)
	}
	sort.Slice(nodeDirs, func(i, j int) bool { return dirNodeID(nodeDirs[i]) < dirNodeID(nodeDirs[j]) })

	topo := &Topology{Siblings: map[int]cpuset.CPUSet{}, ThreadsPerCore: 1}
	for _, dir := range nodeDirs {
		id := dirNodeID(dir)
		cpus, err := readCPUList(filepath.Join(dir, "cpulist"))
		if err != nil {
			return nil, fmt.Errorf("node%d cpulist: %w", id, err)
		}
		mem, err := readMemTotalBytes(filepath.Join(dir, "meminfo"))
		if err != nil {
			return nil, fmt.Errorf("node%d meminfo: %w", id, err)
		}
		dist, err := readInts(filepath.Join(dir, "distance"))
		if err != nil {
			return nil, fmt.Errorf("node%d distance: %w", id, err)
		}
		topo.Zones = append(topo.Zones, Zone{ID: id, CPUs: cpus, MemoryTotalBytes: mem, Distances: dist})

		for _, cpu := range cpus.List() {
			p := filepath.Join(sysfsRoot, fmt.Sprintf("devices/system/cpu/cpu%d/topology/thread_siblings_list", cpu))
			sib, err := readCPUList(p)
			if err != nil {
				return nil, fmt.Errorf("cpu%d siblings: %w", cpu, err)
			}
			topo.Siblings[cpu] = sib
			if sib.Size() > topo.ThreadsPerCore {
				topo.ThreadsPerCore = sib.Size()
			}
		}
	}
	return topo, nil
}

func dirNodeID(dir string) int {
	n, _ := strconv.Atoi(strings.TrimPrefix(filepath.Base(dir), "node"))
	return n
}

func readCPUList(path string) (cpuset.CPUSet, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return cpuset.New(), err
	}
	return cpuset.Parse(strings.TrimSpace(string(b)))
}

// readMemTotalBytes 解析 "Node 0 MemTotal:  32768 kB" 行。
func readMemTotalBytes(path string) (int64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.Contains(line, "MemTotal:") {
			continue
		}
		f := strings.Fields(line)
		if len(f) < 4 {
			return 0, fmt.Errorf("malformed meminfo line: %q", line)
		}
		kb, err := strconv.ParseInt(f[3], 10, 64)
		if err != nil {
			return 0, err
		}
		return kb * 1024, nil
	}
	return 0, fmt.Errorf("MemTotal not found in %s", path)
}

func readInts(path string) ([]int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []int
	for _, f := range strings.Fields(string(b)) {
		n, err := strconv.Atoi(f)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}
