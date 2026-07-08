package main

import (
	"sort"

	"k8s.io/utils/cpuset"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
)

// CellKind 是单个逻辑核的占用状态。
type CellKind int

const (
	CellFree CellKind = iota
	CellReserved
	CellExclusive
	CellPool
)

type Cell struct {
	CPU   int
	Kind  CellKind
	Key   byte   // 图例键（'A'..），Free/Reserved 为 0
	Owner string // pod（ns/name）或池名
}

type ZoneGrid struct {
	ID int
	// Rows：SMT 机器为 threads-per-core 行（每列一个物理核，行 r 是各核第 r 个
	// 超线程）；无 SMT 单行。列按物理核最小 cpu 升序。
	Rows [][]Cell
}

type LegendEntry struct {
	Key   byte
	Owner string
	Kind  CellKind
	CPUs  int // 该占用者持有的核数
}

type NodeGrid struct {
	Node   string
	Zones  []ZoneGrid
	Legend []LegendEntry
	// 占用统计（Used = 独占+池，不含预留）
	UsedCPUs, TotalCPUs   int
	UsedZones, TotalZones int
}

// BuildNodeGrid 把 KNT 账本渲染为核心网格（纯函数，可测）。
func BuildNodeGrid(cr *v1alpha1.KoreNodeTopology) NodeGrid {
	g := NodeGrid{Node: cr.Name}

	owner := map[int]*LegendEntry{} // cpu → 图例项
	var legend []LegendEntry
	assign := func(name string, kind CellKind, cpus string) {
		cs, err := cpuset.Parse(cpus)
		if err != nil {
			return
		}
		e := LegendEntry{Key: byte('A' + len(legend)%26), Owner: name, Kind: kind, CPUs: cs.Size()}
		legend = append(legend, e)
		for _, c := range cs.List() {
			owner[c] = &legend[len(legend)-1]
		}
	}
	// 稳定图例顺序：独占按 pod 名，池按池名
	allocs := append([]v1alpha1.Allocation(nil), cr.Status.Allocations...)
	sort.Slice(allocs, func(i, j int) bool { return allocs[i].Pod < allocs[j].Pod })
	for _, a := range allocs {
		assign(a.Pod, CellExclusive, a.Cpuset)
	}
	pools := append([]v1alpha1.Pool(nil), cr.Status.Pools...)
	sort.Slice(pools, func(i, j int) bool { return pools[i].Name < pools[j].Name })
	for _, p := range pools {
		assign("pool:"+p.Name, CellPool, p.Cpuset)
	}
	g.Legend = legend

	reserved := cpuset.New()
	if cs, err := cpuset.Parse(cr.Status.ReservedSystemCpus); err == nil {
		reserved = cs
	}
	mkCell := func(cpu int) Cell {
		if reserved.Contains(cpu) {
			return Cell{CPU: cpu, Kind: CellReserved}
		}
		if e, ok := owner[cpu]; ok {
			return Cell{CPU: cpu, Kind: e.Kind, Key: e.Key, Owner: e.Owner}
		}
		return Cell{CPU: cpu, Kind: CellFree}
	}

	for _, z := range cr.Status.Zones {
		zg := ZoneGrid{ID: z.ID}
		if len(z.SMTSiblings) > 0 {
			groups := append([][]int(nil), z.SMTSiblings...)
			sort.Slice(groups, func(i, j int) bool { return groups[i][0] < groups[j][0] })
			threads := len(groups[0])
			zg.Rows = make([][]Cell, threads)
			for _, grp := range groups {
				for r := 0; r < threads && r < len(grp); r++ {
					zg.Rows[r] = append(zg.Rows[r], mkCell(grp[r]))
				}
			}
		} else if cs, err := cpuset.Parse(z.Cpus); err == nil {
			row := make([]Cell, 0, cs.Size())
			for _, c := range cs.List() {
				row = append(row, mkCell(c))
			}
			zg.Rows = [][]Cell{row}
		}
		zoneUsed := false
		for _, row := range zg.Rows {
			for _, c := range row {
				g.TotalCPUs++
				if c.Kind == CellExclusive || c.Kind == CellPool {
					g.UsedCPUs++
					zoneUsed = true
				}
			}
		}
		g.TotalZones++
		if zoneUsed {
			g.UsedZones++
		}
		g.Zones = append(g.Zones, zg)
	}
	return g
}
