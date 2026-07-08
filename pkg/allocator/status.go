package allocator

import (
	"k8s.io/apimachinery/pkg/api/resource"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
)

// BuildStatus 把当前分配状态渲染为 KoreNodeTopology 的 status（agent 定期写 CR）。
func BuildStatus(s *State) v1alpha1.KoreNodeTopologyStatus {
	st := v1alpha1.KoreNodeTopologyStatus{ReservedSystemCpus: s.reserved.String()}
	for _, z := range s.topo.Zones {
		zone := v1alpha1.Zone{
			ID:          z.ID,
			Cpus:        z.CPUs.String(),
			Allocatable: z.CPUs.Difference(s.reserved).Size(),
			FreeCpus:    s.FreeInZone(z.ID).String(),
			MemoryTotal: *resource.NewQuantity(z.MemoryTotalBytes, resource.BinarySI),
			Devices:     []v1alpha1.Device{},
		}
		if s.topo.SMTEnabled() {
			seen := map[int]bool{}
			for _, c := range z.CPUs.List() {
				g := s.topo.Siblings[c]
				m := g.List()[0]
				if seen[m] {
					continue
				}
				seen[m] = true
				zone.SMTSiblings = append(zone.SMTSiblings, g.List())
			}
		}
		st.Zones = append(st.Zones, zone)
	}
	for _, a := range s.Allocations() {
		st.Allocations = append(st.Allocations, v1alpha1.Allocation{
			PodUID: a.PodUID, Pod: a.Pod, Container: a.Container,
			Cpuset: a.CPUs.String(), NUMA: a.NUMA,
		})
	}
	return st
}
