// Package agent 是 kore-agent 的顶层接线。
package agent

import (
	"encoding/json"

	"k8s.io/utils/cpuset"

	"github.com/zjusct/kore/pkg/allocator"
	"github.com/zjusct/kore/pkg/topology"
)

// Inspect 发现本机拓扑并输出空状态的 KoreNodeTopology status JSON（真机冒烟用）。
func Inspect(sysfsRoot, reservedCpus string) (string, error) {
	topo, err := topology.Discover(sysfsRoot)
	if err != nil {
		return "", err
	}
	reserved := cpuset.New()
	if reservedCpus != "" {
		if reserved, err = cpuset.Parse(reservedCpus); err != nil {
			return "", err
		}
	}
	st := allocator.BuildStatus(allocator.NewState(topo, reserved))
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}
