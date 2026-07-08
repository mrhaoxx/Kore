// Package config 是 kore-agent 的节点配置（由 ConfigMap 挂载为文件）。
package config

import (
	"fmt"
	"os"

	"k8s.io/utils/cpuset"
	"sigs.k8s.io/yaml"
)

type Config struct {
	// ReservedSystemCpus 是系统预留核（cpulist 语法），不参与独占分配与共享池。
	ReservedSystemCpus string `json:"reservedSystemCpus"`
	DefaultPlacement   string `json:"defaultPlacement"` // pack | scatter
	DefaultSMTPolicy   string `json:"defaultSMTPolicy"` // full-core | logical
	Remediation        string `json:"remediation"`      // strict | repair（spec §6 对账兜底）
	// SharedPoolMin：独占分配/建池后全局共享池的最小保留核数（0 = 不限制）。
	SharedPoolMin int `json:"sharedPoolMin"`
}

func Load(path string) (*Config, error) {
	c := &Config{DefaultPlacement: "pack", DefaultSMTPolicy: "full-core", Remediation: "strict"}
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if err := yaml.Unmarshal(b, c); err != nil {
			return nil, err
		}
	}
	if err := oneOf("defaultPlacement", c.DefaultPlacement, "pack", "scatter"); err != nil {
		return nil, err
	}
	if err := oneOf("defaultSMTPolicy", c.DefaultSMTPolicy, "full-core", "logical"); err != nil {
		return nil, err
	}
	if err := oneOf("remediation", c.Remediation, "strict", "repair"); err != nil {
		return nil, err
	}
	if c.SharedPoolMin < 0 {
		return nil, fmt.Errorf("sharedPoolMin: must be >= 0, got %d", c.SharedPoolMin)
	}
	if _, err := c.Reserved(); err != nil {
		return nil, fmt.Errorf("reservedSystemCpus: %w", err)
	}
	return c, nil
}

func (c *Config) Reserved() (cpuset.CPUSet, error) {
	if c.ReservedSystemCpus == "" {
		return cpuset.New(), nil
	}
	return cpuset.Parse(c.ReservedSystemCpus)
}

func oneOf(field, v string, allowed ...string) error {
	for _, a := range allowed {
		if v == a {
			return nil
		}
	}
	return fmt.Errorf("%s: invalid value %q (allowed: %v)", field, v, allowed)
}
