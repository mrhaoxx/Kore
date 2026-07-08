package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if c.DefaultPlacement != "pack" || c.DefaultSMTPolicy != "full-core" || c.Remediation != "strict" {
		t.Fatalf("defaults: %+v", c)
	}
	r, err := c.Reserved()
	if err != nil || r.Size() != 0 {
		t.Fatalf("reserved: %v %v", r, err)
	}
}

func TestLoadFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "cfg.yaml")
	os.WriteFile(p, []byte("reservedSystemCpus: \"0-1\"\ndefaultPlacement: scatter\nremediation: repair\n"), 0o644)
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.DefaultPlacement != "scatter" || c.Remediation != "repair" || c.DefaultSMTPolicy != "full-core" {
		t.Fatalf("%+v", c)
	}
	if r, _ := c.Reserved(); r.String() != "0-1" {
		t.Fatalf("reserved = %s", r)
	}
}

func TestSharedPoolMin(t *testing.T) {
	p := filepath.Join(t.TempDir(), "cfg.yaml")
	os.WriteFile(p, []byte("sharedPoolMin: 8\n"), 0o644)
	c, err := Load(p)
	if err != nil || c.SharedPoolMin != 8 {
		t.Fatalf("%+v %v", c, err)
	}
	os.WriteFile(p, []byte("sharedPoolMin: -1\n"), 0o644)
	if _, err := Load(p); err == nil {
		t.Fatal("negative sharedPoolMin must be rejected")
	}
}

func TestLoadInvalidEnum(t *testing.T) {
	p := filepath.Join(t.TempDir(), "cfg.yaml")
	os.WriteFile(p, []byte("defaultPlacement: diagonal\n"), 0o644)
	if _, err := Load(p); err == nil {
		t.Fatal("expected error")
	}
}
