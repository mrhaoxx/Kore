// Package topotest 在 t.TempDir() 下构造 fake sysfs 树供拓扑发现测试。
package topotest

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

type Zone struct {
	ID         int
	CPUList    string // 如 "0-3,8-11"
	MemTotalKB int64
	Distances  string // 如 "10 21"
}

// Write 构造 fake sysfs；siblings 是 cpu id -> thread_siblings_list 内容（如 0 -> "0,8"）。
func Write(t *testing.T, zones []Zone, siblings map[int]string) string {
	t.Helper()
	root := t.TempDir()
	for _, z := range zones {
		dir := filepath.Join(root, "devices/system/node", fmt.Sprintf("node%d", z.ID))
		mustMkdir(t, dir)
		mustWrite(t, filepath.Join(dir, "cpulist"), z.CPUList+"\n")
		mustWrite(t, filepath.Join(dir, "meminfo"),
			fmt.Sprintf("Node %d MemTotal:       %d kB\n", z.ID, z.MemTotalKB))
		mustWrite(t, filepath.Join(dir, "distance"), z.Distances+"\n")
	}
	for cpu, sib := range siblings {
		dir := filepath.Join(root, "devices/system/cpu", fmt.Sprintf("cpu%d", cpu), "topology")
		mustMkdir(t, dir)
		mustWrite(t, filepath.Join(dir, "thread_siblings_list"), sib+"\n")
	}
	return root
}

func mustMkdir(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
