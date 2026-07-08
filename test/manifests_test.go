// Package test 校验 deploy/ 下全部 manifest 可解析且带基本必填字段。
package test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestDeployManifestsParse(t *testing.T) {
	root := filepath.Join("..", "deploy")
	count := 0
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".yaml") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for i, doc := range strings.Split(string(b), "\n---") {
			if strings.TrimSpace(stripComments(doc)) == "" {
				continue
			}
			var obj map[string]interface{}
			if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
				t.Errorf("%s doc#%d: %v", path, i, err)
				continue
			}
			if obj["apiVersion"] == nil || obj["kind"] == nil {
				t.Errorf("%s doc#%d: missing apiVersion/kind", path, i)
			}
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count < 15 {
		t.Fatalf("only %d manifest docs found, expected the full deploy set", count)
	}
	t.Logf("validated %d manifest documents", count)
}

func stripComments(doc string) string {
	var out []string
	for _, l := range strings.Split(doc, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "#") {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}
