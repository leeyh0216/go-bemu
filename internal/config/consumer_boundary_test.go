package config

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Keep consumer-specific names and versions in tests/integration. This guard
// deliberately checks source rather than a compiled package graph: config and
// command wiring can otherwise retain an inactive consumer switch unnoticed.
func TestProductionRuntimeDoesNotNameConsumerImplementations(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	for _, directory := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, directory), func(path string, entry os.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			contents, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, forbidden := range []string{"spark-bigquery", "pyspark", "google-cloud-sdk", "bq-cli"} {
				if strings.Contains(strings.ToLower(string(contents)), forbidden) {
					t.Errorf("consumer-specific runtime literal %q in %s", forbidden, path)
				}
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			for _, imported := range file.Imports {
				if strings.Contains(strings.Trim(imported.Path.Value, "\""), "tests/integration") {
					t.Errorf("production runtime imports integration package in %s", path)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, configPath := range []string{filepath.Join(root, "configs", "bqemu.yaml"), filepath.Join(root, "compose.yaml"), filepath.Join(root, "compose.tls.yaml")} {
		contents, err := os.ReadFile(configPath)
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{"spark-bigquery", "pyspark", "google-cloud-sdk", "bq-cli"} {
			if strings.Contains(strings.ToLower(string(contents)), forbidden) {
				t.Errorf("consumer-specific config literal %q in %s", forbidden, configPath)
			}
		}
	}
}
