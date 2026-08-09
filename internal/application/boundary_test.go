package application

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestApplicationBoundaryDoesNotImportAdaptersOrTransports(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(".", entry.Name())
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imported := range file.Imports {
			value := strings.Trim(imported.Path.Value, "\"")
			if strings.Contains(value, "/internal/adapters/") || strings.Contains(value, "/internal/transport/") {
				t.Errorf("application boundary imports %s in %s", value, path)
			}
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(contents), "*sqlite.") || strings.Contains(string(contents), "*duckdb.") {
			t.Errorf("application boundary exposes concrete repository or engine in %s", path)
		}
	}
}
