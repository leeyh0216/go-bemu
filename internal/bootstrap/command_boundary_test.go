package bootstrap

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommandIsLifecycleShell(t *testing.T) {
	path := filepath.Join("..", "..", "cmd", "emulator", "main.go")
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	for _, imported := range file.Imports {
		value := strings.Trim(imported.Path.Value, "\"")
		if strings.Contains(value, "/internal/adapters/") ||
			strings.Contains(value, "/internal/application") ||
			strings.Contains(value, "/internal/ports") ||
			strings.Contains(value, "/internal/transport/") {
			t.Errorf("command composition boundary imports %s", value)
		}
	}
}
