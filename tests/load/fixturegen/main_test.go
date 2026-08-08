package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateMakesFixtureTreeReadable(t *testing.T) {
	root := filepath.Join(t.TempDir(), "seed")
	locked := manifest{Bucket: "bqemu-load-e2e"}
	locked.Objects = []fixture{
		{
			Name:    "invalid/corrupt.parquet",
			Kind:    "corrupt",
			Content: "fixture",
		},
	}

	if err := generate(context.Background(), root, locked, true); err != nil {
		t.Fatalf("generate fixture: %v", err)
	}

	assertPermissions(t, root, 0o755)
	assertPermissions(t, filepath.Join(root, locked.Bucket), 0o755)
	assertPermissions(t, filepath.Join(root, locked.Bucket, "invalid"), 0o755)
	assertPermissions(t, filepath.Join(root, locked.Bucket, "invalid", "corrupt.parquet"), 0o644)
}

func assertPermissions(t *testing.T, path string, expected os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if actual := info.Mode().Perm(); actual != expected {
		t.Fatalf("permissions for %s = %04o, want %04o", path, actual, expected)
	}
}
