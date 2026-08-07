package main

import (
	"io"
	"log/slog"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/adapters/system"
	"github.com/leeyh0216/go-bemu/internal/config"
)

func TestComposeStorageReadSupportsExplicitDisableAndCleanClose(t *testing.T) {
	ctx, cancel := storageReadRuntimeTestContext(t)
	defer cancel()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	resolver := memory.NewCatalogRepository()
	cfg := config.Defaults()
	cfg.Database.TempDirectory = t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cfg.Storage.Read.Enabled = false
	disabled, err := composeStorageRead(cfg, warehouse, resolver, system.Clock{}, system.IDGenerator{}, logger)
	if err != nil || disabled.Service != nil {
		t.Fatalf("disabled Storage Read = %#v, %v", disabled, err)
	}

	cfg.Storage.Read.Enabled = true
	runtime, err := composeStorageRead(cfg, warehouse, resolver, system.Clock{}, system.IDGenerator{}, logger)
	if err != nil || runtime.Service == nil {
		t.Fatalf("enabled Storage Read = %#v, %v", runtime, err)
	}
	if err := runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(ctx); err != nil {
		t.Fatalf("second close must be idempotent: %v", err)
	}
}
