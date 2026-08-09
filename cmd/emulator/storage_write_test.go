package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/adapters/system"
	"github.com/leeyh0216/go-bemu/internal/config"
	writememory "github.com/leeyh0216/go-bemu/internal/storagewrite/adapters/memory"
)

func TestComposeStorageWriteSupportsExplicitDisableAndCleanClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	cfg := config.Defaults()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	resolver := memory.NewCatalogRepository()
	streamRepository := writememory.NewRepository()

	cfg.Storage.Write.Enabled = false
	disabled, err := composeStorageWrite(ctx, cfg, warehouse, resolver, streamRepository, system.Clock{}, system.IDGenerator{}, logger)
	if err != nil || disabled.Service != nil {
		t.Fatalf("disabled Storage Write = %#v, %v", disabled, err)
	}

	cfg.Storage.Write.Enabled = true
	runtime, err := composeStorageWrite(ctx, cfg, warehouse, resolver, streamRepository, system.Clock{}, system.IDGenerator{}, logger)
	if err != nil || runtime.Service == nil {
		t.Fatalf("enabled Storage Write = %#v, %v", runtime, err)
	}
	if err := runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
