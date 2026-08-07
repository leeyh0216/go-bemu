package main

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/system"
	"github.com/leeyh0216/go-bemu/internal/config"
)

func TestComposeStorageWriteSupportsExplicitDisableAndCleanClose(t *testing.T) {
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	cfg := config.Defaults()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cfg.Storage.Write.Enabled = false
	disabled, err := composeStorageWrite(cfg, warehouse, system.Clock{}, system.IDGenerator{}, logger)
	if err != nil || disabled.Service != nil {
		t.Fatalf("disabled Storage Write = %#v, %v", disabled, err)
	}

	cfg.Storage.Write.Enabled = true
	runtime, err := composeStorageWrite(cfg, warehouse, system.Clock{}, system.IDGenerator{}, logger)
	if err != nil || runtime.Service == nil {
		t.Fatalf("enabled Storage Write = %#v, %v", runtime, err)
	}
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
