package main

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/adapters/sqlite"
	"github.com/leeyh0216/go-bemu/internal/adapters/system"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/config"
	"github.com/leeyh0216/go-bemu/internal/contracttest"
)

func TestComposeStorageWriteSupportsExplicitDisableAndCleanClose(t *testing.T) {
	contracttest.Operation(t, "grpc.bigquery-write.create-write-stream")
	contracttest.Operation(t, "grpc.bigquery-write.append-rows")
	contracttest.Operation(t, "grpc.bigquery-write.get-write-stream")
	contracttest.Operation(t, "grpc.bigquery-write.finalize-write-stream")
	contracttest.Operation(t, "grpc.bigquery-write.batch-commit-write-streams")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	cfg := config.Defaults()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	catalog := application.NewCatalogService(memory.NewCatalogRepository(), warehouse, system.Clock{})

	cfg.Storage.Write.Enabled = false
	disabled, err := composeStorageWrite(ctx, cfg, warehouse, catalog, system.Clock{}, system.IDGenerator{}, logger)
	if err != nil || disabled.Service != nil {
		t.Fatalf("disabled Storage Write = %#v, %v", disabled, err)
	}

	cfg.Storage.Write.Enabled = true
	state, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	runtime, err := composeStorageWrite(
		ctx, cfg, warehouse, catalog, system.Clock{}, system.IDGenerator{}, logger, state.WriteState(),
	)
	if err != nil || runtime.Service == nil {
		t.Fatalf("enabled Storage Write = %#v, %v", runtime, err)
	}
	if err := runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
