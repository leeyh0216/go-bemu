package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/system"
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

	cfg.Storage.Write.Enabled = false
	disabled, err := composeStorageWrite(ctx, cfg, warehouse, system.Clock{}, system.IDGenerator{}, logger)
	if err != nil || disabled.Service != nil {
		t.Fatalf("disabled Storage Write = %#v, %v", disabled, err)
	}

	cfg.Storage.Write.Enabled = true
	runtime, err := composeStorageWrite(ctx, cfg, warehouse, system.Clock{}, system.IDGenerator{}, logger)
	if err != nil || runtime.Service == nil {
		t.Fatalf("enabled Storage Write = %#v, %v", runtime, err)
	}
	if err := runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
