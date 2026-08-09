package main

import (
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	stateadapter "github.com/leeyh0216/go-bemu/internal/adapters/sqlite"
	"github.com/leeyh0216/go-bemu/internal/adapters/system"
	"github.com/leeyh0216/go-bemu/internal/config"
	readdomain "github.com/leeyh0216/go-bemu/internal/storageread/domain"
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
	stateStore, err := stateadapter.Open(ctx, stateadapter.DefaultConfig(filepath.Join(t.TempDir(), "state.sqlite")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stateStore.Close() })
	createdAt := time.Now().UTC().Add(-time.Minute)
	staleSession := "projects/reader/locations/US/sessions/stale"
	staleStream := staleSession + "/streams/0"
	if err := stateStore.CreateSession(ctx, readdomain.SessionRecord{
		Name: staleSession, Table: "projects/data/datasets/analytics/tables/events",
		Format: readdomain.FormatArrow, Streams: []readdomain.Stream{{Name: staleStream}},
		RowRestrictionDigest: "sha256:" + strings.Repeat("a", 64),
		SchemaFingerprint:    "sha256:" + strings.Repeat("b", 64),
		CreatedAt:            createdAt, ExpireTime: createdAt.Add(time.Hour),
		Lifecycle: readdomain.SessionActive, LifecycleUpdatedAt: createdAt,
	}); err != nil {
		t.Fatal(err)
	}
	runtime, err := composeStorageRead(cfg, warehouse, resolver, system.Clock{}, system.IDGenerator{}, logger, stateStore)
	if err != nil || runtime.Service == nil {
		t.Fatalf("enabled Storage Read = %#v, %v", runtime, err)
	}
	persisted, err := stateStore.GetStream(ctx, staleStream)
	if err != nil || persisted.Lifecycle != readdomain.SessionUnavailable {
		t.Fatalf("startup reconciliation = %#v, %v", persisted, err)
	}
	if err := runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(ctx); err != nil {
		t.Fatalf("second close must be idempotent: %v", err)
	}
}
