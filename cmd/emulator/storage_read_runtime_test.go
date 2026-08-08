package main

// This test crosses the composition root with the official generated client.
// It intentionally does not replace DuckDB or the gRPC transport with fakes.
// Protocol source:
// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/adapters/system"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/config"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	grpcserver "github.com/leeyh0216/go-bemu/internal/transport/grpc"
)

func TestStorageReadRuntimeServesEightLogicalStreamsFromDuckDB(t *testing.T) {
	ctx, cancel := storageReadRuntimeTestContext(t)
	defer cancel()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	repository := memory.NewCatalogRepository()
	clock := system.Clock{}
	catalog := application.NewCatalogService(repository, warehouse, clock)
	if _, err := catalog.CreateProject(ctx, domain.Project{ID: "reader-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateDataset(ctx, domain.Dataset{ProjectID: "reader-project", ID: "analytics", Location: "US"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateTable(ctx, domain.Table{
		ProjectID: "reader-project", DatasetID: "analytics", ID: "events", Type: "TABLE",
		Schema: []domain.Field{{Name: "id", Type: "INT64"}, {Name: "name", Type: "STRING"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.Query(ctx, ports.QueryRequest{
		ProjectID: "reader-project", SQL: "INSERT INTO `reader-project.analytics.events` VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')",
	}); err != nil {
		t.Fatal(err)
	}

	cfg := config.Defaults()
	cfg.Database.TempDirectory = t.TempDir()
	runtime, err := composeStorageRead(
		cfg, warehouse, newStorageReadPredicateParser(t), repository, clock, system.IDGenerator{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), storageReadRuntimeTestTimeout(t))
		defer closeCancel()
		_ = runtime.Close(closeContext)
	})

	listener := bufconn.Listen(4 << 20)
	server := grpcserver.NewWithServices(grpcserver.Services{Read: runtime.Service})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	health, err := grpc_health_v1.NewHealthClient(connection).Check(ctx, &grpc_health_v1.HealthCheckRequest{
		Service: "google.cloud.bigquery.storage.v1.BigQueryRead",
	})
	if err != nil || health.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("Storage Read health = %#v, %v", health, err)
	}
	client := storagepb.NewBigQueryReadClient(connection)
	session, err := client.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
		Parent: "projects/reader-project", MaxStreamCount: 16, PreferredMinStreamCount: 8,
		ReadSession: &storagepb.ReadSession{
			Table: "projects/reader-project/datasets/analytics/tables/events", DataFormat: storagepb.DataFormat_ARROW,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(session.GetStreams()) != 8 || len(session.GetArrowSchema().GetSerializedSchema()) == 0 {
		t.Fatalf("session streams/schema = %d/%d", len(session.GetStreams()), len(session.GetArrowSchema().GetSerializedSchema()))
	}
	var rows int64
	for _, readStream := range session.GetStreams() {
		responses, err := client.ReadRows(ctx, &storagepb.ReadRowsRequest{ReadStream: readStream.GetName()})
		if err != nil {
			t.Fatal(err)
		}
		for {
			response, err := responses.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(response.GetArrowRecordBatch().GetSerializedRecordBatch()) == 0 {
				t.Fatal("non-empty response lacks an Arrow IPC record batch")
			}
			rows += response.GetRowCount()
		}
	}
	if rows != 4 {
		t.Fatalf("read rows = %d, want 4", rows)
	}
}

func storageReadRuntimeTestContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(t.Context(), storageReadRuntimeTestTimeout(t))
}

func storageReadRuntimeTestTimeout(t *testing.T) time.Duration {
	t.Helper()
	timeout := 10 * time.Second
	if configured := os.Getenv("BQEMU_STORAGE_READ_TEST_TIMEOUT"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		if err != nil || parsed <= 0 {
			t.Fatalf("BQEMU_STORAGE_READ_TEST_TIMEOUT must be a positive Go duration: %v", err)
		}
		timeout = parsed
	}
	return timeout
}
