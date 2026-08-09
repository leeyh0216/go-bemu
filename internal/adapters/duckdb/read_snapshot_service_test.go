package duckdb

// The application service owns stream negotiation and TTL while this adapter
// owns one shared immutable snapshot. These integration tests prove the two
// lifecycles compose without querying DuckDB once per logical stream.
//
// Protocol sources:
//   - multiple streams and snapshot consistency: https://cloud.google.com/bigquery/docs/reference/storage#key_features
//   - stream count request: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#createreadsessionrequest
//   - read session expiration: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readsession

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	readapp "github.com/leeyh0216/go-bemu/internal/storageread/application"
	readdomain "github.com/leeyh0216/go-bemu/internal/storageread/domain"
	readports "github.com/leeyh0216/go-bemu/internal/storageread/ports"
)

func TestDuckDBReadSnapshotSupportsConfiguredStreamMatrixForArrowAndAvro(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	table := catalogdomain.Table{
		ProjectID: "data-project", DatasetID: "analytics", ID: "stream_matrix", Type: "TABLE",
		Schema: []catalogdomain.Field{
			{Name: "id", Type: "INT64", Mode: "REQUIRED"},
			{Name: "name", Type: "STRING"},
		},
	}
	warehouse := newReadTestWarehouse(t, ctx, table)
	physicalTable := quoteIdentifier(physicalSchema(table.ProjectID, table.DatasetID)) + "." + quoteIdentifier(table.ID)
	if _, err := warehouse.db.ExecContext(ctx,
		"INSERT INTO "+physicalTable+" SELECT value, 'row-' || CAST(value AS VARCHAR) FROM range(32) AS rows(value)"); err != nil {
		t.Fatal(err)
	}
	resolver := &readTestSchemaResolver{table: table}
	materializer := newReadTestMaterializer(t, warehouse, resolver, readSnapshotTestConfig(t.TempDir(), 1<<20))

	formats := []readdomain.Format{readdomain.FormatArrow, readdomain.FormatAvro}
	for _, format := range formats {
		for _, streamCount := range []int32{1, 2, 4, 16} {
			name := strings.ToLower(format.String()) + "-" + strconv.Itoa(int(streamCount))
			t.Run(name, func(t *testing.T) {
				ctx, cancel := duckDBReadTestContext(t)
				defer cancel()
				clock := newReadAdapterClock()
				service := newReadAdapterService(t, materializer, clock, 30*time.Minute)
				t.Cleanup(func() { closeReadAdapterService(t, service) })
				session, err := service.CreateSession(ctx, readdomain.CreateSessionRequest{
					Parent:         "projects/reader-project",
					Table:          readTestTableResource(table),
					Format:         format,
					MaxStreamCount: streamCount,
					TraceID:        "reader-stage-matrix",
				})
				if err != nil {
					t.Fatal(err)
				}
				if len(session.Streams) != int(streamCount) || session.EstimatedRowCount != 32 {
					t.Fatalf("session streams/rows = %d/%d, want %d/32", len(session.Streams), session.EstimatedRowCount, streamCount)
				}
				cursor := int64(0)
				var receivedRows int64
				for _, stream := range session.Streams {
					if stream.StartOffset != cursor || stream.EndOffset < stream.StartOffset {
						t.Fatalf("non-contiguous stream after %d: %+v", cursor, stream)
					}
					cursor = stream.EndOffset
					firstChunk := true
					err := service.ReadRows(ctx, readdomain.ReadRowsRequest{StreamName: stream.Name}, func(chunk readdomain.ReadChunk) error {
						if chunk.Batch.RowCount <= 0 || chunk.Batch.RowCount > 7 {
							return fmt.Errorf("invalid chunk row_count %d", chunk.Batch.RowCount)
						}
						if firstChunk != (chunk.Schema != nil) {
							return fmt.Errorf("reference schema first-chunk mismatch")
						}
						firstChunk = false
						switch format {
						case readdomain.FormatArrow:
							assertSingleArrowIPCMessage(t, chunk.Batch.SerializedRows, ipc.MessageRecordBatch)
						case readdomain.FormatAvro:
							if bytes.HasPrefix(chunk.Batch.SerializedRows, []byte{'O', 'b', 'j', 1}) {
								return errors.New("Avro chunk is an object-container file")
							}
						}
						receivedRows += chunk.Batch.RowCount
						return nil
					})
					if err != nil {
						t.Fatal(err)
					}
				}
				if cursor != 32 || receivedRows != 32 {
					t.Fatalf("stream union/received rows = %d/%d, want 32/32", cursor, receivedRows)
				}
				if err := service.Close(ctx); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
	if resolver.callCount() != len(formats)*4 {
		t.Fatalf("materialized snapshots = %d, want %d (one per session, not per stream)", resolver.callCount(), len(formats)*4)
	}
}

func TestDuckDBReadSnapshotServesSixteenSpilledStreamsConcurrently(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	table := catalogdomain.Table{
		ProjectID: "data-project", DatasetID: "analytics", ID: "concurrent_streams", Type: "TABLE",
		Schema: []catalogdomain.Field{{Name: "id", Type: "INT64", Mode: "REQUIRED"}},
	}
	warehouse := newReadTestWarehouse(t, ctx, table)
	physicalTable := quoteIdentifier(physicalSchema(table.ProjectID, table.DatasetID)) + "." + quoteIdentifier(table.ID)
	if _, err := warehouse.db.ExecContext(ctx,
		"INSERT INTO "+physicalTable+" SELECT value FROM range(64) AS rows(value)"); err != nil {
		t.Fatal(err)
	}
	materializer := newReadTestMaterializer(t, warehouse, &readTestSchemaResolver{table: table}, readSnapshotTestConfig(t.TempDir(), 0))
	service := newReadAdapterService(t, materializer, newReadAdapterClock(), 30*time.Minute)
	t.Cleanup(func() { closeReadAdapterService(t, service) })
	session, err := service.CreateSession(ctx, readdomain.CreateSessionRequest{
		Parent: "projects/reader-project", Table: readTestTableResource(table),
		Format: readdomain.FormatAvro, MaxStreamCount: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(session.Streams) != 16 {
		t.Fatalf("streams = %d, want 16", len(session.Streams))
	}

	results := make(chan int64, len(session.Streams))
	errorsByStream := make(chan error, len(session.Streams))
	var readers sync.WaitGroup
	for _, stream := range session.Streams {
		stream := stream
		readers.Add(1)
		go func() {
			defer readers.Done()
			var rows int64
			err := service.ReadRows(ctx, readdomain.ReadRowsRequest{StreamName: stream.Name}, func(chunk readdomain.ReadChunk) error {
				if bytes.HasPrefix(chunk.Batch.SerializedRows, []byte{'O', 'b', 'j', 1}) {
					return errors.New("Avro chunk is an object-container file")
				}
				rows += chunk.Batch.RowCount
				return nil
			})
			if err != nil {
				errorsByStream <- err
				return
			}
			results <- rows
		}()
	}
	readers.Wait()
	close(results)
	close(errorsByStream)
	for err := range errorsByStream {
		t.Errorf("concurrent ReadRows: %v", err)
	}
	var total int64
	for rows := range results {
		total += rows
	}
	if total != 64 {
		t.Fatalf("concurrent stream rows = %d, want 64", total)
	}
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDuckDBReadSnapshotSpillsOnlyInsideConfiguredDirectoryAndRemovesOnClose(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	table := catalogdomain.Table{
		ProjectID: "data-project", DatasetID: "analytics", ID: "spilled_rows", Type: "TABLE",
		Schema: []catalogdomain.Field{{Name: "id", Type: "INT64", Mode: "REQUIRED"}},
	}
	warehouse := newReadTestWarehouse(t, ctx, table)
	insertReadTestRows(t, ctx, warehouse, table, "(1), (2), (3)")
	tempDir := t.TempDir()
	config := readSnapshotTestConfig(tempDir, 0)
	materializer := newReadTestMaterializer(t, warehouse, &readTestSchemaResolver{table: table}, config)
	snapshotPort, err := materializer.Materialize(ctx, readports.MaterializeRequest{
		Table: readTestTableResource(table), Format: readdomain.FormatAvro,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := snapshotPort.(*duckDBReadSnapshot)
	if snapshot.spillPath == "" || len(snapshot.memoryRows) != 0 || len(snapshot.spillRows) != 3 {
		t.Fatalf("unexpected spill state: path=%q memory=%d locations=%d", snapshot.spillPath, len(snapshot.memoryRows), len(snapshot.spillRows))
	}
	relative, err := filepath.Rel(tempDir, snapshot.spillPath)
	if err != nil || relative == "." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		t.Fatalf("spill path %q is outside configured directory %q", snapshot.spillPath, tempDir)
	}
	info, err := os.Stat(snapshot.spillPath)
	if err != nil {
		t.Fatal(err)
	}
	metadata := snapshot.Metadata()
	if metadata.RetainedBytes != metadata.EstimatedBytes+8*metadata.RowCount {
		t.Fatalf("spill retained/encoded rows = %d/%d/%d, want encoded bytes plus one uint64 frame per row",
			metadata.RetainedBytes, metadata.EstimatedBytes, metadata.RowCount)
	}
	if info.Size() != metadata.RetainedBytes {
		t.Fatalf("spill file/retained bytes = %d/%d, want exact file charge", info.Size(), metadata.RetainedBytes)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("spill permissions = %o, want no group/other access", info.Mode().Perm())
	}
	iterator, err := snapshot.OpenRange(ctx, 1, 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	for wantOffset := int64(1); wantOffset < 3; wantOffset++ {
		batch, err := iterator.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if batch.Offset != wantOffset || batch.RowCount != 1 {
			t.Fatalf("spilled batch = %+v", batch)
		}
	}
	if err := iterator.Close(); err != nil {
		t.Fatal(err)
	}
	spillPath := snapshot.spillPath
	if err := snapshot.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(spillPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("spill still exists after close: %v", err)
	}
	if err := snapshot.Close(ctx); err != nil {
		t.Fatalf("idempotent snapshot close: %v", err)
	}
	if _, err := snapshot.OpenRange(ctx, 0, 1, 1); err == nil {
		t.Fatal("closed snapshot accepted a new reader")
	}
}

func TestDuckDBReadSnapshotTTLExpiryRemovesSpill(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	table := catalogdomain.Table{
		ProjectID: "data-project", DatasetID: "analytics", ID: "expiring_rows", Type: "TABLE",
		Schema: []catalogdomain.Field{{Name: "id", Type: "INT64", Mode: "REQUIRED"}},
	}
	warehouse := newReadTestWarehouse(t, ctx, table)
	insertReadTestRows(t, ctx, warehouse, table, "(1), (2)")
	tempDir := t.TempDir()
	materializer := newReadTestMaterializer(t, warehouse, &readTestSchemaResolver{table: table}, readSnapshotTestConfig(tempDir, 0))
	clock := newReadAdapterClock()
	ttl := 37 * time.Minute
	service := newReadAdapterService(t, materializer, clock, ttl)
	t.Cleanup(func() { closeReadAdapterService(t, service) })
	session, err := service.CreateSession(ctx, readdomain.CreateSessionRequest{
		Parent: "projects/reader-project", Table: readTestTableResource(table),
		Format: readdomain.FormatArrow, MaxStreamCount: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if entries := readTempEntries(t, tempDir); len(entries) != 1 {
		t.Fatalf("spill files before expiry = %v, want one", entries)
	}
	clock.Advance(ttl)
	if err := service.SweepExpired(ctx); err != nil {
		t.Fatal(err)
	}
	if entries := readTempEntries(t, tempDir); len(entries) != 0 {
		t.Fatalf("spill files after expiry = %v, want none", entries)
	}
	err = service.ReadRows(ctx, readdomain.ReadRowsRequest{StreamName: session.Streams[0].Name}, func(readdomain.ReadChunk) error { return nil })
	if readdomain.CodeOf(err) != readdomain.ErrorNotFound {
		t.Fatalf("expired read error = %v, want NOT_FOUND", err)
	}
}

func TestDuckDBReadSnapshotMaterializationFailureRemovesPartialSpill(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	table := catalogdomain.Table{
		ProjectID: "data-project", DatasetID: "analytics", ID: "limited_rows", Type: "TABLE",
		Schema: []catalogdomain.Field{{Name: "id", Type: "INT64", Mode: "REQUIRED"}},
	}
	warehouse := newReadTestWarehouse(t, ctx, table)
	insertReadTestRows(t, ctx, warehouse, table, "(1), (2)")
	tempDir := t.TempDir()
	config := readSnapshotTestConfig(tempDir, 0)
	config.MaxSnapshotRows = 1
	materializer := newReadTestMaterializer(t, warehouse, &readTestSchemaResolver{table: table}, config)
	if _, err := materializer.Materialize(ctx, readports.MaterializeRequest{
		Table: readTestTableResource(table), Format: readdomain.FormatArrow,
	}); err == nil || !strings.Contains(err.Error(), "max rows") {
		t.Fatalf("snapshot limit error = %v", err)
	}
	if entries := readTempEntries(t, tempDir); len(entries) != 0 {
		t.Fatalf("partial spill survived failed materialization: %v", entries)
	}
}

func TestDuckDBReadSnapshotByteLimitIsResourceExhaustedAndRemovesPartialSpill(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	table := catalogdomain.Table{
		ProjectID: "data-project", DatasetID: "analytics", ID: "byte_limited_rows", Type: "TABLE",
		Schema: []catalogdomain.Field{{Name: "id", Type: "INT64", Mode: "REQUIRED"}},
	}
	warehouse := newReadTestWarehouse(t, ctx, table)
	insertReadTestRows(t, ctx, warehouse, table, "(1), (2)")
	first, err := encodeSnapshotRow([]snapshotValue{{Int: 1}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := encodeSnapshotRow([]snapshotValue{{Int: 2}})
	if err != nil {
		t.Fatal(err)
	}
	tempDir := t.TempDir()
	config := readSnapshotTestConfig(tempDir, 0)
	config.MaxSnapshotBytes = int64(max(len(first), len(second)) + 8)
	materializer := newReadTestMaterializer(t, warehouse, &readTestSchemaResolver{table: table}, config)
	_, err = materializer.Materialize(ctx, readports.MaterializeRequest{
		Table: readTestTableResource(table), Format: readdomain.FormatArrow,
	})
	if readdomain.CodeOf(err) != readdomain.ErrorResourceExhausted {
		t.Fatalf("snapshot byte limit code = %s, want RESOURCE_EXHAUSTED: %v", readdomain.CodeOf(err), err)
	}
	if entries := readTempEntries(t, tempDir); len(entries) != 0 {
		t.Fatalf("partial spill survived byte-limit failure: %v", entries)
	}
}

func TestDuckDBReadSnapshotLogsStatementAndEncodedPayloadSideEffects(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	table := catalogdomain.Table{
		ProjectID: "data-project", DatasetID: "analytics", ID: "logged_rows", Type: "TABLE",
		Schema: []catalogdomain.Field{
			{Name: "id", Type: "INT64", Mode: "REQUIRED"},
			{Name: "payload", Type: "STRING"},
		},
	}
	warehouse := newReadTestWarehouse(t, ctx, table)
	insertReadTestRows(t, ctx, warehouse, table, "(1, 'restriction-secret')")
	tempDir := t.TempDir()
	materializer := newReadTestMaterializer(t, warehouse, &readTestSchemaResolver{table: table}, readSnapshotTestConfig(tempDir, 0))
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previous)
	snapshotPort, err := materializer.Materialize(ctx, readports.MaterializeRequest{
		Table: readTestTableResource(table), Format: readdomain.FormatArrow,
		RowRestriction: mustParseReadRestriction(t, "payload = 'restriction-secret'"),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := snapshotPort.(*duckDBReadSnapshot)
	iterator, err := snapshot.OpenRange(ctx, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := iterator.Next(ctx); err != nil {
		t.Fatal(err)
	}
	if err := iterator.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(ctx); err != nil {
		t.Fatal(err)
	}
	output := logs.String()
	// The application boundary logs the submitted row restriction. This adapter
	// receives only its immutable AST and logs the lowered statement and encoded
	// storage payloads.
	for _, raw := range []string{tempDir, "SELECT "} {
		if !strings.Contains(output, raw) {
			t.Fatalf("structured logs omitted raw value %q: %s", raw, output)
		}
	}
	for _, required := range []string{
		"side_effect.before", "side_effect.after", "model_version",
		"statement_digest", "restriction_digest", "row_digest", "payload_digest", "temp_dir_digest",
		"materialize_read_snapshot", "create_read_snapshot_spill", "write_read_snapshot_spill_row",
		"open_read_snapshot_spill_reader", "read_snapshot_spill_row", "remove_read_snapshot_spill",
	} {
		if !strings.Contains(output, required) {
			t.Fatalf("structured logs lack %q: %s", required, output)
		}
	}
}

func newReadAdapterService(t *testing.T, materializer *DuckDBReadSnapshotMaterializer, clock *readAdapterClock, ttl time.Duration) *readapp.Service {
	t.Helper()
	service, err := readapp.New(readapp.Config{
		Location:              "test-location",
		ProtocolModelVersion:  readSnapshotTestModelVersion,
		MaxStreams:            16,
		DefaultStreamCount:    4,
		SessionTTL:            ttl,
		CleanupInterval:       time.Minute,
		MaxRowsPerResponse:    7,
		MaxSessions:           32,
		MaxSnapshotBytes:      32 << 20,
		MaxTotalSnapshotBytes: 128 << 20,
	}, materializer, newReadRestrictionParser(t), clock, &readAdapterIDs{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func closeReadAdapterService(t *testing.T, service *readapp.Service) {
	t.Helper()
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	if err := service.Close(ctx); err != nil {
		t.Errorf("close Storage Read service: %v", err)
	}
}

func readTempEntries(t *testing.T, directory string) []string {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(entries))
	for index, entry := range entries {
		names[index] = entry.Name()
	}
	return names
}

type readAdapterClock struct {
	mu  sync.Mutex
	now time.Time
}

func newReadAdapterClock() *readAdapterClock {
	return &readAdapterClock{now: time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)}
}

func (c *readAdapterClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *readAdapterClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}

type readAdapterIDs struct {
	mu   sync.Mutex
	next int
}

func (g *readAdapterIDs) NewID() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.next++
	return fmt.Sprintf("duckdb-read-%d", g.next)
}
