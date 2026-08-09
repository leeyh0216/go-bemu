package duckdb

// These tests cross the outbound port with a real DuckDB query and then read
// the official Storage Read payload shapes. Pure encoder tests alone would not
// catch a driver value-shape change after a duckdb-go upgrade.
//
// Protocol sources:
//   - projection/filter options: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readsession.tablereadoptions
//   - ReadRows offset semantics: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readrowsrequest
//   - Arrow IPC messages: https://arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	readdomain "github.com/leeyh0216/go-bemu/internal/storageread/domain"
)

const readSnapshotTestModelVersion = "google.cloud.bigquery.storage.v1@duckdb-adapter-test"

func TestDuckDBReadSnapshotAppliesProjectionRestrictionAndStableResume(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	table := catalogdomain.Table{
		ProjectID: "data-project", DatasetID: "analytics", ID: "events", Type: "TABLE",
		Schema: []catalogdomain.Field{
			{Name: "id", Type: "INT64", Mode: "REQUIRED", Description: "stable identifier"},
			{Name: "name", Type: "STRING"},
			{Name: "score", Type: "FLOAT64"},
		},
	}
	warehouse := newReadTestWarehouse(t, ctx, table)
	insertReadTestRows(t, ctx, warehouse, table,
		"(1, 'one', 1.5), (2, 'skip', 2.5), (3, 'three', 3.5), (4, 'four', 4.5)")
	resolver := &readTestSchemaResolver{table: table}
	materializer := newReadTestMaterializer(t, warehouse, resolver, readSnapshotTestConfig(t.TempDir(), 1<<20))

	snapshotPort, err := materializer.Materialize(ctx, readdomain.MaterializeRequest{
		Table:          readTestTableResource(table),
		Format:         readdomain.FormatArrow,
		SelectedFields: []string{"id", "name"},
		RowRestriction: "id >= 2 AND name != 'skip'",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := snapshotPort.(*duckDBReadSnapshot)
	defer closeReadSnapshot(t, snapshot)
	metadata := snapshot.Metadata()
	if metadata.RowCount != 2 {
		t.Fatalf("snapshot row_count = %d, want 2", metadata.RowCount)
	}
	if metadata.EstimatedBytes <= 0 {
		t.Fatalf("snapshot estimated bytes = %d, want positive", metadata.EstimatedBytes)
	}
	if metadata.RetainedBytes != metadata.EstimatedBytes {
		t.Fatalf("in-memory retained/estimated bytes = %d/%d, want equal encoded payload charge", metadata.RetainedBytes, metadata.EstimatedBytes)
	}
	if !slices.Equal(metadata.SelectedFields, []string{"id", "name"}) ||
		metadata.FilterShape != (readdomain.FilterShape{PredicateCount: 2, LogicalOperatorCount: 1}) {
		t.Fatalf("canonical projection/filter metadata = fields %v shape %#v", metadata.SelectedFields, metadata.FilterShape)
	}
	if got, want := fieldNames(snapshot.fields), []string{"id", "name"}; !slices.Equal(got, want) {
		t.Fatalf("projected fields = %v, want catalog order %v", got, want)
	}
	assertSingleArrowIPCMessage(t, metadata.Schema.Serialized, ipc.MessageSchema)

	// A session snapshot is immutable even when the source table changes before
	// streams are consumed. Storage Read documents this as snapshot consistency.
	insertReadTestRows(t, ctx, warehouse, table, "(5, 'late-row', 5.5)")
	if got := snapshot.Metadata().RowCount; got != 2 {
		t.Fatalf("row_count changed after source mutation: %d", got)
	}

	iterator, err := snapshot.OpenRange(ctx, 0, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	var batches [][]byte
	for wantOffset := int64(0); wantOffset < 2; wantOffset++ {
		batch, nextErr := iterator.Next(ctx)
		if nextErr != nil {
			t.Fatal(nextErr)
		}
		if batch.Offset != wantOffset || batch.RowCount != 1 {
			t.Fatalf("batch = offset %d rows %d, want offset %d rows 1", batch.Offset, batch.RowCount, wantOffset)
		}
		assertSingleArrowIPCMessage(t, batch.SerializedRows, ipc.MessageRecordBatch)
		batches = append(batches, batch.SerializedRows)
	}
	if _, err := iterator.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("iterator end = %v, want io.EOF", err)
	}
	if err := iterator.Close(); err != nil {
		t.Fatal(err)
	}
	ids, names := decodeProjectedArrowRows(t, metadata.Schema.Serialized, batches)
	if !slices.Equal(ids, []int64{3, 4}) || !slices.Equal(names, []string{"three", "four"}) {
		t.Fatalf("decoded rows = ids %v names %v", ids, names)
	}

	// Opening a range at an absolute offset is the adapter-level resume primitive
	// used by the application service for a stream-relative ReadRows offset.
	resume, err := snapshot.OpenRange(ctx, 1, 2, 8)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := resume.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Offset != 1 || resumed.RowCount != 1 {
		t.Fatalf("resumed batch = offset %d rows %d, want offset 1 rows 1", resumed.Offset, resumed.RowCount)
	}
	resumedIDs, _ := decodeProjectedArrowRows(t, metadata.Schema.Serialized, [][]byte{resumed.SerializedRows})
	if !slices.Equal(resumedIDs, []int64{4}) {
		t.Fatalf("resumed ids = %v, want [4]", resumedIDs)
	}
	if err := resume.Close(); err != nil {
		t.Fatal(err)
	}
	if resolver.callCount() != 1 {
		t.Fatalf("schema resolver calls = %d, want 1", resolver.callCount())
	}
}

func TestDuckDBReadSnapshotEmptySelectedFieldsMeansFullSchema(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	table := catalogdomain.Table{
		ProjectID: "data-project", DatasetID: "analytics", ID: "all_fields", Type: "TABLE",
		Schema: []catalogdomain.Field{
			{Name: "z_last", Type: "STRING"},
			{Name: "a_first", Type: "INT64", Mode: "REQUIRED"},
		},
	}
	warehouse := newReadTestWarehouse(t, ctx, table)
	insertReadTestRows(t, ctx, warehouse, table, "('z', 7)")
	materializer := newReadTestMaterializer(t, warehouse, &readTestSchemaResolver{table: table}, readSnapshotTestConfig(t.TempDir(), 1<<20))

	// The official contract treats an absent/empty selected_fields list as all
	// fields. It is not a zero-column projection.
	snapshotPort, err := materializer.Materialize(ctx, readdomain.MaterializeRequest{
		Table: readTestTableResource(table), Format: readdomain.FormatArrow,
		SelectedFields: []string{},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := snapshotPort.(*duckDBReadSnapshot)
	defer closeReadSnapshot(t, snapshot)
	if got, want := fieldNames(snapshot.fields), []string{"z_last", "a_first"}; !slices.Equal(got, want) {
		t.Fatalf("empty projection fields = %v, want %v", got, want)
	}
	if snapshot.Metadata().RowCount != 1 {
		t.Fatalf("row_count = %d, want 1", snapshot.Metadata().RowCount)
	}
}

func TestProjectReadFieldsPreservesRequestedOrderAndNestedSchema(t *testing.T) {
	schema := []catalogdomain.Field{
		{Name: "id", Type: "INT64"},
		{Name: "profile", Type: "RECORD", Fields: []catalogdomain.Field{
			{Name: "name", Type: "STRING"},
			{Name: "rank", Type: "INT64"},
			{Name: "address", Type: "RECORD", Fields: []catalogdomain.Field{{Name: "city", Type: "STRING"}, {Name: "zip", Type: "INT64"}}},
		}},
		{Name: "enabled", Type: "BOOL"},
	}
	fields, err := projectReadFields(schema, []string{"profile.address.zip", "id", "profile.name", "enabled"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fieldNames(fields), []string{"profile", "id", "enabled"}; !slices.Equal(got, want) {
		t.Fatalf("projected root field order = %v, want %v", got, want)
	}
	profile := fields[0]
	if got, want := fieldNames(profile.Fields), []string{"address", "name"}; !slices.Equal(got, want) {
		t.Fatalf("projected profile fields = %v, want %v", got, want)
	}
	if got, want := fieldNames(profile.Fields[0].Fields), []string{"zip"}; !slices.Equal(got, want) {
		t.Fatalf("projected address fields = %v, want %v", got, want)
	}
}

func TestDuckDBReadSnapshotReadsNestedAndRepeatedDriverValues(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	table := catalogdomain.Table{
		ProjectID: "data-project", DatasetID: "analytics", ID: "nested_values", Type: "TABLE",
		Schema: []catalogdomain.Field{
			{Name: "id", Type: "INT64", Mode: "REQUIRED"},
			{Name: "profile", Type: "RECORD", Fields: []catalogdomain.Field{
				{Name: "name", Type: "STRING"},
				{Name: "rank", Type: "INT64"},
			}},
			{Name: "tags", Type: "STRING", Mode: "REPEATED"},
		},
	}
	warehouse := newReadTestWarehouse(t, ctx, table)
	insertReadTestRows(t, ctx, warehouse, table, "(1, {'name':'alice', 'rank':7}, ['red', 'blue'])")
	materializer := newReadTestMaterializer(t, warehouse, &readTestSchemaResolver{table: table}, readSnapshotTestConfig(t.TempDir(), 1<<20))

	snapshotPort, err := materializer.Materialize(ctx, readdomain.MaterializeRequest{
		Table: readTestTableResource(table), Format: readdomain.FormatArrow,
		RowRestriction: "profile.rank >= 7",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := snapshotPort.(*duckDBReadSnapshot)
	defer closeReadSnapshot(t, snapshot)
	if snapshot.Metadata().RowCount != 1 {
		t.Fatalf("nested-filter row_count = %d, want 1", snapshot.Metadata().RowCount)
	}
	iterator, err := snapshot.OpenRange(ctx, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := iterator.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertSingleArrowIPCMessage(t, batch.SerializedRows, ipc.MessageRecordBatch)
	if err := iterator.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDuckDBReadSnapshotProjectsNestedStructFieldsForArrowAndAvro(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	table := catalogdomain.Table{
		ProjectID: "data-project", DatasetID: "analytics", ID: "nested_projection", Type: "TABLE",
		Schema: []catalogdomain.Field{
			{Name: "id", Type: "INT64", Mode: "REQUIRED"},
			{Name: "profile", Type: "RECORD", Fields: []catalogdomain.Field{{Name: "name", Type: "STRING"}, {Name: "rank", Type: "INT64"}}},
		},
	}
	warehouse := newReadTestWarehouse(t, ctx, table)
	insertReadTestRows(t, ctx, warehouse, table, "(1, {'name':'alice', 'rank':7}), (2, {'name':'bob', 'rank':1})")
	materializer := newReadTestMaterializer(t, warehouse, &readTestSchemaResolver{table: table}, readSnapshotTestConfig(t.TempDir(), 1<<20))
	for _, format := range []readdomain.Format{readdomain.FormatArrow, readdomain.FormatAvro} {
		t.Run(strings.ToLower(format.String()), func(t *testing.T) {
			snapshotPort, err := materializer.Materialize(ctx, readdomain.MaterializeRequest{
				Table: readTestTableResource(table), Format: format,
				SelectedFields: []string{"profile.name", "id"}, RowRestriction: "profile.rank >= 7",
			})
			if err != nil {
				t.Fatal(err)
			}
			snapshot := snapshotPort.(*duckDBReadSnapshot)
			defer closeReadSnapshot(t, snapshot)
			if got, want := fieldNames(snapshot.fields), []string{"profile", "id"}; !slices.Equal(got, want) {
				t.Fatalf("projected fields = %v, want %v", got, want)
			}
			if got, want := fieldNames(snapshot.fields[0].Fields), []string{"name"}; !slices.Equal(got, want) {
				t.Fatalf("projected profile fields = %v, want %v", got, want)
			}
			if snapshot.Metadata().RowCount != 1 {
				t.Fatalf("row count = %d, want 1", snapshot.Metadata().RowCount)
			}
			iterator, err := snapshot.OpenRange(ctx, 0, 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := iterator.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if batch.RowCount != 1 || len(batch.SerializedRows) == 0 {
				t.Fatalf("batch rows/bytes = %d/%d", batch.RowCount, len(batch.SerializedRows))
			}
			if format == readdomain.FormatArrow {
				assertSingleArrowIPCMessage(t, batch.SerializedRows, ipc.MessageRecordBatch)
			}
			if err := iterator.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDuckDBReadSnapshotProjectsNestedRepeatedStructField(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	table := catalogdomain.Table{
		ProjectID: "data-project", DatasetID: "analytics", ID: "repeated_nested_projection", Type: "TABLE",
		Schema: []catalogdomain.Field{
			{Name: "id", Type: "INT64", Mode: "REQUIRED"},
			{Name: "profiles", Type: "RECORD", Mode: "REPEATED", Fields: []catalogdomain.Field{{Name: "name", Type: "STRING"}, {Name: "rank", Type: "INT64"}}},
		},
	}
	warehouse := newReadTestWarehouse(t, ctx, table)
	insertReadTestRows(t, ctx, warehouse, table, "(1, [{'name':'alice', 'rank':7}, {'name':'bob', 'rank':1}])")
	materializer := newReadTestMaterializer(t, warehouse, &readTestSchemaResolver{table: table}, readSnapshotTestConfig(t.TempDir(), 1<<20))
	for _, format := range []readdomain.Format{readdomain.FormatArrow, readdomain.FormatAvro} {
		t.Run(strings.ToLower(format.String()), func(t *testing.T) {
			snapshotPort, err := materializer.Materialize(ctx, readdomain.MaterializeRequest{
				Table: readTestTableResource(table), Format: format, SelectedFields: []string{"profiles.name"},
			})
			if err != nil {
				t.Fatal(err)
			}
			snapshot := snapshotPort.(*duckDBReadSnapshot)
			defer closeReadSnapshot(t, snapshot)
			if got, want := fieldNames(snapshot.fields), []string{"profiles"}; !slices.Equal(got, want) {
				t.Fatalf("projected fields = %v, want %v", got, want)
			}
			if got, want := fieldNames(snapshot.fields[0].Fields), []string{"name"}; !slices.Equal(got, want) {
				t.Fatalf("projected repeated STRUCT fields = %v, want %v", got, want)
			}
			iterator, err := snapshot.OpenRange(ctx, 0, 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := iterator.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if batch.RowCount != 1 || len(batch.SerializedRows) == 0 {
				t.Fatalf("batch rows/bytes = %d/%d", batch.RowCount, len(batch.SerializedRows))
			}
			if format == readdomain.FormatArrow {
				assertSingleArrowIPCMessage(t, batch.SerializedRows, ipc.MessageRecordBatch)
			}
			if err := iterator.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func newReadTestWarehouse(t *testing.T, ctx context.Context, table catalogdomain.Table) *Warehouse {
	t.Helper()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := warehouse.Close(); err != nil {
			t.Errorf("close DuckDB warehouse: %v", err)
		}
	})
	if err := warehouse.CreateDataset(ctx, table.ProjectID, table.DatasetID); err != nil {
		t.Fatal(err)
	}
	if err := warehouse.CreateTable(ctx, table); err != nil {
		t.Fatal(err)
	}
	return warehouse
}

func insertReadTestRows(t *testing.T, ctx context.Context, warehouse *Warehouse, table catalogdomain.Table, values string) {
	t.Helper()
	statement := fmt.Sprintf("INSERT INTO %s.%s VALUES %s",
		quoteIdentifier(physicalSchema(table.ProjectID, table.DatasetID)), quoteIdentifier(table.ID), values)
	if _, err := warehouse.db.ExecContext(ctx, statement); err != nil {
		t.Fatalf("insert read test rows: %v", err)
	}
}

func newReadTestMaterializer(t *testing.T, warehouse *Warehouse, resolver ReadTableSchemaResolver, config ReadSnapshotConfig) *DuckDBReadSnapshotMaterializer {
	t.Helper()
	materializer, err := NewReadSnapshotMaterializer(warehouse, resolver, config)
	if err != nil {
		t.Fatal(err)
	}
	return materializer
}

func readSnapshotTestConfig(tempDir string, spillThreshold int64) ReadSnapshotConfig {
	return ReadSnapshotConfig{
		TempDir:              tempDir,
		TempFilePattern:      "bqemu-storage-read-*",
		SpillThresholdBytes:  spillThreshold,
		MaxRowBytes:          1 << 20,
		MaxBatchBytes:        1 << 20,
		MaxSchemaBytes:       1 << 20,
		MaxSnapshotBytes:     32 << 20,
		MaxSnapshotRows:      10_000,
		ProtocolModelVersion: readSnapshotTestModelVersion,
	}
}

func TestSnapshotStagerAccountsMemoryAndSpillBytes(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		threshold     int64
		wantRetained  int64
		wantSpillFile bool
	}{
		{name: "memory", threshold: 1 << 20, wantRetained: 7},
		{name: "spill", threshold: 0, wantRetained: 23, wantSpillFile: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := duckDBReadTestContext(t)
			defer cancel()
			config := readSnapshotTestConfig(t.TempDir(), testCase.threshold)
			stager := newSnapshotStager(config)
			for _, row := range [][]byte{{1, 2, 3}, {4, 5, 6, 7}} {
				if err := stager.append(ctx, row); err != nil {
					t.Fatal(err)
				}
			}
			storage, err := stager.finish(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if storage.encodedBytes != 7 || storage.retainedBytes != testCase.wantRetained {
				t.Fatalf("encoded/retained bytes = %d/%d, want 7/%d", storage.encodedBytes, storage.retainedBytes, testCase.wantRetained)
			}
			if (storage.spillPath != "") != testCase.wantSpillFile {
				t.Fatalf("spill path present = %t, want %t", storage.spillPath != "", testCase.wantSpillFile)
			}
			if storage.spillPath != "" {
				info, err := os.Stat(storage.spillPath)
				if err != nil {
					t.Fatal(err)
				}
				if info.Size() != storage.retainedBytes {
					t.Fatalf("spill file/retained bytes = %d/%d, want exact file charge", info.Size(), storage.retainedBytes)
				}
				if err := stager.abort(ctx); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestSnapshotStagerRejectsByteLimitBeforeFurtherSpillSideEffect(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	tempDir := t.TempDir()
	config := readSnapshotTestConfig(tempDir, 0)
	config.MaxSnapshotBytes = 11 // three payload bytes plus one uint64 spill frame
	stager := newSnapshotStager(config)
	if err := stager.append(ctx, []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := stager.append(ctx, []byte{4}); readdomain.CodeOf(err) != readdomain.ErrorResourceExhausted {
		t.Fatalf("second append code = %s, want RESOURCE_EXHAUSTED: %v", readdomain.CodeOf(err), err)
	}
	if stager.rowCount() != 1 || stager.retainedBytes != 11 {
		t.Fatalf("stager changed after rejected row: rows=%d retained=%d", stager.rowCount(), stager.retainedBytes)
	}
	if entries := readTempEntries(t, tempDir); len(entries) != 1 {
		t.Fatalf("spill files after accepted row = %v, want one", entries)
	}
	if err := stager.abort(ctx); err != nil {
		t.Fatal(err)
	}
	if entries := readTempEntries(t, tempDir); len(entries) != 0 {
		t.Fatalf("spill files after abort = %v, want none", entries)
	}
}

func readTestTableResource(table catalogdomain.Table) string {
	return fmt.Sprintf("projects/%s/datasets/%s/tables/%s", table.ProjectID, table.DatasetID, table.ID)
}

func fieldNames(fields []catalogdomain.Field) []string {
	result := make([]string, len(fields))
	for index, field := range fields {
		result[index] = field.Name
	}
	return result
}

func decodeProjectedArrowRows(t *testing.T, schema []byte, batches [][]byte) ([]int64, []string) {
	t.Helper()
	var stream bytes.Buffer
	stream.Write(schema)
	for _, batch := range batches {
		stream.Write(batch)
	}
	stream.Write([]byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0})
	reader, err := ipc.NewReader(&stream)
	if err != nil {
		t.Fatalf("open Arrow IPC stream assembled from bare messages: %v", err)
	}
	defer reader.Release()
	arrowFields := reader.Schema().Fields()
	gotNames := make([]string, len(arrowFields))
	for index, field := range arrowFields {
		gotNames[index] = field.Name
	}
	if got := gotNames; !slices.Equal(got, []string{"id", "name"}) {
		t.Fatalf("Arrow field order = %v, want [id name]", got)
	}
	var ids []int64
	var names []string
	for reader.Next() {
		record := reader.RecordBatch()
		idColumn, ok := record.Column(0).(*array.Int64)
		if !ok {
			t.Fatalf("id column = %T, want *array.Int64", record.Column(0))
		}
		nameColumn, ok := record.Column(1).(*array.String)
		if !ok {
			t.Fatalf("name column = %T, want *array.String", record.Column(1))
		}
		for index := 0; index < int(record.NumRows()); index++ {
			ids = append(ids, idColumn.Value(index))
			names = append(names, nameColumn.Value(index))
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("decode Arrow IPC records: %v", err)
	}
	return ids, names
}

func assertSingleArrowIPCMessage(t *testing.T, payload []byte, expected ipc.MessageType) {
	t.Helper()
	reader := ipc.NewMessageReader(bytes.NewReader(payload))
	defer reader.Release()
	message, err := reader.Message()
	if err != nil {
		t.Fatalf("decode Arrow IPC message: %v", err)
	}
	defer message.Release()
	if message.Type() != expected {
		t.Fatalf("Arrow IPC type = %s, want %s", message.Type(), expected)
	}
	if _, err := reader.Message(); !errors.Is(err, io.EOF) {
		t.Fatalf("Arrow payload contains more than one IPC message: %v", err)
	}
}

func closeReadSnapshot(t *testing.T, snapshot *duckDBReadSnapshot) {
	t.Helper()
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	if err := snapshot.Close(ctx); err != nil {
		t.Errorf("close read snapshot: %v", err)
	}
}

func duckDBReadTestContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	timeout := 5 * time.Second
	if configured := os.Getenv("BQEMU_STORAGE_READ_TEST_TIMEOUT"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		if err != nil {
			t.Fatalf("BQEMU_STORAGE_READ_TEST_TIMEOUT: %v", err)
		}
		timeout = parsed
	}
	return context.WithTimeout(context.Background(), timeout)
}

type readTestSchemaResolver struct {
	mu    sync.Mutex
	table catalogdomain.Table
	err   error
	calls int
}

func (r *readTestSchemaResolver) GetTable(ctx context.Context, projectID, datasetID, tableID string) (catalogdomain.Table, error) {
	if err := ctx.Err(); err != nil {
		return catalogdomain.Table{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.err != nil {
		return catalogdomain.Table{}, r.err
	}
	if projectID != r.table.ProjectID || datasetID != r.table.DatasetID || tableID != r.table.ID {
		return catalogdomain.Table{}, fmt.Errorf("unknown test table %s:%s.%s", projectID, datasetID, tableID)
	}
	result := r.table
	result.Schema = cloneCatalogFields(r.table.Schema)
	return result, nil
}

func (r *readTestSchemaResolver) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func TestReadSnapshotConfigRequiresExplicitPortableSettings(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	table := catalogdomain.Table{
		ProjectID: "data-project", DatasetID: "analytics", ID: "config_table", Type: "TABLE",
		Schema: []catalogdomain.Field{{Name: "id", Type: "INT64"}},
	}
	warehouse := newReadTestWarehouse(t, ctx, table)
	resolver := &readTestSchemaResolver{table: table}
	base := readSnapshotTestConfig(t.TempDir(), 1)
	tests := []struct {
		name   string
		mutate func(*ReadSnapshotConfig)
	}{
		{name: "temp directory", mutate: func(config *ReadSnapshotConfig) { config.TempDir = "" }},
		{name: "temp pattern", mutate: func(config *ReadSnapshotConfig) { config.TempFilePattern = "" }},
		{name: "spill threshold", mutate: func(config *ReadSnapshotConfig) { config.SpillThresholdBytes = -1 }},
		{name: "row limit", mutate: func(config *ReadSnapshotConfig) { config.MaxRowBytes = 0 }},
		{name: "batch limit", mutate: func(config *ReadSnapshotConfig) { config.MaxBatchBytes = 0 }},
		{name: "schema limit", mutate: func(config *ReadSnapshotConfig) { config.MaxSchemaBytes = 0 }},
		{name: "snapshot byte limit", mutate: func(config *ReadSnapshotConfig) { config.MaxSnapshotBytes = 0 }},
		{name: "snapshot limit", mutate: func(config *ReadSnapshotConfig) { config.MaxSnapshotRows = 0 }},
		{name: "model version", mutate: func(config *ReadSnapshotConfig) { config.ProtocolModelVersion = "" }},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := duckDBReadTestContext(t)
			defer cancel()
			if err := ctx.Err(); err != nil {
				t.Fatal(err)
			}
			config := base
			testCase.mutate(&config)
			if _, err := NewReadSnapshotMaterializer(warehouse, resolver, config); err == nil {
				t.Fatal("expected invalid explicit configuration to fail")
			}
		})
	}
	missingDir := base
	missingDir.TempDir = filepath.Join(t.TempDir(), "missing")
	if _, err := NewReadSnapshotMaterializer(warehouse, resolver, missingDir); err == nil {
		t.Fatal("expected a missing configured temp directory to fail")
	}
}

func TestDuckDBReadSnapshotBoundsReferenceSchema(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	table := catalogdomain.Table{
		ProjectID: "data-project", DatasetID: "analytics", ID: "bounded_schema", Type: "TABLE",
		Schema: []catalogdomain.Field{{Name: "id", Type: "INT64"}},
	}
	warehouse := newReadTestWarehouse(t, ctx, table)
	config := readSnapshotTestConfig(t.TempDir(), 1<<20)
	config.MaxSchemaBytes = 1
	materializer := newReadTestMaterializer(t, warehouse, &readTestSchemaResolver{table: table}, config)
	_, err := materializer.Materialize(ctx, readdomain.MaterializeRequest{
		Table: readTestTableResource(table), Format: readdomain.FormatArrow,
	})
	if readdomain.CodeOf(err) != readdomain.ErrorResourceExhausted {
		t.Fatalf("schema bound error code = %s, want RESOURCE_EXHAUSTED: %v", readdomain.CodeOf(err), err)
	}
}

func TestDuckDBReadSnapshotBoundsEncodedResponsePayloads(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	table := catalogdomain.Table{
		ProjectID: "data-project", DatasetID: "analytics", ID: "bounded_rows", Type: "TABLE",
		Schema: []catalogdomain.Field{{Name: "value", Type: "STRING"}},
	}
	warehouse := newReadTestWarehouse(t, ctx, table)
	insertReadTestRows(t, ctx, warehouse, table,
		"('aaaaaaaaaaaaaaaaaaaa'), ('bbbbbbbbbbbbbbbbbbbb'), ('cccccccccccccccccccc'), ('dddddddddddddddddddd')")
	config := readSnapshotTestConfig(t.TempDir(), 1<<20)
	config.MaxBatchBytes = 45
	materializer := newReadTestMaterializer(t, warehouse, &readTestSchemaResolver{table: table}, config)
	snapshot, err := materializer.Materialize(ctx, readdomain.MaterializeRequest{
		Table: readTestTableResource(table), Format: readdomain.FormatAvro,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := duckDBReadTestContext(t)
		defer closeCancel()
		_ = snapshot.Close(closeContext)
	})
	iterator, err := snapshot.OpenRange(ctx, 0, 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = iterator.Close() })
	var rows int64
	for {
		batch, err := iterator.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(batch.SerializedRows) > config.MaxBatchBytes {
			t.Fatalf("payload bytes = %d, max = %d", len(batch.SerializedRows), config.MaxBatchBytes)
		}
		rows += batch.RowCount
	}
	if rows != 4 {
		t.Fatalf("read rows = %d, want 4", rows)
	}

	tooSmall := readSnapshotTestConfig(t.TempDir(), 1<<20)
	tooSmall.MaxBatchBytes = 8
	materializer = newReadTestMaterializer(t, warehouse, &readTestSchemaResolver{table: table}, tooSmall)
	snapshot, err = materializer.Materialize(ctx, readdomain.MaterializeRequest{
		Table: readTestTableResource(table), Format: readdomain.FormatAvro,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := duckDBReadTestContext(t)
		defer closeCancel()
		_ = snapshot.Close(closeContext)
	})
	iterator, err = snapshot.OpenRange(ctx, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = iterator.Close() })
	if _, err := iterator.Next(ctx); err == nil || !strings.Contains(err.Error(), "response payload limit") {
		t.Fatalf("oversize single-row error = %v", err)
	}
}

func TestDuckDBReadSnapshotRejectsUnsupportedOptionsBeforeQuery(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	table := catalogdomain.Table{
		ProjectID: "data-project", DatasetID: "analytics", ID: "restricted", Type: "TABLE",
		Schema: []catalogdomain.Field{
			{Name: "id", Type: "INT64"},
			{Name: "tags", Type: "STRING", Mode: "REPEATED"},
		},
	}
	warehouse := newReadTestWarehouse(t, ctx, table)
	insertReadTestRows(t, ctx, warehouse, table, "(1, ['safe'])")
	materializer := newReadTestMaterializer(t, warehouse, &readTestSchemaResolver{table: table}, readSnapshotTestConfig(t.TempDir(), 1<<20))
	tests := []struct {
		name    string
		request readdomain.MaterializeRequest
		want    readdomain.ErrorCode
	}{
		{
			name: "SQL injection grammar",
			request: readdomain.MaterializeRequest{Table: readTestTableResource(table), Format: readdomain.FormatArrow,
				RowRestriction: "id = 1; DROP TABLE restricted"},
			want: readdomain.ErrorInvalidArgument,
		},
		{
			name: "repeated filter",
			request: readdomain.MaterializeRequest{Table: readTestTableResource(table), Format: readdomain.FormatArrow,
				RowRestriction: "tags = 'safe'"},
			want: readdomain.ErrorInvalidArgument,
		},
		{
			name: "invalid nested scalar projection",
			request: readdomain.MaterializeRequest{Table: readTestTableResource(table), Format: readdomain.FormatArrow,
				SelectedFields: []string{"tags.value"}},
			want: readdomain.ErrorInvalidArgument,
		},
		{
			name: "missing projection",
			request: readdomain.MaterializeRequest{Table: readTestTableResource(table), Format: readdomain.FormatArrow,
				SelectedFields: []string{"missing"}},
			want: readdomain.ErrorInvalidArgument,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := duckDBReadTestContext(t)
			defer cancel()
			if _, err := materializer.Materialize(ctx, testCase.request); readdomain.CodeOf(err) != testCase.want {
				t.Fatalf("option error code = %s, want %s: %v", readdomain.CodeOf(err), testCase.want, err)
			}
		})
	}
	historical := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	if _, err := materializer.Materialize(ctx, readdomain.MaterializeRequest{
		Table: readTestTableResource(table), Format: readdomain.FormatArrow, SnapshotTime: &historical,
	}); readdomain.CodeOf(err) != readdomain.ErrorUnimplemented {
		t.Fatalf("historical snapshot error code = %s, want UNIMPLEMENTED: %v", readdomain.CodeOf(err), err)
	}
	var rows int
	statement := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s",
		quoteIdentifier(physicalSchema(table.ProjectID, table.DatasetID)), quoteIdentifier(table.ID))
	if err := warehouse.db.QueryRowContext(ctx, statement).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("table changed after rejected restriction: rows = %d", rows)
	}
}

func TestDuckDBReadSnapshotClassifiesCatalogLookupFailures(t *testing.T) {
	ctx, cancel := duckDBReadTestContext(t)
	defer cancel()
	table := catalogdomain.Table{
		ProjectID: "data-project", DatasetID: "analytics", ID: "catalog_errors", Type: "TABLE",
		Schema: []catalogdomain.Field{{Name: "id", Type: "INT64"}},
	}
	warehouse := newReadTestWarehouse(t, ctx, table)
	request := readdomain.MaterializeRequest{Table: readTestTableResource(table), Format: readdomain.FormatArrow}

	notFound := fmt.Errorf("%w: private catalog key", catalogdomain.ErrNotFound)
	materializer := newReadTestMaterializer(t, warehouse, &readTestSchemaResolver{table: table, err: notFound}, readSnapshotTestConfig(t.TempDir(), 1<<20))
	_, err := materializer.Materialize(ctx, request)
	if readdomain.CodeOf(err) != readdomain.ErrorNotFound || !errors.Is(err, catalogdomain.ErrNotFound) {
		t.Fatalf("catalog error = %v, code = %s; want NOT_FOUND preserving cause", err, readdomain.CodeOf(err))
	}

	backendFailure := fmt.Errorf("catalog backend unavailable")
	materializer = newReadTestMaterializer(t, warehouse, &readTestSchemaResolver{table: table, err: backendFailure}, readSnapshotTestConfig(t.TempDir(), 1<<20))
	_, err = materializer.Materialize(ctx, request)
	var classified *readdomain.Error
	if !errors.Is(err, backendFailure) || errors.As(err, &classified) {
		t.Fatalf("backend lookup error must remain unclassified for application INTERNAL mapping: %v", err)
	}
}
