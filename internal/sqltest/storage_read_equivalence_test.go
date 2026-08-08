package sqltest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	googlesqladapter "github.com/leeyh0216/go-bemu/internal/adapters/googlesql"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/application"
	readdomain "github.com/leeyh0216/go-bemu/internal/storageread/domain"
	readports "github.com/leeyh0216/go-bemu/internal/storageread/ports"
)

func TestStorageReadFilterMatchesGoogleSQLRegressionCase(t *testing.T) {
	test := loadRegressionCaseByID(t, "storage-read-partition-filter")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	clock := regressionClock{instant: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	catalog := application.NewCatalogService(
		memory.NewCatalogRepository(), warehouse, clock,
		application.WithDDLStorage(warehouse), application.WithTableDataReader(warehouse),
	)
	if err := createFixtureCatalog(ctx, catalog, test.Dataset); err != nil {
		t.Fatal(err)
	}
	gateway, err := googlesqladapter.NewGateway(catalog)
	if err != nil {
		t.Fatal(err)
	}
	queries, err := application.NewQueryService(
		memory.NewJobRepository(), clock, &regressionIDs{},
		application.WithGoogleSQLGateway(gateway),
		application.WithStatementExecutor(warehouse),
		application.WithStatementMaterializer(warehouse),
		application.WithQueryDestinationCatalog(catalog),
		application.WithQueryDDLExecutor(catalog),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = queries.Close(closeCtx)
	})
	if err := seedFixtureRows(ctx, queries, test.Dataset); err != nil {
		t.Fatal(err)
	}

	reference, err := queries.RunSync(ctx, application.QueryInput{
		ProjectID: test.DefaultProject, DefaultProjectID: test.DefaultProject,
		DefaultDataset: test.DefaultDataset, Location: fixtureLocation(test), SQL: test.SQL,
	})
	if err != nil {
		t.Fatal(err)
	}
	if reference.Error != nil || reference.Result == nil {
		t.Fatalf("reference query failed: error=%+v result=%+v", reference.Error, reference.Result)
	}
	if err := Compare(test, Outcome{Result: reference.Result}); err != nil {
		t.Fatalf("reference query: %v", err)
	}

	table, err := catalog.GetTable(ctx, "test-project", "analytics", "partitioned_events")
	if err != nil {
		t.Fatal(err)
	}
	if table.TimePartitioning == nil || table.TimePartitioning.Field != "event_date" || table.TimePartitioning.Type != "DAY" {
		t.Fatalf("partition metadata = %+v", table.TimePartitioning)
	}
	restriction, err := gateway.ParseExpression(ctx,
		"id IN (2, 3, 9) AND name LIKE 'prefix-%' AND "+
			"event_date >= CAST('2026-08-02' AS DATE) AND "+
			"event_at < TIMESTAMP '2026-08-04T00:00:00Z' AND nullable_id IS NOT NULL")
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := duckdb.NewReadSnapshotMaterializer(warehouse, catalog, readports.SnapshotMaterializerConfig{
		TempDir: t.TempDir(), TempFilePattern: "bqemu-sql-regression-*",
		SpillThresholdBytes: 1 << 20, MaxRowBytes: 1 << 20, MaxBatchBytes: 1 << 20,
		MaxSchemaBytes: 1 << 20, MaxSnapshotBytes: 32 << 20, MaxSnapshotRows: 10_000,
		ProtocolModelVersion: "google.cloud.bigquery.storage.v1@sql-regression",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := materializer.Materialize(ctx, readports.MaterializeRequest{
		Table:  "projects/test-project/datasets/analytics/tables/partitioned_events",
		Format: readdomain.FormatArrow, SelectedFields: []string{"id", "name"}, RowRestriction: restriction,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = snapshot.Close(context.Background()) }()
	readRows := decodeStorageReadIDNameRows(t, ctx, snapshot)

	fields := fixtureFieldsToDomain(test.Expected.Schema)
	want, err := canonicalRows(fields, reference.Result.Rows)
	if err != nil {
		t.Fatal(err)
	}
	got, err := canonicalRows(fields, readRows)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(want)
	slices.Sort(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Storage Read rows = %v, reference query rows = %v", got, want)
	}
}

func loadRegressionCaseByID(t *testing.T, id string) Case {
	t.Helper()
	cases, err := Load("testdata/cases")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range cases {
		if test.ID == id {
			return test
		}
	}
	t.Fatalf("SQL regression case %q not found", id)
	return Case{}
}

func decodeStorageReadIDNameRows(t *testing.T, ctx context.Context, snapshot readports.ReadSnapshot) [][]any {
	t.Helper()
	metadata := snapshot.Metadata()
	iterator, err := snapshot.OpenRange(ctx, 0, metadata.RowCount, metadata.RowCount)
	if err != nil {
		t.Fatal(err)
	}
	defer iterator.Close()
	var stream bytes.Buffer
	stream.Write(metadata.Schema.Serialized)
	for {
		batch, nextErr := iterator.Next(ctx)
		if nextErr == io.EOF {
			break
		}
		if nextErr != nil {
			t.Fatal(nextErr)
		}
		stream.Write(batch.SerializedRows)
	}
	stream.Write([]byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0})
	reader, err := ipc.NewReader(&stream)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	var rows [][]any
	for reader.Next() {
		record := reader.RecordBatch()
		ids, ok := record.Column(0).(*array.Int64)
		if !ok {
			t.Fatalf("id column = %T", record.Column(0))
		}
		names, ok := record.Column(1).(*array.String)
		if !ok {
			t.Fatalf("name column = %T", record.Column(1))
		}
		for index := 0; index < int(record.NumRows()); index++ {
			rows = append(rows, []any{ids.Value(index), names.Value(index)})
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatal(fmt.Errorf("decode Storage Read rows: %w", err))
	}
	return rows
}
