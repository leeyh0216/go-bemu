package duckdb

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	catalogDomain "github.com/leeyh0216/go-bemu/internal/domain"
	loadDomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
)

func TestParquetLoadWriteDispositionsAreAtomic(t *testing.T) {
	ctx := context.Background()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	if err := warehouse.CreateDataset(ctx, "test-project", "dataset"); err != nil {
		t.Fatal(err)
	}
	table := catalogDomain.Table{
		ProjectID: "test-project", DatasetID: "dataset", ID: "items",
		Schema: []catalogDomain.Field{{Name: "id", Type: "INT64", Mode: "REQUIRED"}, {Name: "name", Type: "STRING"}},
	}
	if err := warehouse.CreateTable(ctx, table); err != nil {
		t.Fatal(err)
	}
	first := createLoadParquet(t, warehouse, "SELECT 1::BIGINT AS id, 'one'::VARCHAR AS name")
	request := loadports.LoadRequest{
		Destination: loadDomain.Table{
			Reference: loadDomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"},
			Schema:    []loadDomain.Field{{Name: "id", Type: "INT64", Mode: "REQUIRED"}, {Name: "name", Type: "STRING"}},
		},
		Schema:  []loadDomain.Field{{Name: "id", Type: "INT64", Mode: "REQUIRED"}, {Name: "name", Type: "STRING"}},
		Objects: []loadports.LocalObject{{Path: first}}, SourceFormat: loadDomain.FormatParquet,
		WriteDisposition: loadDomain.WriteAppend,
	}
	result, err := warehouse.Load(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.OutputRows != 1 || tableRows(t, warehouse) != 1 {
		t.Fatalf("unexpected append result: %+v", result)
	}

	request.WriteDisposition = loadDomain.WriteEmpty
	if _, err := warehouse.Load(ctx, request); !errors.Is(err, loadDomain.ErrPrecondition) {
		t.Fatalf("WRITE_EMPTY error = %v", err)
	}
	if got := tableRows(t, warehouse); got != 1 {
		t.Fatalf("WRITE_EMPTY changed destination: %d", got)
	}

	second := createLoadParquet(t, warehouse, "SELECT 2::BIGINT AS id, 'two'::VARCHAR AS name UNION ALL SELECT 3, 'three'")
	request.Objects = []loadports.LocalObject{{Path: second}}
	request.WriteDisposition = loadDomain.WriteTruncate
	result, err = warehouse.Load(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.OutputRows != 2 || tableRows(t, warehouse) != 2 {
		t.Fatalf("unexpected truncate result: %+v", result)
	}
}

func TestParquetLoadRejectsSchemaMismatchWithoutChangingDestination(t *testing.T) {
	ctx := context.Background()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	if err := warehouse.CreateDataset(ctx, "test-project", "dataset"); err != nil {
		t.Fatal(err)
	}
	if err := warehouse.CreateTable(ctx, catalogDomain.Table{
		ProjectID: "test-project", DatasetID: "dataset", ID: "items",
		Schema: []catalogDomain.Field{{Name: "id", Type: "INT64"}},
	}); err != nil {
		t.Fatal(err)
	}
	parquet := createLoadParquet(t, warehouse, "SELECT 'not-an-integer'::VARCHAR AS id")
	_, err = warehouse.Load(ctx, loadports.LoadRequest{
		Destination: loadDomain.Table{Reference: loadDomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"}},
		Schema:      []loadDomain.Field{{Name: "id", Type: "INT64"}}, Objects: []loadports.LocalObject{{Path: parquet}},
		SourceFormat: loadDomain.FormatParquet, WriteDisposition: loadDomain.WriteTruncate,
	})
	if !errors.Is(err, loadDomain.ErrInvalid) {
		t.Fatalf("schema mismatch error = %v", err)
	}
	if got := tableRows(t, warehouse); got != 0 {
		t.Fatalf("failed load changed destination: %d", got)
	}
	assertNoLoadStagingTables(t, warehouse)
}

func TestParquetLoadPreservesNumericAndBigNumericPhysicalDecimals(t *testing.T) {
	ctx := context.Background()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	if err := warehouse.CreateDataset(ctx, "test-project", "dataset"); err != nil {
		t.Fatal(err)
	}
	precision, scale := int64(20), int64(4)
	fields := []catalogDomain.Field{
		{Name: "numeric_value", Type: "NUMERIC", Precision: &precision, Scale: &scale},
		{Name: "bignumeric_value", Type: "BIGNUMERIC"},
	}
	if err := warehouse.CreateTable(ctx, catalogDomain.Table{
		ProjectID: "test-project", DatasetID: "dataset", ID: "items", Schema: fields,
	}); err != nil {
		t.Fatal(err)
	}
	parquet := createLoadParquet(t, warehouse, "SELECT 123.4500::DECIMAL(20,4) AS numeric_value, 12345678901234567890.123456789012345678::DECIMAL(38,18) AS bignumeric_value")
	result, err := warehouse.Load(ctx, loadports.LoadRequest{
		Destination: loadDomain.Table{
			Reference: loadDomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"},
			Schema:    fields,
		},
		Schema: fields, Objects: []loadports.LocalObject{{Path: parquet}},
		SourceFormat: loadDomain.FormatParquet, WriteDisposition: loadDomain.WriteAppend,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.OutputRows != 1 {
		t.Fatalf("output rows = %d", result.OutputRows)
	}
	var numeric, bignumeric string
	if err := warehouse.db.QueryRowContext(ctx, `SELECT CAST(numeric_value AS VARCHAR), CAST(bignumeric_value AS VARCHAR) FROM "bq_746573742d70726f6a656374_64617461736574"."items"`).Scan(&numeric, &bignumeric); err != nil {
		t.Fatal(err)
	}
	if numeric != "123.4500" || bignumeric != "12345678901234567890.123456789012345678" {
		t.Fatalf("loaded decimals = %q, %q", numeric, bignumeric)
	}
}

func TestParquetLoadRejectsUnsupportedSchemaBeforeReadingObjects(t *testing.T) {
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	precision := int64(39)
	for _, field := range []loadDomain.Field{
		{Name: "amount", Type: "BIGNUMERIC", Precision: &precision},
		{Name: "location", Type: "GEOGRAPHY"},
		{Name: "amounts", Type: "NUMERIC", Mode: "REPEATED"},
	} {
		_, err := warehouse.Load(context.Background(), loadports.LoadRequest{
			Destination: loadDomain.Table{Reference: loadDomain.TableReference{ProjectID: "p", DatasetID: "d", TableID: "t"}},
			Schema:      []loadDomain.Field{field}, Objects: []loadports.LocalObject{{Path: "/path/that/must/not/be-read.parquet"}},
			SourceFormat: loadDomain.FormatParquet, WriteDisposition: loadDomain.WriteAppend,
		})
		if !errors.Is(err, loadDomain.ErrUnsupported) {
			t.Fatalf("field %s error = %v, want ErrUnsupported before object read", field.Type, err)
		}
		if strings.EqualFold(field.Mode, "REPEATED") && !strings.Contains(err.Error(), loadDomain.CapabilityParquetNestedRepeatedV1) {
			t.Fatalf("repeated Parquet error = %v, want stable capability", err)
		}
	}
}

func assertNoLoadStagingTables(t *testing.T, warehouse *Warehouse) {
	t.Helper()
	var count int64
	if err := warehouse.db.QueryRow(`SELECT count(*) FROM duckdb_tables() WHERE temporary AND table_name LIKE 'bqemu_load_%'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("temporary load tables remain: %d", count)
	}
}

func createLoadParquet(t *testing.T, warehouse *Warehouse, query string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source.parquet")
	if _, err := warehouse.db.Exec("COPY (" + query + ") TO " + quoteSQLString(path) + " (FORMAT PARQUET)"); err != nil {
		t.Fatal(err)
	}
	return path
}

func tableRows(t *testing.T, warehouse *Warehouse) int64 {
	t.Helper()
	var rows int64
	if err := warehouse.db.QueryRow(`SELECT count(*) FROM "bq_746573742d70726f6a656374_64617461736574"."items"`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	return rows
}
