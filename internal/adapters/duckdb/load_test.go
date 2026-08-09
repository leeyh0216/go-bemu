package duckdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	catalogDomain "github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/engine"
	loadDomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
	"github.com/leeyh0216/go-bemu/internal/observability"
)

type testLoadRequest struct {
	MutationID        string
	Destination       loadDomain.Table
	CreateDestination bool
	UpdateDestination bool
	BeforeSchema      []loadDomain.Field
	Evolution         catalogDomain.SchemaEvolution
	Partition         *loadDomain.PartitionDecorator
	Schema            []loadDomain.Field
	Objects           []loadports.LocalObject
	SourceFormat      loadDomain.SourceFormat
	WriteDisposition  loadDomain.WriteDisposition
}

var testLoadMutationSequence atomic.Uint64

func TestParquetLoadCreatesMissingDestinationInOneTransaction(t *testing.T) {
	ctx := context.Background()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	if err := warehouse.CreateDataset(ctx, "test-project", "dataset"); err != nil {
		t.Fatal(err)
	}
	fields := []loadDomain.Field{{Name: "id", Type: "INT64", Mode: "REQUIRED"}, {Name: "name", Type: "STRING"}}
	parquet := createLoadParquet(t, warehouse, "SELECT 1::BIGINT AS id, 'one'::VARCHAR AS name")
	result, err := executeTestLoad(ctx, warehouse, testLoadRequest{
		Destination: loadDomain.Table{
			Reference: loadDomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "created_items"},
			Schema:    fields,
		},
		CreateDestination: true,
		Schema:            fields, Objects: []loadports.LocalObject{{Path: parquet}},
		SourceFormat: loadDomain.FormatParquet, WriteDisposition: loadDomain.WriteAppend,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.CreatedDestination || result.OutputRows != 1 {
		t.Fatalf("load result = %+v", result)
	}
	var id int64
	var name string
	if err := warehouse.db.QueryRowContext(ctx,
		`SELECT id, name FROM "bq_746573742d70726f6a656374_64617461736574"."created_items"`,
	).Scan(&id, &name); err != nil {
		t.Fatal(err)
	}
	if id != 1 || name != "one" {
		t.Fatalf("created destination row = %d %q", id, name)
	}

	invalid := createLoadParquet(t, warehouse, "SELECT 1234::BIGINT AS amount")
	precision, scale := int64(3), int64(0)
	_, err = executeTestLoad(ctx, warehouse, testLoadRequest{
		Destination: loadDomain.Table{
			Reference: loadDomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "rolled_back_items"},
			Schema: []loadDomain.Field{{
				Name: "amount", Type: "NUMERIC", Precision: &precision, Scale: &scale,
			}},
		},
		CreateDestination: true,
		Schema: []loadDomain.Field{{
			Name: "amount", Type: "NUMERIC", Precision: &precision, Scale: &scale,
		}}, Objects: []loadports.LocalObject{{Path: invalid}},
		SourceFormat: loadDomain.FormatParquet, WriteDisposition: loadDomain.WriteAppend,
	})
	if err == nil {
		t.Fatalf("invalid new-destination load error = %v", err)
	}
	var exists bool
	if err := warehouse.db.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM duckdb_tables() WHERE schema_name = 'bq_746573742d70726f6a656374_64617461736574'
		AND table_name = 'rolled_back_items'
	)`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("failed load published a physical destination")
	}
}

func TestParquetLoadPreservesNestedAndRepeatedValues(t *testing.T) {
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
	fields := []loadDomain.Field{
		{Name: "payload", Type: "STRUCT", Fields: []loadDomain.Field{
			{Name: "id", Type: "INT64", Mode: "REQUIRED"},
			{Name: "tags", Type: "STRING", Mode: "REPEATED"},
		}},
		{Name: "events", Type: "RECORD", Mode: "REPEATED", Fields: []loadDomain.Field{
			{Name: "at", Type: "TIMESTAMP", Mode: "REQUIRED"},
			{Name: "amount", Type: "NUMERIC", Precision: &precision, Scale: &scale},
		}},
		{Name: "scores", Type: "INT64", Mode: "REPEATED"},
	}
	parquet := createLoadParquet(t, warehouse, `SELECT
		{'id': 7::BIGINT, 'tags': ['alpha'::VARCHAR, 'beta'::VARCHAR]} AS payload,
		[{'at': TIMESTAMPTZ '2026-08-09 01:02:03+00', 'amount': 12.3400::DECIMAL(20,4)}] AS events,
		[10::BIGINT, 20::BIGINT] AS scores`)
	result, err := executeTestLoad(ctx, warehouse, testLoadRequest{
		Destination: loadDomain.Table{
			Reference: loadDomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "nested_items"},
			Schema:    fields,
		},
		CreateDestination: true,
		Schema:            fields, Objects: []loadports.LocalObject{{Path: parquet}},
		SourceFormat: loadDomain.FormatParquet, WriteDisposition: loadDomain.WriteAppend,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.CreatedDestination || result.OutputRows != 1 {
		t.Fatalf("load result = %+v", result)
	}
	var id, score int64
	var tag, amount string
	if err := warehouse.db.QueryRowContext(ctx, `SELECT payload.id, payload.tags[2], events[1].amount::VARCHAR, scores[1]
		FROM "bq_746573742d70726f6a656374_64617461736574"."nested_items"`).Scan(&id, &tag, &amount, &score); err != nil {
		t.Fatal(err)
	}
	if id != 7 || tag != "beta" || amount != "12.3400" || score != 10 {
		t.Fatalf("nested values = id=%d tag=%q amount=%q score=%d", id, tag, amount, score)
	}

	invalid := createLoadParquet(t, warehouse, `SELECT
		{'id': NULL::BIGINT, 'tags': ['valid'::VARCHAR]} AS payload,
		[]::STRUCT("at" TIMESTAMPTZ, "amount" DECIMAL(20,4))[] AS events,
		[1::BIGINT] AS scores`)
	_, err = executeTestLoad(ctx, warehouse, testLoadRequest{
		Destination: loadDomain.Table{
			Reference: loadDomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "invalid_nested_items"},
			Schema:    fields,
		},
		CreateDestination: true,
		Schema:            fields, Objects: []loadports.LocalObject{{Path: invalid}},
		SourceFormat: loadDomain.FormatParquet, WriteDisposition: loadDomain.WriteAppend,
	})
	if !errors.Is(err, loadDomain.ErrInvalid) {
		t.Fatalf("nested REQUIRED error = %v", err)
	}
	var exists bool
	if err := warehouse.db.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM duckdb_tables() WHERE schema_name = 'bq_746573742d70726f6a656374_64617461736574'
		AND table_name = 'invalid_nested_items'
	)`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("invalid nested values created a destination")
	}

	nullList := createLoadParquet(t, warehouse, `SELECT
		{'id': 1::BIGINT, 'tags': ['valid'::VARCHAR]} AS payload,
		[]::STRUCT("at" TIMESTAMPTZ, "amount" DECIMAL(20,4))[] AS events,
		[NULL::BIGINT] AS scores`)
	_, err = executeTestLoad(ctx, warehouse, testLoadRequest{
		Destination: loadDomain.Table{
			Reference: loadDomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "invalid_repeated_items"},
			Schema:    fields,
		},
		CreateDestination: true,
		Schema:            fields, Objects: []loadports.LocalObject{{Path: nullList}},
		SourceFormat: loadDomain.FormatParquet, WriteDisposition: loadDomain.WriteAppend,
	})
	if !errors.Is(err, loadDomain.ErrInvalid) {
		t.Fatalf("REPEATED null element error = %v", err)
	}
	if err := warehouse.db.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM duckdb_tables() WHERE schema_name = 'bq_746573742d70726f6a656374_64617461736574'
		AND table_name = 'invalid_repeated_items'
	)`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("invalid repeated values created a destination")
	}

	narrowing := createLoadParquet(t, warehouse, `SELECT
		{'id': 1::BIGINT, 'tags': ['valid'::VARCHAR]} AS payload,
		[{'at': TIMESTAMPTZ '2026-08-09 01:02:03+00', 'amount': 1.23456::DECIMAL(21,5)}] AS events,
		[1::BIGINT] AS scores`)
	_, err = executeTestLoad(ctx, warehouse, testLoadRequest{
		Destination: loadDomain.Table{
			Reference: loadDomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "narrowed_nested_items"},
			Schema:    fields,
		},
		CreateDestination: true,
		Schema:            fields, Objects: []loadports.LocalObject{{Path: narrowing}},
		SourceFormat: loadDomain.FormatParquet, WriteDisposition: loadDomain.WriteAppend,
	})
	if !errors.Is(err, loadDomain.ErrUnsupported) || !strings.Contains(err.Error(), loadDomain.CapabilityDecimalRoundingV1) {
		t.Fatalf("nested decimal narrowing error = %v", err)
	}
	if err := warehouse.db.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM duckdb_tables() WHERE schema_name = 'bq_746573742d70726f6a656374_64617461736574'
		AND table_name = 'narrowed_nested_items'
	)`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("narrowed nested decimal created a destination")
	}
}

func TestParquetLoadAppliesAdditionsAndRelaxationsWithRowsAtomically(t *testing.T) {
	ctx := context.Background()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	if err := warehouse.CreateDataset(ctx, "test-project", "dataset"); err != nil {
		t.Fatal(err)
	}
	before := []loadDomain.Field{
		{Name: "id", Type: "INT64", Mode: "REQUIRED"},
		{Name: "payload", Type: "RECORD", Fields: []loadDomain.Field{{Name: "name", Type: "STRING", Mode: "REQUIRED"}}},
	}
	if err := warehouse.CreateTable(ctx, catalogDomain.Table{
		ProjectID: "test-project", DatasetID: "dataset", ID: "evolving_items", Schema: before,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.db.ExecContext(ctx, `INSERT INTO "bq_746573742d70726f6a656374_64617461736574"."evolving_items"
		VALUES (1, {'name': 'old'})`); err != nil {
		t.Fatal(err)
	}
	after := []loadDomain.Field{
		{Name: "id", Type: "INT64", Mode: "NULLABLE"},
		{Name: "payload", Type: "RECORD", Fields: []loadDomain.Field{
			{Name: "name", Type: "STRING", Mode: "NULLABLE"},
			{Name: "score", Type: "FLOAT64"},
		}},
		{Name: "metadata", Type: "RECORD", Fields: []loadDomain.Field{{Name: "source", Type: "STRING", Mode: "REQUIRED"}}},
	}
	evolution, err := catalogDomain.ValidateSchemaUpdate(before, after, catalogDomain.SchemaEvolutionOptions{
		AllowFieldAddition: true, AllowFieldRelaxation: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	parquet := createLoadParquet(t, warehouse, `SELECT
		NULL::BIGINT AS id,
		{'name': NULL::VARCHAR, 'score': 1.5::DOUBLE} AS payload,
		{'source': 'new'::VARCHAR} AS metadata`)
	result, err := executeTestLoad(ctx, warehouse, testLoadRequest{
		Destination: loadDomain.Table{
			Reference: loadDomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "evolving_items"},
			Schema:    after,
		},
		UpdateDestination: true, BeforeSchema: before, Evolution: evolution,
		Objects: []loadports.LocalObject{{Path: parquet}}, SourceFormat: loadDomain.FormatParquet,
		WriteDisposition: loadDomain.WriteAppend,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.UpdatedDestination || result.OutputRows != 1 {
		t.Fatalf("load result = %+v", result)
	}
	var oldRows, newRows int64
	if err := warehouse.db.QueryRowContext(ctx, `SELECT count(*) FROM "bq_746573742d70726f6a656374_64617461736574"."evolving_items"
		WHERE id = 1 AND payload.score IS NULL AND metadata IS NULL`).Scan(&oldRows); err != nil {
		t.Fatal(err)
	}
	if err := warehouse.db.QueryRowContext(ctx, `SELECT count(*) FROM "bq_746573742d70726f6a656374_64617461736574"."evolving_items"
		WHERE id IS NULL AND payload.name IS NULL AND payload.score = 1.5 AND metadata.source = 'new'`).Scan(&newRows); err != nil {
		t.Fatal(err)
	}
	if oldRows != 1 || newRows != 1 {
		t.Fatalf("evolved rows old=%d new=%d", oldRows, newRows)
	}
}

func TestParquetLoadRollsBackPartialSchemaUpdate(t *testing.T) {
	ctx := context.Background()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	if err := warehouse.CreateDataset(ctx, "test-project", "dataset"); err != nil {
		t.Fatal(err)
	}
	before := []loadDomain.Field{{Name: "id", Type: "INT64"}}
	if err := warehouse.CreateTable(ctx, catalogDomain.Table{
		ProjectID: "test-project", DatasetID: "dataset", ID: "rollback_items", Schema: before,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.db.ExecContext(ctx, `ALTER TABLE "bq_746573742d70726f6a656374_64617461736574"."rollback_items" ADD COLUMN collision VARCHAR`); err != nil {
		t.Fatal(err)
	}
	after := []loadDomain.Field{
		{Name: "id", Type: "INT64"}, {Name: "score", Type: "FLOAT64"}, {Name: "collision", Type: "STRING"},
	}
	evolution, err := catalogDomain.ValidateSchemaUpdate(before, after, catalogDomain.SchemaEvolutionOptions{AllowFieldAddition: true})
	if err != nil {
		t.Fatal(err)
	}
	parquet := createLoadParquet(t, warehouse, `SELECT 1::BIGINT AS id, 2.5::DOUBLE AS score, 'value'::VARCHAR AS collision`)
	_, err = executeTestLoad(ctx, warehouse, testLoadRequest{
		Destination: loadDomain.Table{
			Reference: loadDomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "rollback_items"},
			Schema:    after,
		},
		UpdateDestination: true, BeforeSchema: before, Evolution: evolution,
		Objects: []loadports.LocalObject{{Path: parquet}}, SourceFormat: loadDomain.FormatParquet,
		WriteDisposition: loadDomain.WriteAppend,
	})
	if err == nil {
		t.Fatal("load with physical schema drift succeeded")
	}
	var scoreColumns int64
	if err := warehouse.db.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.columns
		WHERE table_schema = 'bq_746573742d70726f6a656374_64617461736574'
		AND table_name = 'rollback_items' AND column_name = 'score'`).Scan(&scoreColumns); err != nil {
		t.Fatal(err)
	}
	if scoreColumns != 0 {
		t.Fatal("failed load retained the first schema addition")
	}
}

func TestParquetSchemaInferenceRequiresExplicitListSemantics(t *testing.T) {
	ctx := context.Background()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	parquet := createLoadParquet(t, warehouse, `SELECT
		1::BIGINT AS id,
		{'label': 'one'::VARCHAR, 'amount': 12.3400::DECIMAL(20,4)} AS payload,
		[{'code': 7::BIGINT}] AS items`)
	info, err := os.Stat(parquet)
	if err != nil {
		t.Fatal(err)
	}
	objects := []loadports.LocalObject{{Path: parquet, Size: info.Size(), Fingerprint: strings.Repeat("a", 64)}}
	if _, err := warehouse.InferParquetSchema(ctx, objects, loadports.ParquetSchemaOptions{}); !errors.Is(err, loadDomain.ErrUnsupported) ||
		!strings.Contains(err.Error(), loadDomain.CapabilityParquetListInferenceV1) {
		t.Fatalf("disabled LIST inference error = %v", err)
	}
	fields, err := warehouse.InferParquetSchema(ctx, objects, loadports.ParquetSchemaOptions{EnableListInference: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 3 || fields[0].Name != "id" || fields[0].Type != "INTEGER" ||
		fields[1].Name != "payload" || fields[1].Type != "RECORD" || len(fields[1].Fields) != 2 ||
		fields[1].Fields[1].Type != "NUMERIC" || fields[1].Fields[1].Precision == nil || *fields[1].Fields[1].Precision != 20 ||
		fields[1].Fields[1].Scale == nil || *fields[1].Fields[1].Scale != 4 ||
		fields[2].Name != "items" || fields[2].Mode != "REPEATED" || fields[2].Type != "RECORD" ||
		len(fields[2].Fields) != 1 || fields[2].Fields[0].Name != "code" || fields[2].Fields[0].Type != "INTEGER" {
		t.Fatalf("inferred schema = %#v", fields)
	}
}

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
	request := testLoadRequest{
		Destination: loadDomain.Table{
			Reference: loadDomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"},
			Schema:    []loadDomain.Field{{Name: "id", Type: "INT64", Mode: "REQUIRED"}, {Name: "name", Type: "STRING"}},
		},
		Schema:  []loadDomain.Field{{Name: "id", Type: "INT64", Mode: "REQUIRED"}, {Name: "name", Type: "STRING"}},
		Objects: []loadports.LocalObject{{Path: first}}, SourceFormat: loadDomain.FormatParquet,
		WriteDisposition: loadDomain.WriteAppend,
	}
	result, err := executeTestLoad(ctx, warehouse, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.OutputRows != 1 || tableRows(t, warehouse) != 1 {
		t.Fatalf("unexpected append result: %+v", result)
	}

	request.WriteDisposition = loadDomain.WriteEmpty
	if _, err := executeTestLoad(ctx, warehouse, request); !errors.Is(err, loadDomain.ErrPrecondition) {
		t.Fatalf("WRITE_EMPTY error = %v", err)
	}
	if got := tableRows(t, warehouse); got != 1 {
		t.Fatalf("WRITE_EMPTY changed destination: %d", got)
	}

	second := createLoadParquet(t, warehouse, "SELECT 2::BIGINT AS id, 'two'::VARCHAR AS name UNION ALL SELECT 3, 'three'")
	request.Objects = []loadports.LocalObject{{Path: second}}
	request.WriteDisposition = loadDomain.WriteTruncate
	result, err = executeTestLoad(ctx, warehouse, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.OutputRows != 2 || tableRows(t, warehouse) != 2 {
		t.Fatalf("unexpected truncate result: %+v", result)
	}
}

func TestParquetLoadReceiptPreventsDuplicateMutationAfterRestart(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "load-receipt.duckdb")
	warehouse, err := New(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := warehouse.CreateDataset(ctx, "test-project", "dataset"); err != nil {
		t.Fatal(err)
	}
	schema := []loadDomain.Field{{Name: "id", Type: "INT64"}}
	if err := warehouse.CreateTable(ctx, catalogDomain.Table{
		ProjectID: "test-project", DatasetID: "dataset", ID: "items", Schema: schema,
	}); err != nil {
		t.Fatal(err)
	}
	parquet := createLoadParquet(t, warehouse, "SELECT 1::BIGINT AS id")
	request := testLoadRequest{
		MutationID: strings.Repeat("e", 64),
		Destination: loadDomain.Table{
			Reference: loadDomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"},
			Schema:    schema,
		},
		Schema: schema, Objects: []loadports.LocalObject{{Path: parquet}},
		SourceFormat: loadDomain.FormatParquet, WriteDisposition: loadDomain.WriteAppend,
	}
	first, err := executeTestLoad(ctx, warehouse, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := warehouse.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := New(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	receipt, found, err := restarted.InspectLoadMutation(ctx, request.MutationID)
	if err != nil || !found || receipt.Result != first || len(receipt.PlanFingerprint) != 64 {
		t.Fatalf("restarted load receipt = %#v, found=%v, err=%v", receipt, found, err)
	}
	second, err := executeTestLoad(ctx, restarted, request)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || tableRows(t, restarted) != 1 {
		t.Fatalf("idempotent receipt result = first:%+v second:%+v rows:%d", first, second, tableRows(t, restarted))
	}
	request.WriteDisposition = loadDomain.WriteTruncate
	if _, err := executeTestLoad(ctx, restarted, request); !errors.Is(err, loadDomain.ErrPrecondition) {
		t.Fatalf("conflicting mutation plan error = %v", err)
	}
	if rows := tableRows(t, restarted); rows != 1 {
		t.Fatalf("conflicting mutation changed destination: %d", rows)
	}
	if err := restarted.DiscardLoadedTable(ctx, request.MutationID, request.Destination.Reference); err != nil {
		t.Fatal(err)
	}
	var tableCount, receiptCount int64
	if err := restarted.db.QueryRowContext(ctx, `SELECT count(*) FROM duckdb_tables()
WHERE schema_name = ? AND table_name = ?`, physicalSchema("test-project", "dataset"), "items").Scan(&tableCount); err != nil {
		t.Fatal(err)
	}
	if err := restarted.db.QueryRowContext(ctx, `SELECT count(*) FROM "_bqemu_load"."receipts" WHERE mutation_id = ?`, request.MutationID).Scan(&receiptCount); err != nil {
		t.Fatal(err)
	}
	if tableCount != 0 || receiptCount != 0 {
		t.Fatalf("discarded load mutation retained table=%d receipt=%d", tableCount, receiptCount)
	}
}

func TestParquetLoadPartitionDecoratorsScopeAndValidateWrites(t *testing.T) {
	ctx := context.Background()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	if err := warehouse.CreateDataset(ctx, "test-project", "dataset"); err != nil {
		t.Fatal(err)
	}
	physical := quoteIdentifier(physicalSchema("test-project", "dataset"))

	ingestionSchema := []loadDomain.Field{{Name: "id", Type: "INT64", Mode: "REQUIRED"}}
	ingestionTable := catalogDomain.Table{
		ProjectID: "test-project", DatasetID: "dataset", ID: "ingestion_events", Schema: ingestionSchema,
		TimePartitioning: &catalogDomain.TimePartitioning{Type: "DAY"},
	}
	if err := warehouse.CreateTable(ctx, ingestionTable); err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.db.ExecContext(ctx, "INSERT INTO "+physical+`."ingestion_events" ("id", "_PARTITIONTIME") VALUES `+
		"(1, TIMESTAMPTZ '2026-08-08T00:00:00Z'), (2, TIMESTAMPTZ '2026-08-09T00:00:00Z')"); err != nil {
		t.Fatal(err)
	}
	ingestionSource := createLoadParquet(t, warehouse, "SELECT 3::BIGINT AS id")
	ingestionDestination := loadDomain.Table{
		Reference: loadDomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "ingestion_events"},
		Schema:    ingestionSchema, TimePartitioning: &catalogDomain.TimePartitioning{Type: "DAY"},
	}
	ingestionPartition, err := loadDomain.ResolvePartitionDecorator("20260809", ingestionDestination)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executeTestLoad(ctx, warehouse, testLoadRequest{
		Destination: ingestionDestination, Schema: ingestionSchema, Partition: ingestionPartition,
		Objects: []loadports.LocalObject{{Path: ingestionSource}}, SourceFormat: loadDomain.FormatParquet,
		WriteDisposition: loadDomain.WriteTruncate,
	}); err != nil {
		t.Fatal(err)
	}
	var prior, replaced int64
	if err := warehouse.db.QueryRowContext(ctx, "SELECT count(*) FILTER (WHERE id = 1), count(*) FILTER (WHERE id = 3) FROM "+physical+`."ingestion_events"`).Scan(&prior, &replaced); err != nil {
		t.Fatal(err)
	}
	if prior != 1 || replaced != 1 {
		t.Fatalf("ingestion-time partition rows = prior:%d replacement:%d", prior, replaced)
	}

	columnSchema := []loadDomain.Field{{Name: "id", Type: "INT64"}, {Name: "event_date", Type: "DATE"}}
	columnTable := catalogDomain.Table{
		ProjectID: "test-project", DatasetID: "dataset", ID: "column_events", Schema: columnSchema,
		TimePartitioning: &catalogDomain.TimePartitioning{Type: "DAY", Field: "event_date"},
	}
	if err := warehouse.CreateTable(ctx, columnTable); err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.db.ExecContext(ctx, "INSERT INTO "+physical+`."column_events" VALUES (1, DATE '2026-08-09')`); err != nil {
		t.Fatal(err)
	}
	columnDestination := loadDomain.Table{
		Reference: loadDomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "column_events"},
		Schema:    columnSchema, TimePartitioning: &catalogDomain.TimePartitioning{Type: "DAY", Field: "event_date"},
	}
	columnPartition, err := loadDomain.ResolvePartitionDecorator("20260809", columnDestination)
	if err != nil {
		t.Fatal(err)
	}
	mismatchSource := createLoadParquet(t, warehouse, "SELECT 2::BIGINT AS id, DATE '2026-08-10' AS event_date")
	if _, err := executeTestLoad(ctx, warehouse, testLoadRequest{
		Destination: columnDestination, Schema: columnSchema, Partition: columnPartition,
		Objects: []loadports.LocalObject{{Path: mismatchSource}}, SourceFormat: loadDomain.FormatParquet,
		WriteDisposition: loadDomain.WriteTruncate,
	}); !errors.Is(err, loadDomain.ErrInvalid) {
		t.Fatalf("column partition mismatch error = %v", err)
	}
	var columnRows int64
	if err := warehouse.db.QueryRowContext(ctx, "SELECT count(*) FROM "+physical+`."column_events" WHERE id = 1`).Scan(&columnRows); err != nil {
		t.Fatal(err)
	}
	if columnRows != 1 {
		t.Fatalf("column partition mismatch changed destination: %d", columnRows)
	}

	rangeSchema := []loadDomain.Field{{Name: "bucket_id", Type: "INT64"}}
	rangePartitioning := &catalogDomain.RangePartitioning{
		Field: "bucket_id", Range: catalogDomain.Range{Start: 0, End: 100, Interval: 20},
	}
	rangeTable := catalogDomain.Table{
		ProjectID: "test-project", DatasetID: "dataset", ID: "range_events", Schema: rangeSchema,
		RangePartitioning: rangePartitioning,
	}
	if err := warehouse.CreateTable(ctx, rangeTable); err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.db.ExecContext(ctx, "INSERT INTO "+physical+`."range_events" VALUES (5), (25)`); err != nil {
		t.Fatal(err)
	}
	rangeDestination := loadDomain.Table{
		Reference: loadDomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "range_events"},
		Schema:    rangeSchema, RangePartitioning: rangePartitioning,
	}
	rangePartition, err := loadDomain.ResolvePartitionDecorator("20", rangeDestination)
	if err != nil {
		t.Fatal(err)
	}
	rangeSource := createLoadParquet(t, warehouse, "SELECT 21::BIGINT AS bucket_id UNION ALL SELECT 22")
	if _, err := executeTestLoad(ctx, warehouse, testLoadRequest{
		Destination: rangeDestination, Schema: rangeSchema, Partition: rangePartition,
		Objects: []loadports.LocalObject{{Path: rangeSource}}, SourceFormat: loadDomain.FormatParquet,
		WriteDisposition: loadDomain.WriteTruncate,
	}); err != nil {
		t.Fatal(err)
	}
	var outsideRows, partitionRows int64
	if err := warehouse.db.QueryRowContext(ctx, "SELECT count(*) FILTER (WHERE bucket_id = 5), count(*) FILTER (WHERE bucket_id >= 20 AND bucket_id < 40) FROM "+physical+`."range_events"`).Scan(&outsideRows, &partitionRows); err != nil {
		t.Fatal(err)
	}
	if outsideRows != 1 || partitionRows != 2 {
		t.Fatalf("range partition rows = outside:%d replacement:%d", outsideRows, partitionRows)
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
	_, err = executeTestLoad(ctx, warehouse, testLoadRequest{
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
	result, err := executeTestLoad(ctx, warehouse, testLoadRequest{
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

func TestParquetLoadRejectsDecimalRoundingBeforeDestinationMutation(t *testing.T) {
	ctx := context.Background()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	if err := warehouse.CreateDataset(ctx, "test-project", "dataset"); err != nil {
		t.Fatal(err)
	}
	precision, scale := int64(5), int64(2)
	fields := []catalogDomain.Field{{Name: "amount", Type: "NUMERIC", Precision: &precision, Scale: &scale}}
	if err := warehouse.CreateTable(ctx, catalogDomain.Table{
		ProjectID: "test-project", DatasetID: "dataset", ID: "items", Schema: fields,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.db.ExecContext(ctx, `INSERT INTO "bq_746573742d70726f6a656374_64617461736574"."items" VALUES (9.99)`); err != nil {
		t.Fatal(err)
	}
	parquet := createLoadParquet(t, warehouse, "SELECT 1.025::DECIMAL(6,3) AS amount")
	_, err = executeTestLoad(ctx, warehouse, testLoadRequest{
		Destination: loadDomain.Table{
			Reference: loadDomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"}, Schema: fields,
		},
		Schema: fields, Objects: []loadports.LocalObject{{Path: parquet}},
		SourceFormat: loadDomain.FormatParquet, WriteDisposition: loadDomain.WriteTruncate,
	})
	if !errors.Is(err, loadDomain.ErrUnsupported) || !strings.Contains(err.Error(), loadDomain.CapabilityDecimalRoundingV1) {
		t.Fatalf("decimal rounding error = %v", err)
	}
	var amount string
	if err := warehouse.db.QueryRowContext(ctx, `SELECT CAST(amount AS VARCHAR) FROM "bq_746573742d70726f6a656374_64617461736574"."items"`).Scan(&amount); err != nil {
		t.Fatal(err)
	}
	if amount != "9.99" {
		t.Fatalf("rejected decimal load changed destination to %q", amount)
	}
	assertNoLoadStagingTables(t, warehouse)
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
	} {
		_, err := executeTestLoad(context.Background(), warehouse, testLoadRequest{
			Destination: loadDomain.Table{Reference: loadDomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"}},
			Schema:      []loadDomain.Field{field}, Objects: []loadports.LocalObject{{Path: "/path/that/must/not/be-read.parquet"}},
			SourceFormat: loadDomain.FormatParquet, WriteDisposition: loadDomain.WriteAppend,
		})
		if !errors.Is(err, loadDomain.ErrUnsupported) {
			t.Fatalf("field %s error = %v, want ErrUnsupported before object read", field.Type, err)
		}
	}
}

func TestDuckDBLoadPlanningRejectsSchemaAndArtifactDriftBeforeTransaction(t *testing.T) {
	ctx := context.Background()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	foreign, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = foreign.Close() })
	schema := []loadDomain.Field{{Name: "id", Type: "INT64"}}
	intent, err := engine.NewSchemaIntent(engine.SchemaIntentDescriptor{
		Operation:   engine.SchemaOperationValidate,
		Target:      catalogDomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"},
		AfterSchema: schema,
	})
	if err != nil {
		t.Fatal(err)
	}
	foreignSchemaPlan, err := foreign.PlanSchema(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	request := loadports.LoadPlanRequest{
		MutationID: strings.Repeat("d", 64),
		Destination: loadDomain.Table{
			Reference: loadDomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"},
			Schema:    schema,
		},
		SchemaPlan: foreignSchemaPlan, SourceFormat: loadDomain.FormatParquet,
		WriteDisposition: loadDomain.WriteAppend,
		Objects:          []loadports.ResolvedObject{{Fingerprint: strings.Repeat("b", 64), Size: 7}},
	}
	if _, err := warehouse.PlanLoad(ctx, request); !errors.Is(err, loadDomain.ErrPrecondition) {
		t.Fatalf("foreign schema plan error = %v", err)
	}

	schemaPlan, err := warehouse.PlanSchema(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	request.SchemaPlan = schemaPlan
	request.Destination.Schema = []loadDomain.Field{{Name: "id", Type: "STRING"}}
	if _, err := warehouse.PlanLoad(ctx, request); !errors.Is(err, loadDomain.ErrPrecondition) {
		t.Fatalf("schema drift error = %v", err)
	}

	request.Destination.Schema = schema
	plan, err := warehouse.PlanLoad(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	_, err = warehouse.ExecuteLoad(ctx, plan, []loadports.LocalObject{{
		Path: "/path/that/must/not/be-read.parquet", Fingerprint: strings.Repeat("b", 64), Size: 8,
	}})
	if !errors.Is(err, loadDomain.ErrPrecondition) {
		t.Fatalf("artifact size drift error = %v", err)
	}
	assertNoLoadStagingTables(t, warehouse)
}

func executeTestLoad(ctx context.Context, warehouse *Warehouse, request testLoadRequest) (loadports.LoadResult, error) {
	if len(request.Destination.Schema) == 0 {
		request.Destination.Schema = catalogDomain.CloneFields(request.Schema)
	}
	operation := engine.SchemaOperationValidate
	if request.CreateDestination {
		operation = engine.SchemaOperationCreate
	} else if request.UpdateDestination {
		operation = engine.SchemaOperationUpdate
	}
	intent, err := engine.NewSchemaIntent(engine.SchemaIntentDescriptor{
		Operation: operation,
		Target: catalogDomain.TableReference{
			ProjectID: request.Destination.Reference.ProjectID,
			DatasetID: request.Destination.Reference.DatasetID,
			TableID:   request.Destination.Reference.TableID,
		},
		BeforeSchema: request.BeforeSchema, AfterSchema: request.Destination.Schema,
		Additions: request.Evolution.Additions, Relaxations: request.Evolution.Relaxations,
	})
	if err != nil {
		return loadports.LoadResult{}, translateCatalogLoadTestError(err)
	}
	schemaPlan, err := warehouse.PlanSchema(ctx, intent)
	if err != nil {
		return loadports.LoadResult{}, translateCatalogLoadTestError(err)
	}
	resolved := make([]loadports.ResolvedObject, len(request.Objects))
	local := append([]loadports.LocalObject(nil), request.Objects...)
	for index := range local {
		local[index].Fingerprint = strings.TrimPrefix(
			observability.Digest([]byte("load-test\x00"+local[index].Path)), "sha256:",
		)
		if info, statErr := os.Stat(local[index].Path); statErr == nil {
			local[index].Size = info.Size()
		}
		resolved[index] = loadports.ResolvedObject{Fingerprint: local[index].Fingerprint, Size: local[index].Size}
	}
	mutationID := request.MutationID
	if mutationID == "" {
		mutationID = fmt.Sprintf("%064x", testLoadMutationSequence.Add(1))
	}
	plan, err := warehouse.PlanLoad(ctx, loadports.LoadPlanRequest{
		MutationID:  mutationID,
		Destination: request.Destination, CreateDestination: request.CreateDestination,
		UpdateDestination: request.UpdateDestination, SchemaPlan: schemaPlan,
		Partition:    request.Partition,
		SourceFormat: request.SourceFormat, WriteDisposition: request.WriteDisposition,
		Objects: resolved,
	})
	if err != nil {
		return loadports.LoadResult{}, err
	}
	return warehouse.ExecuteLoad(ctx, plan, local)
}

func translateCatalogLoadTestError(err error) error {
	if errors.Is(err, catalogDomain.ErrUnsupported) {
		return loadports.UnsupportedLoadPlan(catalogDomain.CapabilityEngineSchemaV1)
	}
	if errors.Is(err, catalogDomain.ErrInvalid) {
		return loadports.InvalidLoadPlan()
	}
	return loadports.StaleLoadPlan()
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
