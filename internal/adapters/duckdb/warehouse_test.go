package duckdb

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/engine"
)

func TestWarehouseMapsNestedAndRepeatedTypes(t *testing.T) {
	tests := []struct {
		field domain.Field
		want  string
	}{
		{domain.Field{Name: "n", Type: "NUMERIC"}, "DECIMAL(38,9)"},
		{domain.Field{Name: "b", Type: "BIGNUMERIC"}, "DECIMAL(38,18)"},
		{domain.Field{Name: "bp", Type: "BIGNUMERIC", Precision: int64Pointer(38), Scale: int64Pointer(38)}, "DECIMAL(38,38)"},
		{domain.Field{Name: "tags", Type: "STRING", Mode: "REPEATED"}, "VARCHAR[]"},
		{domain.Field{Name: "r", Type: "RECORD", Fields: []domain.Field{{Name: "id", Type: "INT64"}}}, `STRUCT("id" BIGINT)`},
		{domain.Field{Name: "rs", Type: "STRUCT", Mode: "REPEATED", Fields: []domain.Field{{Name: "amount", Type: "NUMERIC", Precision: int64Pointer(10), Scale: int64Pointer(2)}}}, `STRUCT("amount" DECIMAL(10,2))[]`},
	}
	for _, test := range tests {
		got, err := duckDBType(test.field)
		if err != nil {
			t.Fatal(err)
		}
		if got != test.want {
			t.Errorf("%s: got %q, want %q", test.field.Name, got, test.want)
		}
	}
}

func int64Pointer(value int64) *int64 { return &value }

func TestWarehousePublishesPortableSchemaCapabilities(t *testing.T) {
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	capabilities := warehouse.Capabilities()
	if capabilities.Decimal().MaxPrecision != 38 || capabilities.Decimal().MaxScale != 38 ||
		capabilities.Composite().MaxStructDepth == 0 || capabilities.Composite().MaxListDepth == 0 {
		t.Fatalf("unexpected capabilities: %#v", capabilities.Descriptor())
	}
	intent, err := engine.NewSchemaIntent(engine.SchemaIntentDescriptor{
		Operation:   engine.SchemaOperationCreate,
		Target:      domain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"},
		AfterSchema: []domain.Field{{Name: "amount", Type: "BIGNUMERIC", Precision: int64Pointer(39)}},
	})
	if err == nil {
		_, err = warehouse.PlanSchema(context.Background(), intent)
	}
	if err == nil {
		t.Fatal("schema planner accepted precision above the engine maximum")
	}
}

func TestWarehousePublishesImmutableEngineCapabilities(t *testing.T) {
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })

	capabilities := warehouse.Capabilities()
	if capabilities.Identity().ID() != "duckdb" || capabilities.Identity().Version() == "" {
		t.Fatalf("engine identity = %s@%s", capabilities.Identity().ID(), capabilities.Identity().Version())
	}
	if decimal := capabilities.Decimal(); !decimal.Supported || decimal.MaxPrecision != domain.SupportedDecimalMaxPrecision ||
		decimal.MaxScale != domain.SupportedDecimalMaxScale {
		t.Fatalf("decimal capabilities = %#v", decimal)
	}
	if !capabilities.SupportsTransaction(engine.TransactionScopeSingleTable) ||
		!capabilities.SupportsTransaction(engine.TransactionScopeMultiTable) ||
		!capabilities.SupportsAtomicReplacement(engine.AtomicReplacementTable) ||
		!capabilities.SupportsAtomicReplacement(engine.AtomicReplacementPartition) {
		t.Fatal("DuckDB runtime omitted implemented transaction or replacement capabilities")
	}
	if !capabilities.SupportsInspection(engine.InspectionTableShape) {
		t.Fatal("DuckDB runtime omitted implemented table-shape inspection")
	}
	for _, operation := range []engine.DDLOperation{
		engine.DDLCreateTable, engine.DDLDropTable, engine.DDLAddColumn,
		engine.DDLDropColumn, engine.DDLRenameColumn, engine.DDLChangeColumnType,
	} {
		if _, supported := capabilities.DDL(operation); !supported {
			t.Fatalf("DuckDB runtime omitted implemented DDL operation %q", operation)
		}
	}

	descriptor := capabilities.Descriptor()
	descriptor.Decimal.MaxPrecision = 1
	descriptor.Transactions[engine.TransactionScopeSingleTable] = false
	if again := warehouse.Capabilities(); again.Decimal().MaxPrecision != domain.SupportedDecimalMaxPrecision ||
		!again.SupportsTransaction(engine.TransactionScopeSingleTable) {
		t.Fatal("published capabilities retained a mutable descriptor")
	}
}

func TestWarehouseAppliesTopLevelAndNestedSchemaAdditions(t *testing.T) {
	ctx := context.Background()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	if err := warehouse.CreateDataset(ctx, "test-project", "analytics"); err != nil {
		t.Fatal(err)
	}
	table := domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events",
		Schema: []domain.Field{
			{Name: "id", Type: "INT64"},
			{Name: "payload", Type: "RECORD", Fields: []domain.Field{{Name: "name", Type: "STRING"}}},
		},
	}
	if err := warehouse.CreateTable(ctx, table); err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.db.ExecContext(ctx, `INSERT INTO "`+physicalSchema("test-project", "analytics")+`"."events" VALUES (1, {'name':'first'})`); err != nil {
		t.Fatal(err)
	}
	additions := []domain.SchemaAddition{
		{Path: []string{"payload", "score"}, Field: domain.Field{Name: "score", Type: "FLOAT64"}},
		{Path: []string{"tags"}, Field: domain.Field{Name: "tags", Type: "STRING", Mode: "REPEATED"}},
	}
	if err := warehouse.ApplySchemaAdditions(ctx, table, additions); err != nil {
		t.Fatal(err)
	}
	row := warehouse.db.QueryRowContext(ctx, `SELECT "payload"."score", "tags" FROM "`+physicalSchema("test-project", "analytics")+`"."events"`)
	var score, tags any
	if err := row.Scan(&score, &tags); err != nil {
		t.Fatal(err)
	}
	if score != nil || tags != nil {
		t.Fatalf("new fields for existing rows must be NULL, got score=%#v tags=%#v", score, tags)
	}
}

func TestWarehouseAppliesSchemaAdditionInsideRepeatedRecord(t *testing.T) {
	ctx := context.Background()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	if err := warehouse.CreateDataset(ctx, "test-project", "analytics"); err != nil {
		t.Fatal(err)
	}
	table := domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events",
		Schema: []domain.Field{{
			Name: "payloads", Type: "RECORD", Mode: "REPEATED",
			Fields: []domain.Field{{Name: "name", Type: "STRING"}},
		}},
	}
	if err := warehouse.CreateTable(ctx, table); err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.db.ExecContext(ctx, `INSERT INTO "`+physicalSchema("test-project", "analytics")+`"."events" VALUES ([{'name':'first'}])`); err != nil {
		t.Fatal(err)
	}
	addition := domain.SchemaAddition{
		Path:  []string{"payloads", "score"},
		Field: domain.Field{Name: "score", Type: "FLOAT64"},
	}
	table.Schema[0].Fields = append(table.Schema[0].Fields, addition.Field)
	if err := warehouse.ApplySchemaAdditions(ctx, table, []domain.SchemaAddition{addition}); err != nil {
		t.Fatal(err)
	}
	var score any
	if err := warehouse.db.QueryRowContext(ctx, `SELECT "payloads"[1]."score" FROM "`+physicalSchema("test-project", "analytics")+`"."events"`).Scan(&score); err != nil {
		t.Fatal(err)
	}
	if score != nil {
		t.Fatalf("new nested field for existing repeated rows must be NULL, got %#v", score)
	}
}

func TestWarehouseSchemaAdditionsRollbackAtomically(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "schema-update.duckdb")
	warehouse, err := New(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := warehouse.CreateDataset(ctx, "test-project", "analytics"); err != nil {
		t.Fatal(err)
	}
	table := domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events",
		Schema: []domain.Field{{Name: "id", Type: "INT64"}},
	}
	if err := warehouse.CreateTable(ctx, table); err != nil {
		t.Fatal(err)
	}
	err = warehouse.ApplySchemaAdditions(ctx, table, []domain.SchemaAddition{
		{Path: []string{"added"}, Field: domain.Field{Name: "added", Type: "STRING"}},
		{Path: []string{"missing", "nested"}, Field: domain.Field{Name: "nested", Type: "STRING"}},
	})
	if err == nil {
		t.Fatal("expected the second ALTER to fail")
	}
	// A DuckDB binder failure invalidates the current connection. Reopening the
	// durable database also verifies that no partial DDL was persisted.
	if err := warehouse.Close(); err != nil {
		t.Fatal(err)
	}
	warehouse, err = New(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	var count int
	if err := warehouse.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = ? AND table_name = 'events' AND column_name = 'added'`, physicalSchema("test-project", "analytics")).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("first ALTER remained visible after transaction rollback")
	}
}
