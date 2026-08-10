package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	googlesqladapter "github.com/leeyh0216/go-bemu/internal/adapters/googlesql"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

func TestCatalogServiceExecutesSemanticDDLThroughPlannedStorage(t *testing.T) {
	ctx := context.Background()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	service := NewCatalogService(
		memory.NewCatalogRepository(), warehouse,
		fixedClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)},
		WithDDLStorage(warehouse), WithTableDataReader(warehouse),
	)
	if _, err := service.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics", Location: "US"}); err != nil {
		t.Fatal(err)
	}
	target := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}
	execute := func(id string, descriptor domain.DDLCommandDescriptor) error {
		t.Helper()
		descriptor.Table = target
		command, err := domain.NewDDLCommand(descriptor)
		if err != nil {
			t.Fatal(err)
		}
		return service.ExecuteDDL(ctx, command, id)
	}

	schema := []domain.Field{
		{Name: "id", Type: "INT64", Mode: "REQUIRED"},
		{Name: "note", Type: "STRING"},
		{Name: "convertible", Type: "INT64"},
	}
	if err := execute("create", domain.DDLCommandDescriptor{Kind: domain.DDLCreateTable, Schema: schema}); err != nil {
		t.Fatal(err)
	}
	gateway, err := googlesqladapter.NewGateway(service)
	if err != nil {
		t.Fatal(err)
	}
	insert, err := gateway.Analyze(ctx, ports.QueryRequest{
		ProjectID: "test-project",
		SQL:       "INSERT INTO `test-project.analytics.events` VALUES (1, 'not-an-integer', 7)",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.ExecuteStatement(ctx, insert); err != nil {
		t.Fatal(err)
	}
	precision, scale := int64(10), int64(2)
	if err := execute("add", domain.DDLCommandDescriptor{
		Kind:  domain.DDLAddColumn,
		Field: domain.Field{Name: "score", Type: "NUMERIC", Mode: "NULLABLE", Precision: &precision, Scale: &scale},
	}); err != nil {
		t.Fatal(err)
	}
	if err := execute("rename", domain.DDLCommandDescriptor{
		Kind: domain.DDLRenameColumn, Name: "note", NewName: "message",
	}); err != nil {
		t.Fatal(err)
	}
	if err := execute("bad-type", domain.DDLCommandDescriptor{
		Kind: domain.DDLAlterColumnType, Name: "message",
		Field: domain.Field{Name: "message", Type: "INT64", Mode: "NULLABLE"},
	}); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("incompatible SET DATA TYPE error = %v", err)
	}
	if err := execute("type", domain.DDLCommandDescriptor{
		Kind: domain.DDLAlterColumnType, Name: "convertible",
		Field: domain.Field{Name: "convertible", Type: "NUMERIC", Mode: "NULLABLE"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := execute("drop-column", domain.DDLCommandDescriptor{Kind: domain.DDLDropColumn, Name: "score"}); err != nil {
		t.Fatal(err)
	}
	table, err := service.GetTable(ctx, target.ProjectID, target.DatasetID, target.TableID)
	if err != nil {
		t.Fatal(err)
	}
	if len(table.Schema) != 3 || table.Schema[1].Name != "message" || table.Schema[2].Type != "NUMERIC" {
		t.Fatalf("ALTER schema = %#v", table.Schema)
	}
	page, err := service.ListTableData(ctx, target.ProjectID, target.DatasetID, target.TableID, 0, ports.TableDataMaxResults{Value: 10, Present: true})
	if err != nil || page.TotalRows != 1 || page.Rows[0][1] != "not-an-integer" || page.Rows[0][2] != "7" {
		t.Fatalf("ALTER data = %#v, error = %v", page, err)
	}
	if err := execute("truncate", domain.DDLCommandDescriptor{Kind: domain.DDLTruncateTable}); err != nil {
		t.Fatal(err)
	}
	page, err = service.ListTableData(ctx, target.ProjectID, target.DatasetID, target.TableID, 0, ports.TableDataMaxResults{Value: 10, Present: true})
	if err != nil || page.TotalRows != 0 {
		t.Fatalf("TRUNCATE data = %#v, error = %v", page, err)
	}
	if err := execute("drop-table", domain.DDLCommandDescriptor{Kind: domain.DDLDropTable}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetTable(ctx, target.ProjectID, target.DatasetID, target.TableID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("DROP TABLE error = %v", err)
	}
}
