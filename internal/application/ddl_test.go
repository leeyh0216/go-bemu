package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type failTableUpdateRepository struct {
	ports.CatalogRepository
	fail bool
}

func (r *failTableUpdateRepository) UpdateTable(ctx context.Context, table domain.Table) error {
	if r.fail {
		return errors.New("injected catalog update failure")
	}
	return r.CatalogRepository.UpdateTable(ctx, table)
}

func TestDDLRenameCompensatesPhysicalSchemaWhenCatalogUpdateFails(t *testing.T) {
	ctx := context.Background()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	repository := &failTableUpdateRepository{CatalogRepository: memory.NewCatalogRepository()}
	service := NewCatalogService(repository, warehouse, fixedClock{now: time.Unix(1, 0)})
	if _, err := service.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics", Location: "US"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateTable(ctx, domain.Table{ProjectID: "test-project", DatasetID: "analytics", ID: "events", Schema: []domain.Field{{Name: "note", Type: "STRING"}}}); err != nil {
		t.Fatal(err)
	}
	repository.fail = true
	err = service.ExecuteDDL(ctx, DDLCommand{Kind: "RENAME_COLUMN", Table: domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}, Name: "note", NewName: "message"})
	if err == nil {
		t.Fatal("rename succeeded despite injected catalog failure")
	}
	table, err := service.GetTable(ctx, "test-project", "analytics", "events")
	if err != nil {
		t.Fatal(err)
	}
	if got := table.Schema[0].Name; got != "note" {
		t.Fatalf("canonical schema drifted to %q", got)
	}
	result, err := warehouse.Query(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: "SELECT note FROM `test-project.analytics.events`"})
	if err != nil {
		t.Fatalf("physical schema was not compensated: %v", err)
	}
	if len(result.Columns) != 1 || result.Columns[0].Name != "note" {
		t.Fatalf("physical schema after compensation = %#v", result.Columns)
	}
}

func TestDDLDestructiveColumnCommandsFailBeforePhysicalMutation(t *testing.T) {
	ctx := context.Background()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	service := NewCatalogService(memory.NewCatalogRepository(), warehouse, fixedClock{now: time.Unix(1, 0)})
	if _, err := service.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics", Location: "US"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateTable(ctx, domain.Table{ProjectID: "test-project", DatasetID: "analytics", ID: "events", Schema: []domain.Field{{Name: "value", Type: "STRING"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.Query(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: "INSERT INTO `test-project.analytics.events` VALUES ('not-an-integer')"}); err != nil {
		t.Fatal(err)
	}
	for _, command := range []DDLCommand{
		{Kind: "ALTER_COLUMN_TYPE", Table: domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}, Name: "value", Field: domain.Field{Name: "value", Type: "INT64", Mode: "NULLABLE"}},
		{Kind: "DROP_COLUMN", Table: domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}, Name: "value"},
	} {
		if err := service.ExecuteDDL(ctx, command); !errors.Is(err, domain.ErrUnsupported) {
			t.Fatalf("%s error = %v, want unsupported", command.Kind, err)
		}
	}
	table, err := service.GetTable(ctx, "test-project", "analytics", "events")
	if err != nil {
		t.Fatal(err)
	}
	if table.Schema[0].Name != "value" || table.Schema[0].Type != "STRING" {
		t.Fatalf("canonical destructive DDL drift = %#v", table.Schema)
	}
	result, err := warehouse.Query(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: "SELECT value FROM `test-project.analytics.events`"})
	if err != nil {
		t.Fatalf("physical destructive DDL drift: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != "not-an-integer" {
		t.Fatalf("physical data drift = %#v", result.Rows)
	}
}
