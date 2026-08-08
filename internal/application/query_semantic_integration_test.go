package application

import (
	"context"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	googlesqladapter "github.com/leeyh0216/go-bemu/internal/adapters/googlesql"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestGoogleSQLGatewayExecutesDDLAndDMLThroughOnePreparedBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	clock := fixedClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	catalog := NewCatalogService(
		memory.NewCatalogRepository(), warehouse, clock, WithDDLStorage(warehouse),
	)
	if _, err := catalog.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateDataset(ctx, domain.Dataset{
		ProjectID: "test-project", ID: "analytics", Location: "US",
	}); err != nil {
		t.Fatal(err)
	}
	gateway, err := googlesqladapter.NewGateway(catalog)
	if err != nil {
		t.Fatal(err)
	}
	queries := newTestQueryService(
		memory.NewJobRepository(), warehouse, clock, fixedQueryID("semantic"),
		WithGoogleSQLGateway(gateway), WithStatementExecutor(warehouse),
		WithStatementMaterializer(warehouse), WithQueryDestinationCatalog(catalog),
		WithQueryDDLExecutor(catalog),
	)

	for _, input := range []QueryInput{
		{
			ProjectID: "test-project", JobID: "create-table",
			SQL: "CREATE TABLE `test-project.analytics.events` (id INT64, amount NUMERIC(20, 4))",
		},
		{
			ProjectID: "test-project", JobID: "insert-row",
			SQL: "INSERT INTO `test-project.analytics.events` (id, amount) VALUES (1, NUMERIC '12.3400')",
		},
	} {
		job, err := queries.RunSync(ctx, input)
		if err != nil {
			t.Fatal(err)
		}
		if job.State != domain.JobDone || job.Error != nil {
			t.Fatalf("job %s state=%s error=%#v", input.JobID, job.State, job.Error)
		}
	}

	job, err := queries.RunSync(ctx, QueryInput{
		ProjectID: "test-project", JobID: "select-row",
		SQL: "SELECT id, amount FROM `test-project.analytics.events` WHERE id = 1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if job.Error != nil || job.Result == nil || len(job.Result.Rows) != 1 {
		t.Fatalf("SELECT job state=%s error=%#v result=%#v", job.State, job.Error, job.Result)
	}
	if got := job.Result.Columns; len(got) != 2 || got[1].Type != "NUMERIC" ||
		got[1].Precision == nil || *got[1].Precision != 20 || got[1].Scale == nil || *got[1].Scale != 4 {
		t.Fatalf("SELECT schema=%#v", got)
	}
}
