package application

import (
	"context"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	googlesqladapter "github.com/leeyh0216/go-bemu/internal/adapters/googlesql"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
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
		SQL: "SELECT id, amount FROM `test-project.analytics.events` WHERE id BETWEEN 1 AND 1",
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

func TestGoogleSQLGatewayAcceptsGoogleSQLLiteralsAndRejectsDuckDBSyntaxBeforeExecution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	clock := fixedClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	catalog := NewCatalogService(memory.NewCatalogRepository(), warehouse, clock, WithDDLStorage(warehouse))
	if _, err := catalog.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics", Location: "US"}); err != nil {
		t.Fatal(err)
	}
	gateway, err := googlesqladapter.NewGateway(catalog)
	if err != nil {
		t.Fatal(err)
	}
	queries := newTestQueryService(
		memory.NewJobRepository(), warehouse, clock, fixedQueryID("literal"),
		WithGoogleSQLGateway(gateway), WithStatementExecutor(warehouse),
		WithStatementMaterializer(warehouse), WithQueryDestinationCatalog(catalog), WithQueryDDLExecutor(catalog),
	)

	job, err := queries.RunSync(ctx, QueryInput{
		ProjectID: "test-project", JobID: "googlesql-literals",
		SQL: "SELECT TIMESTAMP '2026-08-08 01:02:03+00' AS event_time, STRUCT(1 AS id, 'ok' AS label) AS payload, [1, 2] AS ids",
	})
	if err != nil || job.Error != nil || job.Result == nil || len(job.Result.Rows) != 1 {
		t.Fatalf("GoogleSQL literal query = job=%#v err=%v", job, err)
	}

	for name, sql := range map[string]string{
		"DuckDB TIMESTAMPTZ literal": "SELECT TIMESTAMPTZ '2026-08-08 01:02:03+00'",
		"DuckDB STRUCT literal":      "SELECT {field: 'not-googlesql'}",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := gateway.Analyze(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: sql})
			if err == nil {
				t.Fatal("expected GoogleSQL admission rejection")
			}
		})
	}

	job, err = queries.Submit(ctx, QueryInput{
		ProjectID: "test-project", JobID: "semantic-invalid-column", SQL: "SELECT missing_column",
	})
	if err != nil {
		t.Fatalf("Submit invalid semantic query: %v", err)
	}
	completed := waitForQueryJobDone(t, ctx, queries, job.Reference)
	if completed.Error == nil || completed.Error.Reason != "invalidQuery" {
		t.Fatalf("invalid query job error = %#v", completed.Error)
	}
}
