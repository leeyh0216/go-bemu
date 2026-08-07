package duckdb

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

func TestAnalyzeQueryFindsStructuralRelationsWithoutPayloadLogging(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })

	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	observability.Configure(true)
	t.Cleanup(func() {
		slog.SetDefault(previous)
		observability.Configure(false)
	})
	const secretLiteral = "analysis-secret-must-not-appear"
	analysis, err := warehouse.AnalyzeQuery(ctx, ports.QueryRequest{
		ProjectID: "test-project",
		SQL: "WITH local AS (SELECT id FROM `test-project.eu_source.events`) " +
			"SELECT local.id, lookup.name, '" + secretLiteral + "' AS marker " +
			"FROM `local` JOIN `test-project.eu_lookup.names` AS lookup ON lookup.id = local.id " +
			"-- `test-project.ignored.comment`",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !analysis.ProducesRows {
		t.Fatal("SELECT analysis must report a row-producing statement")
	}
	if len(analysis.ReferencedTables) != 2 {
		t.Fatalf("referenced tables = %#v, want source and lookup", analysis.ReferencedTables)
	}
	want := []struct{ project, dataset, table string }{
		{"test-project", "eu_source", "events"},
		{"test-project", "eu_lookup", "names"},
	}
	for index, expected := range want {
		actual := analysis.ReferencedTables[index]
		if actual.ProjectID != expected.project || actual.DatasetID != expected.dataset || actual.TableID != expected.table {
			t.Fatalf("reference %d = %#v, want %#v", index, actual, expected)
		}
	}
	if output := logs.String(); strings.Contains(output, secretLiteral) {
		t.Fatalf("query payload leaked from analysis logs: %s", output)
	} else if !strings.Contains(output, "query_digest") || !strings.Contains(output, "referenced_table_count") {
		t.Fatalf("safe analysis shape fields missing from logs: %s", output)
	}
}

func TestAnalyzeQueryUsesDefaultDatasetAndClassifiesDML(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })

	analysis, err := warehouse.AnalyzeQuery(ctx, ports.QueryRequest{
		ProjectID: "test-project", DefaultProjectID: "data-project", DefaultDataset: "analytics",
		SQL: "INSERT INTO `events` VALUES (1)",
	})
	if err != nil {
		t.Fatal(err)
	}
	if analysis.ProducesRows || len(analysis.ReferencedTables) != 1 {
		t.Fatalf("DML analysis = %#v", analysis)
	}
	ref := analysis.ReferencedTables[0]
	if ref.ProjectID != "data-project" || ref.DatasetID != "analytics" || ref.TableID != "events" {
		t.Fatalf("default-dataset reference = %#v", ref)
	}
	if len(analysis.MutationTargets) != 1 || analysis.MutationTargets[0] != ref {
		t.Fatalf("DML mutation targets = %#v, want %#v", analysis.MutationTargets, ref)
	}
}

func TestAnalyzeQueryClassifiesRowProducingStatementAfterLeadingComments(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })

	for _, query := range []string{
		"-- connector trace\nSELECT 1 AS value",
		"# shell trace\nWITH source AS (SELECT 1) SELECT * FROM source",
		"/* request metadata */ VALUES (1)",
	} {
		analysis, err := warehouse.AnalyzeQuery(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: query})
		if err != nil {
			t.Fatal(err)
		}
		if !analysis.ProducesRows {
			t.Fatalf("comment-prefixed row query classified as non-row-producing: %q", query)
		}
	}
}

func TestAnalyzeQueryMarksCatalogDDLBeforeExecution(t *testing.T) {
	ctx, cancel := duckDBQueryTestContext(t)
	defer cancel()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })

	for _, query := range []string{
		"CREATE TABLE `test-project.analytics.created` (id BIGINT)",
		"ALTER TABLE `test-project.analytics.events` ADD COLUMN note VARCHAR",
		"DROP TABLE `test-project.analytics.events`",
		"TRUNCATE TABLE `test-project.analytics.events`",
	} {
		analysis, err := warehouse.AnalyzeQuery(ctx, ports.QueryRequest{ProjectID: "test-project", SQL: query})
		if err != nil {
			t.Fatal(err)
		}
		if !analysis.RequiresCatalogMutation {
			t.Fatalf("catalog DDL was not classified before execution: %q", query)
		}
	}
}
