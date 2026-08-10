//go:build realbigquery

// Package bigqueryintegration contains opt-in tests against BigQuery itself.
//
// Run with an authenticated Application Default Credential and a disposable
// project:
//
//	BQEMU_REAL_BIGQUERY_PROJECT=my-project go test -tags=realbigquery ./tests/integration/bigquery
package bigqueryintegration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
)

func TestSchemaEvolutionWideningAgainstRealBigQuery(t *testing.T) {
	projectID := os.Getenv("BQEMU_REAL_BIGQUERY_PROJECT")
	if projectID == "" {
		t.Skip("set BQEMU_REAL_BIGQUERY_PROJECT and authenticate ADC to run the real-BigQuery differential")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		t.Fatalf("create BigQuery client: %v", err)
	}
	defer client.Close()

	datasetID := fmt.Sprintf("bqemu_schema_evolution_%d", time.Now().UnixNano())
	dataset := client.Dataset(datasetID)
	if err := dataset.Create(ctx, &bigquery.DatasetMetadata{Location: "US"}); err != nil {
		t.Fatalf("create temporary dataset: %v", err)
	}
	defer func() {
		if err := dataset.DeleteWithContents(ctx); err != nil {
			t.Errorf("delete temporary dataset %q: %v", datasetID, err)
		}
	}()

	table := fmt.Sprintf("`%s.%s.conversions`", projectID, datasetID)
	runQuery(t, ctx, client, "CREATE TABLE "+table+" (id INT64, amount NUMERIC)")

	// These are the documented widening paths implemented by BQEMU #21.
	runQuery(t, ctx, client, "ALTER TABLE "+table+" ALTER COLUMN id SET DATA TYPE NUMERIC")
	runQuery(t, ctx, client, "ALTER TABLE "+table+" ALTER COLUMN amount SET DATA TYPE BIGNUMERIC")
	assertFieldTypes(t, ctx, client, table, map[string]string{
		"id":     "NUMERIC",
		"amount": "BIGNUMERIC",
	})

	// A narrowing conversion must fail and leave the published schema intact.
	if err := runQueryError(ctx, client, "ALTER TABLE "+table+" ALTER COLUMN amount SET DATA TYPE NUMERIC"); err == nil {
		t.Fatal("BIGNUMERIC -> NUMERIC unexpectedly succeeded")
	}
	assertFieldTypes(t, ctx, client, table, map[string]string{
		"id":     "NUMERIC",
		"amount": "BIGNUMERIC",
	})
}

func runQuery(t *testing.T, ctx context.Context, client *bigquery.Client, sql string) {
	t.Helper()
	if err := runQueryError(ctx, client, sql); err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
}

func runQueryError(ctx context.Context, client *bigquery.Client, sql string) error {
	job, err := client.Query(sql).Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	return status.Err()
}

func assertFieldTypes(t *testing.T, ctx context.Context, client *bigquery.Client, qualifiedTable string, want map[string]string) {
	t.Helper()
	path := strings.Trim(qualifiedTable, "`")
	parts := strings.Split(path, ".")
	if len(parts) != 3 {
		t.Fatalf("invalid qualified table %q", qualifiedTable)
	}
	metadata, err := client.DatasetInProject(parts[0], parts[1]).Table(parts[2]).Metadata(ctx)
	if err != nil {
		t.Fatalf("read table metadata: %v", err)
	}
	got := make(map[string]string, len(metadata.Schema))
	for _, field := range metadata.Schema {
		got[field.Name] = string(field.Type)
	}
	for name, fieldType := range want {
		if got[name] != fieldType {
			t.Errorf("field %q type = %q, want %q (all fields: %#v)", name, got[name], fieldType, got)
		}
	}
}
