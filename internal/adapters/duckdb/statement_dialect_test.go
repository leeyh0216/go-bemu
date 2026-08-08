package duckdb

import (
	"testing"
)

func TestDuckDBGeneratedLiteralDialectAcceptsBoundValues(t *testing.T) {
	t.Parallel()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })

	var timestamp, array, structValue any
	err = warehouse.db.QueryRowContext(t.Context(), `
		SELECT
			CAST(? AS TIMESTAMPTZ),
			CAST([?, ?] AS BIGINT[]),
			struct_pack("id" := ?, "label" := ?)`,
		"2026-08-08 12:34:56+09:00", int64(1), int64(2), int64(7), "bound-value",
	).Scan(&timestamp, &array, &structValue)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDuckDBGeneratedArrayAggIgnoreNullsDialect(t *testing.T) {
	t.Parallel()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })

	var values any
	err = warehouse.db.QueryRowContext(t.Context(), `
		SELECT array_agg(DISTINCT value) FILTER (WHERE value IS NOT NULL)
		FROM (VALUES (?), (?), (NULL), (?)) AS input(value)`,
		int64(2), int64(1), int64(2),
	).Scan(&values)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDuckDBGeneratedStaticMergeDialect(t *testing.T) {
	t.Parallel()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })

	for _, statement := range []string{
		`CREATE TABLE target_rows (id BIGINT, label VARCHAR)`,
		`CREATE TABLE source_rows (id BIGINT, label VARCHAR)`,
		`INSERT INTO target_rows VALUES (1, 'old')`,
		`INSERT INTO source_rows VALUES (2, 'new')`,
		`MERGE INTO target_rows AS target
		 USING source_rows AS source
		 ON FALSE
		 WHEN NOT MATCHED THEN INSERT BY NAME
		 WHEN NOT MATCHED BY SOURCE THEN DELETE`,
	} {
		if _, err := warehouse.db.ExecContext(t.Context(), statement); err != nil {
			t.Fatalf("generated DuckDB statement failed: %v", err)
		}
	}
	var id int64
	var label string
	if err := warehouse.db.QueryRowContext(t.Context(), `SELECT id, label FROM target_rows`).Scan(&id, &label); err != nil {
		t.Fatal(err)
	}
	if id != 2 || label != "new" {
		t.Fatalf("static MERGE result = (%d, %q), want (2, %q)", id, label, "new")
	}
}
