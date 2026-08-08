package duckdb

import (
	"errors"
	"strings"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestRenderPhysicalTableUsesCanonicalReference(t *testing.T) {
	t.Parallel()
	reference := domain.TableReference{
		ProjectID: "data-project", DatasetID: "analytics", TableID: `events"archive`,
	}
	got, err := renderPhysicalTable(reference)
	if err != nil {
		t.Fatal(err)
	}
	want := quoteIdentifier(physicalSchema("data-project", "analytics")) + `.` + quoteIdentifier(`events"archive`)
	if got != want {
		t.Fatalf("physical table = %q, want %q", got, want)
	}
}

func TestRenderPhysicalTableRejectsIncompleteCanonicalReference(t *testing.T) {
	t.Parallel()
	for _, test := range []domain.TableReference{
		{},
		{ProjectID: "project", DatasetID: "dataset"},
		{ProjectID: "project", TableID: "events"},
		{DatasetID: "dataset", TableID: "events"},
	} {
		if _, err := renderPhysicalTable(test); !errors.Is(err, domain.ErrInvalidQuery) {
			t.Fatalf("reference=%#v error=%v, want ErrInvalidQuery", test, err)
		}
	}
}

func TestDuckDBStatementPlanOwnsBindArguments(t *testing.T) {
	t.Parallel()
	payload := []byte("payload")
	input := []any{"value", payload}
	const analysisFingerprint = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	plan, err := newDuckDBStatementPlan("SELECT ?, ?", input, true, analysisFingerprint)
	if err != nil {
		t.Fatal(err)
	}
	input[0] = "changed"
	payload[0] = 'X'
	first := plan.bindArguments()
	if first[0] != "value" || string(first[1].([]byte)) != "payload" {
		t.Fatalf("plan arguments changed through constructor input: %#v", first)
	}
	first[0] = "changed again"
	first[1].([]byte)[0] = 'Y'
	second := plan.bindArguments()
	if second[0] != "value" || string(second[1].([]byte)) != "payload" {
		t.Fatalf("plan arguments changed through accessor output: %#v", second)
	}
	if plan.statementSQL() != "SELECT ?, ?" || !plan.returnsRows() ||
		plan.semanticAnalysisFingerprint() != analysisFingerprint {
		t.Fatalf("unexpected plan metadata: %#v", plan)
	}
}

func TestDuckDBStatementPlanRejectsEmptyGeneratedSQL(t *testing.T) {
	t.Parallel()
	if _, err := newDuckDBStatementPlan(" \n\t", nil, false, strings.Repeat("0", 64)); !errors.Is(err, domain.ErrInvalidQuery) {
		t.Fatalf("error = %v, want ErrInvalidQuery", err)
	}
}
