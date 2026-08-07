package domain

import (
	"strings"
	"testing"
)

func TestValidateSchemaEvolutionAcceptsTopLevelAndNestedAdditions(t *testing.T) {
	current := []Field{
		{Name: "id", Type: "INT64", Mode: "REQUIRED"},
		{Name: "payload", Type: "RECORD", Fields: []Field{{Name: "name", Type: "STRING"}}},
	}
	proposed := []Field{
		{Name: "id", Type: "INTEGER", Mode: "REQUIRED", Description: "description updates are metadata-only"},
		{Name: "payload", Type: "STRUCT", Fields: []Field{
			{Name: "name", Type: "STRING"},
			{Name: "tags", Type: "STRING", Mode: "REPEATED"},
		}},
		{Name: "observed_at", Type: "TIMESTAMP", Mode: "NULLABLE"},
	}
	additions, err := ValidateSchemaEvolution(current, proposed)
	if err != nil {
		t.Fatal(err)
	}
	if len(additions) != 2 || strings.Join(additions[0].Path, ".") != "payload.tags" || strings.Join(additions[1].Path, ".") != "observed_at" {
		t.Fatalf("unexpected additions: %#v", additions)
	}
}

func TestValidateSchemaEvolutionRejectsDestructiveChanges(t *testing.T) {
	current := []Field{{Name: "id", Type: "INT64"}, {Name: "name", Type: "STRING"}}
	tests := map[string][]Field{
		"required addition": append(append([]Field(nil), current...), Field{Name: "new_id", Type: "INT64", Mode: "REQUIRED"}),
		"removal":           current[:1],
		"reorder":           {current[1], current[0]},
		"type":              {{Name: "id", Type: "STRING"}, current[1]},
		"mode":              {{Name: "id", Type: "INT64", Mode: "REPEATED"}, current[1]},
	}
	for name, proposed := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := ValidateSchemaEvolution(current, proposed)
			if err == nil || !strings.Contains(err.Error(), CapabilitySchemaAdditiveV1) || !strings.Contains(err.Error(), "fix_hint=") {
				t.Fatalf("expected actionable capability error, got %v", err)
			}
		})
	}
}

func TestValidateSchemaEvolutionAppliesRulesInsideRepeatedRecord(t *testing.T) {
	current := []Field{{
		Name: "items", Type: "RECORD", Mode: "REPEATED",
		Fields: []Field{{Name: "sku", Type: "STRING"}},
	}}
	proposed := []Field{{
		Name: "items", Type: "STRUCT", Mode: "REPEATED",
		Fields: []Field{{Name: "sku", Type: "STRING"}, {Name: "quantity", Type: "INT64"}},
	}}
	additions, err := ValidateSchemaEvolution(current, proposed)
	if err != nil {
		t.Fatal(err)
	}
	if len(additions) != 1 || strings.Join(additions[0].Path, ".") != "items.quantity" {
		t.Fatalf("unexpected repeated RECORD additions: %#v", additions)
	}

	proposed[0].Fields[1].Mode = "REQUIRED"
	if _, err := ValidateSchemaEvolution(current, proposed); err == nil || !strings.Contains(err.Error(), "items.quantity") {
		t.Fatalf("nested REQUIRED addition must be rejected with its full path, got %v", err)
	}
}

func TestTableValidationRejectsDuplicateNestedFields(t *testing.T) {
	table := Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events",
		Schema: []Field{{Name: "payload", Type: "RECORD", Fields: []Field{{Name: "id", Type: "INT64"}, {Name: "ID", Type: "STRING"}}}},
	}
	if err := table.Validate(); err == nil || !strings.Contains(err.Error(), "payload.ID") {
		t.Fatalf("expected nested duplicate error, got %v", err)
	}
}
