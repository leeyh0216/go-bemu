package contract

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompileRejectsAnnotationDriftAndSortsOperations(t *testing.T) {
	write := func(name, text string) string {
		path := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	for _, test := range []struct {
		name, source, want string
		selectors          map[string][]string
	}{
		{"duplicate", "# bqemu:operation z scenario=s\n# bqemu:operation z scenario=s\n", "duplicate", map[string][]string{"s": {"z"}}},
		{"unknown", "# bqemu:operation z scenario=s\n", "unknown", map[string][]string{"s": {"missing"}}},
		{"mismatch", "# bqemu:operation z scenario=s\n", "mismatches", map[string][]string{"other": {"z"}}},
		{"orphan", "# bqemu:operation z scenario=s\n", "not selected", map[string][]string{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "case.py"), []byte(test.source), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := Compile(root, test.selectors)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v want %q", err, test.want)
			}
		})
	}
	root := t.TempDir()
	_ = write("unused.py", "")
	if err := os.WriteFile(filepath.Join(root, "case.py"), []byte("# bqemu:operation z scenario=s\n# bqemu:operation a scenario=s\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	operations, err := Compile(root, map[string][]string{"s": {"z", "a"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(operations) != 2 || operations[0].ID != "a" {
		t.Fatalf("operations=%#v", operations)
	}
}

func TestValidateExceptionsRequiresReviewedReason(t *testing.T) {
	if err := ValidateExceptions([]Exception{{Scenario: "load", Reason: "runner owns immutable fixture lifecycle"}}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateExceptions([]Exception{{Scenario: "load"}}); err == nil {
		t.Fatal("empty exception reason accepted")
	}
	if err := ValidateExceptions([]Exception{{Scenario: "load", Reason: "one"}, {Scenario: "load", Reason: "two"}}); err == nil {
		t.Fatal("duplicate exception accepted")
	}
}

func TestCheckedInManifestMatchesLiteralIntegrationAnnotations(t *testing.T) {
	operations, err := Compile("..", map[string][]string{
		"query-parameters":     {"bigquery.jobs.query.parameters"},
		"tabledata-insert-all": {"bigquery.tabledata.insert-all"},
		"parquet-media-upload": {"bigquery.jobs.insert.media-upload"},
		"dataset-label-filter": {"bigquery.datasets.list.filter"},
		"dataset-metadata-view": {"bigquery.datasets.get.metadata-view"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(operations) != 5 {
		t.Fatalf("operation count=%d", len(operations))
	}
}

func TestWriteCompatibilityDocumentsProjectsOperationsInBothLanguages(t *testing.T) {
	root := t.TempDir()
	english := filepath.Join(root, "en", "generated.md")
	korean := filepath.Join(root, "ko", "generated.md")
	operations := []Operation{{ID: "bigquery.jobs.query.parameters", Scenario: "query-parameters", Source: "test.py:1"}}
	if err := WriteCompatibilityDocuments(english, korean, operations, nil); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{english, korean} {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(contents), "`bigquery.jobs.query.parameters`") {
			t.Fatalf("%s did not contain generated operation: %s", path, contents)
		}
	}
}
