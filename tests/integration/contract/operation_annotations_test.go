package integrationcontract

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIntegrationOperationAnnotationsBelongToScenarioEvidence(t *testing.T) {
	root := integrationAnnotationRoot(t)
	path := filepath.Join(root, "tests", "integration", "python", "test_sample.py")
	source := "import pytest\n\n@pytest.mark.operation(\"example.query\")\ndef test_query():\n    pass\n"
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest := ConsumerManifest{Scenarios: []ConsumerScenario{{
		ID:           "query",
		OperationIDs: []string{"example.query"},
		TestEvidence: []string{"python:tests/integration/python/test_sample.py:test_query"},
	}}}
	known := map[string]bool{"example.query": true}
	if err := ValidateIntegrationOperationAnnotations(root, manifest, known); err != nil {
		t.Fatal(err)
	}

	manifest.Scenarios[0].TestEvidence = nil
	if err := ValidateIntegrationOperationAnnotations(root, manifest, known); err == nil || !strings.Contains(err.Error(), "absent from scenario evidence") {
		t.Fatalf("missing evidence error = %v", err)
	}
	manifest.Scenarios[0].TestEvidence = []string{"python:tests/integration/python/test_sample.py:test_query"}
	unknown := strings.Replace(source, "example.query", "example.unknown", 1)
	if err := os.WriteFile(path, []byte(unknown), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ValidateIntegrationOperationAnnotations(root, manifest, known); err == nil || !strings.Contains(err.Error(), "unknown operation") {
		t.Fatalf("unknown operation error = %v", err)
	}
}

func TestIntegrationCommandMarkerRejectsAdditionalMetadata(t *testing.T) {
	root := integrationAnnotationRoot(t)
	path := filepath.Join(root, "tests", "integration", "bqcli", "runner.py")
	source := "from operation_contract import operation\n\n@operation(\"example.query\", \"metadata\")\ndef main():\n    pass\n"
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest := ConsumerManifest{Scenarios: []ConsumerScenario{{
		ID:           "query",
		OperationIDs: []string{"example.query"},
		TestEvidence: []string{"bq:tests/integration/bqcli/runner.py:main"},
	}}}
	if err := ValidateIntegrationOperationAnnotations(root, manifest, map[string]bool{"example.query": true}); err == nil || !strings.Contains(err.Error(), "one literal") {
		t.Fatalf("malformed command marker error = %v", err)
	}
}

func integrationAnnotationRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, directory := range []string{"python", "spark", "bqcli"} {
		if err := os.MkdirAll(filepath.Join(root, "tests", "integration", directory), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return root
}
