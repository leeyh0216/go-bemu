package integrationcontract

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAnnotationScenarioDerivesPythonOperationsAndEvidence(t *testing.T) {
	root := integrationAnnotationRoot(t)
	path := filepath.Join(root, "tests", "integration", "python", "test_sample.py")
	source := `
import pytest

@pytest.mark.operation("example.query")
@pytest.mark.operation("example.poll")
def test_query():
    pass
`
	writeIntegrationAnnotationSource(t, path, source)
	manifest := ConsumerManifest{Scenarios: []ConsumerScenario{{
		ID:            "query",
		TrafficSource: TrafficSourceAnnotations(),
		Selectors:     []string{"pytest:tests/integration/python/test_sample.py"},
	}}}
	derived, err := DeriveIntegrationScenarioEvidence(root, manifest, map[string]bool{
		"example.poll": true, "example.query": true,
	})
	if err != nil {
		t.Fatal(err)
	}
	scenario := derived.Scenarios[0]
	if got, want := strings.Join(scenario.OperationIDs, ","), "example.poll,example.query"; got != want {
		t.Fatalf("operation IDs = %q, want %q", got, want)
	}
	if got, want := strings.Join(scenario.TestEvidence, ","), "python:tests/integration/python/test_sample.py:test_query"; got != want {
		t.Fatalf("test evidence = %q, want %q", got, want)
	}

	manifest.Scenarios[0].Selectors = []string{"pytest:tests/integration/python/test_other.py"}
	if _, err := DeriveIntegrationScenarioEvidence(root, manifest, map[string]bool{"example.poll": true, "example.query": true}); err == nil || !strings.Contains(err.Error(), "not selected") {
		t.Fatalf("orphan annotation error = %v", err)
	}
}

func TestAnnotationScenarioRejectsAmbiguousSelectors(t *testing.T) {
	root := integrationAnnotationRoot(t)
	path := filepath.Join(root, "tests", "integration", "python", "test_sample.py")
	writeIntegrationAnnotationSource(t, path, `
import pytest

@pytest.mark.operation("example.query")
def test_query():
    pass
`)
	manifest := ConsumerManifest{Scenarios: []ConsumerScenario{
		{ID: "one", TrafficSource: TrafficSourceAnnotations(), Selectors: []string{"pytest:tests/integration/python/test_sample.py"}},
		{ID: "two", TrafficSource: TrafficSourceAnnotations(), Selectors: []string{"pytest:tests/integration/python/test_sample.py"}},
	}}
	if _, err := DeriveIntegrationScenarioEvidence(root, manifest, map[string]bool{"example.query": true}); err == nil || !strings.Contains(err.Error(), "selected by both") {
		t.Fatalf("ambiguous annotation error = %v", err)
	}
}

func TestSparkContractCaseDerivesOperationsBySelector(t *testing.T) {
	root := integrationAnnotationRoot(t)
	path := filepath.Join(root, "tests", "integration", "spark", "test_sample.py")
	writeIntegrationAnnotationSource(t, path, `
from conftest import contract_case

@contract_case(
    "SBQ-READ-EXAMPLE-V1",
    state="verified",
    category="read",
    summary="Read a table",
    profile="spark-bigquery-connector-dsv1-0.44.2",
    wire_flow="read-arrow",
    operations=("example.query",),
)
def test_query():
    pass
`)
	manifest := ConsumerManifest{Scenarios: []ConsumerScenario{{
		ID:            "spark-query",
		TrafficSource: TrafficSourceAnnotations(),
		Selectors:     []string{"pytest:tests/integration/spark/test_sample.py"},
	}}}
	derived, err := DeriveIntegrationScenarioEvidence(root, manifest, map[string]bool{"example.query": true})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(derived.Scenarios[0].TestEvidence, ","), "spark:tests/integration/spark/test_sample.py:test_query"; got != want {
		t.Fatalf("test evidence = %q, want %q", got, want)
	}
}

func TestBqScenarioLabelsDeriveSharedRunnerEvidence(t *testing.T) {
	root := integrationAnnotationRoot(t)
	path := filepath.Join(root, "tests", "integration", "bqcli", "runner.py")
	writeIntegrationAnnotationSource(t, path, `
from operation_contract import operation

@operation("example.metadata", scenario="metadata")
@operation("example.query", scenario="query")
def main():
    pass

@operation("example.poll", scenario="query")
def helper():
    pass
`)
	manifest := ConsumerManifest{Scenarios: []ConsumerScenario{
		{ID: "metadata", TrafficSource: TrafficSourceAnnotations(), Selectors: []string{"bq:tests/integration/bqcli/runner.py:main"}},
		{ID: "query", TrafficSource: TrafficSourceAnnotations(), Selectors: []string{"bq:tests/integration/bqcli/runner.py:main"}},
	}}
	derived, err := DeriveIntegrationScenarioEvidence(root, manifest, map[string]bool{
		"example.metadata": true, "example.poll": true, "example.query": true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(derived.Scenarios[0].OperationIDs, ","), "example.metadata"; got != want {
		t.Fatalf("metadata operations = %q, want %q", got, want)
	}
	if got, want := strings.Join(derived.Scenarios[1].OperationIDs, ","), "example.poll,example.query"; got != want {
		t.Fatalf("query operations = %q, want %q", got, want)
	}
	if got, want := strings.Join(derived.Scenarios[1].TestEvidence, ","), "bq:tests/integration/bqcli/runner.py:helper,bq:tests/integration/bqcli/runner.py:main"; got != want {
		t.Fatalf("query test evidence = %q, want %q", got, want)
	}
}

func TestRunnerEvidenceIsLoadOnlyAndRequiresReason(t *testing.T) {
	root := integrationAnnotationRoot(t)
	manifest := ConsumerManifest{Scenarios: []ConsumerScenario{{
		ID: "load",
		TrafficSource: ScenarioTrafficSource{
			Kind: trafficSourceRunnerEvidence, Reason: "The load runner generates traffic outside a selected test function.",
		},
		Selectors: []string{"load:python"},
		OperationExpectations: []ConsumerOperationExpectation{
			{OperationID: "example.load", Min: 1},
		},
	}}}
	derived, err := DeriveIntegrationScenarioEvidence(root, manifest, map[string]bool{"example.load": true})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(derived.Scenarios[0].OperationIDs, ","), "example.load"; got != want || len(derived.Scenarios[0].TestEvidence) != 0 {
		t.Fatalf("runner evidence = %#v", derived.Scenarios[0])
	}

	manifest.Scenarios[0].TrafficSource.Reason = ""
	if _, err := DeriveIntegrationScenarioEvidence(root, manifest, map[string]bool{"example.load": true}); err == nil || !strings.Contains(err.Error(), "must declare a reason") {
		t.Fatalf("missing reason error = %v", err)
	}
	manifest.Scenarios[0].TrafficSource.Reason = "A reason"
	manifest.Scenarios[0].Selectors = []string{"pytest:tests/integration/python/test_sample.py"}
	if _, err := DeriveIntegrationScenarioEvidence(root, manifest, map[string]bool{"example.load": true}); err == nil || !strings.Contains(err.Error(), "reserved for load") {
		t.Fatalf("non-load runner error = %v", err)
	}
}

func TestIntegrationAnnotationExtractorRejectsDynamicBqScenario(t *testing.T) {
	root := integrationAnnotationRoot(t)
	path := filepath.Join(root, "tests", "integration", "bqcli", "runner.py")
	writeIntegrationAnnotationSource(t, path, `
from operation_contract import operation

@operation("example.query", scenario=SCENARIO)
def main():
    pass
`)
	manifest := ConsumerManifest{Scenarios: []ConsumerScenario{{
		ID: "query", TrafficSource: TrafficSourceAnnotations(), Selectors: []string{"bq:tests/integration/bqcli/runner.py:main"},
	}}}
	if _, err := DeriveIntegrationScenarioEvidence(root, manifest, map[string]bool{"example.query": true}); err == nil || !strings.Contains(err.Error(), "operation scenario must be one literal string") {
		t.Fatalf("dynamic bq scenario error = %v", err)
	}
}

func TrafficSourceAnnotations() ScenarioTrafficSource {
	return ScenarioTrafficSource{Kind: trafficSourceAnnotations}
}

func writeIntegrationAnnotationSource(t *testing.T, path, source string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(strings.TrimSpace(source)+"\n"), 0o600); err != nil {
		t.Fatal(err)
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
	extractor, err := os.ReadFile("extract_integration_annotations.py")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "tests", "integration", "contract", "extract_integration_annotations.py")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, extractor, 0o600); err != nil {
		t.Fatal(err)
	}
	return root
}
