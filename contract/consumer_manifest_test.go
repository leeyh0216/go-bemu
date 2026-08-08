package contract

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConsumerYAMLRejectsUnknownFieldsAndMultipleDocuments(t *testing.T) {
	manifest := `schemaVersion: "1"
runtimeProfiles: []
runnerAdapters: []
compatibilityProfiles: []
scenarios: []
scenarioSets: []
`
	if _, err := DecodeConsumerManifest([]byte(strings.Replace(manifest, "schemaVersion:", "mystery: true\nschemaVersion:", 1))); err == nil {
		t.Fatal("expected unknown manifest field to fail")
	}
	consumerCase := validConsumerCaseYAML("case-one")
	if _, err := DecodeConsumerCase([]byte(strings.Replace(consumerCase, "family:", "mystery: true\nfamily:", 1))); err == nil {
		t.Fatal("expected unknown case field to fail")
	}
	if _, err := DecodeConsumerCase([]byte(consumerCase + "---\n{}\n")); err == nil {
		t.Fatal("expected multiple YAML documents to fail")
	}
}

func TestConsumerManifestRejectsInvalidReferencesAndRuntimeContracts(t *testing.T) {
	validManifest, validCases, operations := validConsumerFixture()
	tests := map[string]func(*ConsumerManifest, *[]ConsumerCase, map[string]bool){
		"duplicate runtime": func(manifest *ConsumerManifest, _ *[]ConsumerCase, _ map[string]bool) {
			manifest.RuntimeProfiles = append(manifest.RuntimeProfiles, manifest.RuntimeProfiles[0])
		},
		"unknown adapter": func(_ *ConsumerManifest, cases *[]ConsumerCase, _ map[string]bool) {
			(*cases)[0].RunnerAdapterID = "missing"
		},
		"unknown operation": func(manifest *ConsumerManifest, _ *[]ConsumerCase, _ map[string]bool) {
			manifest.Scenarios[0].OperationIDs = append(manifest.Scenarios[0].OperationIDs, "bigquery.unknown")
		},
		"unknown scenario": func(manifest *ConsumerManifest, _ *[]ConsumerCase, _ map[string]bool) {
			manifest.ScenarioSets[0].ScenarioIDs = append(manifest.ScenarioSets[0].ScenarioIDs, "missing")
		},
		"bad digest": func(_ *ConsumerManifest, cases *[]ConsumerCase, _ map[string]bool) {
			(*cases)[0].Artifacts[0].SHA256 = "SHA256:bad"
		},
		"runtime mismatch": func(_ *ConsumerManifest, cases *[]ConsumerCase, _ map[string]bool) {
			(*cases)[0].Family = "spark"
		},
		"missing adapter version": func(_ *ConsumerManifest, cases *[]ConsumerCase, _ map[string]bool) {
			delete((*cases)[0].Versions, "client")
		},
		"duplicate scenario ID": func(manifest *ConsumerManifest, _ *[]ConsumerCase, _ map[string]bool) {
			manifest.Scenarios = append(manifest.Scenarios, manifest.Scenarios[0])
		},
		"duplicate case ID": func(_ *ConsumerManifest, cases *[]ConsumerCase, _ map[string]bool) {
			*cases = append(*cases, (*cases)[0])
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			manifest := cloneConsumerManifest(t, validManifest)
			cases := cloneConsumerCases(t, validCases)
			operationCopy := cloneBoolMap(operations)
			mutate(&manifest, &cases, operationCopy)
			if _, err := NormalizeConsumerManifest(manifest, cases, operationCopy); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestNormalizedConsumerManifestIsDeterministicAndFullyExpanded(t *testing.T) {
	manifest, cases, operations := validConsumerFixture()
	normalized, err := NormalizeConsumerManifest(manifest, cases, operations)
	if err != nil {
		t.Fatal(err)
	}
	first, err := MarshalNormalizedConsumerManifest(normalized)
	if err != nil {
		t.Fatal(err)
	}
	second, err := MarshalNormalizedConsumerManifest(normalized)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) || !strings.HasSuffix(string(first), "\n") {
		t.Fatal("normalized consumer manifest is not deterministic")
	}
	decoded, err := DecodeNormalizedConsumerManifest(first)
	if err != nil {
		t.Fatal(err)
	}
	consumerCase := decoded.Cases[0]
	if consumerCase.RuntimeProfile.Versions["client"] != "3.43.0" || consumerCase.RunnerAdapter.ID == "" || len(consumerCase.ScenarioSet.Scenarios) != 1 || len(consumerCase.Artifacts) != 1 {
		t.Fatalf("normalized case is not fully expanded: %#v", consumerCase)
	}
}

func TestAddingOneCaseYAMLAutoAddsOneCIMatrixRow(t *testing.T) {
	repositoryRoot := filepath.Clean("..")
	temporaryRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(temporaryRoot, consumerCasesDirectory), 0o755); err != nil {
		t.Fatal(err)
	}
	copyFile(t, filepath.Join(repositoryRoot, consumerManifestPath), filepath.Join(temporaryRoot, consumerManifestPath))
	copyFile(t, filepath.Join(repositoryRoot, "contract/operations.normalized.json"), filepath.Join(temporaryRoot, "contract/operations.normalized.json"))
	paths, err := filepath.Glob(filepath.Join(repositoryRoot, consumerCasesDirectory, "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		copyFile(t, path, filepath.Join(temporaryRoot, consumerCasesDirectory, filepath.Base(path)))
	}
	base, err := os.ReadFile(filepath.Join(repositoryRoot, consumerCasesDirectory, "google-cloud-bigquery-python-3.43.0.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	patchCase := strings.Replace(string(base), "google-cloud-bigquery-python-3.43.0\n", "google-cloud-bigquery-python-3.43.0-patch\n", 1)
	if err := os.WriteFile(filepath.Join(temporaryRoot, consumerCasesDirectory, "google-cloud-bigquery-python-3.43.0-patch.yaml"), []byte(patchCase), 0o600); err != nil {
		t.Fatal(err)
	}
	normalized, err := CompileConsumerManifest(temporaryRoot)
	if err != nil {
		t.Fatal(err)
	}
	contents, err := MarshalNormalizedConsumerManifest(normalized)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(temporaryRoot, consumerNormalizedPath), contents, 0o600); err != nil {
		t.Fatal(err)
	}
	matrix, err := ConsumerMatrix(temporaryRoot, "python", "required")
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Include []ExpandedConsumerCase `json:"include"`
	}
	if err := json.Unmarshal(matrix, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Include) != 2 || payload.Include[1].ID != "google-cloud-bigquery-python-3.43.0-patch" {
		t.Fatalf("matrix rows = %#v", payload.Include)
	}
}

func validConsumerFixture() (ConsumerManifest, []ConsumerCase, map[string]bool) {
	manifest := ConsumerManifest{
		SchemaVersion:         "1",
		RuntimeProfiles:       []RuntimeProfile{{ID: "python", Family: "python", Kind: "python-pytest"}},
		RunnerAdapters:        []RunnerAdapter{{ID: "pytest-v1", Family: "python", RuntimeKind: "python-pytest", RequiredVersions: []string{"python", "client"}, Bootstrap: map[string]string{}}},
		CompatibilityProfiles: []CompatibilityProfile{{ID: "python-v1", ScenarioIDs: []string{"query"}}},
		Scenarios:             []ConsumerScenario{{ID: "query", OperationIDs: []string{"bigquery.jobs.query"}}},
		ScenarioSets:          []ScenarioSet{{ID: "query-set", ScenarioIDs: []string{"query"}}},
	}
	cases := []ConsumerCase{{
		SchemaVersion: "1", ID: "case-one", DisplayName: "Case one", Family: "python", Lane: "required",
		RuntimeProfileID: "python", RunnerAdapterID: "pytest-v1", CompatibilityProfileID: "python-v1", ScenarioSetID: "query-set",
		Versions:  map[string]string{"python": "3.13", "client": "3.43.0"},
		Artifacts: []ConsumerArtifact{{ID: "wheel", URI: "https://example.invalid/client.whl", SHA256: strings.Repeat("a", 64)}},
	}}
	return manifest, cases, map[string]bool{"bigquery.jobs.query": true}
}

func validConsumerCaseYAML(id string) string {
	return `schemaVersion: "1"
id: ` + id + `
displayName: Example
family: python
lane: required
runtimeProfile: python
runnerAdapter: pytest-v1
compatibilityProfile: python-v1
scenarioSet: query-set
versions: {python: "3.13", client: "3.43.0"}
artifacts:
  - id: wheel
    uri: https://example.invalid/client.whl
    sha256: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
`
}

func cloneConsumerManifest(t *testing.T, source ConsumerManifest) ConsumerManifest {
	t.Helper()
	contents, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	var result ConsumerManifest
	if err := json.Unmarshal(contents, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func cloneConsumerCases(t *testing.T, source []ConsumerCase) []ConsumerCase {
	t.Helper()
	contents, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	var result []ConsumerCase
	if err := json.Unmarshal(contents, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func cloneBoolMap(source map[string]bool) map[string]bool {
	result := make(map[string]bool, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func copyFile(t *testing.T, source, destination string) {
	t.Helper()
	contents, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, contents, 0o600); err != nil {
		t.Fatal(err)
	}
}
