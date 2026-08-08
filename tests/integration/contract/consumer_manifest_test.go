package integrationcontract

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConsumerYAMLRejectsUnknownFieldsAndMultipleDocuments(t *testing.T) {
	manifest := `schemaVersion: "2"
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
			(*cases)[0].Executions[0].RunnerAdapterID = "missing"
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
		"non-HTTPS execution artifact": func(_ *ConsumerManifest, cases *[]ConsumerCase, _ map[string]bool) {
			(*cases)[0].Artifacts[0].URI = "file:///tmp/client.whl"
		},
		"unknown artifact usage": func(_ *ConsumerManifest, cases *[]ConsumerCase, _ map[string]bool) {
			(*cases)[0].Artifacts[0].Usage = "guessed-wheel"
		},
		"missing required artifact usage": func(_ *ConsumerManifest, cases *[]ConsumerCase, _ map[string]bool) {
			(*cases)[0].Artifacts = []ConsumerArtifact{{ID: "provenance", Role: "tool-provenance", Usage: "cloud-sdk-release-provenance", URI: "oci://example.invalid/tool@sha256:" + strings.Repeat("a", 64), SHA256: strings.Repeat("a", 64)}}
		},
		"duplicate required artifact usage": func(_ *ConsumerManifest, cases *[]ConsumerCase, _ map[string]bool) {
			duplicate := (*cases)[0].Artifacts[0]
			duplicate.ID = "wheel-two"
			(*cases)[0].Artifacts = append((*cases)[0].Artifacts, duplicate)
		},
		"runtime mismatch": func(_ *ConsumerManifest, cases *[]ConsumerCase, _ map[string]bool) {
			(*cases)[0].Family = "spark"
		},
		"missing adapter version": func(_ *ConsumerManifest, cases *[]ConsumerCase, _ map[string]bool) {
			delete((*cases)[0].Versions, "client")
		},
		"unknown adapter version": func(_ *ConsumerManifest, cases *[]ConsumerCase, _ map[string]bool) {
			(*cases)[0].Versions["clientTypo"] = "3.43.0"
		},
		"Scala binary mismatch": func(manifest *ConsumerManifest, cases *[]ConsumerCase, _ map[string]bool) {
			manifest.RuntimeProfiles[0].Family = "spark"
			manifest.RuntimeProfiles[0].Kind = "spark-pyspark"
			manifest.RunnerAdapters[0] = RunnerAdapter{
				ID: "spark-pyspark-pytest-v1", Family: "spark", RuntimeKind: "spark-pyspark", SelectorPrefix: "pytest",
				RequiredVersions:       []string{"spark", "connector", "scala", "scalaBinary", "java", "python"},
				RequiredArtifactUsages: []string{"spark-connector-dsv1-jar", "spark-connector-dsv2-jar", "spark-python-bridge", "spark-runtime"},
			}
			(*cases)[0].Family = "spark"
			(*cases)[0].Executions[0].RunnerAdapterID = "spark-pyspark-pytest-v1"
			(*cases)[0].Versions = map[string]string{"spark": "3.5.8", "connector": "0.44.2", "scala": "2.12.18", "scalaBinary": "2.13", "java": "17", "python": "3.11"}
			(*cases)[0].SourceProvenance = []ConsumerSourceReference{{Name: "connector", VersionKey: "connector", URI: "https://github.com/example/project/tree/v0.44.2"}}
			(*cases)[0].Artifacts = []ConsumerArtifact{
				{ID: "dsv1", Role: "execution", Usage: "spark-connector-dsv1-jar", URI: "https://example.invalid/dsv1.jar", SHA256: strings.Repeat("a", 64)},
				{ID: "dsv2", Role: "execution", Usage: "spark-connector-dsv2-jar", URI: "https://example.invalid/dsv2.jar", SHA256: strings.Repeat("b", 64)},
				{ID: "bridge", Role: "execution", Usage: "spark-python-bridge", URI: "https://example.invalid/bridge.whl", SHA256: strings.Repeat("c", 64)},
				{ID: "runtime", Role: "execution", Usage: "spark-runtime", URI: "https://example.invalid/runtime.tar.gz", SHA256: strings.Repeat("d", 64)},
			}
		},
		"duplicate scenario ID": func(manifest *ConsumerManifest, _ *[]ConsumerCase, _ map[string]bool) {
			manifest.Scenarios = append(manifest.Scenarios, manifest.Scenarios[0])
		},
		"duplicate operation across scenario set": func(manifest *ConsumerManifest, _ *[]ConsumerCase, _ map[string]bool) {
			manifest.Scenarios = append(manifest.Scenarios, ConsumerScenario{ID: "query-two", OperationIDs: []string{"bigquery.jobs.query"}, Selectors: []string{"pytest:tests/python/test_query_two.py"}})
			manifest.ScenarioSets[0].ScenarioIDs = append(manifest.ScenarioSets[0].ScenarioIDs, "query-two")
			manifest.CompatibilityProfiles[0].ScenarioIDs = append(manifest.CompatibilityProfiles[0].ScenarioIDs, "query-two")
		},
		"self ordering dependency": func(manifest *ConsumerManifest, _ *[]ConsumerCase, _ map[string]bool) {
			manifest.Scenarios[0].OperationExpectations = []ConsumerOperationExpectation{{OperationID: "bigquery.jobs.query", Min: 1, After: []string{"bigquery.jobs.query"}}}
		},
		"ordering dependency cycle": func(manifest *ConsumerManifest, _ *[]ConsumerCase, operations map[string]bool) {
			operations["bigquery.jobs.get"] = true
			manifest.Scenarios[0].OperationIDs = append(manifest.Scenarios[0].OperationIDs, "bigquery.jobs.get")
			manifest.Scenarios[0].OperationExpectations = []ConsumerOperationExpectation{
				{OperationID: "bigquery.jobs.query", Min: 1, After: []string{"bigquery.jobs.get"}},
				{OperationID: "bigquery.jobs.get", Min: 1, After: []string{"bigquery.jobs.query"}},
			}
		},
		"selector adapter mismatch": func(manifest *ConsumerManifest, _ *[]ConsumerCase, _ map[string]bool) {
			manifest.Scenarios[0].Selectors = []string{"bq:tests/bqcli/run_contract.py:main"}
		},
		"mutable source provenance": func(_ *ConsumerManifest, cases *[]ConsumerCase, _ map[string]bool) {
			(*cases)[0].SourceProvenance[0].URI = "https://github.com/googleapis/google-cloud-python/tree/main/packages/google-cloud-bigquery"
		},
		"unknown provenance version key": func(_ *ConsumerManifest, cases *[]ConsumerCase, _ map[string]bool) {
			(*cases)[0].SourceProvenance[0].VersionKey = "missing"
		},
		"duplicate execution ID": func(_ *ConsumerManifest, cases *[]ConsumerCase, _ map[string]bool) {
			(*cases)[0].Executions = append((*cases)[0].Executions, (*cases)[0].Executions[0])
		},
		"OCI digest mismatch": func(_ *ConsumerManifest, cases *[]ConsumerCase, _ map[string]bool) {
			(*cases)[0].Artifacts[0].URI = "oci://example.invalid/client@sha256:" + strings.Repeat("b", 64)
		},
		"duplicate case ID": func(_ *ConsumerManifest, cases *[]ConsumerCase, _ map[string]bool) {
			*cases = append(*cases, (*cases)[0])
		},
		"unsafe case ID": func(_ *ConsumerManifest, cases *[]ConsumerCase, _ map[string]bool) {
			(*cases)[0].ID = "../case"
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
	if consumerCase.RuntimeProfile.Versions["client"] != "3.43.0" || len(consumerCase.Executions) != 1 || consumerCase.Executions[0].RunnerAdapter.ID == "" || len(consumerCase.Executions[0].ScenarioSet.Scenarios) != 1 || len(consumerCase.Artifacts) != 1 || consumerCase.SourceProvenance[0].Version != "3.43.0" {
		t.Fatalf("normalized case is not fully expanded: %#v", consumerCase)
	}
}

func TestNormalizedConsumerManifestDecodeFailsClosed(t *testing.T) {
	contents, err := os.ReadFile(filepath.Base(consumerNormalizedPath))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeNormalizedConsumerManifest([]byte(strings.Replace(string(contents), `"schemaVersion":`, `"unknown": true, "schemaVersion":`, 1))); err == nil {
		t.Fatal("expected unknown normalized field to fail")
	}
	if _, err := DecodeNormalizedConsumerManifest([]byte(strings.Replace(string(contents), `"schemaVersion": "2",`, `"schemaVersion": "2", "schemaVersion": "2",`, 1))); err == nil {
		t.Fatal("expected duplicate normalized key to fail")
	}
	valid, err := DecodeNormalizedConsumerManifest(contents)
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string]func(*NormalizedConsumerManifest){
		"unsafe case ID": func(manifest *NormalizedConsumerManifest) {
			manifest.Cases[0].ID = "../case"
		},
		"duplicate case ID": func(manifest *NormalizedConsumerManifest) {
			manifest.Cases = append(manifest.Cases, manifest.Cases[0])
		},
		"unknown adapter": func(manifest *NormalizedConsumerManifest) {
			manifest.Cases[0].Executions[0].RunnerAdapter.ID = "unknown-adapter"
		},
		"unknown usage": func(manifest *NormalizedConsumerManifest) {
			manifest.Cases[0].Artifacts[0].Usage = "guessed-artifact"
		},
		"bad digest": func(manifest *NormalizedConsumerManifest) {
			manifest.Cases[0].Artifacts[0].SHA256 = "bad"
		},
		"invalid artifact URI": func(manifest *NormalizedConsumerManifest) {
			for index := range manifest.Cases {
				if manifest.Cases[index].Family == "python" {
					manifest.Cases[index].Artifacts[0].URI = "file:///tmp/client.whl"
					return
				}
			}
		},
		"role usage mismatch": func(manifest *NormalizedConsumerManifest) {
			manifest.Cases[0].Artifacts[0].Role = "execution"
		},
		"known artifact outside adapter": func(manifest *NormalizedConsumerManifest) {
			artifact := manifest.Cases[0].Artifacts[0]
			artifact.ID = "extra-runtime"
			artifact.Role = "execution"
			artifact.Usage = "spark-runtime"
			artifact.URI = "https://example.invalid/runtime.tar.gz"
			manifest.Cases[0].Artifacts = append(manifest.Cases[0].Artifacts, artifact)
		},
		"adapter requirement drift": func(manifest *NormalizedConsumerManifest) {
			manifest.Cases[0].Executions[0].RunnerAdapter.RequiredArtifactUsages = []string{"spark-runtime"}
		},
		"duplicate execution ID": func(manifest *NormalizedConsumerManifest) {
			manifest.Cases[0].Executions = append(manifest.Cases[0].Executions, manifest.Cases[0].Executions[0])
		},
		"mutable source provenance": func(manifest *NormalizedConsumerManifest) {
			manifest.Cases[0].SourceProvenance[0].URI = "https://github.com/example/project/tree/main"
		},
		"runtime version drift": func(manifest *NormalizedConsumerManifest) {
			manifest.Cases[0].RuntimeProfile.Versions["unexpected"] = "1"
		},
		"Scala binary version drift": func(manifest *NormalizedConsumerManifest) {
			for index := range manifest.Cases {
				if manifest.Cases[index].Family == "spark" {
					manifest.Cases[index].RuntimeProfile.Versions["scalaBinary"] = "2.13"
					return
				}
			}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			encoded, err := json.Marshal(valid)
			if err != nil {
				t.Fatal(err)
			}
			var mutated NormalizedConsumerManifest
			if err := json.Unmarshal(encoded, &mutated); err != nil {
				t.Fatal(err)
			}
			mutate(&mutated)
			encoded, err = json.Marshal(mutated)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeNormalizedConsumerManifest(encoded); err == nil {
				t.Fatal("expected normalized decode error")
			}
		})
	}
}

func TestAddingOneConnectorPatchCaseYAMLAutoAddsContractAndAuthMatrixRows(t *testing.T) {
	repositoryRoot := filepath.Clean("../../..")
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
	base, err := os.ReadFile(filepath.Join(repositoryRoot, consumerCasesDirectory, "spark-pyspark-3.5.8-connector-0.44.2.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	patchCase := strings.ReplaceAll(string(base), "0.44.2", "0.44.3")
	if err := os.WriteFile(filepath.Join(temporaryRoot, consumerCasesDirectory, "spark-pyspark-3.5.8-connector-0.44.3.yaml"), []byte(patchCase), 0o600); err != nil {
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
	matrix, err := ConsumerMatrix(temporaryRoot, "spark", "required", "public")
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Include []ConsumerMatrixRow `json:"include"`
	}
	if err := json.Unmarshal(matrix, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Include) != 3 {
		t.Fatalf("matrix rows = %#v", payload.Include)
	}
	var patchRow *ConsumerMatrixRow
	for index := range payload.Include {
		if payload.Include[index].ID == "spark-pyspark-3.5.8-connector-0.44.3" {
			patchRow = &payload.Include[index]
		}
	}
	if patchRow == nil || patchRow.ExecutionID != "public" || patchRow.RunnerAdapter.ID != "spark-pyspark-pytest-v1" || patchRow.RuntimeProfile.Versions["connector"] != "0.44.3" || !strings.Contains(patchRow.Artifacts[0].URI, "/0.44.3/") || patchRow.SourceProvenance[0].Version != "0.44.3" {
		t.Fatalf("patch matrix row = %#v", patchRow)
	}
	loadMatrix, err := ConsumerMatrix(temporaryRoot, "spark", "required", "indirect-load")
	if err != nil {
		t.Fatal(err)
	}
	var loadPayload struct {
		Include []ConsumerMatrixRow `json:"include"`
	}
	if err := json.Unmarshal(loadMatrix, &loadPayload); err != nil {
		t.Fatal(err)
	}
	if len(loadPayload.Include) != 3 {
		t.Fatalf("load matrix rows = %#v", loadPayload.Include)
	}
	foundLoadPatch := false
	for _, row := range loadPayload.Include {
		if row.ID == patchRow.ID {
			foundLoadPatch = row.ExecutionID == "indirect-load" &&
				row.RunnerAdapter.ID == "spark-pyspark-indirect-load-v1" &&
				row.RuntimeProfile.Versions["connector"] == patchRow.RuntimeProfile.Versions["connector"] &&
				row.Artifacts[0].SHA256 == patchRow.Artifacts[0].SHA256
		}
	}
	if !foundLoadPatch {
		t.Fatalf("load matrix does not reuse the connector patch case: %#v", loadPayload.Include)
	}
}

func TestConsumerMatrixSeparatesRequiredPreviewAndNightlyLanes(t *testing.T) {
	root := t.TempDir()
	repositoryContents, err := os.ReadFile(filepath.Base(consumerNormalizedPath))
	if err != nil {
		t.Fatal(err)
	}
	repositoryManifest, err := DecodeNormalizedConsumerManifest(repositoryContents)
	if err != nil {
		t.Fatal(err)
	}
	var base ExpandedConsumerCase
	for _, consumerCase := range repositoryManifest.Cases {
		if consumerCase.Family == "spark" {
			base = consumerCase
			break
		}
	}
	if base.ID == "" {
		t.Fatal("normalized manifest has no Spark case")
	}
	nightly, preview, required := base, base, base
	nightly.ID, nightly.Lane = "nightly-case", "nightly"
	preview.ID, preview.Lane = "preview-case", "preview"
	required.ID, required.Lane = "required-case", "required"
	manifest := NormalizedConsumerManifest{SchemaVersion: consumerSchemaVersion, Cases: []ExpandedConsumerCase{
		nightly,
		preview,
		required,
	}}
	contents, err := MarshalNormalizedConsumerManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, consumerNormalizedPath)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, lane := range []string{"required", "preview", "nightly"} {
		t.Run(lane, func(t *testing.T) {
			matrix, err := ConsumerMatrix(root, "spark", lane, "public")
			if err != nil {
				t.Fatal(err)
			}
			var payload struct {
				Include []ConsumerMatrixRow `json:"include"`
			}
			if err := json.Unmarshal(matrix, &payload); err != nil {
				t.Fatal(err)
			}
			if len(payload.Include) != 1 || payload.Include[0].Lane != lane {
				t.Fatalf("lane %s matrix = %#v", lane, payload.Include)
			}
		})
	}
	matrix, count, err := ConsumerMatrixWithCount(root, "python", "preview,nightly", "public")
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 || string(matrix) != `{"include":[]}` {
		t.Fatalf("empty non-required matrix = %s count=%d", matrix, count)
	}
	if _, _, err := ConsumerMatrixWithCount(root, "spark", "preview,unknown", "public"); err == nil {
		t.Fatal("expected unknown lane to fail")
	}
	if _, _, err := ConsumerMatrixWithCount(root, "spark", "required", "public,public"); err == nil {
		t.Fatal("expected duplicate execution filter to fail")
	}
}

func validConsumerFixture() (ConsumerManifest, []ConsumerCase, map[string]bool) {
	manifest := ConsumerManifest{
		SchemaVersion:   consumerSchemaVersion,
		RuntimeProfiles: []RuntimeProfile{{ID: "python", Family: "python", Kind: "python-pytest"}},
		RunnerAdapters: []RunnerAdapter{{
			ID: "python-pytest-v1", Family: "python", RuntimeKind: "python-pytest", SelectorPrefix: "pytest",
			RequiredVersions: []string{"python", "client"}, RequiredArtifactUsages: []string{"python-wheel"}, Bootstrap: map[string]string{},
			SetupOperationIDs: []string{"bqemu.health.ready", "bqemu.projects.create", "bqemu.projects.delete"},
		}},
		CompatibilityProfiles: []CompatibilityProfile{{ID: "python-v1", ScenarioIDs: []string{"query"}}},
		Scenarios:             []ConsumerScenario{{ID: "query", OperationIDs: []string{"bigquery.jobs.query"}, Selectors: []string{"pytest:tests/python/test_query_contract.py"}}},
		ScenarioSets:          []ScenarioSet{{ID: "query-set", ScenarioIDs: []string{"query"}}},
	}
	cases := []ConsumerCase{{
		SchemaVersion: consumerSchemaVersion, ID: "case-one", DisplayName: "Case one", Family: "python", Lane: "required",
		RuntimeProfileID: "python",
		Versions:         map[string]string{"python": "3.13", "client": "3.43.0"},
		Artifacts:        []ConsumerArtifact{{ID: "wheel", Role: "execution", Usage: "python-wheel", URI: "https://example.invalid/client.whl", SHA256: strings.Repeat("a", 64)}},
		SourceProvenance: []ConsumerSourceReference{{Name: "client", VersionKey: "client", URI: "https://github.com/example/client/tree/v3.43.0"}},
		Executions: []ConsumerExecution{{
			ID: "public", RunnerAdapterID: "python-pytest-v1", CompatibilityProfileID: "python-v1", ScenarioSetID: "query-set",
		}},
	}}
	return manifest, cases, map[string]bool{
		"bigquery.jobs.query":   true,
		"bqemu.health.ready":    true,
		"bqemu.projects.create": true,
		"bqemu.projects.delete": true,
	}
}

func validConsumerCaseYAML(id string) string {
	return `schemaVersion: "2"
id: ` + id + `
displayName: Example
family: python
lane: required
runtimeProfile: python
versions: {python: "3.13", client: "3.43.0"}
sourceProvenance:
  - name: client
    versionKey: client
    uri: https://github.com/example/client/tree/v3.43.0
artifacts:
  - id: wheel
    role: execution
    usage: python-wheel
    uri: https://example.invalid/client.whl
    sha256: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
executions:
  - id: public
    runnerAdapter: python-pytest-v1
    compatibilityProfile: python-v1
    scenarioSet: query-set
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
