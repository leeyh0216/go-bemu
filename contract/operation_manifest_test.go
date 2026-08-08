package contract

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/contractspec"
)

func TestOperationManifestRejectsUnknownDuplicateAndUnclassifiedInput(t *testing.T) {
	valid := validOperationManifestYAML()
	tests := map[string]string{
		"unknown field":        strings.Replace(valid, "    support: implemented\n", "    support: implemented\n    mystery: true\n", 1),
		"unknown support":      strings.Replace(valid, "support: implemented", "support: basic", 1),
		"unknown verification": strings.Replace(valid, "verification: transport", "verification: smoke", 1),
		"unknown limitation":   strings.Replace(valid, "policy: none", "policy: prose", 1),
		"tracked without issue": strings.Replace(
			valid, "policy: none", "policy: tracked", 1,
		),
		"partial without limitation": strings.Replace(valid, "support: implemented", "support: partial", 1),
		"unknown by-design scope": strings.Replace(
			valid,
			"policy: none\n      byDesign: []",
			"policy: by-design\n      byDesign: [local-only]",
			1,
		),
		"by-design with issue": strings.Replace(
			strings.Replace(
				valid,
				"policy: none\n      byDesign: []",
				"policy: by-design\n      byDesign: [google-iam]",
				1,
			),
			"issues: []",
			"issues: ['#1']",
			1,
		),
		"mixed without issue": strings.Replace(
			valid,
			"policy: none\n      byDesign: []",
			"policy: mixed\n      byDesign: [google-control-plane]",
			1,
		),
		"unknown condition": strings.Replace(
			valid,
			"    conditions: []",
			"    conditions: [{setting: query.enabled, equals: true, effect: input-branch, en: Enabled., ko: 활성화합니다.}]",
			1,
		),
		"condition without evidence": strings.Replace(
			valid,
			"    conditions: []",
			"    conditions: [{setting: load.enabled, equals: true, effect: input-branch, verification: transport, tests: [], en: Enabled., ko: 활성화합니다.}]",
			1,
		),
		"condition missing verification": strings.Replace(
			valid,
			"    conditions: []",
			"    conditions: [{setting: load.enabled, equals: true, effect: input-branch, tests: [go:internal/transport/rest/example:TestOperation], en: Enabled., ko: 활성화합니다.}]",
			1,
		),
		"condition unknown verification": strings.Replace(
			valid,
			"    conditions: []",
			"    conditions: [{setting: load.enabled, equals: true, effect: input-branch, verification: smoke, tests: [go:internal/transport/rest/example:TestOperation], en: Enabled., ko: 활성화합니다.}]",
			1,
		),
		"condition none verification": strings.Replace(
			valid,
			"    conditions: []",
			"    conditions: [{setting: load.enabled, equals: true, effect: input-branch, verification: none, tests: [go:internal/transport/rest/example:TestOperation], en: Enabled., ko: 활성화합니다.}]",
			1,
		),
		"missing test":       strings.Replace(valid, "tests: [go:internal/transport/rest/example:TestOperation]", "tests: []", 1),
		"duplicate id":       valid + strings.TrimPrefix(valid, strings.SplitN(valid, "operations:\n", 2)[0]+"operations:\n"),
		"multiple documents": valid + "---\n{}\n",
	}
	for name, contents := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeOperationManifest([]byte(contents)); err == nil {
				t.Fatal("expected strict manifest validation error")
			}
		})
	}
}

func TestVerificationRequiresPrimaryLevelAndAllowsSupplementalTests(t *testing.T) {
	tests := []string{
		"python:tests/python/test_sample.py:test_public_process",
		"go:internal/transport/rest/sample:TestTransportSupplement",
	}
	if err := validateVerificationTests(VerificationPublicProcess, tests); err != nil {
		t.Fatalf("public process with transport supplement = %v", err)
	}
	if err := validateVerificationTests(VerificationPublicProcess, tests[1:]); err == nil || !strings.Contains(err.Error(), "at least one") {
		t.Fatalf("transport-only public verification error = %v", err)
	}
}

func TestOperationManifestRejectsDuplicateRESTShape(t *testing.T) {
	manifest, err := DecodeOperationManifest([]byte(validOperationManifestYAML()))
	if err != nil {
		t.Fatal(err)
	}
	duplicate := cloneOperation(manifest.Operations[0])
	duplicate.ID = "example.rest.other"
	manifest.Operations = append(manifest.Operations, duplicate)
	if err := ValidateOperationManifest(manifest); err == nil || !strings.Contains(err.Error(), "REST shape duplicates") {
		t.Fatalf("duplicate REST shape error = %v", err)
	}
}

func TestNormalizedOperationManifestIsDeterministic(t *testing.T) {
	manifest, err := DecodeOperationManifest([]byte(validOperationManifestYAML()))
	if err != nil {
		t.Fatal(err)
	}
	first, err := MarshalNormalizedOperationManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	second, err := MarshalNormalizedOperationManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) || !strings.HasSuffix(string(first), "\n") {
		t.Fatal("normalized manifest output is not deterministic")
	}
}

func TestOperationAnnotationsRejectUnknownAndMissingLinks(t *testing.T) {
	root := t.TempDir()
	source := `package sample
import (
  "testing"
  "github.com/leeyh0216/go-bemu/internal/contracttest"
)
func TestAnnotated(t *testing.T) { contracttest.Operation(t, "example.unknown") }
`
	if err := os.WriteFile(filepath.Join(root, "sample_test.go"), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest := OperationManifest{Operations: []Operation{{ID: "example.known", Tests: []string{"go:sample:TestAnnotated"}}}}
	if err := ValidateOperationAnnotations(root, manifest); err == nil || !strings.Contains(err.Error(), "unknown operation") {
		t.Fatalf("unknown annotation error = %v", err)
	}

	source = strings.Replace(source, "example.unknown", "example.known", 1)
	if err := os.WriteFile(filepath.Join(root, "sample_test.go"), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest.Operations[0].Tests = []string{"go:sample:TestAnother"}
	if err := ValidateOperationAnnotations(root, manifest); err == nil || !strings.Contains(err.Error(), "missing from manifest tests") {
		t.Fatalf("missing manifest link error = %v", err)
	}
}

func TestPythonOperationMarkerCarriesOnlyOperationID(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "tests", "python")
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	source := "import pytest\n\n@pytest.mark.operation(\"example.python.get\")\ndef test_public_process():\n    pass\n"
	if err := os.WriteFile(filepath.Join(directory, "test_sample.py"), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest := OperationManifest{Operations: []Operation{{
		ID: "example.python.get", Verification: VerificationPublicProcess,
		Tests: []string{"python:tests/python/test_sample.py:test_public_process"},
	}}}
	if err := ValidateOperationAnnotations(root, manifest); err != nil {
		t.Fatal(err)
	}
}

func TestBQOperationMarkerCarriesOnlyCanonicalOperationID(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "tests", "bqcli")
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	source := "from operation_contract import operation\n\n@operation(\"example.bq.get\")\ndef run_contract():\n    pass\n"
	path := filepath.Join(directory, "runner.py")
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest := OperationManifest{Operations: []Operation{{
		ID: "example.bq.get", Verification: VerificationPublicProcess,
		Tests: []string{"bq:tests/bqcli/runner.py:run_contract"},
	}}}
	if err := ValidateOperationAnnotations(root, manifest); err != nil {
		t.Fatal(err)
	}
	unknown := strings.Replace(source, "example.bq.get", "example.bq.unknown", 1)
	if err := os.WriteFile(path, []byte(unknown), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ValidateOperationAnnotations(root, manifest); err == nil || !strings.Contains(err.Error(), "unknown operation") {
		t.Fatalf("unknown bq marker error = %v", err)
	}
	malformed := strings.Replace(source, `@operation("example.bq.get")`, `@operation("example.bq.get", "metadata")`, 1)
	if err := os.WriteFile(path, []byte(malformed), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ValidateOperationAnnotations(root, manifest); err == nil || !strings.Contains(err.Error(), "only one literal") {
		t.Fatalf("malformed bq marker error = %v", err)
	}
}

func TestGeneratedOperationArtifactsAreCurrent(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckOperationArtifacts(root); err != nil {
		t.Fatal(err)
	}
}

func TestOperationRegistryReturnsDefensiveCopies(t *testing.T) {
	routes := contractspec.RESTRoutes()
	if len(routes) == 0 {
		t.Fatal("generated operation registry is empty")
	}
	original := routes[0]
	routes[0].OperationID = "changed"
	routes[0].Path = "/changed"
	loaded, ok := contractspec.RESTRoute(original.OperationID)
	if !ok || loaded != original {
		t.Fatalf("operation registry was mutated through a returned value: %#v", loaded)
	}
}

func TestRuntimePackagesDoNotImportContractTooling(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	for _, relativeRoot := range []string{"cmd/emulator", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, relativeRoot), func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			for _, importSpec := range file.Imports {
				importPath, err := strconv.Unquote(importSpec.Path.Value)
				if err != nil {
					return err
				}
				if importPath == "github.com/leeyh0216/go-bemu/contract" {
					t.Errorf("runtime package %s imports contract compiler and test tooling", filepath.ToSlash(path))
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestRuntimeContractSpecPackagesContainOnlyGeneratedGo(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	for _, directory := range []string{"internal/capabilityspec", "internal/contractspec"} {
		entries, err := os.ReadDir(filepath.Join(root, directory))
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if !entry.IsDir() && filepath.Ext(entry.Name()) == ".go" && !strings.HasSuffix(entry.Name(), "_gen.go") {
				t.Errorf("runtime contract package contains handwritten Go file: %s/%s", directory, entry.Name())
			}
		}
	}
}

func validOperationManifestYAML() string {
	return `schemaVersion: 1
sources:
  - id: official
    url: https://example.com/reference
operations:
  - id: example.rest.get
    protocol: rest
    component: public-core
    summary:
      en: Get an example.
      ko: 예시를 조회합니다.
    support: implemented
    verification: transport
    rest:
      method: GET
      path: /example
      discovery: false
    supportedInput:
      en: No input.
      ko: 입력이 없습니다.
    conditions: []
    limitations:
      policy: none
      byDesign: []
      en: Example only.
      ko: 예시로만 사용합니다.
    issues: []
    sources: [official]
    tests: [go:internal/transport/rest/example:TestOperation]
`
}
