package integrationcontract

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompileCapabilityIndexDerivesCasesAndAPICoverage(t *testing.T) {
	root := capabilityAnnotationRoot(t)
	writeCapabilityAnnotationSource(t, root, "test_read.py", `
import pytest

@contract_case(
    "SBQ-READ-EXAMPLE-V1",
    state="partial",
    category="read",
    summary="Read one Arrow table",
    profile="spark-bigquery-connector-dsv1-0.44.2",
    wire_flow="read-arrow",
    operations=("bigquery.tables.get", "grpc.bigquery-read.create-read-session"),
    issue="https://github.com/leeyh0216/go-bemu/issues/34",
    limitation="Nested values remain bounded.",
)
def test_read():
    pass
`)
	index, err := CompileCapabilityIndex(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(index.Cases) != 1 {
		t.Fatalf("case count = %d, want 1", len(index.Cases))
	}
	capability := index.Cases[0]
	if capability.ID != "SBQ-READ-EXAMPLE-V1" || capability.State != CapabilityCasePartial ||
		len(capability.Tests) != 1 || capability.Tests[0] != "spark:tests/integration/spark/test_read.py:test_read" ||
		strings.Join(capability.OperationIDs, ",") != "bigquery.tables.get,grpc.bigquery-read.create-read-session" {
		t.Fatalf("capability = %#v", capability)
	}
	if len(index.APICoverage) != 2 || index.APICoverage[0].OperationID != "bigquery.tables.get" {
		t.Fatalf("API coverage = %#v", index.APICoverage)
	}
	if len(index.Claims) != 1 || index.Claims[0].Test != "spark:tests/integration/spark/test_read.py:test_read" ||
		strings.Join(index.Claims[0].OperationIDs, ",") != "bigquery.tables.get,grpc.bigquery-read.create-read-session" {
		t.Fatalf("claims = %#v", index.Claims)
	}
	encoded, err := MarshalCapabilityIndex(index)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCapabilityIndex(encoded)
	if err != nil || len(decoded.Cases) != 1 || len(decoded.Claims) != 1 {
		t.Fatalf("decoded index = %#v, err=%v", decoded, err)
	}
}

func TestCapabilityAnnotationsFailClosed(t *testing.T) {
	tests := map[string]struct {
		source string
		want   string
	}{
		"unknown operation": {
			source: capabilityAnnotationSource("", `state="verified", operations=("bigquery.unknown",)`),
			want:   "unknown operation",
		},
		"partial lacks limitation": {
			source: capabilityAnnotationSource("", `state="partial", issue="https://github.com/leeyh0216/go-bemu/issues/34"`),
			want:   "must declare a GitHub issue and limitation",
		},
		"gap lacks strict xfail": {
			source: capabilityAnnotationSource("", `state="gap", issue="https://github.com/leeyh0216/go-bemu/issues/34", limitation="Not implemented."`),
			want:   "must declare strict_xfail=True",
		},
		"wire flow must be reviewed": {
			source: capabilityAnnotationSource("", `state="verified", wire_flow="unreviewed"`),
			want:   "unknown profile wire_flow",
		},
		"supported case needs operation annotation": {
			source: capabilityAnnotationSource("", `state="verified", operations=()`),
			want:   "must declare at least one literal public operation",
		},
		"legacy marker is rejected": {
			source: "import pytest\n\n@pytest.mark.capability(\"SBQ-READ-EXAMPLE-V1\")\ndef test_legacy():\n    pass\n",
			want:   "retired pytest.mark.capability",
		},
		"separate spark operation marker is rejected": {
			source: capabilityAnnotationSource(`@pytest.mark.operation("bigquery.tables.get")`, `state="verified"`),
			want:   "Spark operation IDs belong in contract_case",
		},
		"dynamic metadata is rejected": {
			source: capabilityAnnotationSource("", `state=STATE`),
			want:   "contract_case state must be one literal string",
		},
		"alias is rejected": {
			source: "from conftest import contract_case\nalias = contract_case\n\n@alias(\"SBQ-READ-EXAMPLE-V1\")\ndef test_alias():\n    pass\n",
			want:   "contract_case must not be aliased",
		},
		"operations must be literal": {
			source: capabilityAnnotationSource("", `state="verified", operations=READ_OPERATIONS`),
			want:   "operations must be a literal tuple or list",
		},
		"outside decorator is rejected": {
			source: "import pytest\n\nCASE = contract_case(\"SBQ-READ-EXAMPLE-V1\")\ndef test_outside():\n    pass\n",
			want:   "must appear in a test decorator",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			root := capabilityAnnotationRoot(t)
			writeCapabilityAnnotationSource(t, root, "test_case.py", test.source)
			_, err := CompileCapabilityIndex(root)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestCapabilityAnnotationsMergeEquivalentTests(t *testing.T) {
	root := capabilityAnnotationRoot(t)
	writeCapabilityAnnotationSource(t, root, "test_one.py", capabilityAnnotationSource("", `state="verified", operations=("bigquery.tables.get",)`))
	writeCapabilityAnnotationSource(t, root, "test_two.py", capabilityAnnotationSource("", `state="verified", operations=("grpc.bigquery-read.create-read-session",)`))
	index, err := CompileCapabilityIndex(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(index.Cases) != 1 || len(index.Cases[0].Tests) != 2 {
		t.Fatalf("index = %#v", index)
	}
	if strings.Join(index.Cases[0].OperationIDs, ",") != "bigquery.tables.get,grpc.bigquery-read.create-read-session" {
		t.Fatalf("aggregate operations = %#v", index.Cases[0].OperationIDs)
	}
	if len(index.Claims) != 2 ||
		strings.Join(index.Claims[0].OperationIDs, ",") != "bigquery.tables.get" ||
		strings.Join(index.Claims[1].OperationIDs, ",") != "grpc.bigquery-read.create-read-session" {
		t.Fatalf("test-local claims = %#v", index.Claims)
	}
}

func TestCapabilityAnnotationsRejectDuplicateTestLocalClaims(t *testing.T) {
	root := capabilityAnnotationRoot(t)
	writeCapabilityAnnotationSource(t, root, "test_duplicate.py", `
import pytest

@contract_case(
    "SBQ-READ-EXAMPLE-V1",
    state="verified",
    category="read",
    summary="Read one Arrow table",
    profile="spark-bigquery-connector-dsv1-0.44.2",
    wire_flow="read-arrow",
    operations=("bigquery.tables.get",),
)
@contract_case(
    "SBQ-READ-EXAMPLE-V1",
    state="verified",
    category="read",
    summary="Read one Arrow table",
    profile="spark-bigquery-connector-dsv1-0.44.2",
    wire_flow="read-arrow",
    operations=("bigquery.tables.get",),
)
def test_duplicate():
    pass
`)
	_, err := CompileCapabilityIndex(root)
	if err == nil || !strings.Contains(err.Error(), "duplicate contract_case metadata") {
		t.Fatalf("error = %v", err)
	}
}

func TestCapabilityAnnotationsUsePythonASTForNestedParamMetadata(t *testing.T) {
	root := capabilityAnnotationRoot(t)
	writeCapabilityAnnotationSource(t, root, "test_nested.py", `
import pytest

# The extractor must ignore comments and read the nested pytest.param marker.
NOTE = 'contract_case("not-a-decorator")'
@pytest.mark.parametrize(
    "count",
    (
        pytest.param(
            1,
            marks=contract_case(
                "SBQ-READ-NESTED-PARAM-V1",
                state="verified",
                category="read",
                summary="Nested parameter metadata",
                profile="spark-bigquery-connector-dsv1-0.44.2",
                wire_flow="read-arrow",
                operations=("bigquery.tables.get",),
            ),
        ),
    ),
)
def test_nested(count):
    assert count == 1
`)
	index, err := CompileCapabilityIndex(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(index.Cases) != 1 || index.Cases[0].ID != "SBQ-READ-NESTED-PARAM-V1" ||
		strings.Join(index.Cases[0].OperationIDs, ",") != "bigquery.tables.get" {
		t.Fatalf("index = %#v", index)
	}
}

func capabilityAnnotationRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "tests", "integration", "spark"), 0o755); err != nil {
		t.Fatal(err)
	}
	extractor, err := os.ReadFile("extract_integration_annotations.py")
	if err != nil {
		t.Fatal(err)
	}
	extractorPath := filepath.Join(root, "tests", "integration", "contract", "extract_integration_annotations.py")
	if err := os.MkdirAll(filepath.Dir(extractorPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(extractorPath, extractor, 0o600); err != nil {
		t.Fatal(err)
	}
	operations, err := os.ReadFile(filepath.Join("..", "..", "..", "contract", "operations.normalized.json"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "contract", "operations.normalized.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, operations, 0o600); err != nil {
		t.Fatal(err)
	}
	return root
}

func writeCapabilityAnnotationSource(t *testing.T, root, name, source string) {
	t.Helper()
	path := filepath.Join(root, "tests", "integration", "spark", name)
	if err := os.WriteFile(path, []byte(strings.TrimSpace(source)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func capabilityAnnotationSource(operationDecorator, metadata string) string {
	wireFlow := "wire_flow=\"read-arrow\","
	if strings.Contains(metadata, "wire_flow=") {
		wireFlow = ""
	}
	operations := "operations=(\"bigquery.tables.get\",),"
	if strings.Contains(metadata, "operations=") {
		operations = ""
	}
	return `
import pytest

@contract_case(
    "SBQ-READ-EXAMPLE-V1",
    ` + metadata + `,
    category="read",
    summary="Read one Arrow table",
    profile="spark-bigquery-connector-dsv1-0.44.2",
    ` + wireFlow + `
    ` + operations + `
)
` + operationDecorator + `
def test_case():
    pass
`
}
