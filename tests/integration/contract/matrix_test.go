package integrationcontract

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestSparkCapabilityMatrixIsCompleteAndClassified(t *testing.T) {
	matrices, err := SparkCapabilityMatrices()
	if err != nil {
		t.Fatal(err)
	}
	if len(matrices) != 2 {
		t.Fatalf("matrix count = %d, want DSv1 and raw DSv2", len(matrices))
	}
	matrix := matrixByArtifactVariant(t, matrices, "dsv1-with-dependencies-2.12")

	required := map[string]map[string]bool{
		"format":            stringSet("ARROW", "AVRO", "PROTO_ROWS", "PARQUET", "ORC"),
		"execution":         stringSet("batch", "structured-streaming"),
		"delivery":          stringSet("exactly-once", "at-least-once"),
		"saveMode":          stringSet("append", "overwrite", "ignore", "error-if-exists"),
		"readShape":         stringSet("table", "projection", "filter", "count", "query", "view"),
		"tablePartitioning": stringSet("unpartitioned", "time", "ingestion-time", "integer-range", "dynamic-overwrite"),
		"parallelism":       stringSet("read-stream-1", "read-stream-2", "read-stream-4", "read-stream-16", "read-stream-negotiated", "spark-partition-1", "spark-partition-2", "spark-partition-4", "spark-partition-negotiated"),
		"typeFamily":        stringSet("boolean-integer-float", "string-bytes", "numeric-bignumeric", "temporal", "struct-array", "json", "map", "ml-vector-matrix", "geography"),
		"auth":              stringSet("static-access-token", "service-account-file", "service-account-base64", "adc-service-account", "adc-user", "wif-external-account", "impersonation", "custom-token-provider"),
	}
	for _, entry := range matrix.Entries {
		values := map[string]string{
			"format": entry.Axes.Format, "execution": entry.Axes.Execution,
			"delivery": entry.Axes.Delivery, "saveMode": entry.Axes.SaveMode,
			"readShape": entry.Axes.ReadShape, "tablePartitioning": entry.Axes.TablePartitioning,
			"parallelism": entry.Axes.Parallelism,
			"typeFamily":  entry.Axes.TypeFamily, "auth": entry.Axes.Auth,
		}
		for axis, value := range values {
			delete(required[axis], value)
		}
	}
	for axis, missing := range required {
		if len(missing) != 0 {
			t.Errorf("matrix does not enumerate %s values: %v", axis, sortedKeys(missing))
		}
	}

	assertCombination(t, matrix, "write-direct-pending", "exactly-once", "append")
	assertCombination(t, matrix, "write-direct-overwrite", "exactly-once", "overwrite")
	assertCombination(t, matrix, "write-direct-default", "at-least-once", "append")
	assertCombination(t, matrix, "write-direct-default", "at-least-once", "overwrite")
	for _, format := range []string{"PARQUET", "AVRO", "ORC"} {
		assertFormat(t, matrix, "write-indirect-load", format)
	}
	for _, format := range []string{"ARROW", "AVRO"} {
		for _, parallelism := range []string{"read-stream-1", "read-stream-2", "read-stream-4", "read-stream-16"} {
			assertFormatParallelism(t, matrix, "read-storage", format, parallelism)
		}
	}
	for _, parallelism := range []string{"spark-partition-1", "spark-partition-2", "spark-partition-4"} {
		assertFlowParallelism(t, matrix, "write-direct-pending", parallelism)
		assertFlowParallelism(t, matrix, "write-direct-default", parallelism)
	}
}

func TestSparkCapabilityMatrixRejectsDriftClasses(t *testing.T) {
	matrices, err := SparkCapabilityMatrices()
	if err != nil {
		t.Fatal(err)
	}
	base := matrixByArtifactVariant(t, matrices, "dsv1-with-dependencies-2.12")
	tests := map[string]func(*CapabilityMatrix){
		"unclassified": func(matrix *CapabilityMatrix) { matrix.Entries[0].State = "" },
		"duplicate-id": func(matrix *CapabilityMatrix) { matrix.Entries[1].ID = matrix.Entries[0].ID },
		"duplicate-axes": func(matrix *CapabilityMatrix) {
			matrix.Entries[1].Axes = matrix.Entries[0].Axes
			matrix.Entries[1].Flow = matrix.Entries[0].Flow
		},
		"mutable-source": func(matrix *CapabilityMatrix) {
			matrix.Sources["connector-readme"] = MatrixSource{Kind: "source", URL: "https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/master/README.md"}
		},
		"unknown-flow": func(matrix *CapabilityMatrix) { matrix.Entries[0].Flow = "future-unreviewed-flow" },
		"unknown-axis": func(matrix *CapabilityMatrix) { matrix.Entries[0].Axes.Format = "future-format" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			matrix := cloneCapabilityMatrix(t, base)
			mutate(&matrix)
			if err := validateCapabilityMatrix("mutation.json", matrix); err == nil {
				t.Fatalf("%s drift was accepted", name)
			}
		})
	}
}

func TestEveryNonVerifiedSparkEntryLinksBilingualIssue(t *testing.T) {
	matrices, err := SparkCapabilityMatrices()
	if err != nil {
		t.Fatal(err)
	}
	for _, matrix := range matrices {
		for _, entry := range matrix.Entries {
			if entry.State == MatrixVerified {
				continue
			}
			issue := matrix.Issues[entry.IssueRef]
			if !strings.HasPrefix(issue.URL, "https://github.com/leeyh0216/go-bemu/issues/") || strings.Join(issue.Languages, ",") != "en,ko" {
				t.Errorf("%s does not have an EN/KO issue: %#v", entry.ID, issue)
			}
		}
	}
}

func TestSparkEvidenceMatchesCommittedBytes(t *testing.T) {
	matrices, err := SparkCapabilityMatrices()
	if err != nil {
		t.Fatal(err)
	}
	for _, matrix := range matrices {
		for _, entry := range matrix.Entries {
			for _, evidence := range entry.Evidence {
				clean := filepath.ToSlash(filepath.Clean(evidence.Ref))
				allowedLock := clean == "tests/spark/artifacts.lock.json" || clean == "tests/spark/artifacts-dsv2.lock.json"
				if strings.HasPrefix(clean, "../") || (!strings.HasPrefix(clean, "tests/spark/evidence/") && !allowedLock) {
					t.Fatalf("%s evidence escapes reviewed Spark paths: %q", entry.ID, evidence.Ref)
				}
				contents, err := os.ReadFile(filepath.Join("../../..", filepath.FromSlash(clean)))
				if err != nil {
					t.Fatalf("%s evidence %s: %v", entry.ID, evidence.Ref, err)
				}
				fingerprint := fmt.Sprintf("sha256:%x", sha256.Sum256(contents))
				if fingerprint != evidence.Fingerprint {
					t.Fatalf("%s evidence drift ref=%s got=%s want=%s", entry.ID, evidence.Ref, fingerprint, evidence.Fingerprint)
				}
			}
		}
	}
}

func TestSparkArtifactVariantsRemainDistinct(t *testing.T) {
	matrices, err := SparkCapabilityMatrices()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"dsv1-with-dependencies-2.12": "spark-bigquery-with-dependencies_2.12",
		"dsv2-spark-3.5-raw":          "spark-3.5-bigquery-raw",
	}
	for _, matrix := range matrices {
		if want[matrix.ArtifactVariant] != matrix.Connector.Name {
			t.Fatalf("artifact profile drift: variant=%q consumer=%q", matrix.ArtifactVariant, matrix.Connector.Name)
		}
		delete(want, matrix.ArtifactVariant)
	}
	if len(want) != 0 {
		t.Fatalf("missing artifact variants: %v", sortedKeysString(want))
	}
}

func TestRawDSv2MatrixCoversExactAndAtLeastOnceStreaming(t *testing.T) {
	matrices, err := SparkCapabilityMatrices()
	if err != nil {
		t.Fatal(err)
	}
	raw := matrixByArtifactVariant(t, matrices, "dsv2-spark-3.5-raw")
	want := map[string]string{
		"SBQ-DSV2-RAW-STREAM-EXACT-APPEND-V1": "exactly-once",
		"SBQ-DSV2-RAW-STREAM-ALO-APPEND-V1":   "at-least-once",
	}
	guardFound := false
	for _, entry := range raw.Entries {
		if entry.Flow == "artifact-bootstrap" {
			if guardFound || entry.ID != "SBQ-DSV2-ARTIFACT-CLASSPATH-GUARD-V1" ||
				entry.State != MatrixPartial || entry.Axes.Operation != "bootstrap" ||
				entry.Axes.Transport != "local-classpath" || len(entry.Evidence) != 2 {
				t.Fatalf("raw DSv2 classpath guard row is incomplete: %#v", entry)
			}
			guardFound = true
			continue
		}
		delivery, ok := want[entry.ID]
		if !ok {
			t.Fatalf("raw DSv2 matrix contains an unreviewed row %q", entry.ID)
		}
		if entry.Flow != "write-structured-streaming" || entry.Axes.Execution != "structured-streaming" ||
			entry.Axes.Delivery != delivery || entry.Axes.SaveMode != "append" || entry.State != MatrixGap ||
			entry.IssueRef != "dsv2-streaming" || entry.Limitation == "" {
			t.Fatalf("raw DSv2 row is incomplete: %#v", entry)
		}
		if entry.ID == "SBQ-DSV2-RAW-STREAM-EXACT-APPEND-V1" && len(entry.Evidence) != 2 {
			t.Fatalf("raw DSv2 exact failure must retain trace and artifact evidence: %#v", entry.Evidence)
		}
		delete(want, entry.ID)
	}
	if !guardFound {
		t.Fatal("raw DSv2 matrix is missing the classpath guard row")
	}
	if len(want) != 0 {
		t.Fatalf("raw DSv2 matrix is missing rows: %v", sortedKeysString(want))
	}
}

func matrixByArtifactVariant(t *testing.T, matrices []CapabilityMatrix, variant string) CapabilityMatrix {
	t.Helper()
	for _, matrix := range matrices {
		if matrix.ArtifactVariant == variant {
			return matrix
		}
	}
	t.Fatalf("missing matrix artifact variant %q", variant)
	return CapabilityMatrix{}
}

func sortedKeysString(set map[string]string) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func cloneCapabilityMatrix(t *testing.T, source CapabilityMatrix) CapabilityMatrix {
	t.Helper()
	encoded, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	var clone CapabilityMatrix
	if err := json.Unmarshal(encoded, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}

func assertCombination(t *testing.T, matrix CapabilityMatrix, flow, delivery, saveMode string) {
	t.Helper()
	for _, entry := range matrix.Entries {
		if entry.Flow == flow && entry.Axes.Delivery == delivery && entry.Axes.SaveMode == saveMode {
			return
		}
	}
	t.Errorf("missing combination flow=%s delivery=%s saveMode=%s", flow, delivery, saveMode)
}

func assertFormat(t *testing.T, matrix CapabilityMatrix, flow, format string) {
	t.Helper()
	for _, entry := range matrix.Entries {
		if entry.Flow == flow && entry.Axes.Format == format {
			return
		}
	}
	t.Errorf("missing combination flow=%s format=%s", flow, format)
}

func assertFormatParallelism(t *testing.T, matrix CapabilityMatrix, flow, format, parallelism string) {
	t.Helper()
	for _, entry := range matrix.Entries {
		if entry.Flow == flow && entry.Axes.Format == format && entry.Axes.Parallelism == parallelism {
			return
		}
	}
	t.Errorf("missing combination flow=%s format=%s parallelism=%s", flow, format, parallelism)
}

func assertFlowParallelism(t *testing.T, matrix CapabilityMatrix, flow, parallelism string) {
	t.Helper()
	for _, entry := range matrix.Entries {
		if entry.Flow == flow && entry.Axes.Parallelism == parallelism {
			return
		}
	}
	t.Errorf("missing combination flow=%s parallelism=%s", flow, parallelism)
}

func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
