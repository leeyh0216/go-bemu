package contract

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
	if len(matrices) != 1 {
		t.Fatalf("matrix count = %d, want exactly one reviewed connector version", len(matrices))
	}
	matrix := matrices[0]

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
	base := matrices[0]
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
	for _, entry := range matrices[0].Entries {
		if entry.State == MatrixVerified {
			continue
		}
		issue := matrices[0].Issues[entry.IssueRef]
		if !strings.HasPrefix(issue.URL, "https://github.com/leeyh0216/go-bemu/issues/") || strings.Join(issue.Languages, ",") != "en,ko" {
			t.Errorf("%s does not have an EN/KO issue: %#v", entry.ID, issue)
		}
	}
}

func TestSparkEvidenceMatchesCommittedBytes(t *testing.T) {
	matrices, err := SparkCapabilityMatrices()
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range matrices[0].Entries {
		for _, evidence := range entry.Evidence {
			clean := filepath.ToSlash(filepath.Clean(evidence.Ref))
			if strings.HasPrefix(clean, "../") || (!strings.HasPrefix(clean, "tests/spark/evidence/") && clean != "tests/spark/artifacts.lock.json") {
				t.Fatalf("%s evidence escapes reviewed Spark paths: %q", entry.ID, evidence.Ref)
			}
			contents, err := os.ReadFile(filepath.Join("..", filepath.FromSlash(clean)))
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
