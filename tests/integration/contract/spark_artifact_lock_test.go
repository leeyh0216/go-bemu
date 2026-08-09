package integrationcontract

// Exact Maven artifact source:
// https://repo.maven.apache.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.44.2/

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
)

const supportedSparkConnectorCommit = "719817782a214b8ca72be520870013a3e0253d92"

type sparkArtifactLock struct {
	SchemaVersion      string `json:"schemaVersion"`
	ConnectorVersion   string `json:"connectorVersion"`
	SparkVersion       string `json:"sparkVersion"`
	ScalaBinaryVersion string `json:"scalaBinaryVersion"`
	SourceCommit       string `json:"sourceCommit"`
	ArtifactVariant    string `json:"artifactVariant,omitempty"`
	Artifacts          []struct {
		ID     string `json:"id"`
		Kind   string `json:"kind"`
		URL    string `json:"url"`
		Output string `json:"output"`
		Size   int64  `json:"size"`
		SHA256 string `json:"sha256"`
	} `json:"artifacts"`
	PythonArtifacts []struct {
		ID      string `json:"id"`
		Version string `json:"version"`
		URL     string `json:"url"`
		Size    int64  `json:"size"`
		SHA256  string `json:"sha256"`
	} `json:"pythonArtifacts"`
}

func readSparkArtifactLock(t *testing.T, path string) sparkArtifactLock {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var lock sparkArtifactLock
	decoder := json.NewDecoder(strings.NewReader(string(contents)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&lock); err != nil {
		t.Fatal(err)
	}
	return lock
}

func validateSparkArtifactLock(lock sparkArtifactLock) error {
	if lock.SchemaVersion != "1" || lock.ConnectorVersion != "0.44.2" ||
		lock.SparkVersion != "3.5.8" || lock.ScalaBinaryVersion != "2.12" ||
		lock.SourceCommit != supportedSparkConnectorCommit || lock.ArtifactVariant != "" {
		return fmt.Errorf("unreviewed Spark artifact binding: schema=%s variant=%s", lock.SchemaVersion, lock.ArtifactVariant)
	}
	return nil
}

func TestSparkArtifactIsExactAndHashLocked(t *testing.T) {
	lock := readSparkArtifactLock(t, "../spark/artifacts.lock.json")
	if err := validateSparkArtifactLock(lock); err != nil {
		t.Fatal(err)
	}
	if len(lock.Artifacts) != 1 || len(lock.PythonArtifacts) != 2 {
		t.Fatalf("unexpected artifact inventory: connector=%d python=%d", len(lock.Artifacts), len(lock.PythonArtifacts))
	}
	artifact := lock.Artifacts[0]
	if artifact.ID != "spark-bigquery-connector" || artifact.Kind != "maven-jar" ||
		artifact.Output != "spark-bigquery-with-dependencies_2.12-0.44.2.jar" ||
		artifact.Size != 42160396 ||
		artifact.SHA256 != "516699b6ef6bd5208b16b79a8b9fcefad9903ad2f8871d99a7c7c4cd1fe7f23e" ||
		!strings.HasPrefix(artifact.URL, "https://repo.maven.apache.org/maven2/com/google/cloud/spark/") ||
		!strings.Contains(artifact.URL, "/0.44.2/") || !strings.HasSuffix(artifact.URL, "-0.44.2.jar") ||
		!regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(artifact.SHA256) {
		t.Fatalf("connector artifact is mutable, ambiguous, or incomplete: %#v", artifact)
	}

	wantPython := map[string]string{"pyspark": "3.5.8", "py4j": "0.10.9.7"}
	normalizedContents, err := os.ReadFile("consumers.normalized.json")
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := DecodeNormalizedConsumerManifest(normalizedContents)
	if err != nil {
		t.Fatal(err)
	}
	usageByID := map[string]string{"pyspark": "spark-runtime", "py4j": "spark-python-bridge"}
	for _, pythonArtifact := range lock.PythonArtifacts {
		if wantPython[pythonArtifact.ID] != pythonArtifact.Version || pythonArtifact.Size <= 0 ||
			!strings.HasPrefix(pythonArtifact.URL, "https://files.pythonhosted.org/") ||
			!regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(pythonArtifact.SHA256) {
			t.Errorf("Python artifact is mutable or incomplete: %#v", pythonArtifact)
		}
		for _, consumerCase := range normalized.Cases {
			if consumerCase.Family != "spark" {
				continue
			}
			matches := 0
			for _, caseArtifact := range consumerCase.Artifacts {
				if caseArtifact.Usage == usageByID[pythonArtifact.ID] {
					matches++
					if caseArtifact.Role != "execution" || caseArtifact.URI != pythonArtifact.URL || caseArtifact.SHA256 != pythonArtifact.SHA256 {
						t.Errorf("normalized case %s drifts from Spark artifact lock: %#v", consumerCase.ID, caseArtifact)
					}
				}
			}
			if matches != 1 {
				t.Errorf("normalized case %s has %d artifacts for usage %s", consumerCase.ID, matches, usageByID[pythonArtifact.ID])
			}
		}
		delete(wantPython, pythonArtifact.ID)
	}
	if len(wantPython) != 0 {
		t.Fatalf("missing Python artifact locks: %v", wantPython)
	}
	for _, consumerCase := range normalized.Cases {
		if consumerCase.Family != "spark" {
			continue
		}
		matches := 0
		for _, caseArtifact := range consumerCase.Artifacts {
			if caseArtifact.Usage == "spark-connector-dsv1-jar" {
				matches++
				if caseArtifact.URI != artifact.URL || caseArtifact.SHA256 != artifact.SHA256 {
					t.Errorf("normalized case %s drifts from Spark connector lock: %#v", consumerCase.ID, caseArtifact)
				}
			}
		}
		if matches != 1 {
			t.Errorf("normalized case %s has %d connector artifacts", consumerCase.ID, matches)
		}
	}
}

func TestSparkArtifactLockRejectsBindingMutations(t *testing.T) {
	lock := readSparkArtifactLock(t, "../spark/artifacts.lock.json")
	for _, test := range []struct {
		name   string
		mutate func(*sparkArtifactLock)
	}{
		{"schema", func(lock *sparkArtifactLock) { lock.SchemaVersion = "2" }},
		{"connector-version", func(lock *sparkArtifactLock) { lock.ConnectorVersion = "0.44.3" }},
		{"spark-version", func(lock *sparkArtifactLock) { lock.SparkVersion = "3.5.0" }},
		{"variant", func(lock *sparkArtifactLock) { lock.ArtifactVariant = "unreviewed" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			mutated := lock
			test.mutate(&mutated)
			if err := validateSparkArtifactLock(mutated); err == nil {
				t.Fatal("artifact binding mutation was accepted")
			}
		})
	}
}
