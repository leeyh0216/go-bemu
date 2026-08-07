package contract

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
)

type sparkArtifactLock struct {
	SchemaVersion      string `json:"schemaVersion"`
	ConnectorVersion   string `json:"connectorVersion"`
	SparkVersion       string `json:"sparkVersion"`
	ScalaBinaryVersion string `json:"scalaBinaryVersion"`
	SourceCommit       string `json:"sourceCommit"`
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

func TestSparkArtifactsAreExactAndHashLocked(t *testing.T) {
	contents, err := os.ReadFile("../tests/spark/artifacts.lock.json")
	if err != nil {
		t.Fatal(err)
	}
	var lock sparkArtifactLock
	decoder := json.NewDecoder(strings.NewReader(string(contents)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&lock); err != nil {
		t.Fatal(err)
	}
	if lock.SchemaVersion != "1" || lock.ConnectorVersion != "0.44.2" ||
		lock.SparkVersion != "3.5.8" || lock.ScalaBinaryVersion != "2.12" ||
		lock.SourceCommit != sparkConnector0442Commit {
		t.Fatalf("unreviewed Spark artifact binding: %#v", lock)
	}
	if len(lock.Artifacts) != 1 || len(lock.PythonArtifacts) != 2 {
		t.Fatalf("unexpected artifact inventory: maven=%d python=%d", len(lock.Artifacts), len(lock.PythonArtifacts))
	}
	jar := lock.Artifacts[0]
	if jar.Kind != "maven-jar" || jar.Size <= 0 ||
		!strings.HasPrefix(jar.URL, "https://repo.maven.apache.org/maven2/com/google/cloud/spark/") ||
		!strings.Contains(jar.URL, "/0.44.2/") || !strings.HasSuffix(jar.URL, "-0.44.2.jar") ||
		!regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(jar.SHA256) {
		t.Fatalf("connector artifact is mutable or incomplete: %#v", jar)
	}
	matrices, err := SparkCapabilityMatrices()
	if err != nil {
		t.Fatal(err)
	}
	matrixArtifact := matrices[0].Sources["scala-2.12-maven-jar"]
	if matrixArtifact.URL != jar.URL || matrixArtifact.Fingerprint != "sha256:"+jar.SHA256 {
		t.Fatalf("matrix and process lock select different connector artifacts: matrix=%#v lock=%#v", matrixArtifact, jar)
	}
	wantPython := map[string]string{"pyspark": "3.5.8", "py4j": "0.10.9.7"}
	requirements, err := os.ReadFile("../tests/spark/requirements.lock")
	if err != nil {
		t.Fatal(err)
	}
	for _, artifact := range lock.PythonArtifacts {
		if wantPython[artifact.ID] != artifact.Version || artifact.Size <= 0 ||
			!strings.HasPrefix(artifact.URL, "https://files.pythonhosted.org/") ||
			!regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(artifact.SHA256) {
			t.Errorf("Python artifact is mutable or incomplete: %#v", artifact)
		}
		if !strings.Contains(string(requirements), artifact.ID+" @ "+artifact.URL) ||
			!strings.Contains(string(requirements), "--hash=sha256:"+artifact.SHA256) {
			t.Errorf("Spark process requirements drifted from artifact lock: %#v", artifact)
		}
		delete(wantPython, artifact.ID)
	}
	if len(wantPython) != 0 {
		t.Fatalf("missing Python artifact locks: %v", wantPython)
	}
}
