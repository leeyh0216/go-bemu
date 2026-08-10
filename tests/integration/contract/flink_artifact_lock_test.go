package integrationcontract

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
)

type flinkArtifactLock struct {
	SchemaVersion    string `json:"schemaVersion"`
	ConnectorVersion string `json:"connectorVersion"`
	FlinkVersion     string `json:"flinkVersion"`
	Artifact         struct {
		ID     string `json:"id"`
		Kind   string `json:"kind"`
		URL    string `json:"url"`
		Output string `json:"output"`
		Size   int64  `json:"size"`
		SHA256 string `json:"sha256"`
	} `json:"artifact"`
	Source struct {
		Kind              string `json:"kind"`
		URL               string `json:"url"`
		CDCSchemaProvider string `json:"cdcSchemaProvider"`
	} `json:"source"`
}

func TestFlinkConnectorArtifactIsExactAndHashLocked(t *testing.T) {
	contents, err := os.ReadFile("../flink/artifacts.lock.json")
	if err != nil {
		t.Fatal(err)
	}
	var lock flinkArtifactLock
	decoder := json.NewDecoder(strings.NewReader(string(contents)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&lock); err != nil {
		t.Fatal(err)
	}
	if lock.SchemaVersion != "1" || lock.ConnectorVersion != "1.2.0" || lock.FlinkVersion != "1.17.1" ||
		lock.Artifact.ID != "flink-1.17-connector-bigquery-shaded" || lock.Artifact.Kind != "maven-jar" ||
		lock.Artifact.URL != "https://repo.maven.apache.org/maven2/com/google/cloud/flink/flink-1.17-connector-bigquery/1.2.0/flink-1.17-connector-bigquery-1.2.0-shaded.jar" ||
		lock.Artifact.Output != "flink-1.17-connector-bigquery-1.2.0-shaded.jar" || lock.Artifact.Size != 28955121 ||
		lock.Artifact.SHA256 != "5d5328be73505972deb867d4814915365b15e955a420369a2250aa1ac3d04699" ||
		lock.Source.Kind != "release-tag" || lock.Source.URL != "https://github.com/GoogleCloudDataproc/flink-bigquery-connector/tree/1.2.0" ||
		lock.Source.CDCSchemaProvider != "https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/1.2.0/flink-1.17-connector-bigquery/flink-connector-bigquery/src/main/java/com/google/cloud/flink/bigquery/sink/serializer/BigQueryCdcSchemaProvider.java" {
		t.Fatalf("unreviewed Flink connector artifact binding: %#v", lock)
	}
	if !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(lock.Artifact.SHA256) {
		t.Fatalf("Flink connector artifact checksum is invalid: %q", lock.Artifact.SHA256)
	}
}
