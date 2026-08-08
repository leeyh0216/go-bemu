package integrationcontract

// Exact Maven artifact sources:
//   - DSv1: https://repo.maven.apache.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.44.2/
//   - DSv2: https://repo.maven.apache.org/maven2/com/google/cloud/spark/spark-3.5-bigquery/0.44.2/

import (
	"encoding/json"
	"fmt"
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
	ArtifactVariant    string `json:"artifactVariant,omitempty"`
	ArtifactBuild      struct {
		SparkVersion       string `json:"sparkVersion"`
		ScalaBinaryVersion string `json:"scalaBinaryVersion"`
		JavaToolchain      string `json:"javaToolchain"`
		Source             struct {
			Kind   string `json:"kind"`
			URL    string `json:"url"`
			Output string `json:"output"`
			Size   int64  `json:"size"`
			SHA256 string `json:"sha256"`
		} `json:"source"`
	} `json:"artifactBuild,omitempty"`
	TestRuntime struct {
		SparkVersion       string `json:"sparkVersion"`
		ScalaBinaryVersion string `json:"scalaBinaryVersion"`
		ScalaVersion       string `json:"scalaVersion"`
		JavaVersion        string `json:"javaVersion"`
	} `json:"testRuntime,omitempty"`
	ExecutionClasspathPolicy string `json:"executionClasspathPolicy,omitempty"`
	Artifacts                []struct {
		ID                string `json:"id"`
		DataSourceVersion string `json:"dataSourceVersion,omitempty"`
		ProviderClass     string `json:"providerClass,omitempty"`
		Kind              string `json:"kind"`
		URL               string `json:"url"`
		Output            string `json:"output"`
		Size              int64  `json:"size"`
		SHA256            string `json:"sha256"`
	} `json:"artifacts"`
	PythonArtifacts []struct {
		ID      string `json:"id"`
		Version string `json:"version"`
		URL     string `json:"url"`
		Size    int64  `json:"size"`
		SHA256  string `json:"sha256"`
	} `json:"pythonArtifacts,omitempty"`
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

func TestSparkArtifactsAreExactAndHashLocked(t *testing.T) {
	dsv1 := readSparkArtifactLock(t, "../../spark/artifacts.lock.json")
	dsv2 := readSparkArtifactLock(t, "../../spark/artifacts-dsv2.lock.json")
	if err := validateDSv1ArtifactLock(dsv1); err != nil {
		t.Fatal(err)
	}
	if err := validateDSv2ArtifactLock(dsv2); err != nil {
		t.Fatal(err)
	}
	if len(dsv1.Artifacts) != 1 || len(dsv1.PythonArtifacts) != 2 ||
		len(dsv2.Artifacts) != 1 || len(dsv2.PythonArtifacts) != 0 {
		t.Fatalf(
			"unexpected artifact inventory: dsv1=%d dsv2=%d python=%d",
			len(dsv1.Artifacts), len(dsv2.Artifacts), len(dsv1.PythonArtifacts),
		)
	}

	type exactArtifact struct {
		id, variant, dataSourceVersion, provider, output, sha256 string
		size                                                     int64
	}
	expected := []exactArtifact{
		{
			id: "spark-bigquery-connector", variant: "dsv1-with-dependencies-2.12",
			dataSourceVersion: "", provider: "",
			output: "spark-bigquery-with-dependencies_2.12-0.44.2.jar", size: 42160396,
			sha256: "516699b6ef6bd5208b16b79a8b9fcefad9903ad2f8871d99a7c7c4cd1fe7f23e",
		},
		{
			id: "spark-bigquery-connector-dsv2-raw", variant: "dsv2-spark-3.5-raw",
			dataSourceVersion: "V2",
			provider:          "com.google.cloud.spark.bigquery.v2.Spark35BigQueryTableProvider",
			output:            "spark-3.5-bigquery-0.44.2.jar", size: 42618495,
			sha256: "2e6bbb41bcaf56ae17a5488dd4453698bd35f13e9849f4daed744ca7b57b053f",
		},
	}
	locks := []sparkArtifactLock{dsv1, dsv2}
	for index, lock := range locks {
		artifact, want := lock.Artifacts[0], expected[index]
		variant := lock.ArtifactVariant
		if variant == "" {
			variant = "dsv1-with-dependencies-2.12"
		}
		if artifact.ID != want.id || variant != want.variant ||
			artifact.DataSourceVersion != want.dataSourceVersion || artifact.ProviderClass != want.provider ||
			artifact.Output != want.output || artifact.Size != want.size || artifact.SHA256 != want.sha256 ||
			artifact.Kind != "maven-jar" ||
			!strings.HasPrefix(artifact.URL, "https://repo.maven.apache.org/maven2/com/google/cloud/spark/") ||
			!strings.Contains(artifact.URL, "/0.44.2/") || !strings.HasSuffix(artifact.URL, "-0.44.2.jar") ||
			!regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(artifact.SHA256) {
			t.Fatalf("connector artifact is mutable, ambiguous, or incomplete: %#v", artifact)
		}
	}

	matrices, err := SparkCapabilityMatrices()
	if err != nil {
		t.Fatal(err)
	}
	for _, matrix := range matrices {
		var artifact sparkArtifactLock
		var sourceID string
		switch matrix.ArtifactVariant {
		case "dsv1-with-dependencies-2.12":
			artifact, sourceID = dsv1, "scala-2.12-maven-jar"
		case "dsv2-spark-3.5-raw":
			artifact, sourceID = dsv2, "spark-3.5-maven-jar"
		default:
			t.Fatalf("matrix has unreviewed artifact variant %q", matrix.ArtifactVariant)
		}
		jar, matrixArtifact := artifact.Artifacts[0], matrix.Sources[sourceID]
		if matrixArtifact.URL != jar.URL || matrixArtifact.Fingerprint != "sha256:"+jar.SHA256 {
			t.Fatalf("matrix and lock select different artifacts: variant=%s matrix=%#v", matrix.ArtifactVariant, matrixArtifact)
		}
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
	for _, artifact := range dsv1.PythonArtifacts {
		if wantPython[artifact.ID] != artifact.Version || artifact.Size <= 0 ||
			!strings.HasPrefix(artifact.URL, "https://files.pythonhosted.org/") ||
			!regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(artifact.SHA256) {
			t.Errorf("Python artifact is mutable or incomplete: %#v", artifact)
		}
		for _, consumerCase := range normalized.Cases {
			if consumerCase.Family != "spark" {
				continue
			}
			matches := 0
			for _, caseArtifact := range consumerCase.Artifacts {
				if caseArtifact.Usage == usageByID[artifact.ID] {
					matches++
					if caseArtifact.Role != "execution" || caseArtifact.URI != artifact.URL || caseArtifact.SHA256 != artifact.SHA256 {
						t.Errorf("normalized case %s drifts from Spark artifact lock: %#v", consumerCase.ID, caseArtifact)
					}
				}
			}
			if matches != 1 {
				t.Errorf("normalized case %s has %d artifacts for usage %s", consumerCase.ID, matches, usageByID[artifact.ID])
			}
		}
		delete(wantPython, artifact.ID)
	}
	if len(wantPython) != 0 {
		t.Fatalf("missing Python artifact locks: %v", wantPython)
	}
}

func validateDSv1ArtifactLock(lock sparkArtifactLock) error {
	if lock.SchemaVersion != "1" || lock.ConnectorVersion != "0.44.2" ||
		lock.SparkVersion != "3.5.8" || lock.ScalaBinaryVersion != "2.12" ||
		lock.SourceCommit != sparkConnector0442Commit || lock.ArtifactVariant != "" ||
		lock.ArtifactBuild.SparkVersion != "" || lock.ArtifactBuild.ScalaBinaryVersion != "" ||
		lock.ArtifactBuild.JavaToolchain != "" || lock.ArtifactBuild.Source.Kind != "" ||
		lock.ArtifactBuild.Source.URL != "" || lock.ArtifactBuild.Source.Output != "" ||
		lock.ArtifactBuild.Source.Size != 0 || lock.ArtifactBuild.Source.SHA256 != "" ||
		lock.TestRuntime.SparkVersion != "" || lock.TestRuntime.ScalaBinaryVersion != "" ||
		lock.TestRuntime.ScalaVersion != "" || lock.TestRuntime.JavaVersion != "" ||
		lock.ExecutionClasspathPolicy != "" {
		return fmt.Errorf("unreviewed DSv1 artifact binding: schema=%s variant=%s", lock.SchemaVersion, lock.ArtifactVariant)
	}
	return nil
}

func validateDSv2ArtifactLock(lock sparkArtifactLock) error {
	if lock.SchemaVersion != "2" || lock.ConnectorVersion != "0.44.2" ||
		lock.SparkVersion != "" || lock.ScalaBinaryVersion != "" ||
		lock.SourceCommit != sparkConnector0442Commit || lock.ArtifactVariant != "dsv2-spark-3.5-raw" ||
		lock.ArtifactBuild.SparkVersion != "3.5.0" || lock.ArtifactBuild.ScalaBinaryVersion != "2.13" ||
		lock.ArtifactBuild.JavaToolchain != "[11,12)" ||
		lock.ArtifactBuild.Source.Kind != "maven-pom" ||
		lock.ArtifactBuild.Source.URL != "https://repo.maven.apache.org/maven2/com/google/cloud/spark/spark-3.5-bigquery/0.44.2/spark-3.5-bigquery-0.44.2.pom" ||
		lock.ArtifactBuild.Source.Output != "spark-3.5-bigquery-0.44.2.pom" ||
		lock.ArtifactBuild.Source.Size != 4572 ||
		lock.ArtifactBuild.Source.SHA256 != "70a849546be6e7aa2daf05669db4db44672cccb3f383881114c4a63dd0f18238" ||
		lock.TestRuntime.SparkVersion != "3.5.8" || lock.TestRuntime.ScalaBinaryVersion != "2.12" ||
		lock.TestRuntime.ScalaVersion != "2.12.18" || lock.TestRuntime.JavaVersion != "17" ||
		lock.ExecutionClasspathPolicy != "exactly-one-connector-variant" || len(lock.PythonArtifacts) != 0 {
		return fmt.Errorf(
			"unreviewed DSv2 build/runtime binding: schema=%s variant=%s build=%s/%s runtime=%s/%s/%s",
			lock.SchemaVersion, lock.ArtifactVariant,
			lock.ArtifactBuild.SparkVersion, lock.ArtifactBuild.ScalaBinaryVersion,
			lock.TestRuntime.SparkVersion, lock.TestRuntime.ScalaBinaryVersion, lock.TestRuntime.ScalaVersion,
		)
	}
	return nil
}

func TestSparkArtifactLocksRejectCrossSchemaMutations(t *testing.T) {
	dsv1 := readSparkArtifactLock(t, "../../spark/artifacts.lock.json")
	dsv2 := readSparkArtifactLock(t, "../../spark/artifacts-dsv2.lock.json")
	tests := []struct {
		name     string
		lock     sparkArtifactLock
		mutate   func(*sparkArtifactLock)
		validate func(sparkArtifactLock) error
	}{
		{"v1-v2-variant", dsv1, func(lock *sparkArtifactLock) { lock.ArtifactVariant = "dsv2-spark-3.5-raw" }, validateDSv1ArtifactLock},
		{"v1-v2-build-axis", dsv1, func(lock *sparkArtifactLock) { lock.ArtifactBuild.SparkVersion = "3.5.0" }, validateDSv1ArtifactLock},
		{"v1-v2-java-toolchain", dsv1, func(lock *sparkArtifactLock) { lock.ArtifactBuild.JavaToolchain = "[11,12)" }, validateDSv1ArtifactLock},
		{"v1-v2-runtime-scala", dsv1, func(lock *sparkArtifactLock) { lock.TestRuntime.ScalaVersion = "2.12.18" }, validateDSv1ArtifactLock},
		{"v2-v1-top-level-runtime", dsv2, func(lock *sparkArtifactLock) { lock.SparkVersion = "3.5.8" }, validateDSv2ArtifactLock},
		{"v2-binary-is-full-version", dsv2, func(lock *sparkArtifactLock) { lock.TestRuntime.ScalaBinaryVersion = "2.12.18" }, validateDSv2ArtifactLock},
		{"v2-runtime-version-missing", dsv2, func(lock *sparkArtifactLock) { lock.TestRuntime.ScalaVersion = "" }, validateDSv2ArtifactLock},
		{"v2-build-runtime-conflated", dsv2, func(lock *sparkArtifactLock) { lock.ArtifactBuild.SparkVersion = lock.TestRuntime.SparkVersion }, validateDSv2ArtifactLock},
		{"v2-mixed-classpath", dsv2, func(lock *sparkArtifactLock) { lock.ExecutionClasspathPolicy = "allow-mixed-variants" }, validateDSv2ArtifactLock},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lock := test.lock
			test.mutate(&lock)
			if err := test.validate(lock); err == nil {
				t.Fatal("cross-schema artifact mutation was accepted")
			}
		})
	}
}
