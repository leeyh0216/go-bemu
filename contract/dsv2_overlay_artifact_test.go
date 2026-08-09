package contract

// The overlay lock is intentionally independent from the released connector
// lock. It binds one released class and the two streaming hook descriptors to
// deterministic output bytes; an upstream implementation must be reviewed
// instead of being silently overwritten.
//
// Official sources:
//   - https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/719817782a214b8ca72be520870013a3e0253d92/spark-bigquery-connector-common/src/main/java/com/google/cloud/spark/bigquery/write/context/DataSourceWriterContext.java#L38-L50
//   - https://spark.apache.org/docs/3.5.8/api/java/org/apache/spark/sql/connector/write/streaming/StreamingWrite.html
//   - https://repo.maven.apache.org/maven2/org/javassist/javassist/3.30.2-GA/

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const (
	overlayVariant = "dsv2-spark-3.5-streaming-visibility-overlay"
	overlayClass   = "com.google.cloud.spark.bigquery.write.context.BigQueryDirectDataSourceWriterContext"
	overlayEntry   = "com/google/cloud/spark/bigquery/write/context/BigQueryDirectDataSourceWriterContext.class"
	messageArray   = "[Lcom/google/cloud/spark/bigquery/write/context/WriterCommitMessageContext;"
)

type overlaySourceBinding struct {
	Ref    string `json:"ref"`
	SHA256 string `json:"sha256"`
}

type overlayArtifactIdentity struct {
	Kind        string `json:"kind"`
	Output      string `json:"output"`
	Size        int64  `json:"size"`
	SHA256      string `json:"sha256"`
	ClassEntry  string `json:"classEntry"`
	ClassSize   int64  `json:"classSize"`
	ClassSHA256 string `json:"classSha256"`
}

type dsv2OverlayArtifactLock struct {
	SchemaVersion    string                 `json:"schemaVersion"`
	ArtifactVariant  string                 `json:"artifactVariant"`
	ConnectorVersion string                 `json:"connectorVersion"`
	SourceCommit     string                 `json:"sourceCommit"`
	BuildContract    overlaySourceBinding   `json:"buildContract"`
	BuilderSources   []overlaySourceBinding `json:"builderSources"`
	BaseArtifact     struct {
		Kind   string `json:"kind"`
		Output string `json:"output"`
		Size   int64  `json:"size"`
		SHA256 string `json:"sha256"`
	} `json:"baseArtifact"`
	OverlayArtifact overlayArtifactIdentity `json:"overlayArtifact"`
	TestRuntime     struct {
		SparkVersion       string `json:"sparkVersion"`
		ScalaBinaryVersion string `json:"scalaBinaryVersion"`
		ScalaVersion       string `json:"scalaVersion"`
		JavaVersion        string `json:"javaVersion"`
	} `json:"testRuntime"`
	WritePolicy              string `json:"writePolicy"`
	ExecutionClasspathPolicy string `json:"executionClasspathPolicy"`
}

type lockedMethod struct {
	Name       string `json:"name"`
	Descriptor string `json:"descriptor"`
	CodeBytes  int64  `json:"codeBytes,omitempty"`
	CodeSHA256 string `json:"codeSha256,omitempty"`
	Delegate   string `json:"delegate,omitempty"`
}

type dsv2OverlayBuildLock struct {
	SchemaVersion    string `json:"schemaVersion"`
	OverlayID        string `json:"overlayId"`
	ConnectorVersion string `json:"connectorVersion"`
	SourceCommit     string `json:"sourceCommit"`
	InputArtifact    struct {
		Kind   string `json:"kind"`
		URL    string `json:"url"`
		Output string `json:"output"`
		Size   int64  `json:"size"`
		SHA256 string `json:"sha256"`
	} `json:"inputArtifact"`
	TargetClass struct {
		Name             string         `json:"name"`
		Entry            string         `json:"entry"`
		Size             int64          `json:"size"`
		SHA256           string         `json:"sha256"`
		RequiredMethods  []lockedMethod `json:"requiredMethods"`
		ForbiddenMethods []lockedMethod `json:"forbiddenMethods"`
	} `json:"targetClass"`
	Patch struct {
		CommitGuard struct {
			WriteAtLeastOnce          bool   `json:"writeAtLeastOnce"`
			TableToWriteDeleteOnAbort bool   `json:"tableToWriteDeleteOnAbort"`
			Failure                   string `json:"failure"`
		} `json:"commitGuard"`
		Methods []lockedMethod `json:"methods"`
	} `json:"patch"`
	OutputArtifact struct {
		Kind        string   `json:"kind"`
		Output      string   `json:"output"`
		Entries     []string `json:"entries"`
		ClassSize   int64    `json:"classSize"`
		ClassSHA256 string   `json:"classSha256"`
		Size        int64    `json:"size"`
		SHA256      string   `json:"sha256"`
	} `json:"outputArtifact"`
	BuilderSource struct {
		Ref    string `json:"ref"`
		Size   int64  `json:"size"`
		SHA256 string `json:"sha256"`
	} `json:"builderSource"`
	BuildTool struct {
		JavaRelease      string `json:"javaRelease"`
		RuntimeJavaMajor string `json:"runtimeJavaMajor"`
		Javassist        struct {
			Version string `json:"version"`
			URL     string `json:"url"`
			Output  string `json:"output"`
			Size    int64  `json:"size"`
			SHA256  string `json:"sha256"`
		} `json:"javassist"`
	} `json:"buildTool"`
}

func readStrictJSON[T any](t *testing.T, path string) T {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	value, err := decodeStrictJSON[T](contents)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func decodeStrictJSON[T any](contents []byte) (T, error) {
	var value T
	decoder := json.NewDecoder(strings.NewReader(string(contents)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("trailing JSON value")
		}
		return value, err
	}
	return value, nil
}

func TestOverlayStrictJSONRejectsTrailingValue(t *testing.T) {
	if _, err := decodeStrictJSON[dsv2OverlayArtifactLock]([]byte(`{} {}`)); err == nil {
		t.Fatal("trailing lock value was accepted")
	}
}

func validateDSv2OverlayArtifactLock(lock dsv2OverlayArtifactLock) error {
	if lock.SchemaVersion != "1" || lock.ArtifactVariant != overlayVariant ||
		lock.ConnectorVersion != "0.44.2" || lock.SourceCommit != sparkConnector0442Commit ||
		lock.WritePolicy != "direct-exact-pre-existing-table-append-only" ||
		lock.ExecutionClasspathPolicy != "exactly-one-base-connector-plus-one-class-overlay" {
		return fmt.Errorf("unreviewed overlay identity/version/policy")
	}
	if lock.BaseArtifact.Kind != "maven-jar" ||
		lock.BaseArtifact.Output != "spark-3.5-bigquery-0.44.2.jar" ||
		lock.BaseArtifact.Size != 42618495 ||
		lock.BaseArtifact.SHA256 != "2e6bbb41bcaf56ae17a5488dd4453698bd35f13e9849f4daed744ca7b57b053f" {
		return fmt.Errorf("unreviewed overlay base artifact")
	}
	wantOverlay := overlayArtifactIdentity{
		Kind: "one-class-overlay-jar", Output: "spark-3.5-bigquery-0.44.2-dsv2-streaming-overlay.jar",
		Size: 20949, SHA256: "1e4d3705834745aa662442eb41f4aa99e6f7a1a89aa51b8aae1eb93c7c6c5bd3",
		ClassEntry: overlayEntry, ClassSize: 20673,
		ClassSHA256: "1f41fa60279c39e9fbc144ff2d6252b78dffa1a505f329e51af60dcee91af67d",
	}
	if lock.OverlayArtifact != wantOverlay {
		return fmt.Errorf("unreviewed overlay artifact or class bytes")
	}
	if lock.TestRuntime.SparkVersion != "3.5.8" || lock.TestRuntime.ScalaBinaryVersion != "2.12" ||
		lock.TestRuntime.ScalaVersion != "2.12.18" || lock.TestRuntime.JavaVersion != "17" {
		return fmt.Errorf("unreviewed overlay test runtime")
	}
	wantSources := []overlaySourceBinding{
		{Ref: "tools/dsv2-overlay/build.py", SHA256: "8cfaee723b2033b245c5a645278081a2afb193ffdddd29c1caf697f40d5ac379"},
		{Ref: "tools/dsv2-overlay/src/dev/bqemu/overlay/OverlayBuilder.java", SHA256: "405950777605aa26440e59aa2c99da50b65734c3d33970eb4f232ba6da610c4a"},
	}
	if lock.BuildContract != (overlaySourceBinding{Ref: "tools/dsv2-overlay/overlay.lock.json", SHA256: "74acdde7e907a6b737557aedc406263d79def41fb1acd84109168d7c97132098"}) ||
		!reflect.DeepEqual(lock.BuilderSources, wantSources) {
		return fmt.Errorf("unreviewed overlay build contract or builder source")
	}
	return nil
}

func validateDSv2OverlayBuildLock(lock dsv2OverlayBuildLock) error {
	batchDescriptor := "(" + messageArray + ")V"
	streamDescriptor := "(J" + messageArray + ")V"
	if lock.SchemaVersion != "1" || lock.OverlayID != "dsv2-spark-3.5-streaming-visibility-0.44.2" ||
		lock.ConnectorVersion != "0.44.2" || lock.SourceCommit != sparkConnector0442Commit {
		return fmt.Errorf("unreviewed overlay build version")
	}
	if lock.InputArtifact.Kind != "maven-jar" ||
		lock.InputArtifact.URL != "https://repo.maven.apache.org/maven2/com/google/cloud/spark/spark-3.5-bigquery/0.44.2/spark-3.5-bigquery-0.44.2.jar" ||
		lock.InputArtifact.Output != "spark-3.5-bigquery-0.44.2.jar" ||
		lock.InputArtifact.Size != 42618495 || lock.InputArtifact.SHA256 != "2e6bbb41bcaf56ae17a5488dd4453698bd35f13e9849f4daed744ca7b57b053f" {
		return fmt.Errorf("unreviewed overlay build input")
	}
	if lock.TargetClass.Name != overlayClass || lock.TargetClass.Entry != overlayEntry ||
		lock.TargetClass.Size != 20291 || lock.TargetClass.SHA256 != "3df68a5c1912fee08a1099399f2616bb1918566abf93a3f31194412584d31a63" {
		return fmt.Errorf("unreviewed overlay target class")
	}
	wantRequired := []lockedMethod{
		{Name: "commit", Descriptor: batchDescriptor, CodeBytes: 436, CodeSHA256: "04246c9daf6eb684c0704a9005e4346b9f2755f834ca949177ab54f9b8335cc6"},
		{Name: "abort", Descriptor: batchDescriptor, CodeBytes: 56, CodeSHA256: "b9b627f168c61c5c8e7e5c64225a8b1a55318c106758d3c084edb6f60eafa747"},
	}
	wantForbidden := []lockedMethod{
		{Name: "onDataStreamingWriterCommit", Descriptor: streamDescriptor},
		{Name: "onDataStreamingWriterAbort", Descriptor: streamDescriptor},
	}
	wantPatched := []lockedMethod{
		{Name: "onDataStreamingWriterCommit", Descriptor: streamDescriptor, Delegate: "commit", CodeBytes: 34, CodeSHA256: "6174e9886129f75e6e0ecb894887605ab3ce4d430e2c9c76ae3eec79d615e8e1"},
		{Name: "onDataStreamingWriterAbort", Descriptor: streamDescriptor, Delegate: "abort", CodeBytes: 6, CodeSHA256: "c2f4cab39d82fb3d4459c1c1d4f6d37ceacf45e381b3aeb4b9260dd0d2f0a3ee"},
	}
	if !reflect.DeepEqual(lock.TargetClass.RequiredMethods, wantRequired) ||
		!reflect.DeepEqual(lock.TargetClass.ForbiddenMethods, wantForbidden) ||
		!reflect.DeepEqual(lock.Patch.Methods, wantPatched) {
		return fmt.Errorf("unreviewed overlay method descriptor or bytecode")
	}
	if lock.Patch.CommitGuard.WriteAtLeastOnce || lock.Patch.CommitGuard.TableToWriteDeleteOnAbort ||
		lock.Patch.CommitGuard.Failure != "java.lang.IllegalStateException" {
		return fmt.Errorf("unreviewed overlay execution-mode guard")
	}
	if lock.OutputArtifact.Kind != "one-class-overlay-jar" ||
		!reflect.DeepEqual(lock.OutputArtifact.Entries, []string{overlayEntry}) ||
		lock.OutputArtifact.Output != "spark-3.5-bigquery-0.44.2-dsv2-streaming-overlay.jar" ||
		lock.OutputArtifact.ClassSize != 20673 || lock.OutputArtifact.ClassSHA256 != "1f41fa60279c39e9fbc144ff2d6252b78dffa1a505f329e51af60dcee91af67d" ||
		lock.OutputArtifact.Size != 20949 || lock.OutputArtifact.SHA256 != "1e4d3705834745aa662442eb41f4aa99e6f7a1a89aa51b8aae1eb93c7c6c5bd3" {
		return fmt.Errorf("unreviewed overlay output")
	}
	if lock.BuilderSource.Ref != "tools/dsv2-overlay/src/dev/bqemu/overlay/OverlayBuilder.java" ||
		lock.BuilderSource.Size != 14332 || lock.BuilderSource.SHA256 != "405950777605aa26440e59aa2c99da50b65734c3d33970eb4f232ba6da610c4a" {
		return fmt.Errorf("unreviewed overlay builder source")
	}
	if lock.BuildTool.JavaRelease != "11" || lock.BuildTool.RuntimeJavaMajor != "17" || lock.BuildTool.Javassist.Version != "3.30.2-GA" ||
		lock.BuildTool.Javassist.URL != "https://repo.maven.apache.org/maven2/org/javassist/javassist/3.30.2-GA/javassist-3.30.2-GA.jar" ||
		lock.BuildTool.Javassist.Output != "javassist-3.30.2-GA.jar" || lock.BuildTool.Javassist.Size != 794714 ||
		lock.BuildTool.Javassist.SHA256 != "eba37290994b5e4868f3af98ff113f6244a6b099385d9ad46881307d3cb01aaf" {
		return fmt.Errorf("unreviewed overlay build tool")
	}
	return nil
}

func TestDSv2OverlayArtifactAndBuildAreExactlyLocked(t *testing.T) {
	artifactLock := readStrictJSON[dsv2OverlayArtifactLock](t, "../tests/spark/artifacts-dsv2-overlay.lock.json")
	buildLock := readStrictJSON[dsv2OverlayBuildLock](t, "../tools/dsv2-overlay/overlay.lock.json")
	if err := validateDSv2OverlayArtifactLock(artifactLock); err != nil {
		t.Fatal(err)
	}
	if err := validateDSv2OverlayBuildLock(buildLock); err != nil {
		t.Fatal(err)
	}
	if artifactLock.BaseArtifact.Output != buildLock.InputArtifact.Output ||
		artifactLock.BaseArtifact.Size != buildLock.InputArtifact.Size ||
		artifactLock.BaseArtifact.SHA256 != buildLock.InputArtifact.SHA256 ||
		artifactLock.OverlayArtifact.Output != buildLock.OutputArtifact.Output ||
		artifactLock.OverlayArtifact.Size != buildLock.OutputArtifact.Size ||
		artifactLock.OverlayArtifact.SHA256 != buildLock.OutputArtifact.SHA256 ||
		artifactLock.OverlayArtifact.ClassEntry != buildLock.OutputArtifact.Entries[0] ||
		artifactLock.OverlayArtifact.ClassSize != buildLock.OutputArtifact.ClassSize ||
		artifactLock.OverlayArtifact.ClassSHA256 != buildLock.OutputArtifact.ClassSHA256 {
		t.Fatal("artifact lock and build lock select different input or output bytes")
	}
	for _, binding := range append([]overlaySourceBinding{artifactLock.BuildContract}, artifactLock.BuilderSources...) {
		if filepath.IsAbs(binding.Ref) || strings.Contains(filepath.Clean(binding.Ref), "..") {
			t.Fatalf("overlay source binding escapes the repository: %q", binding.Ref)
		}
		contents, err := os.ReadFile(filepath.Join("..", binding.Ref))
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(contents)
		if hex.EncodeToString(digest[:]) != binding.SHA256 {
			t.Fatalf("overlay source binding drifted: ref=%s", binding.Ref)
		}
	}
}

func TestDSv2OverlayLocksRejectVersionClassMethodAndFingerprintDrift(t *testing.T) {
	artifactBase := readStrictJSON[dsv2OverlayArtifactLock](t, "../tests/spark/artifacts-dsv2-overlay.lock.json")
	buildBase := readStrictJSON[dsv2OverlayBuildLock](t, "../tools/dsv2-overlay/overlay.lock.json")
	artifactTests := map[string]func(*dsv2OverlayArtifactLock){
		"variant":           func(lock *dsv2OverlayArtifactLock) { lock.ArtifactVariant = "dsv2-spark-3.5-raw" },
		"connector-version": func(lock *dsv2OverlayArtifactLock) { lock.ConnectorVersion = "0.44.3" },
		"base-hash":         func(lock *dsv2OverlayArtifactLock) { lock.BaseArtifact.SHA256 = strings.Repeat("0", 64) },
		"overlay-class":     func(lock *dsv2OverlayArtifactLock) { lock.OverlayArtifact.ClassEntry = "drift.class" },
		"overlay-hash":      func(lock *dsv2OverlayArtifactLock) { lock.OverlayArtifact.SHA256 = strings.Repeat("0", 64) },
		"builder-source":    func(lock *dsv2OverlayArtifactLock) { lock.BuilderSources[0].SHA256 = strings.Repeat("0", 64) },
		"write-policy":      func(lock *dsv2OverlayArtifactLock) { lock.WritePolicy = "all-modes" },
		"classpath-policy":  func(lock *dsv2OverlayArtifactLock) { lock.ExecutionClasspathPolicy = "mixed" },
	}
	for name, mutate := range artifactTests {
		t.Run("artifact-"+name, func(t *testing.T) {
			lock := cloneOverlayLock(t, artifactBase)
			mutate(&lock)
			if validateDSv2OverlayArtifactLock(lock) == nil {
				t.Fatal("overlay artifact drift was accepted")
			}
		})
	}
	buildTests := map[string]func(*dsv2OverlayBuildLock){
		"source-version":    func(lock *dsv2OverlayBuildLock) { lock.ConnectorVersion = "0.44.3" },
		"input-hash":        func(lock *dsv2OverlayBuildLock) { lock.InputArtifact.SHA256 = strings.Repeat("0", 64) },
		"target-class":      func(lock *dsv2OverlayBuildLock) { lock.TargetClass.Name = "drift.Class" },
		"target-class-hash": func(lock *dsv2OverlayBuildLock) { lock.TargetClass.SHA256 = strings.Repeat("0", 64) },
		"batch-descriptor":  func(lock *dsv2OverlayBuildLock) { lock.TargetClass.RequiredMethods[0].Descriptor = "()V" },
		"batch-bytecode": func(lock *dsv2OverlayBuildLock) {
			lock.TargetClass.RequiredMethods[0].CodeSHA256 = strings.Repeat("0", 64)
		},
		"upstream-hook-present": func(lock *dsv2OverlayBuildLock) { lock.TargetClass.ForbiddenMethods = nil },
		"hook-descriptor":       func(lock *dsv2OverlayBuildLock) { lock.Patch.Methods[0].Descriptor = "()V" },
		"hook-bytecode":         func(lock *dsv2OverlayBuildLock) { lock.Patch.Methods[0].CodeSHA256 = strings.Repeat("0", 64) },
		"delegate":              func(lock *dsv2OverlayBuildLock) { lock.Patch.Methods[0].Delegate = "abort" },
		"mode-guard":            func(lock *dsv2OverlayBuildLock) { lock.Patch.CommitGuard.WriteAtLeastOnce = true },
		"output-class": func(lock *dsv2OverlayBuildLock) {
			lock.OutputArtifact.Entries = append(lock.OutputArtifact.Entries, "extra.class")
		},
		"builder-source":    func(lock *dsv2OverlayBuildLock) { lock.BuilderSource.SHA256 = strings.Repeat("0", 64) },
		"java-runtime":      func(lock *dsv2OverlayBuildLock) { lock.BuildTool.RuntimeJavaMajor = "latest" },
		"javassist-version": func(lock *dsv2OverlayBuildLock) { lock.BuildTool.Javassist.Version = "latest" },
	}
	for name, mutate := range buildTests {
		t.Run("build-"+name, func(t *testing.T) {
			lock := cloneOverlayLock(t, buildBase)
			mutate(&lock)
			if validateDSv2OverlayBuildLock(lock) == nil {
				t.Fatal("overlay build drift was accepted")
			}
		})
	}
}

func cloneOverlayLock[T any](t *testing.T, original T) T {
	t.Helper()
	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var clone T
	if err := json.Unmarshal(encoded, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}
