package contract

import (
	"errors"
	"strings"
	"testing"
)

func TestRegistryResolvesPinnedConnectorVersions(t *testing.T) {
	registry := DefaultRegistry()
	profile, ok := registry.ForConnectorVersion("0.44.2")
	if !ok || profile.ID != "spark-bigquery-connector-dsv1-0.44.2" {
		t.Fatalf("version resolved to %#v, %t", profile, ok)
	}
	for _, unknown := range []string{"0.44.0", "0.44.1", "0.45.0"} {
		if _, ok := registry.ForConnectorVersion(unknown); ok {
			t.Fatalf("unknown connector version %s must not silently select a profile", unknown)
		}
	}
}

func TestRegistryResolvesDSv2ByArtifactIdentity(t *testing.T) {
	registry := DefaultRegistry()
	dsv1, ok := registry.ForConsumerVersion(
		"connector-artifact", "spark-bigquery-with-dependencies_2.12", "0.44.2",
	)
	if !ok || dsv1.ID != "spark-bigquery-connector-dsv1-0.44.2" {
		t.Fatalf("DSv1 artifact identity resolved to %#v, %t", dsv1, ok)
	}
	profile, ok := registry.ForConsumerVersion(
		"connector-artifact", "spark-3.5-bigquery-raw", "0.44.2",
	)
	if !ok || profile.ID != "spark-bigquery-connector-dsv2-raw-0.44.2" {
		t.Fatalf("DSv2 artifact identity resolved to %#v, %t", profile, ok)
	}
	if _, ok := registry.ForConsumerVersion(
		"connector-artifact", "spark-3.5-bigquery", "0.44.2",
	); ok {
		t.Fatal("unqualified DSv2 coordinate must not silently select an artifact profile")
	}
}

func TestRegistryResolvesPinnedPythonClientVersion(t *testing.T) {
	registry := DefaultRegistry()
	profile, ok := registry.ForClientVersion("google-cloud-bigquery-python", "3.43.0")
	if !ok || profile.ID != "google-cloud-bigquery-python-3.43.0" {
		t.Fatalf("version resolved to %#v, %t", profile, ok)
	}
	for _, capability := range []string{"CAP-REST-METADATA-PATCH-V1", "CAP-SCHEMA-ADDITIVE-V1"} {
		if profile.Capabilities[capability] != CapabilityVerified {
			t.Fatalf("capability %s is not verified: %#v", capability, profile.Capabilities)
		}
	}
	for _, unknown := range []string{"3.42.0", "3.43.1", "4.0.0"} {
		if _, ok := registry.ForClientVersion("google-cloud-bigquery-python", unknown); ok {
			t.Fatalf("unknown client version %s must not silently select a profile", unknown)
		}
	}
}

func TestRegistryResolvesPinnedBQCLIClientVersion(t *testing.T) {
	registry := DefaultRegistry()
	profile, ok := registry.ForClientVersion("bq-cli", "2.1.31")
	if !ok || profile.ID != "bq-cli-2.1.31" {
		t.Fatalf("version resolved to %#v, %t", profile, ok)
	}
	for _, capability := range []string{"rest.discovery.bq-cli", "rest.jobs.list", "CAP-SCHEMA-ADDITIVE-V1"} {
		if profile.Capabilities[capability] != CapabilityVerified {
			t.Fatalf("capability %s is not verified: %#v", capability, profile.Capabilities)
		}
	}
	for _, unknown := range []string{"2.1.30", "2.1.32", "3.0.0"} {
		if _, ok := registry.ForClientVersion("bq-cli", unknown); ok {
			t.Fatalf("unknown bq CLI version %s must not silently select a profile", unknown)
		}
	}
}

func TestRegistryReturnsDefensiveProfileCopies(t *testing.T) {
	registry := DefaultRegistry()
	profile, _ := registry.ForConnectorVersion("0.44.2")
	profile.Capabilities["rest.discovery"] = CapabilityPlanned
	profile.Flows["read-arrow"][0].RequiredFields[0] = "corrupted"
	python, _ := registry.ForClientVersion("google-cloud-bigquery-python", "3.43.0")
	python.Consumers[0].Versions[0] = "corrupted"

	again, _ := registry.ForConnectorVersion("0.44.2")
	if again.Capabilities["rest.discovery"] != CapabilityVerified {
		t.Fatal("caller mutated the registry capability map")
	}
	if again.Flows["read-arrow"][0].RequiredFields[0] == "corrupted" {
		t.Fatal("caller mutated the registry flow slice")
	}
	pythonAgain, _ := registry.ForClientVersion("google-cloud-bigquery-python", "3.43.0")
	if pythonAgain.Consumers[0].Versions[0] == "corrupted" {
		t.Fatal("caller mutated the registry consumer version slice")
	}
}

func TestSparkWriteProfilePinsExactAndDefaultStreamInitialization(t *testing.T) {
	profile, ok := DefaultRegistry().ForConnectorVersion("0.44.2")
	if !ok {
		t.Fatal("Spark connector profile is missing")
	}
	tests := []struct {
		flow       string
		wantCreate bool
		wantGet    bool
	}{
		{flow: "direct-append-pending", wantCreate: true, wantGet: false},
		{flow: "direct-at-least-once-default", wantCreate: false, wantGet: true},
	}
	for _, test := range tests {
		calls := profile.Flows[test.flow]
		var hasCreateWriteStream, hasGetWriteStream, hasContinuation bool
		for _, call := range calls {
			if call.Target == "/google.cloud.bigquery.storage.v1.BigQueryWrite/CreateWriteStream" {
				hasCreateWriteStream = true
			}
			if call.Target == "/google.cloud.bigquery.storage.v1.BigQueryWrite/GetWriteStream" {
				hasGetWriteStream = true
			}
			for _, field := range call.RequiredFields {
				if field == "request.continuation.proto_rows.writer_schema" {
					hasContinuation = true
				}
			}
		}
		if hasCreateWriteStream != test.wantCreate ||
			hasGetWriteStream != test.wantGet ||
			!hasContinuation {
			t.Fatalf(
				"flow %s initialization/continuation mismatch: create=%t get=%t calls=%#v",
				test.flow, hasCreateWriteStream, hasGetWriteStream, calls,
			)
		}
	}
}

func TestSparkStaticOverwriteProfilePinsAtomicReplaceShape(t *testing.T) {
	profile, ok := DefaultRegistry().ForConnectorVersion("0.44.2")
	if !ok {
		t.Fatal("Spark connector profile is missing")
	}
	if profile.Capabilities["sql.connector_overwrite"] != CapabilityPartial {
		t.Fatalf("static overwrite capability = %s, want partial", profile.Capabilities["sql.connector_overwrite"])
	}
	flow := profile.Flows["direct-overwrite-static"]
	if len(flow) != 4 {
		t.Fatalf("static overwrite flow has %d calls, want jobs.insert, queries.get, jobs.get, tables.delete", len(flow))
	}
	if flow[0].Target != "/bigquery/v2/projects/{project}/jobs" ||
		flow[1].Target != "/bigquery/v2/projects/{project}/queries/{job}" ||
		flow[2].Target != "/bigquery/v2/projects/{project}/jobs/{job}" ||
		flow[3].Method != "DELETE" {
		t.Fatalf("unexpected static overwrite flow: %#v", flow)
	}
}

func TestRawDSv2StreamingProfileEndsAtFinalizeWithoutBatchCommit(t *testing.T) {
	// The wrapper calls an interface hook which is empty in connector 0.44.2:
	// https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/719817782a214b8ca72be520870013a3e0253d92/spark-bigquery-connector-common/src/main/java/com/google/cloud/spark/bigquery/write/context/DataSourceWriterContext.java#L42-L50
	profile, ok := DefaultRegistry().ForConsumerVersion(
		"connector-artifact", "spark-3.5-bigquery-raw", "0.44.2",
	)
	if !ok {
		t.Fatal("raw DSv2 profile is missing")
	}
	flow := profile.Flows["dsv2-direct-exact-streaming-raw"]
	wantStages := []string{
		"destination_metadata", "create_pending_stream", "append_rows", "finalize_stream",
	}
	if len(flow) != len(wantStages) {
		t.Fatalf("raw DSv2 flow has %d calls, want exactly %d ending at finalize", len(flow), len(wantStages))
	}
	for index, call := range flow {
		if call.Stage != wantStages[index] {
			t.Fatalf("raw DSv2 stage[%d]=%q, want %q", index, call.Stage, wantStages[index])
		}
		if strings.HasSuffix(call.Target, "/GetWriteStream") ||
			strings.HasSuffix(call.Target, "/BatchCommitWriteStreams") {
			t.Fatal("raw DSv2 profile must retain zero GetWriteStream and zero BatchCommitWriteStreams")
		}
	}
	if !strings.HasSuffix(flow[len(flow)-1].Target, "/FinalizeWriteStream") {
		t.Fatalf("raw DSv2 flow does not terminate at FinalizeWriteStream: %#v", flow)
	}
	appendFields := make(map[string]bool)
	for _, field := range flow[2].RequiredFields {
		appendFields[field] = true
	}
	for _, required := range []string{
		"request.first.offset",
		"request.continuation.write_stream",
		"request.continuation.proto_rows.writer_schema",
	} {
		if !appendFields[required] {
			t.Fatalf("raw DSv2 append contract omitted %q", required)
		}
	}

	traces, err := GoldenTraces()
	if err != nil {
		t.Fatal(err)
	}
	for _, trace := range traces {
		if trace.ProfileID != profile.ID || trace.Flow != "dsv2-direct-exact-streaming-raw" {
			continue
		}
		if len(trace.Events) != len(wantStages) ||
			!strings.HasSuffix(trace.Events[len(trace.Events)-1].Target, "/FinalizeWriteStream") {
			t.Fatalf("raw DSv2 golden must end at finalize with no batch commit: %#v", trace.Events)
		}
		for _, event := range trace.Events {
			if strings.HasSuffix(event.Target, "/GetWriteStream") ||
				strings.HasSuffix(event.Target, "/BatchCommitWriteStreams") {
				t.Fatal("raw DSv2 golden must retain zero GetWriteStream and zero BatchCommitWriteStreams")
			}
		}
		return
	}
	t.Fatal("raw DSv2 golden trace is missing")
}

func TestEveryGoldenTraceSatisfiesItsProfile(t *testing.T) {
	traces, err := GoldenTraces()
	if err != nil {
		t.Fatal(err)
	}
	if len(traces) < 3 {
		t.Fatalf("expected REST and gRPC golden flows, got %d", len(traces))
	}
	for _, trace := range traces {
		t.Run(trace.Flow, func(t *testing.T) {
			if err := DefaultRegistry().Validate(trace); err != nil {
				t.Fatal(err)
			}
		})
	}
	wantTraces := 0
	for _, profile := range DefaultRegistry().Profiles() {
		wantTraces += len(profile.Flows)
	}
	if len(traces) != wantTraces {
		t.Fatalf("every supported flow needs one exact-version golden: flows=%d traces=%d", wantTraces, len(traces))
	}
}

func TestGoldenMismatchNamesStageAndShowsWireDiff(t *testing.T) {
	traces, err := GoldenTraces()
	if err != nil {
		t.Fatal(err)
	}
	var expected Trace
	for _, trace := range traces {
		if trace.Flow == "read-arrow" {
			expected = trace
			break
		}
	}
	actual := expected
	actual.Events = append([]WireEvent(nil), expected.Events...)
	actual.Events[1] = expected.Events[1]
	actual.Events[1].Target = "/google.cloud.bigquery.storage.v1.BigQueryRead/CreateSessionV2"
	err = CompareGolden(expected, actual)
	if err == nil {
		t.Fatal("expected a wire mismatch")
	}
	message := err.Error()
	var drift *DriftError
	if !errors.As(err, &drift) {
		t.Fatalf("expected DriftError, got %T", err)
	}
	for _, field := range []string{drift.Version, drift.Operation, drift.Stage, drift.Shape, drift.Fingerprint, drift.FixHint} {
		if field == "" {
			t.Fatalf("drift diagnostic has an empty field: %#v", drift)
		}
	}
	for _, fragment := range []string{
		"version=connector-artifact:spark-bigquery-with-dependencies_2.12@0.44.2",
		"operation=read-arrow",
		"stage=create_read_session",
		"shape=gRPC:UNARY",
		"fingerprint=sha256:",
		"fix_hint=",
		"--- expected",
		"+++ actual",
		"CreateSessionV2",
	} {
		if !strings.Contains(message, fragment) {
			t.Fatalf("mismatch did not contain %q:\n%s", fragment, message)
		}
	}
}

func TestPythonGoldenDriftNamesExactClientVersionAndOperation(t *testing.T) {
	traces, err := GoldenTraces()
	if err != nil {
		t.Fatal(err)
	}
	var expected Trace
	for _, trace := range traces {
		if trace.Flow == "python-table-schema-admin" {
			expected = trace
			break
		}
	}
	actual := expected
	actual.Events = append([]WireEvent(nil), expected.Events...)
	actual.Events[2] = expected.Events[2]
	actual.Events[2].Method = "PUT"
	err = CompareGolden(expected, actual)
	if err == nil {
		t.Fatal("expected a client wire mismatch")
	}
	message := err.Error()
	for _, fragment := range []string{
		"version=client:google-cloud-bigquery-python@3.43.0",
		"operation=python-table-schema-admin",
		"stage=patch_table_schema",
		"shape=REST:PUT",
	} {
		if !strings.Contains(message, fragment) {
			t.Fatalf("mismatch did not contain %q:\n%s", fragment, message)
		}
	}
}

func TestUnknownConsumerVersionReturnsStructuredDrift(t *testing.T) {
	trace := Trace{
		ProfileID:       "google-cloud-bigquery-python-3.43.0",
		ConsumerKind:    "client",
		ConsumerName:    "google-cloud-bigquery-python",
		ConsumerVersion: "3.44.0",
		FixtureKind:     "source-derived",
		SourceRefs:      []string{"https://pypi.org/project/google-cloud-bigquery/3.43.0/"},
		Flow:            "python-dataset-admin",
	}
	err := DefaultRegistry().Validate(trace)
	var drift *DriftError
	if !errors.As(err, &drift) {
		t.Fatalf("expected structured drift, got %T: %v", err, err)
	}
	for _, fragment := range []string{
		"version=client:google-cloud-bigquery-python@3.44.0",
		"operation=python-dataset-admin",
		"stage=profile_selection",
		"fingerprint=sha256:",
		"fix_hint=",
	} {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("profile selection drift did not contain %q:\n%s", fragment, err)
		}
	}
}
