package contract

import (
	"errors"
	"strings"
	"testing"
)

func TestRegistryResolvesPinnedConnectorVersions(t *testing.T) {
	registry := DefaultRegistry()
	profile, ok := registry.ForConnectorVersion("0.44.2")
	if !ok || profile.ID != "spark-bigquery-connector-0.44.2" {
		t.Fatalf("version resolved to %#v, %t", profile, ok)
	}
	for _, unknown := range []string{"0.44.0", "0.44.1", "0.45.0"} {
		if _, ok := registry.ForConnectorVersion(unknown); ok {
			t.Fatalf("unknown connector version %s must not silently select a profile", unknown)
		}
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
		"version=connector:spark-bigquery-connector@0.44.2",
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
