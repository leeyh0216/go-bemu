package contract

import (
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

func TestRegistryReturnsDefensiveProfileCopies(t *testing.T) {
	registry := DefaultRegistry()
	profile, _ := registry.ForConnectorVersion("0.44.2")
	profile.Capabilities["rest.discovery"] = CapabilityPlanned
	profile.Flows["read-arrow"][0].RequiredFields[0] = "corrupted"

	again, _ := registry.ForConnectorVersion("0.44.2")
	if again.Capabilities["rest.discovery"] != CapabilityVerified {
		t.Fatal("caller mutated the registry capability map")
	}
	if again.Flows["read-arrow"][0].RequiredFields[0] == "corrupted" {
		t.Fatal("caller mutated the registry flow slice")
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
	profile, _ := DefaultRegistry().ForConnectorVersion("0.44.2")
	if len(traces) != len(profile.Flows) {
		t.Fatalf("every supported flow needs one exact-version golden: flows=%d traces=%d", len(profile.Flows), len(traces))
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
	for _, fragment := range []string{"read-arrow", "create_read_session", "--- expected", "+++ actual", "CreateSessionV2"} {
		if !strings.Contains(message, fragment) {
			t.Fatalf("mismatch did not contain %q:\n%s", fragment, message)
		}
	}
}
