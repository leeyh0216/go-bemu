package contract

// Public client lifecycle contract sources:
//   - Python 3.43.0 query helper:
//     https://github.com/googleapis/google-cloud-python/blob/google-cloud-bigquery-v3.43.0/packages/google-cloud-bigquery/google/cloud/bigquery/_job_helpers.py#L420-L641
//   - Query configuration:
//     https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery
//   - Query result pagination:
//     https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults
//   - Job list pagination:
//     https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/list

import (
	"strings"
	"testing"
)

func TestPythonClientProfilePinsQueryLifecycleAndRequestControlGap(t *testing.T) {
	profile, ok := DefaultRegistry().ForClientVersion("google-cloud-bigquery-python", "3.43.0")
	if !ok {
		t.Fatal("google-cloud-bigquery-python 3.43.0 profile is missing")
	}
	if profile.Capabilities["rest.query.destination"] != CapabilityVerified {
		t.Fatalf("destination query capability = %q, want verified", profile.Capabilities["rest.query.destination"])
	}
	if profile.Capabilities["query.sync.request-controls-v1"] != CapabilityPartial {
		t.Fatalf("request control capability = %q, want partial", profile.Capabilities["query.sync.request-controls-v1"])
	}
	if profile.Capabilities["tabledata.list"] != CapabilityVerified {
		t.Fatalf("tabledata.list capability = %q, want verified", profile.Capabilities["tabledata.list"])
	}
	assertProfileStages(t, profile, "python-query-destination-lifecycle", []string{
		"insert_destination_query_job",
		"reject_duplicate_query_job",
		"get_location_scoped_job",
		"poll_destination_query_results",
	})
	assertProfileStages(t, profile, "python-query-pagination-location", []string{
		"reject_wrong_job_location",
		"get_query_results_first_page",
		"get_query_results_next_page",
		"list_jobs_first_page",
		"list_jobs_next_page",
	})
	assertProfileStages(t, profile, "python-query-request-idempotency", []string{
		"query_response_lost_after_edge",
		"retry_query_with_same_request_id",
	})
	assertProfileStages(t, profile, "python-tabledata-list", []string{
		"get_tabledata_first_page",
		"get_tabledata_next_page",
		"get_tabledata_start_index",
		"get_tabledata_explicit_zero",
	})
}

func TestPythonTableDataProvenancePinsMonorepoReleaseTags(t *testing.T) {
	want := []string{
		"https://github.com/googleapis/google-cloud-python/blob/google-cloud-bigquery-v3.43.0/packages/google-cloud-bigquery/google/cloud/bigquery/client.py#L4118-L4237",
		"https://github.com/googleapis/google-cloud-python/blob/google-cloud-bigquery-v3.43.0/packages/google-cloud-bigquery/google/cloud/bigquery/table.py#L1838-L1995",
		"https://github.com/googleapis/google-cloud-python/blob/google-api-core-v2.34.0/packages/google-api-core/google/api_core/page_iterator.py#L397-L420",
	}
	profile, ok := DefaultRegistry().ForClientVersion("google-cloud-bigquery-python", "3.43.0")
	if !ok {
		t.Fatal("google-cloud-bigquery-python 3.43.0 profile is missing")
	}
	traces, err := GoldenTraces()
	if err != nil {
		t.Fatal(err)
	}
	var sources []string
	for _, trace := range traces {
		if trace.Flow == "python-tabledata-list" {
			sources = trace.SourceRefs
			break
		}
	}
	if len(sources) == 0 {
		t.Fatal("python-tabledata-list golden provenance is missing")
	}
	for _, source := range want {
		if !containsString(profile.Sources, source) {
			t.Fatalf("Python profile lacks immutable tabledata source %q", source)
		}
		if !containsString(sources, source) {
			t.Fatalf("Python tabledata golden lacks immutable source %q", source)
		}
	}
	for _, source := range profile.Sources {
		if strings.Contains(source, "github.com/googleapis/python-bigquery/") || strings.Contains(source, "github.com/googleapis/python-api-core/") {
			t.Fatalf("Python profile retains unreachable legacy source %q", source)
		}
	}
	for _, source := range sources {
		if strings.Contains(source, "github.com/googleapis/python-bigquery/") || strings.Contains(source, "github.com/googleapis/python-api-core/") {
			t.Fatalf("Python tabledata golden retains unreachable legacy source %q", source)
		}
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestBQCLIProfilePinsMetadataAndQueryLifecycle(t *testing.T) {
	profile, ok := DefaultRegistry().ForClientVersion("bq-cli", "2.1.31")
	if !ok {
		t.Fatal("bq CLI 2.1.31 profile is missing")
	}
	for _, capability := range []string{
		"CAP-REST-METADATA-PATCH-V1",
		"rest.query.destination",
		"rest.jobs.location",
		"rest.jobs.duplicate-conflict",
	} {
		if profile.Capabilities[capability] != CapabilityVerified {
			t.Fatalf("capability %s = %q, want verified", capability, profile.Capabilities[capability])
		}
	}
	assertProfileStages(t, profile, "bq-metadata-patch", []string{
		"patch_dataset_metadata",
		"patch_table_metadata_and_schema",
	})
	assertProfileStages(t, profile, "bq-query-destination-lifecycle", []string{
		"insert_destination_query_job",
		"get_destination_query_job",
		"reject_duplicate_query_job",
		"reject_wrong_job_location",
		"get_query_results_pages",
		"list_jobs_pages",
	})
}

func assertProfileStages(t *testing.T, profile Profile, flow string, want []string) {
	t.Helper()
	calls, ok := profile.Flows[flow]
	if !ok {
		t.Fatalf("profile %s has no flow %s", profile.ID, flow)
	}
	if len(calls) != len(want) {
		t.Fatalf("profile %s flow %s has %d stages, want %d", profile.ID, flow, len(calls), len(want))
	}
	for index, stage := range want {
		if calls[index].Stage != stage {
			t.Fatalf("profile %s flow %s stage %d = %q, want %q", profile.ID, flow, index, calls[index].Stage, stage)
		}
	}
}
