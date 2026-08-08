package domain

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestJobStateMachine(t *testing.T) {
	now := time.Date(2026, 8, 8, 1, 2, 3, 0, time.UTC)
	job, err := NewQueryJob(JobReference{ProjectID: "test-project", JobID: "job-1"}, "SELECT 1", now)
	if err != nil {
		t.Fatal(err)
	}
	if job.State != JobPending || job.Reference.Location != "US" {
		t.Fatalf("unexpected new job: %#v", job)
	}
	if err := job.Start(now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	result := QueryResult{Rows: [][]any{{int64(1)}}}
	if err := job.Complete(result, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if job.State != JobDone || job.Result == nil || job.EndedAt == nil {
		t.Fatalf("unexpected completed job: %#v", job)
	}
	if err := job.Start(now); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected state conflict, got %v", err)
	}
}

func TestFailedJobIsTerminalDoneWithErrorResult(t *testing.T) {
	now := time.Date(2026, 8, 8, 1, 2, 3, 0, time.UTC)
	job, err := NewQueryJob(JobReference{ProjectID: "test-project", JobID: "job-failed"}, "SELECT missing", now)
	if err != nil {
		t.Fatal(err)
	}
	if err := job.Start(now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := job.Fail("invalidQuery", "column not found", now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if job.State != JobDone || job.Error == nil || job.Error.Reason != "invalidQuery" || job.Result != nil || job.EndedAt == nil {
		t.Fatalf("failed jobs must be terminal DONE with errorResult: %#v", job)
	}
}

func TestQueryCapabilityAndGapIDsAreStable(t *testing.T) {
	want := map[string]string{
		"exact_schema":          "query.destination.exact-schema-v1",
		"decimal_rounding":      "query.destination.decimal-rounding.unsupported-v1",
		"result_memory":         "query.results.unbounded-memory-v1",
		"complex_result_schema": "query.results.complex-schema-v1",
		"execution_timeout":     "query.execution.bounded-v1",
		"anonymous_table":       "query.destination.anonymous-v1",
		"truncate_replacement":  "query.destination.truncate-schema-replacement-v1",
		"cross_repo_identity":   "query.jobs.cross-repository-identity-v1",
		"sync_controls":         "query.sync.request-controls-v1",
		"location_inference":    "query.location.dataset-inference-v1",
		"terminal_persistence":  "query.terminal-persistence-v1",
		"exact_replay":          "query.jobs.exact-replay-extension-v1",
		"unsupported_options":   "query.options.unsupported-v1",
		"ddl_catalog_sync":      "query.ddl.catalog-sync-v1",
		"scripts":               "query.scripts.unsupported-v1",
	}
	got := map[string]string{
		"exact_schema":          CapabilityQueryDestinationExactSchemaV1,
		"decimal_rounding":      CapabilityQueryDecimalRoundingV1,
		"result_memory":         GapQueryResultsUnboundedMemoryV1,
		"complex_result_schema": GapQueryComplexResultSchemaV1,
		"execution_timeout":     CapabilityQueryBoundedExecutionV1,
		"anonymous_table":       CapabilityQueryAnonymousDestinationV1,
		"truncate_replacement":  GapQueryTruncateSchemaReplacementV1,
		"cross_repo_identity":   GapQueryCrossRepositoryIdentityV1,
		"sync_controls":         GapQuerySyncRequestControlsV1,
		"location_inference":    CapabilityQueryDatasetLocationV1,
		"terminal_persistence":  GapQueryTerminalPersistenceV1,
		"exact_replay":          GapQueryExactReplayExtensionV1,
		"unsupported_options":   GapQueryUnsupportedOptionsV1,
		"ddl_catalog_sync":      GapQueryDDLCatalogSyncV1,
		"scripts":               GapQueryScriptsUnsupportedV1,
	}
	for name, expected := range want {
		if got[name] != expected {
			t.Fatalf("%s ID = %q, want %q", name, got[name], expected)
		}
	}
}

func TestJobReferenceRejectsControlCharactersAndOversizedComponents(t *testing.T) {
	valid := JobReference{ProjectID: "test-project", Location: "us-central1", JobID: "spark_job-1"}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid job reference: %v", err)
	}
	for name, reference := range map[string]JobReference{
		"project NUL":        {ProjectID: "test\x00project", Location: "US", JobID: "job"},
		"location newline":   {ProjectID: "test-project", Location: "US\n", JobID: "job"},
		"job NUL":            {ProjectID: "test-project", Location: "US", JobID: "job\x00other"},
		"oversized job ID":   {ProjectID: "test-project", Location: "US", JobID: strings.Repeat("j", 1025)},
		"oversized location": {ProjectID: "test-project", Location: strings.Repeat("u", 1025), JobID: "job"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := reference.Validate(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("validation error = %v, want invalid", err)
			}
		})
	}
}

func TestQueryConfigurationDigestNormalizesLocationCase(t *testing.T) {
	configuration := QueryConfiguration{SQL: "SELECT 1"}
	upper, err := QueryConfigurationDigest(JobReference{ProjectID: "test-project", Location: "US", JobID: "job"}, configuration)
	if err != nil {
		t.Fatal(err)
	}
	lower, err := QueryConfigurationDigest(JobReference{ProjectID: "test-project", Location: "us", JobID: "job"}, configuration)
	if err != nil {
		t.Fatal(err)
	}
	if upper != lower {
		t.Fatalf("location case changed digest: upper=%s lower=%s", upper, lower)
	}
}

func TestQueryPriorityAndLabelsAreValidatedAndFingerprintBound(t *testing.T) {
	reference := JobReference{ProjectID: "test-project", Location: "US", JobID: "job"}
	base := QueryConfiguration{SQL: "SELECT 1", Priority: QueryPriorityInteractive, Labels: map[string]string{}}
	job, err := NewConfiguredQueryJob(reference, base, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if job.Configuration.Priority != QueryPriorityInteractive || job.Configuration.Labels == nil || len(job.Configuration.Labels) != 0 {
		t.Fatalf("connector metadata was not preserved: %#v", job.Configuration)
	}
	batch := base
	batch.Priority = QueryPriorityBatch
	batch.Labels = map[string]string{"spark_connector": "copy"}
	batchDigest, err := QueryConfigurationDigest(reference, batch)
	if err != nil {
		t.Fatal(err)
	}
	if batchDigest == job.ConfigurationDigest {
		t.Fatal("priority/labels did not change query configuration fingerprint")
	}
	for name, configuration := range map[string]QueryConfiguration{
		"priority":    {SQL: "SELECT 1", Priority: "URGENT"},
		"label key":   {SQL: "SELECT 1", Labels: map[string]string{"Uppercase": "value"}},
		"label value": {SQL: "SELECT 1", Labels: map[string]string{"valid": "UPPERCASE"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewConfiguredQueryJob(reference, configuration, time.Unix(1, 0)); !errors.Is(err, ErrInvalid) {
				t.Fatalf("validation error = %v, want invalid", err)
			}
		})
	}
}

func TestFieldValidationRejectsDuplicateAndInvalidTypes(t *testing.T) {
	table := Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events",
		Schema: []Field{{Name: "id", Type: "INT64"}, {Name: "ID", Type: "INT64"}},
	}
	if err := table.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected invalid duplicate field, got %v", err)
	}
	table.Schema = []Field{{Name: "payload", Type: "NOT_A_TYPE"}}
	if err := table.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected invalid field type, got %v", err)
	}
}
