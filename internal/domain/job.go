package domain

// BigQuery Job and JobStatus state provenance:
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/Job
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/JobStatus
//
// DONE is terminal for both success and failure. A successful DONE job has a
// result and no errorResult; a failed DONE job has an errorResult. Clients must
// inspect both state and errorResult rather than treating DONE as success.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

type JobState string

const (
	JobPending JobState = "PENDING"
	JobRunning JobState = "RUNNING"
	JobDone    JobState = "DONE"
)

// Stable capability/gap IDs are emitted in diagnostics and referenced by the
// bilingual compatibility contract. Renaming one is a contract change.
const (
	CapabilityQueryDestinationExactSchemaV1 = "query.destination.exact-schema-v1"
	CapabilityQueryDecimalRoundingV1        = "query.destination.decimal-rounding.unsupported-v1"
	CapabilityQueryAnonymousDestinationV1   = "query.destination.anonymous-v1"
	CapabilityQueryDatasetLocationV1        = "query.location.dataset-inference-v1"
	CapabilityQueryBoundedExecutionV1       = "query.execution.bounded-v1"
	GapQueryResultsUnboundedMemoryV1        = "query.results.unbounded-memory-v1"
	GapQueryComplexResultSchemaV1           = "query.results.complex-schema-v1"
	GapQueryTruncateSchemaReplacementV1     = "query.destination.truncate-schema-replacement-v1"
	GapQueryCrossRepositoryIdentityV1       = "query.jobs.cross-repository-identity-v1"
	GapQuerySyncRequestControlsV1           = "query.sync.request-controls-v1"
	GapQueryTerminalPersistenceV1           = "query.terminal-persistence-v1"
	GapQueryExactReplayExtensionV1          = "query.jobs.exact-replay-extension-v1"
	GapQueryUnsupportedOptionsV1            = "query.options.unsupported-v1"
	GapQueryDDLCatalogSyncV1                = "query.ddl.catalog-sync-v1"
	GapQueryScriptsUnsupportedV1            = "query.scripts.unsupported-v1"
)

type JobReference struct {
	ProjectID string
	JobID     string
	Location  string
}

var (
	// JobReference.jobId accepts letters, digits, underscores, and dashes and
	// is limited to 1,024 characters by the REST contract. Location values use
	// the documented region/multi-region token shape. Keeping both patterns at
	// the domain boundary also prevents control characters from entering
	// composite repository keys.
	// https://cloud.google.com/bigquery/docs/reference/rest/v2/JobReference
	jobIDPattern       = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	jobLocationPattern = regexp.MustCompile(`^[A-Za-z0-9-]+$`)
	labelKeyPattern    = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)
	labelValuePattern  = regexp.MustCompile(`^[a-z0-9_-]*$`)
)

func (reference JobReference) Validate() error {
	if !projectIDPattern.MatchString(reference.ProjectID) ||
		len(reference.JobID) > 1024 || len(reference.Location) > 1024 ||
		!jobIDPattern.MatchString(reference.JobID) ||
		!jobLocationPattern.MatchString(reference.Location) {
		return fmt.Errorf("%w: invalid projectId, location, or jobId in jobReference", ErrInvalid)
	}
	return nil
}

func ValidateJobListScope(projectID, location string) error {
	if !projectIDPattern.MatchString(projectID) || (location != "" && !jobLocationPattern.MatchString(location)) {
		return fmt.Errorf("%w: invalid projectId or location in jobs.list scope", ErrInvalid)
	}
	return nil
}

type WriteDisposition string

const (
	WriteAppend   WriteDisposition = "WRITE_APPEND"
	WriteEmpty    WriteDisposition = "WRITE_EMPTY"
	WriteTruncate WriteDisposition = "WRITE_TRUNCATE"
)

type CreateDisposition string

const (
	CreateIfNeeded CreateDisposition = "CREATE_IF_NEEDED"
	CreateNever    CreateDisposition = "CREATE_NEVER"
)

type QueryPriority string

const (
	QueryPriorityInteractive QueryPriority = "INTERACTIVE"
	QueryPriorityBatch       QueryPriority = "BATCH"
)

type TableReference struct {
	ProjectID string
	DatasetID string
	TableID   string
}

// QueryConfiguration is the stable application representation of
// JobConfigurationQuery. The defaults below are protocol defaults, not DuckDB
// policy: existing destinations default to WRITE_EMPTY and missing destinations
// default to CREATE_IF_NEEDED.
// https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery
type QueryConfiguration struct {
	SQL               string
	DefaultProjectID  string
	DefaultDataset    string
	Destination       *TableReference
	WriteDisposition  WriteDisposition
	CreateDisposition CreateDisposition
	Priority          QueryPriority
	Labels            map[string]string
	// AnonymousDestination is application-generated output metadata. It is not
	// accepted from the REST request, but causes the completed job to expose the
	// generated destinationTable as BigQuery does for cached query results.
	// https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery
	AnonymousDestination bool
}

type JobError struct {
	Reason  string
	Message string
}

// Column is retained as a source-compatible name for the canonical recursive
// schema field used by catalog, query, load, and Storage APIs.
type Column = Field

type QueryResult struct {
	Columns      []Column
	Rows         [][]any
	AffectedRows int64
}

type Job struct {
	Reference           JobReference
	Query               string
	Configuration       QueryConfiguration
	ConfigurationDigest string
	State               JobState
	Error               *JobError
	Result              *QueryResult
	CreatedAt           time.Time
	StartedAt           *time.Time
	EndedAt             *time.Time
}

func NewQueryJob(reference JobReference, query string, now time.Time) (*Job, error) {
	return NewConfiguredQueryJob(reference, QueryConfiguration{SQL: query}, now)
}

func NewConfiguredQueryJob(reference JobReference, configuration QueryConfiguration, now time.Time) (*Job, error) {
	if reference.Location == "" {
		// US is BigQuery's documented default multi-region for callers that use
		// the domain constructor directly. Runtime composition supplies its
		// configured default before this boundary.
		// https://cloud.google.com/bigquery/docs/locations
		reference.Location = "US"
	}
	configuration = normalizeQueryConfiguration(configuration)
	if err := validateQueryJob(reference, configuration); err != nil {
		return nil, err
	}
	digest, err := QueryConfigurationDigest(reference, configuration)
	if err != nil {
		return nil, err
	}
	return &Job{
		Reference: reference, Query: configuration.SQL, Configuration: configuration,
		ConfigurationDigest: digest, State: JobPending, CreatedAt: now.UTC(),
	}, nil
}

func normalizeQueryConfiguration(configuration QueryConfiguration) QueryConfiguration {
	configuration.SQL = strings.TrimSpace(configuration.SQL)
	if configuration.Priority == "" {
		configuration.Priority = QueryPriorityInteractive
	} else {
		configuration.Priority = QueryPriority(strings.ToUpper(string(configuration.Priority)))
	}
	if configuration.Labels != nil {
		labels := make(map[string]string, len(configuration.Labels))
		for key, value := range configuration.Labels {
			labels[key] = value
		}
		configuration.Labels = labels
	}
	if configuration.Destination != nil {
		destination := *configuration.Destination
		configuration.Destination = &destination
		if configuration.WriteDisposition == "" {
			configuration.WriteDisposition = WriteEmpty
		} else {
			configuration.WriteDisposition = WriteDisposition(strings.ToUpper(string(configuration.WriteDisposition)))
		}
		if configuration.CreateDisposition == "" {
			configuration.CreateDisposition = CreateIfNeeded
		} else {
			configuration.CreateDisposition = CreateDisposition(strings.ToUpper(string(configuration.CreateDisposition)))
		}
	}
	return configuration
}

func validateQueryJob(reference JobReference, configuration QueryConfiguration) error {
	if err := reference.Validate(); err != nil {
		return err
	}
	if configuration.SQL == "" {
		return fmt.Errorf("%w: query is required", ErrInvalid)
	}
	if configuration.Priority != QueryPriorityInteractive && configuration.Priority != QueryPriorityBatch {
		return fmt.Errorf("%w: query priority must be INTERACTIVE or BATCH", ErrInvalid)
	}
	if err := validateQueryLabels(configuration.Labels); err != nil {
		return err
	}
	if configuration.DefaultDataset == "" && configuration.DefaultProjectID != "" {
		return fmt.Errorf("%w: defaultDataset.projectId requires defaultDataset.datasetId", ErrInvalid)
	}
	if configuration.DefaultDataset != "" {
		if !resourceIDPattern.MatchString(configuration.DefaultDataset) {
			return fmt.Errorf("%w: invalid defaultDataset.datasetId", ErrInvalid)
		}
		if configuration.DefaultProjectID != "" && !projectIDPattern.MatchString(configuration.DefaultProjectID) {
			return fmt.Errorf("%w: invalid defaultDataset.projectId", ErrInvalid)
		}
	}
	if configuration.Destination == nil {
		if configuration.AnonymousDestination {
			return fmt.Errorf("%w: anonymous query destination metadata requires destinationTable", ErrInvalid)
		}
		if configuration.WriteDisposition != "" || configuration.CreateDisposition != "" {
			return fmt.Errorf("%w: writeDisposition and createDisposition require destinationTable", ErrInvalid)
		}
		return nil
	}
	destination := *configuration.Destination
	if destination.ProjectID == "" || destination.DatasetID == "" || destination.TableID == "" {
		return fmt.Errorf("%w: destinationTable projectId, datasetId, and tableId are required", ErrInvalid)
	}
	if !projectIDPattern.MatchString(destination.ProjectID) ||
		!resourceIDPattern.MatchString(destination.DatasetID) ||
		!resourceIDPattern.MatchString(destination.TableID) {
		return fmt.Errorf("%w: invalid destinationTable reference", ErrInvalid)
	}
	switch configuration.WriteDisposition {
	case WriteAppend, WriteEmpty, WriteTruncate:
	default:
		return fmt.Errorf("%w: unknown writeDisposition %q", ErrInvalid, configuration.WriteDisposition)
	}
	switch configuration.CreateDisposition {
	case CreateIfNeeded, CreateNever:
	default:
		return fmt.Errorf("%w: unknown createDisposition %q", ErrInvalid, configuration.CreateDisposition)
	}
	if configuration.AnonymousDestination &&
		(configuration.WriteDisposition != WriteEmpty || configuration.CreateDisposition != CreateIfNeeded) {
		return fmt.Errorf("%w: anonymous query destinations require WRITE_EMPTY and CREATE_IF_NEEDED", ErrInvalid)
	}
	return nil
}

// Query-job labels use the same key/value constraints as BigQuery resource
// labels. An empty, non-nil map is valid and remains distinguishable from an
// omitted labels field for connector request/response round trips.
// https://cloud.google.com/bigquery/docs/adding-labels#requirements
func validateQueryLabels(labels map[string]string) error {
	if len(labels) > 64 {
		return fmt.Errorf("%w: query job labels exceed 64 entries", ErrInvalid)
	}
	for key, value := range labels {
		if len(key) > 63 || len(value) > 63 || !labelKeyPattern.MatchString(key) || !labelValuePattern.MatchString(value) {
			return fmt.Errorf("%w: invalid query job label key or value", ErrInvalid)
		}
	}
	return nil
}

// QueryConfigurationDigest is persisted with the job identity for safe drift
// diagnostics. Reuse of (project, location, jobId) is rejected with duplicate
// even when this digest matches, following BigQuery's at-most-once contract.
// https://cloud.google.com/bigquery/docs/reliability-intro#retry_failed_job_insertions
func QueryConfigurationDigest(reference JobReference, configuration QueryConfiguration) (string, error) {
	normalized := normalizeQueryConfiguration(configuration)
	payload, err := json.Marshal(struct {
		ProjectID     string
		Location      string
		Configuration QueryConfiguration
	}{ProjectID: reference.ProjectID, Location: strings.ToUpper(reference.Location), Configuration: normalized})
	if err != nil {
		return "", fmt.Errorf("%w: encode query configuration fingerprint: %v", ErrInvalid, err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func (j *Job) Start(now time.Time) error {
	if j.State != JobPending {
		return fmt.Errorf("%w: cannot start job in state %s", ErrConflict, j.State)
	}
	j.State = JobRunning
	j.StartedAt = &now
	return nil
}

func (j *Job) Complete(result QueryResult, now time.Time) error {
	if j.State != JobRunning {
		return fmt.Errorf("%w: cannot complete job in state %s", ErrConflict, j.State)
	}
	j.State = JobDone
	j.Result = &result
	j.EndedAt = &now
	return nil
}

// Fail intentionally transitions RUNNING to DONE. This mirrors the REST wire
// contract where status.state remains DONE and status.errorResult carries the
// terminal failure; there is no separate FAILED state.
func (j *Job) Fail(reason, message string, now time.Time) error {
	if j.State != JobRunning {
		return fmt.Errorf("%w: cannot fail job in state %s", ErrConflict, j.State)
	}
	j.State = JobDone
	j.Error = &JobError{Reason: reason, Message: message}
	j.EndedAt = &now
	return nil
}
