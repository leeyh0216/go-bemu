package domain

// Load-job wire and state semantics:
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/JobStatus

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
)

type JobState string

const (
	JobPending JobState = "PENDING"
	JobRunning JobState = "RUNNING"
	JobDone    JobState = "DONE"
)

type SourceFormat string

const (
	FormatCSV                  SourceFormat = "CSV"
	FormatNewlineDelimitedJSON SourceFormat = "NEWLINE_DELIMITED_JSON"
	FormatAvro                 SourceFormat = "AVRO"
	FormatParquet              SourceFormat = "PARQUET"
	FormatORC                  SourceFormat = "ORC"
)

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

type JobReference struct {
	ProjectID string
	Location  string
	JobID     string
}

type TableReference struct {
	ProjectID string
	DatasetID string
	TableID   string
}

type Field = catalogdomain.Field

type Table struct {
	Reference TableReference
	Location  string
	Schema    []Field
}

type LoadConfiguration struct {
	SourceURIs          []string
	Destination         TableReference
	SourceFormat        SourceFormat
	WriteDisposition    WriteDisposition
	CreateDisposition   CreateDisposition
	Schema              []Field
	Autodetect          bool
	SchemaUpdateOptions []string
	IgnoreUnknownValues bool
	MaxBadRecords       int64
	UnsupportedOptions  []string
}

type JobError struct {
	Reason  string
	Message string
}

type Statistics struct {
	InputFiles int64
	InputBytes int64
	// OutputBytes is the REST statistics.load.outputBytes value. The current
	// local loader approximates it with the successful job's input byte total.
	// https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobStatistics3
	OutputBytes int64
	OutputRows  int64
}

type Job struct {
	Reference           JobReference
	Configuration       LoadConfiguration
	ConfigurationDigest string
	State               JobState
	Error               *JobError
	Statistics          Statistics
	CreatedAt           time.Time
	StartedAt           *time.Time
	EndedAt             *time.Time
}

func NewJob(reference JobReference, configuration LoadConfiguration, now time.Time) (*Job, error) {
	configuration = normalizeConfiguration(configuration)
	if err := validateReference(reference); err != nil {
		return nil, err
	}
	if err := ValidateConfiguration(configuration); err != nil {
		return nil, err
	}
	digest, err := ConfigurationDigest(configuration)
	if err != nil {
		return nil, err
	}
	return &Job{
		Reference: reference, Configuration: configuration, ConfigurationDigest: digest,
		State: JobPending, CreatedAt: now.UTC(),
	}, nil
}

func normalizeConfiguration(configuration LoadConfiguration) LoadConfiguration {
	if configuration.SourceFormat == "" {
		configuration.SourceFormat = FormatCSV
	} else {
		configuration.SourceFormat = SourceFormat(strings.ToUpper(string(configuration.SourceFormat)))
	}
	if configuration.WriteDisposition == "" {
		configuration.WriteDisposition = WriteAppend
	} else {
		configuration.WriteDisposition = WriteDisposition(strings.ToUpper(string(configuration.WriteDisposition)))
	}
	if configuration.CreateDisposition == "" {
		configuration.CreateDisposition = CreateIfNeeded
	} else {
		configuration.CreateDisposition = CreateDisposition(strings.ToUpper(string(configuration.CreateDisposition)))
	}
	configuration.SourceURIs = append([]string(nil), configuration.SourceURIs...)
	configuration.Schema = cloneFields(configuration.Schema)
	configuration.SchemaUpdateOptions = append([]string(nil), configuration.SchemaUpdateOptions...)
	configuration.UnsupportedOptions = append([]string(nil), configuration.UnsupportedOptions...)
	return configuration
}

func ValidateConfiguration(configuration LoadConfiguration) error {
	if len(configuration.SourceURIs) == 0 {
		return fmt.Errorf("%w: sourceUris must contain at least one URI", ErrInvalid)
	}
	for _, rawURI := range configuration.SourceURIs {
		if strings.TrimSpace(rawURI) == "" {
			return fmt.Errorf("%w: sourceUris cannot contain an empty URI", ErrInvalid)
		}
	}
	if configuration.Destination.ProjectID == "" || configuration.Destination.DatasetID == "" || configuration.Destination.TableID == "" {
		return fmt.Errorf("%w: destinationTable projectId, datasetId, and tableId are required", ErrInvalid)
	}
	switch configuration.SourceFormat {
	case FormatCSV, FormatNewlineDelimitedJSON, FormatAvro, FormatParquet, FormatORC:
	default:
		return fmt.Errorf("%w: unknown sourceFormat %q", ErrInvalid, configuration.SourceFormat)
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
	if configuration.MaxBadRecords < 0 {
		return fmt.Errorf("%w: maxBadRecords must not be negative", ErrInvalid)
	}
	if err := ValidateSchema(configuration.Schema); err != nil {
		return err
	}
	return nil
}

func ValidateSchema(fields []Field) error {
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if err := field.Validate(); err != nil {
			switch {
			case errors.Is(err, catalogdomain.ErrUnsupported):
				return fmt.Errorf("%w: %v", ErrUnsupported, err)
			default:
				return fmt.Errorf("%w: %v", ErrInvalid, err)
			}
		}
		key := strings.ToLower(field.Name)
		if _, exists := seen[key]; exists {
			return fmt.Errorf("%w: duplicate schema field %q", ErrInvalid, field.Name)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func ConfigurationDigest(configuration LoadConfiguration) (string, error) {
	normalized := normalizeConfiguration(configuration)
	payload, err := json.Marshal(normalized)
	if err != nil {
		return "", fmt.Errorf("%w: encode load configuration fingerprint: %v", ErrInvalid, err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func (j *Job) Start(now time.Time) error {
	if j.State != JobPending {
		return fmt.Errorf("%w: cannot start load job in state %s", ErrConflict, j.State)
	}
	value := now.UTC()
	j.StartedAt = &value
	j.State = JobRunning
	return nil
}

func (j *Job) Complete(statistics Statistics, now time.Time) error {
	if j.State != JobRunning {
		return fmt.Errorf("%w: cannot complete load job in state %s", ErrConflict, j.State)
	}
	value := now.UTC()
	j.EndedAt = &value
	j.Statistics = statistics
	j.State = JobDone
	j.Error = nil
	return nil
}

func (j *Job) Fail(reason, message string, statistics Statistics, now time.Time) error {
	if j.State != JobRunning {
		return fmt.Errorf("%w: cannot fail load job in state %s", ErrConflict, j.State)
	}
	value := now.UTC()
	j.EndedAt = &value
	j.Statistics = statistics
	j.State = JobDone
	j.Error = &JobError{Reason: reason, Message: message}
	return nil
}

func (j *Job) Clone() *Job {
	if j == nil {
		return nil
	}
	copyJob := *j
	copyJob.Configuration = normalizeConfiguration(j.Configuration)
	if j.Error != nil {
		value := *j.Error
		copyJob.Error = &value
	}
	if j.StartedAt != nil {
		value := *j.StartedAt
		copyJob.StartedAt = &value
	}
	if j.EndedAt != nil {
		value := *j.EndedAt
		copyJob.EndedAt = &value
	}
	return &copyJob
}

func cloneFields(fields []Field) []Field {
	return catalogdomain.CloneFields(fields)
}

func validateReference(reference JobReference) error {
	if reference.ProjectID == "" || reference.Location == "" || reference.JobID == "" {
		return fmt.Errorf("%w: projectId, location, and jobId are required", ErrInvalid)
	}
	return nil
}
