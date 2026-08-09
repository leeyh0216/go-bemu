package rest

// Official load-job JSON shape:
// https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad

import (
	"encoding/json"
	"strconv"

	loadDomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
)

type combinedJobProbe struct {
	Configuration struct {
		Query json.RawMessage `json:"query"`
		Load  json.RawMessage `json:"load"`
	} `json:"configuration"`
}

type loadConfigurationResource struct {
	SourceURIs               []string                `json:"sourceUris"`
	DestinationTable         tableReference          `json:"destinationTable"`
	Schema                   *tableSchema            `json:"schema,omitempty"`
	SourceFormat             string                  `json:"sourceFormat,omitempty"`
	WriteDisposition         string                  `json:"writeDisposition,omitempty"`
	CreateDisposition        string                  `json:"createDisposition,omitempty"`
	Autodetect               bool                    `json:"autodetect,omitempty"`
	SchemaUpdateOptions      []string                `json:"schemaUpdateOptions,omitempty"`
	IgnoreUnknownValues      bool                    `json:"ignoreUnknownValues,omitempty"`
	MaxBadRecords            int64                   `json:"maxBadRecords,omitempty"`
	ParquetOptions           *parquetOptionsResource `json:"parquetOptions,omitempty"`
	DecimalTargetTypes       []string                `json:"decimalTargetTypes,omitempty"`
	NullMarkers              []string                `json:"nullMarkers,omitempty"`
	ProjectionFields         []string                `json:"projectionFields,omitempty"`
	TimestampTargetPrecision []int32                 `json:"timestampTargetPrecision,omitempty"`
}

// ParquetOptions is an optional, typed part of JobConfigurationLoad. The
// emulator accepts only the empty/default shape until the DuckDB load adapter
// implements the non-default inference behavior.
// https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#ParquetOptions
type parquetOptionsResource struct {
	EnableListInference *bool   `json:"enableListInference,omitempty"`
	EnumAsString        *bool   `json:"enumAsString,omitempty"`
	MapTargetType       *string `json:"mapTargetType,omitempty"`
}

type loadJobRequest struct {
	JobReference  jobReferenceResource `json:"jobReference"`
	Configuration struct {
		Load   json.RawMessage    `json:"load"`
		Labels *map[string]string `json:"labels,omitempty"`
	} `json:"configuration"`
}

type loadJobConfigurationResource struct {
	Load   loadConfigurationResource `json:"load"`
	Labels *map[string]string        `json:"labels,omitempty"`
}

type loadJobStatusResource struct {
	State       loadDomain.JobState `json:"state"`
	ErrorResult *errorProto         `json:"errorResult,omitempty"`
	Errors      []errorProto        `json:"errors,omitempty"`
}

type loadJobStatisticsResource struct {
	CreationTime string                `json:"creationTime,omitempty"`
	StartTime    string                `json:"startTime,omitempty"`
	EndTime      string                `json:"endTime,omitempty"`
	Load         loadJobStatisticsLoad `json:"load"`
}

type loadJobStatisticsLoad struct {
	InputFiles     string `json:"inputFiles"`
	InputFileBytes string `json:"inputFileBytes"`
	OutputBytes    string `json:"outputBytes,omitempty"`
	OutputRows     string `json:"outputRows"`
	BadRecords     string `json:"badRecords"`
}

type loadJobResource struct {
	Kind          string                       `json:"kind"`
	JobReference  jobReferenceResource         `json:"jobReference"`
	Configuration loadJobConfigurationResource `json:"configuration"`
	Status        loadJobStatusResource        `json:"status"`
	Statistics    loadJobStatisticsResource    `json:"statistics"`
}

func loadJobFromDomain(job *loadDomain.Job) loadJobResource {
	configuration := job.Configuration
	load := loadConfigurationResource{
		SourceURIs: append([]string(nil), configuration.SourceURIs...),
		DestinationTable: tableReference{
			ProjectID: configuration.Destination.ProjectID,
			DatasetID: configuration.Destination.DatasetID,
			TableID:   configuration.Destination.TableID,
		},
		SourceFormat: string(configuration.SourceFormat), WriteDisposition: string(configuration.WriteDisposition),
		CreateDisposition: string(configuration.CreateDisposition), Autodetect: configuration.Autodetect,
		SchemaUpdateOptions: append([]string(nil), configuration.SchemaUpdateOptions...),
		IgnoreUnknownValues: configuration.IgnoreUnknownValues, MaxBadRecords: configuration.MaxBadRecords,
	}
	if len(configuration.Schema) > 0 {
		load.Schema = &tableSchema{Fields: loadFieldsToWire(configuration.Schema)}
	}
	resource := loadJobResource{
		Kind: "bigquery#job",
		JobReference: jobReferenceResource{
			ProjectID: job.Reference.ProjectID, Location: job.Reference.Location, JobID: job.Reference.JobID,
		},
		Configuration: loadJobConfigurationResource{Load: load, Labels: cloneLoadLabels(configuration.Labels)},
		Status:        loadJobStatusResource{State: job.State},
		Statistics: loadJobStatisticsResource{
			CreationTime: millis(job.CreatedAt),
			Load: loadJobStatisticsLoad{
				InputFiles:     strconv.FormatInt(job.Statistics.InputFiles, 10),
				InputFileBytes: strconv.FormatInt(job.Statistics.InputBytes, 10),
				OutputRows:     strconv.FormatInt(job.Statistics.OutputRows, 10), BadRecords: "0",
			},
		},
	}
	// BQEMU publishes its approximated JobStatistics3.outputBytes only for a
	// successful terminal job. Avoid presenting a synthetic zero while a job is
	// pending/running or after a failed load.
	// https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobStatistics3
	if job.State == loadDomain.JobDone && job.Error == nil {
		resource.Statistics.Load.OutputBytes = strconv.FormatInt(job.Statistics.OutputBytes, 10)
	}
	if job.StartedAt != nil {
		resource.Statistics.StartTime = millis(*job.StartedAt)
	}
	if job.EndedAt != nil {
		resource.Statistics.EndTime = millis(*job.EndedAt)
	}
	if job.Error != nil {
		jobError := errorProto{Reason: job.Error.Reason, Message: job.Error.Message}
		resource.Status.ErrorResult = &jobError
		resource.Status.Errors = []errorProto{jobError}
	}
	return resource
}

func cloneLoadLabels(labels map[string]string) *map[string]string {
	if labels == nil {
		return nil
	}
	copy := make(map[string]string, len(labels))
	for key, value := range labels {
		copy[key] = value
	}
	return &copy
}

func loadFieldsToWire(fields []loadDomain.Field) []tableFieldSchema {
	result := make([]tableFieldSchema, len(fields))
	for index, field := range fields {
		result[index] = tableFieldSchema{Name: field.Name, Type: field.Type, Mode: field.Mode, Fields: loadFieldsToWire(field.Fields)}
	}
	return result
}

func loadFieldsFromWire(fields []tableFieldSchema) []loadDomain.Field {
	result := make([]loadDomain.Field, len(fields))
	for index, field := range fields {
		result[index] = loadDomain.Field{Name: field.Name, Type: field.Type, Mode: field.Mode, Fields: loadFieldsFromWire(field.Fields)}
	}
	return result
}
