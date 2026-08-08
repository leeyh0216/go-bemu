package rest

// Official job/query wire resources: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

type queryRequest struct {
	Query              string             `json:"query"`
	UseLegacySQL       bool               `json:"useLegacySql"`
	MaxResults         int                `json:"maxResults,omitempty"`
	DefaultDataset     *datasetReference  `json:"defaultDataset,omitempty"`
	DestinationTable   *tableReference    `json:"destinationTable,omitempty"`
	WriteDisposition   string             `json:"writeDisposition,omitempty"`
	CreateDisposition  string             `json:"createDisposition,omitempty"`
	Location           string             `json:"location,omitempty"`
	RequestID          string             `json:"requestId,omitempty"`
	TimeoutMs          *int64             `json:"timeoutMs,omitempty"`
	JobTimeoutMs       json.RawMessage    `json:"jobTimeoutMs,omitempty"`
	FormatOptions      *dataFormatOptions `json:"formatOptions,omitempty"`
	DryRun             json.RawMessage    `json:"dryRun,omitempty"`
	Priority           json.RawMessage    `json:"priority,omitempty"`
	ParameterMode      json.RawMessage    `json:"parameterMode,omitempty"`
	QueryParameters    json.RawMessage    `json:"queryParameters,omitempty"`
	Labels             json.RawMessage    `json:"labels,omitempty"`
	UseQueryCache      json.RawMessage    `json:"useQueryCache,omitempty"`
	MaximumBytesBilled json.RawMessage    `json:"maximumBytesBilled,omitempty"`
}

// DataFormatOptions is emitted by google-cloud-bigquery's query_and_wait
// helper. The current query row encoder has no timestamp-specific branch, so
// this option is accepted as wire-compatible metadata and covered by the type
// matrix before timestamp rows are advertised as fully compatible.
// https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#dataformatoptions
type dataFormatOptions struct {
	UseInt64Timestamp bool `json:"useInt64Timestamp,omitempty"`
}

type jobReferenceResource struct {
	ProjectID string `json:"projectId"`
	JobID     string `json:"jobId"`
	Location  string `json:"location,omitempty"`
}

type jobStatusResource struct {
	State       domain.JobState `json:"state"`
	ErrorResult *errorProto     `json:"errorResult,omitempty"`
	Errors      []errorProto    `json:"errors,omitempty"`
}

type jobConfigurationQuery struct {
	Query              string            `json:"query"`
	UseLegacySQL       bool              `json:"useLegacySql"`
	DefaultDataset     *datasetReference `json:"defaultDataset,omitempty"`
	DestinationTable   *tableReference   `json:"destinationTable,omitempty"`
	WriteDisposition   string            `json:"writeDisposition,omitempty"`
	CreateDisposition  string            `json:"createDisposition,omitempty"`
	Priority           string            `json:"priority,omitempty"`
	ParameterMode      json.RawMessage   `json:"parameterMode,omitempty"`
	QueryParameters    json.RawMessage   `json:"queryParameters,omitempty"`
	UseQueryCache      json.RawMessage   `json:"useQueryCache,omitempty"`
	MaximumBytesBilled json.RawMessage   `json:"maximumBytesBilled,omitempty"`
}

type jobConfiguration struct {
	Query        *jobConfigurationQuery `json:"query,omitempty"`
	DryRun       json.RawMessage        `json:"dryRun,omitempty"`
	JobTimeoutMs json.RawMessage        `json:"jobTimeoutMs,omitempty"`
	Labels       *map[string]string     `json:"labels,omitempty"`
}

type jobStatistics struct {
	CreationTime       string             `json:"creationTime,omitempty"`
	StartTime          string             `json:"startTime,omitempty"`
	EndTime            string             `json:"endTime,omitempty"`
	NumDMLAffectedRows string             `json:"numDmlAffectedRows,omitempty"`
	Query              jobQueryStatistics `json:"query"`
}

type jobQueryStatistics struct {
	StatementType       string `json:"statementType"`
	TotalBytesProcessed string `json:"totalBytesProcessed"`
	TotalBytesBilled    string `json:"totalBytesBilled"`
	CacheHit            bool   `json:"cacheHit"`
	NumDMLAffectedRows  string `json:"numDmlAffectedRows,omitempty"`
}

type jobResource struct {
	Kind          string               `json:"kind"`
	JobReference  jobReferenceResource `json:"jobReference"`
	Configuration jobConfiguration     `json:"configuration"`
	Status        jobStatusResource    `json:"status"`
	Statistics    jobStatistics        `json:"statistics"`
}

type tableCell struct {
	Value any `json:"v"`
}

type tableRow struct {
	Fields []tableCell `json:"f"`
}

type queryResponse struct {
	Kind               string               `json:"kind"`
	JobReference       jobReferenceResource `json:"jobReference"`
	JobComplete        bool                 `json:"jobComplete"`
	Schema             *tableSchema         `json:"schema,omitempty"`
	Rows               []tableRow           `json:"rows,omitempty"`
	TotalRows          string               `json:"totalRows,omitempty"`
	PageToken          string               `json:"pageToken,omitempty"`
	NumDMLAffectedRows string               `json:"numDmlAffectedRows,omitempty"`
	Errors             []errorProto         `json:"errors,omitempty"`
}

func jobFromDomain(job *domain.Job) jobResource {
	query := job.Configuration
	if query.SQL == "" {
		query.SQL = job.Query
	}
	wireQuery := &jobConfigurationQuery{
		Query: query.SQL, UseLegacySQL: false,
		WriteDisposition: string(query.WriteDisposition), CreateDisposition: string(query.CreateDisposition),
		Priority: string(query.Priority),
	}
	if query.DefaultDataset != "" {
		projectID := query.DefaultProjectID
		if projectID == "" {
			projectID = job.Reference.ProjectID
		}
		wireQuery.DefaultDataset = &datasetReference{ProjectID: projectID, DatasetID: query.DefaultDataset}
	}
	if query.Destination != nil {
		wireQuery.DestinationTable = &tableReference{
			ProjectID: query.Destination.ProjectID, DatasetID: query.Destination.DatasetID, TableID: query.Destination.TableID,
		}
	}
	wireConfiguration := jobConfiguration{Query: wireQuery}
	if query.Labels != nil {
		labels := make(map[string]string, len(query.Labels))
		for key, value := range query.Labels {
			labels[key] = value
		}
		wireConfiguration.Labels = &labels
	}
	resource := jobResource{
		Kind: "bigquery#job",
		JobReference: jobReferenceResource{
			ProjectID: job.Reference.ProjectID, JobID: job.Reference.JobID, Location: job.Reference.Location,
		},
		Configuration: wireConfiguration,
		Status:        jobStatusResource{State: job.State},
		Statistics: jobStatistics{
			CreationTime: millis(job.CreatedAt),
			Query: jobQueryStatistics{
				StatementType: queryStatementType(query), TotalBytesProcessed: "0", TotalBytesBilled: "0",
			},
		},
	}
	if job.StartedAt != nil {
		resource.Statistics.StartTime = millis(*job.StartedAt)
	}
	if job.EndedAt != nil {
		resource.Statistics.EndTime = millis(*job.EndedAt)
	}
	if job.Result != nil && job.Result.AffectedRows != 0 {
		resource.Statistics.NumDMLAffectedRows = strconv.FormatInt(job.Result.AffectedRows, 10)
		resource.Statistics.Query.NumDMLAffectedRows = strconv.FormatInt(job.Result.AffectedRows, 10)
	}
	if job.Error != nil {
		jobError := errorProto{Reason: job.Error.Reason, Message: job.Error.Message}
		resource.Status.ErrorResult = &jobError
		resource.Status.Errors = []errorProto{jobError}
	}
	return resource
}

func queryStatementType(query domain.QueryConfiguration) string {
	return query.StatementType
}

func queryResponseFromDomain(job *domain.Job, startIndex, endIndex int, nextPageToken string) (queryResponse, error) {
	response := queryResponse{
		Kind: "bigquery#getQueryResultsResponse",
		JobReference: jobReferenceResource{
			ProjectID: job.Reference.ProjectID, JobID: job.Reference.JobID, Location: job.Reference.Location,
		},
		JobComplete: job.State == domain.JobDone,
	}
	if job.Error != nil {
		response.Errors = []errorProto{{Reason: job.Error.Reason, Message: job.Error.Message}}
	}
	if job.Result == nil {
		return response, nil
	}
	if job.Result.RowsUnavailable {
		return queryResponse{}, queryResultPayloadUnavailableError()
	}
	fields := fieldsFromDomain(job.Result.Columns)
	if len(fields) > 0 {
		response.Schema = &tableSchema{Fields: fields}
	}
	if startIndex > len(job.Result.Rows) {
		startIndex = len(job.Result.Rows)
	}
	if endIndex < startIndex || endIndex > len(job.Result.Rows) {
		endIndex = len(job.Result.Rows)
	}
	response.Rows = make([]tableRow, 0, endIndex-startIndex)
	for _, row := range job.Result.Rows[startIndex:endIndex] {
		if len(row) != len(job.Result.Columns) {
			return queryResponse{}, fmt.Errorf("query result row has %d values for %d fields", len(row), len(job.Result.Columns))
		}
		cells := make([]tableCell, len(row))
		for i, value := range row {
			encoded, err := tableDataValue(job.Result.Columns[i], value, tableDataFormatOptions{})
			if err != nil {
				return queryResponse{}, fmt.Errorf("encode query result field %q: %w", job.Result.Columns[i].Name, err)
			}
			cells[i] = tableCell{Value: encoded}
		}
		response.Rows = append(response.Rows, tableRow{Fields: cells})
	}
	totalRows := job.Result.TotalRows
	if totalRows == 0 {
		totalRows = int64(len(job.Result.Rows))
	}
	response.TotalRows = strconv.FormatInt(totalRows, 10)
	response.PageToken = nextPageToken
	if job.Result.AffectedRows != 0 {
		response.NumDMLAffectedRows = strconv.FormatInt(job.Result.AffectedRows, 10)
	}
	return response, nil
}

func queryResultPayloadUnavailableError() error {
	return fmt.Errorf("%w: query result row payload is unavailable after emulator restart; capability=%s",
		domain.ErrBackend, domain.GapQueryRestartResultPayloadV1)
}

func encodeCell(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case []byte:
		return base64.StdEncoding.EncodeToString(typed)
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano)
	case bool:
		return strconv.FormatBool(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case int:
		return strconv.Itoa(typed)
	case float64:
		return encodeRESTFloat(typed, 64)
	case float32:
		return encodeRESTFloat(float64(typed), 32)
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}

// BigQuery's REST type contract uses exact, case-sensitive string tokens for
// non-finite FLOAT64 values. Go's strconv spellings (+Inf and -Inf) are not wire
// compatible, so every REST row encoder crosses this shared conversion boundary.
// https://cloud.google.com/bigquery/docs/reference/rest/v2/StandardSqlDataType
func encodeRESTFloat(value float64, bitSize int) any {
	switch {
	case math.IsNaN(value):
		return "NaN"
	case math.IsInf(value, 1):
		return "Infinity"
	case math.IsInf(value, -1):
		return "-Infinity"
	default:
		if bitSize == 32 {
			return float32(value)
		}
		return value
	}
}
