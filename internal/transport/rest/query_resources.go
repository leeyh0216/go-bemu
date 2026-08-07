package rest

// Official job/query wire resources: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

type queryRequest struct {
	Query          string            `json:"query"`
	UseLegacySQL   bool              `json:"useLegacySql"`
	MaxResults     int               `json:"maxResults,omitempty"`
	DefaultDataset *datasetReference `json:"defaultDataset,omitempty"`
	Location       string            `json:"location,omitempty"`
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
	Query          string            `json:"query"`
	UseLegacySQL   bool              `json:"useLegacySql"`
	DefaultDataset *datasetReference `json:"defaultDataset,omitempty"`
}

type jobConfiguration struct {
	Query *jobConfigurationQuery `json:"query,omitempty"`
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
	NumDMLAffectedRows string               `json:"numDmlAffectedRows,omitempty"`
	Errors             []errorProto         `json:"errors,omitempty"`
}

func jobFromDomain(job *domain.Job) jobResource {
	resource := jobResource{
		Kind: "bigquery#job",
		JobReference: jobReferenceResource{
			ProjectID: job.Reference.ProjectID, JobID: job.Reference.JobID, Location: job.Reference.Location,
		},
		Configuration: jobConfiguration{Query: &jobConfigurationQuery{Query: job.Query, UseLegacySQL: false}},
		Status:        jobStatusResource{State: job.State},
		Statistics: jobStatistics{
			CreationTime: millis(job.CreatedAt),
			Query: jobQueryStatistics{
				StatementType: statementType(job.Query), TotalBytesProcessed: "0", TotalBytesBilled: "0",
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

func statementType(sql string) string {
	fields := strings.Fields(strings.ToUpper(strings.TrimSpace(sql)))
	if len(fields) == 0 {
		return "SELECT"
	}
	if fields[0] == "WITH" {
		return "SELECT"
	}
	if fields[0] == "CREATE" && len(fields) > 1 {
		return "CREATE_" + fields[1]
	}
	if fields[0] == "ALTER" && len(fields) > 1 {
		return "ALTER_" + fields[1]
	}
	if fields[0] == "DROP" && len(fields) > 1 {
		return "DROP_" + fields[1]
	}
	return fields[0]
}

func queryResponseFromDomain(job *domain.Job, maxResults, startIndex int) queryResponse {
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
		return response
	}
	fields := make([]tableFieldSchema, len(job.Result.Columns))
	for i, column := range job.Result.Columns {
		fields[i] = tableFieldSchema{Name: column.Name, Type: column.Type, Mode: "NULLABLE"}
	}
	if len(fields) > 0 {
		response.Schema = &tableSchema{Fields: fields}
	}
	if startIndex < 0 {
		startIndex = 0
	}
	if startIndex > len(job.Result.Rows) {
		startIndex = len(job.Result.Rows)
	}
	end := len(job.Result.Rows)
	if maxResults > 0 && startIndex+maxResults < end {
		end = startIndex + maxResults
	}
	response.Rows = make([]tableRow, 0, end-startIndex)
	for _, row := range job.Result.Rows[startIndex:end] {
		cells := make([]tableCell, len(row))
		for i, value := range row {
			cells[i] = tableCell{Value: encodeCell(value)}
		}
		response.Rows = append(response.Rows, tableRow{Fields: cells})
	}
	response.TotalRows = strconv.Itoa(len(job.Result.Rows))
	if job.Result.AffectedRows != 0 {
		response.NumDMLAffectedRows = strconv.FormatInt(job.Result.AffectedRows, 10)
	}
	return response
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
		return strconv.FormatFloat(typed, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(typed), 'g', -1, 32)
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}
