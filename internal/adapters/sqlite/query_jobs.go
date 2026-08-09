package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/sqlite/sqlcgen"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type queryJobRepository struct {
	queries *sqlcgen.Queries

	mu       sync.RWMutex
	payloads map[string]*domain.QueryResult
}

var _ ports.JobRepository = (*queryJobRepository)(nil)

func newQueryJobRepository(db *sql.DB) *queryJobRepository {
	return &queryJobRepository{queries: sqlcgen.New(db), payloads: make(map[string]*domain.QueryResult)}
}

func (r *queryJobRepository) CreateOrGet(ctx context.Context, job *domain.Job) (*domain.Job, bool, error) {
	values, persistedResult, err := encodeQueryJob(job)
	if err != nil {
		return nil, false, err
	}
	rowsAffected, err := r.queries.CreateQueryJob(ctx, values.createParams())
	if err != nil {
		return nil, false, queryJobRepositoryError(ctx, "create", err)
	}
	if rowsAffected == 0 {
		existing, getErr := r.Get(ctx, job.Reference)
		return existing, false, getErr
	}
	r.cacheResult(job.Reference, persistedResult)
	created, err := r.Get(ctx, job.Reference)
	if err != nil {
		return nil, false, err
	}
	return created, true, nil
}

func (r *queryJobRepository) Update(ctx context.Context, job *domain.Job) error {
	values, persistedResult, err := encodeQueryJob(job)
	if err != nil {
		return err
	}
	rowsAffected, err := r.queries.UpdateQueryJob(ctx, values.updateParams())
	if err != nil {
		return queryJobRepositoryError(ctx, "update", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("%w: query job %s", domain.ErrNotFound, job.Reference.JobID)
	}
	r.cacheResult(job.Reference, persistedResult)
	return nil
}

func (r *queryJobRepository) Get(ctx context.Context, reference domain.JobReference) (*domain.Job, error) {
	key, err := queryJobKey(reference)
	if err != nil {
		return nil, err
	}
	row, err := r.queries.GetQueryJob(ctx, sqlcgen.GetQueryJobParams{
		ProjectID: reference.ProjectID, LocationKey: strings.ToUpper(reference.Location), JobID: reference.JobID,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: query job %s", domain.ErrNotFound, reference.JobID)
	}
	if err != nil {
		return nil, queryJobRepositoryError(ctx, "get", err)
	}
	job, err := decodeGetQueryJob(row)
	if err != nil {
		return nil, queryJobRepositoryError(ctx, "get", err)
	}
	r.mu.RLock()
	payload := cloneQueryResult(r.payloads[key])
	r.mu.RUnlock()
	if payload != nil && job.Result != nil {
		job.Result = payload
	}
	return job, nil
}

func (r *queryJobRepository) List(ctx context.Context, projectID, location string) ([]*domain.Job, error) {
	if err := domain.ValidateJobListScope(projectID, location); err != nil {
		return nil, err
	}
	var jobs []*domain.Job
	if location == "" {
		rows, err := r.queries.ListQueryJobs(ctx, projectID)
		if err != nil {
			return nil, queryJobRepositoryError(ctx, "list", err)
		}
		jobs, err = decodeListedQueryJobs(rows)
		if err != nil {
			return nil, queryJobRepositoryError(ctx, "list", err)
		}
	} else {
		rows, err := r.queries.ListQueryJobsAtLocation(ctx, sqlcgen.ListQueryJobsAtLocationParams{
			ProjectID: projectID, LocationKey: strings.ToUpper(location),
		})
		if err != nil {
			return nil, queryJobRepositoryError(ctx, "list", err)
		}
		jobs, err = decodeListedQueryJobsAtLocation(rows)
		if err != nil {
			return nil, queryJobRepositoryError(ctx, "list", err)
		}
	}
	for _, job := range jobs {
		key, _ := queryJobKey(job.Reference)
		r.mu.RLock()
		payload := cloneQueryResult(r.payloads[key])
		r.mu.RUnlock()
		if payload != nil && job.Result != nil {
			job.Result = payload
		}
	}
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].CreatedAt.Equal(jobs[j].CreatedAt) {
			if jobs[i].Reference.JobID == jobs[j].Reference.JobID {
				return strings.ToUpper(jobs[i].Reference.Location) < strings.ToUpper(jobs[j].Reference.Location)
			}
			return jobs[i].Reference.JobID < jobs[j].Reference.JobID
		}
		return jobs[i].CreatedAt.After(jobs[j].CreatedAt)
	})
	return jobs, nil
}

type queryJobPersistence struct {
	projectID            string
	locationKey          string
	location             string
	jobID                string
	configurationVersion int64
	configurationJSON    string
	configurationDigest  string
	state                string
	errorReason          sql.NullString
	errorMessage         sql.NullString
	createdAt            string
	startedAt            sql.NullString
	endedAt              sql.NullString
	resultPresent        int64
	resultSchemaJSON     sql.NullString
	resultRowCount       int64
	affectedRows         int64
}

func encodeQueryJob(job *domain.Job) (queryJobPersistence, *domain.QueryResult, error) {
	if job == nil {
		return queryJobPersistence{}, nil, fmt.Errorf("%w: query job is required", domain.ErrInvalid)
	}
	if err := job.Reference.Validate(); err != nil {
		return queryJobPersistence{}, nil, err
	}
	validated, err := domain.NewConfiguredQueryJob(job.Reference, job.Configuration, job.CreatedAt)
	if err != nil {
		return queryJobPersistence{}, nil, err
	}
	if validated.ConfigurationDigest != job.ConfigurationDigest {
		return queryJobPersistence{}, nil, fmt.Errorf("%w: query configuration digest does not match job metadata", domain.ErrInvalid)
	}
	configurationJSON, err := json.Marshal(job.Configuration)
	if err != nil {
		return queryJobPersistence{}, nil, fmt.Errorf("%w: encode query job configuration: %v", domain.ErrInvalid, err)
	}
	errorReason, errorMessage := optionalQueryError(job.Error)
	resultPresent, schemaJSON, rowCount, affectedRows := int64(0), sql.NullString{}, int64(0), int64(0)
	var cached *domain.QueryResult
	if job.Result != nil {
		resultPresent = 1
		encodedSchema, marshalErr := json.Marshal(job.Result.Columns)
		if marshalErr != nil {
			return queryJobPersistence{}, nil, fmt.Errorf("%w: encode query result schema: %v", domain.ErrInvalid, marshalErr)
		}
		schemaJSON = sql.NullString{String: string(encodedSchema), Valid: true}
		rowCount = int64(len(job.Result.Rows))
		if job.Result.TotalRows > rowCount {
			rowCount = job.Result.TotalRows
		}
		affectedRows = job.Result.AffectedRows
		if len(job.Result.Rows) > 0 && !job.Result.RowsUnavailable {
			cached = cloneQueryResult(job.Result)
			cached.TotalRows = rowCount
		}
	}
	return queryJobPersistence{
		projectID: job.Reference.ProjectID, locationKey: strings.ToUpper(job.Reference.Location),
		location: job.Reference.Location, jobID: job.Reference.JobID, configurationVersion: 1,
		configurationJSON: string(configurationJSON), configurationDigest: job.ConfigurationDigest,
		state: string(job.State), errorReason: errorReason, errorMessage: errorMessage,
		createdAt: encodeTime(job.CreatedAt), startedAt: optionalTime(job.StartedAt), endedAt: optionalTime(job.EndedAt),
		resultPresent: resultPresent, resultSchemaJSON: schemaJSON, resultRowCount: rowCount, affectedRows: affectedRows,
	}, cached, nil
}

func (values queryJobPersistence) createParams() sqlcgen.CreateQueryJobParams {
	return sqlcgen.CreateQueryJobParams{
		ProjectID: values.projectID, LocationKey: values.locationKey, Location: values.location, JobID: values.jobID,
		ConfigurationVersion: values.configurationVersion, ConfigurationJson: values.configurationJSON,
		ConfigurationDigest: values.configurationDigest, State: values.state,
		ErrorReason: values.errorReason, ErrorMessage: values.errorMessage,
		CreatedAt: values.createdAt, StartedAt: values.startedAt, EndedAt: values.endedAt,
		ResultPresent: values.resultPresent, ResultSchemaJson: values.resultSchemaJSON,
		ResultRowCount: values.resultRowCount, AffectedRows: values.affectedRows,
	}
}

func (values queryJobPersistence) updateParams() sqlcgen.UpdateQueryJobParams {
	return sqlcgen.UpdateQueryJobParams{
		Location: values.location, ConfigurationVersion: values.configurationVersion,
		ConfigurationJson: values.configurationJSON, ConfigurationDigest: values.configurationDigest,
		State: values.state, ErrorReason: values.errorReason, ErrorMessage: values.errorMessage,
		CreatedAt: values.createdAt, StartedAt: values.startedAt, EndedAt: values.endedAt,
		ResultPresent: values.resultPresent, ResultSchemaJson: values.resultSchemaJSON,
		ResultRowCount: values.resultRowCount, AffectedRows: values.affectedRows,
		ProjectID: values.projectID, LocationKey: values.locationKey, JobID: values.jobID,
	}
}

func decodeGetQueryJob(row sqlcgen.GetQueryJobRow) (*domain.Job, error) {
	return decodeQueryJob(
		row.ProjectID, row.Location, row.JobID, row.ConfigurationVersion,
		row.ConfigurationJson, row.ConfigurationDigest, row.State, row.ErrorReason, row.ErrorMessage,
		row.CreatedAt, row.StartedAt, row.EndedAt,
		row.ResultPresent, row.ResultSchemaJson, row.ResultRowCount, row.AffectedRows,
	)
}

func decodeListedQueryJobs(rows []sqlcgen.ListQueryJobsRow) ([]*domain.Job, error) {
	jobs := make([]*domain.Job, 0, len(rows))
	for _, row := range rows {
		job, err := decodeQueryJob(
			row.ProjectID, row.Location, row.JobID, row.ConfigurationVersion,
			row.ConfigurationJson, row.ConfigurationDigest, row.State, row.ErrorReason, row.ErrorMessage,
			row.CreatedAt, row.StartedAt, row.EndedAt,
			row.ResultPresent, row.ResultSchemaJson, row.ResultRowCount, row.AffectedRows,
		)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func decodeListedQueryJobsAtLocation(rows []sqlcgen.ListQueryJobsAtLocationRow) ([]*domain.Job, error) {
	jobs := make([]*domain.Job, 0, len(rows))
	for _, row := range rows {
		job, err := decodeQueryJob(
			row.ProjectID, row.Location, row.JobID, row.ConfigurationVersion,
			row.ConfigurationJson, row.ConfigurationDigest, row.State, row.ErrorReason, row.ErrorMessage,
			row.CreatedAt, row.StartedAt, row.EndedAt,
			row.ResultPresent, row.ResultSchemaJson, row.ResultRowCount, row.AffectedRows,
		)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func decodeQueryJob(
	projectID, location, jobID string,
	configurationVersion int64,
	configurationJSON, digest, state string,
	errorReason, errorMessage sql.NullString,
	createdAt string,
	startedAt, endedAt sql.NullString,
	resultPresent int64,
	schemaJSON sql.NullString,
	rowCount, affectedRows int64,
) (*domain.Job, error) {
	if configurationVersion != 1 {
		return nil, fmt.Errorf("unsupported query job configuration version %d", configurationVersion)
	}
	var configuration domain.QueryConfiguration
	if err := json.Unmarshal([]byte(configurationJSON), &configuration); err != nil {
		return nil, fmt.Errorf("decode query job configuration: %w", err)
	}
	created, err := decodeTime(createdAt)
	if err != nil {
		return nil, err
	}
	job, err := domain.NewConfiguredQueryJob(domain.JobReference{
		ProjectID: projectID, Location: location, JobID: jobID,
	}, configuration, created)
	if err != nil {
		return nil, fmt.Errorf("validate persisted query job: %w", err)
	}
	if job.ConfigurationDigest != digest {
		return nil, errors.New("persisted query configuration digest does not match its metadata")
	}
	job.State = domain.JobState(state)
	job.Error = decodeQueryError(errorReason, errorMessage)
	job.StartedAt, err = decodeOptionalTime(startedAt)
	if err != nil {
		return nil, err
	}
	job.EndedAt, err = decodeOptionalTime(endedAt)
	if err != nil {
		return nil, err
	}
	if resultPresent == 1 {
		var fields []domain.Field
		if !schemaJSON.Valid || json.Unmarshal([]byte(schemaJSON.String), &fields) != nil {
			return nil, errors.New("decode persisted query result schema")
		}
		job.Result = &domain.QueryResult{
			Columns: fields, AffectedRows: affectedRows, TotalRows: rowCount,
			RowsUnavailable: rowCount > 0,
		}
	}
	return job, nil
}

func (r *queryJobRepository) cacheResult(reference domain.JobReference, result *domain.QueryResult) {
	key, err := queryJobKey(reference)
	if err != nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if result == nil {
		delete(r.payloads, key)
		return
	}
	r.payloads[key] = cloneQueryResult(result)
}

func queryJobKey(reference domain.JobReference) (string, error) {
	if err := reference.Validate(); err != nil {
		return "", err
	}
	return reference.ProjectID + "\x00" + strings.ToUpper(reference.Location) + "\x00" + reference.JobID, nil
}

func cloneQueryResult(result *domain.QueryResult) *domain.QueryResult {
	if result == nil {
		return nil
	}
	clone := *result
	clone.Columns = domain.CloneFields(result.Columns)
	clone.Rows = make([][]any, len(result.Rows))
	for rowIndex, row := range result.Rows {
		clone.Rows[rowIndex] = make([]any, len(row))
		for valueIndex, value := range row {
			clone.Rows[rowIndex][valueIndex] = cloneQueryValue(value)
		}
	}
	clone.RowsUnavailable = false
	return &clone
}

func cloneQueryValue(value any) any {
	switch typed := value.(type) {
	case []byte:
		return append([]byte(nil), typed...)
	case []any:
		result := make([]any, len(typed))
		for index, nested := range typed {
			result[index] = cloneQueryValue(nested)
		}
		return result
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, nested := range typed {
			result[key] = cloneQueryValue(nested)
		}
		return result
	default:
		return typed
	}
}

func optionalQueryError(jobError *domain.JobError) (sql.NullString, sql.NullString) {
	if jobError == nil {
		return sql.NullString{}, sql.NullString{}
	}
	return sql.NullString{String: jobError.Reason, Valid: true}, sql.NullString{String: jobError.Message, Valid: true}
}

func decodeQueryError(reason, message sql.NullString) *domain.JobError {
	if !reason.Valid || !message.Valid {
		return nil
	}
	return &domain.JobError{Reason: reason.String, Message: message.String}
}

func optionalTime(value *time.Time) sql.NullString {
	if value == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: encodeTime(*value), Valid: true}
}

func decodeOptionalTime(value sql.NullString) (*time.Time, error) {
	if !value.Valid {
		return nil, nil
	}
	parsed, err := decodeTime(value.String)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func queryJobRepositoryError(ctx context.Context, operation string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return fmt.Errorf("%w: SQLite query jobs %s: %v", domain.ErrBackend, operation, err)
}
