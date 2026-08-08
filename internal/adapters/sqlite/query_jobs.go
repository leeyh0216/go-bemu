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

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

const queryJobSelect = `SELECT project_id, location, job_id, configuration_version,
    configuration_json, configuration_digest, state, error_reason, error_message,
    created_at, started_at, ended_at, result_present, result_schema_json,
    result_row_count, affected_rows
FROM bqemu_query_jobs`

type queryJobRepository struct {
	db *sql.DB

	mu       sync.RWMutex
	payloads map[string]*domain.QueryResult
}

var _ ports.JobRepository = (*queryJobRepository)(nil)

func newQueryJobRepository(db *sql.DB) *queryJobRepository {
	return &queryJobRepository{db: db, payloads: make(map[string]*domain.QueryResult)}
}

func (r *queryJobRepository) CreateOrGet(ctx context.Context, job *domain.Job) (*domain.Job, bool, error) {
	values, persistedResult, err := encodeQueryJob(job)
	if err != nil {
		return nil, false, err
	}
	result, err := r.db.ExecContext(ctx, `INSERT INTO bqemu_query_jobs (
    project_id, location_key, location, job_id, configuration_version,
    configuration_json, configuration_digest, state, error_reason, error_message,
    created_at, started_at, ended_at, result_present, result_schema_json,
    result_row_count, affected_rows
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (project_id, location_key, job_id) DO NOTHING`, values...)
	if err != nil {
		return nil, false, queryJobRepositoryError(ctx, "create", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, false, queryJobRepositoryError(ctx, "inspect create", err)
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
	result, err := r.db.ExecContext(ctx, `UPDATE bqemu_query_jobs SET
    location = ?, configuration_version = ?, configuration_json = ?,
    configuration_digest = ?, state = ?, error_reason = ?, error_message = ?,
    created_at = ?, started_at = ?, ended_at = ?, result_present = ?,
    result_schema_json = ?, result_row_count = ?, affected_rows = ?
WHERE project_id = ? AND location_key = ? AND job_id = ?`,
		values[2], values[4], values[5], values[6], values[7], values[8], values[9],
		values[10], values[11], values[12], values[13], values[14], values[15], values[16],
		values[0], values[1], values[3])
	if err != nil {
		return queryJobRepositoryError(ctx, "update", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return queryJobRepositoryError(ctx, "inspect update", err)
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
	job, err := scanQueryJob(r.db.QueryRowContext(ctx, queryJobSelect+`
WHERE project_id = ? AND location_key = ? AND job_id = ?`,
		reference.ProjectID, strings.ToUpper(reference.Location), reference.JobID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: query job %s", domain.ErrNotFound, reference.JobID)
	}
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
	statement := queryJobSelect + ` WHERE project_id = ?`
	args := []any{projectID}
	if location != "" {
		statement += ` AND location_key = ?`
		args = append(args, strings.ToUpper(location))
	}
	rows, err := r.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, queryJobRepositoryError(ctx, "list", err)
	}
	defer rows.Close()
	jobs := make([]*domain.Job, 0)
	for rows.Next() {
		job, scanErr := scanQueryJob(rows)
		if scanErr != nil {
			return nil, queryJobRepositoryError(ctx, "list", scanErr)
		}
		key, _ := queryJobKey(job.Reference)
		r.mu.RLock()
		payload := cloneQueryResult(r.payloads[key])
		r.mu.RUnlock()
		if payload != nil && job.Result != nil {
			job.Result = payload
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, queryJobRepositoryError(ctx, "list", err)
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

func encodeQueryJob(job *domain.Job) ([]any, *domain.QueryResult, error) {
	if job == nil {
		return nil, nil, fmt.Errorf("%w: query job is required", domain.ErrInvalid)
	}
	if err := job.Reference.Validate(); err != nil {
		return nil, nil, err
	}
	validated, err := domain.NewConfiguredQueryJob(job.Reference, job.Configuration, job.CreatedAt)
	if err != nil {
		return nil, nil, err
	}
	if validated.ConfigurationDigest != job.ConfigurationDigest {
		return nil, nil, fmt.Errorf("%w: query configuration digest does not match job metadata", domain.ErrInvalid)
	}
	configurationJSON, err := json.Marshal(job.Configuration)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: encode query job configuration: %v", domain.ErrInvalid, err)
	}
	errorReason, errorMessage := optionalQueryError(job.Error)
	resultPresent, schemaJSON, rowCount, affectedRows := 0, any(nil), int64(0), int64(0)
	var cached *domain.QueryResult
	if job.Result != nil {
		resultPresent = 1
		encodedSchema, marshalErr := json.Marshal(job.Result.Columns)
		if marshalErr != nil {
			return nil, nil, fmt.Errorf("%w: encode query result schema: %v", domain.ErrInvalid, marshalErr)
		}
		schemaJSON = string(encodedSchema)
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
	return []any{
		job.Reference.ProjectID, strings.ToUpper(job.Reference.Location), job.Reference.Location, job.Reference.JobID,
		1, string(configurationJSON), job.ConfigurationDigest, string(job.State), errorReason, errorMessage,
		encodeTime(job.CreatedAt), optionalTime(job.StartedAt), optionalTime(job.EndedAt),
		resultPresent, schemaJSON, rowCount, affectedRows,
	}, cached, nil
}

func scanQueryJob(scanner rowScanner) (*domain.Job, error) {
	var projectID, location, jobID, configurationJSON, digest, state, createdAt string
	var configurationVersion, resultPresent int
	var errorReason, errorMessage, startedAt, endedAt, schemaJSON sql.NullString
	var rowCount, affectedRows int64
	if err := scanner.Scan(
		&projectID, &location, &jobID, &configurationVersion, &configurationJSON, &digest,
		&state, &errorReason, &errorMessage, &createdAt, &startedAt, &endedAt,
		&resultPresent, &schemaJSON, &rowCount, &affectedRows,
	); err != nil {
		return nil, err
	}
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

func optionalQueryError(jobError *domain.JobError) (any, any) {
	if jobError == nil {
		return nil, nil
	}
	return jobError.Reason, jobError.Message
}

func decodeQueryError(reason, message sql.NullString) *domain.JobError {
	if !reason.Valid || !message.Valid {
		return nil
	}
	return &domain.JobError{Reason: reason.String, Message: message.String}
}

func optionalTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return encodeTime(*value)
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
