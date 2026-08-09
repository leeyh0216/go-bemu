package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	loaddomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
)

const loadJobSelect = `SELECT project_id, location, job_id, configuration_version,
    configuration_json, configuration_digest, state, error_reason, error_message,
    created_at, started_at, ended_at, input_files, input_bytes, output_rows, output_bytes
FROM bqemu_load_jobs`

type loadJobRepository struct {
	db *sql.DB
}

var _ loadports.JobRepository = (*loadJobRepository)(nil)

func (r *loadJobRepository) CreateOrGet(ctx context.Context, job *loaddomain.Job) (*loaddomain.Job, bool, error) {
	values, err := encodeLoadJob(job)
	if err != nil {
		return nil, false, err
	}
	result, err := r.db.ExecContext(ctx, `INSERT INTO bqemu_load_jobs (
    project_id, location_key, location, job_id, configuration_version,
    configuration_json, configuration_digest, state, error_reason, error_message,
    created_at, started_at, ended_at, input_files, input_bytes, output_rows, output_bytes
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (project_id, location_key, job_id) DO NOTHING`, values...)
	if err != nil {
		return nil, false, loadJobRepositoryError(ctx, "create", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, false, loadJobRepositoryError(ctx, "inspect create", err)
	}
	if rowsAffected == 0 {
		existing, getErr := r.Get(ctx, job.Reference)
		return existing, false, getErr
	}
	created, err := r.Get(ctx, job.Reference)
	if err != nil {
		return nil, false, err
	}
	return created, true, nil
}

func (r *loadJobRepository) Update(ctx context.Context, job *loaddomain.Job) error {
	values, err := encodeLoadJob(job)
	if err != nil {
		return err
	}
	result, err := r.db.ExecContext(ctx, `UPDATE bqemu_load_jobs SET
    location = ?, configuration_version = ?, configuration_json = ?,
    configuration_digest = ?, state = ?, error_reason = ?, error_message = ?,
    created_at = ?, started_at = ?, ended_at = ?, input_files = ?, input_bytes = ?,
    output_rows = ?, output_bytes = ?
WHERE project_id = ? AND location_key = ? AND job_id = ?`,
		values[2], values[4], values[5], values[6], values[7], values[8], values[9],
		values[10], values[11], values[12], values[13], values[14], values[15], values[16],
		values[0], values[1], values[3])
	if err != nil {
		return loadJobRepositoryError(ctx, "update", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return loadJobRepositoryError(ctx, "inspect update", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("%w: load job %s", loaddomain.ErrNotFound, job.Reference.JobID)
	}
	return nil
}

func (r *loadJobRepository) Get(ctx context.Context, reference loaddomain.JobReference) (*loaddomain.Job, error) {
	if err := validateLoadJobReference(reference); err != nil {
		return nil, err
	}
	job, err := scanLoadJob(r.db.QueryRowContext(ctx, loadJobSelect+`
WHERE project_id = ? AND location_key = ? AND job_id = ?`,
		reference.ProjectID, strings.ToUpper(reference.Location), reference.JobID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: load job %s", loaddomain.ErrNotFound, reference.JobID)
	}
	if err != nil {
		return nil, loadJobRepositoryError(ctx, "get", err)
	}
	return job, nil
}

func (r *loadJobRepository) List(ctx context.Context, projectID, location string) ([]*loaddomain.Job, error) {
	if strings.TrimSpace(projectID) == "" {
		return nil, fmt.Errorf("%w: projectId is required", loaddomain.ErrInvalid)
	}
	statement := loadJobSelect + ` WHERE project_id = ?`
	args := []any{projectID}
	if location != "" {
		statement += ` AND location_key = ?`
		args = append(args, strings.ToUpper(location))
	}
	rows, err := r.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, loadJobRepositoryError(ctx, "list", err)
	}
	defer rows.Close()
	jobs := make([]*loaddomain.Job, 0)
	for rows.Next() {
		job, scanErr := scanLoadJob(rows)
		if scanErr != nil {
			return nil, loadJobRepositoryError(ctx, "list", scanErr)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, loadJobRepositoryError(ctx, "list", err)
	}
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].CreatedAt.Equal(jobs[j].CreatedAt) {
			return jobs[i].Reference.JobID < jobs[j].Reference.JobID
		}
		return jobs[i].CreatedAt.Before(jobs[j].CreatedAt)
	})
	return jobs, nil
}

func (r *loadJobRepository) ListInterrupted(ctx context.Context) ([]*loaddomain.Job, error) {
	rows, err := r.db.QueryContext(ctx, loadJobSelect+` WHERE state IN ('PENDING', 'RUNNING')
ORDER BY created_at, project_id, location_key, job_id`)
	if err != nil {
		return nil, loadJobRepositoryError(ctx, "list interrupted", err)
	}
	defer rows.Close()
	jobs := make([]*loaddomain.Job, 0)
	for rows.Next() {
		job, scanErr := scanLoadJob(rows)
		if scanErr != nil {
			return nil, loadJobRepositoryError(ctx, "list interrupted", scanErr)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, loadJobRepositoryError(ctx, "list interrupted", err)
	}
	return jobs, nil
}

func encodeLoadJob(job *loaddomain.Job) ([]any, error) {
	if job == nil {
		return nil, fmt.Errorf("%w: load job is required", loaddomain.ErrInvalid)
	}
	if err := validateLoadJobReference(job.Reference); err != nil {
		return nil, err
	}
	validated, err := loaddomain.NewJob(job.Reference, job.Configuration, job.CreatedAt)
	if err != nil {
		return nil, err
	}
	if validated.ConfigurationDigest != job.ConfigurationDigest {
		return nil, fmt.Errorf("%w: load configuration digest does not match job metadata", loaddomain.ErrInvalid)
	}
	configurationJSON, err := json.Marshal(job.Configuration)
	if err != nil {
		return nil, fmt.Errorf("%w: encode load job configuration: %v", loaddomain.ErrInvalid, err)
	}
	errorReason, errorMessage := optionalLoadError(job.Error)
	return []any{
		job.Reference.ProjectID, strings.ToUpper(job.Reference.Location), job.Reference.Location, job.Reference.JobID,
		1, string(configurationJSON), job.ConfigurationDigest, string(job.State), errorReason, errorMessage,
		encodeTime(job.CreatedAt), optionalTime(job.StartedAt), optionalTime(job.EndedAt),
		job.Statistics.InputFiles, job.Statistics.InputBytes, job.Statistics.OutputRows, job.Statistics.OutputBytes,
	}, nil
}

func scanLoadJob(scanner rowScanner) (*loaddomain.Job, error) {
	var projectID, location, jobID, configurationJSON, digest, state, createdAt string
	var configurationVersion int
	var errorReason, errorMessage, startedAt, endedAt sql.NullString
	var statistics loaddomain.Statistics
	if err := scanner.Scan(
		&projectID, &location, &jobID, &configurationVersion, &configurationJSON, &digest,
		&state, &errorReason, &errorMessage, &createdAt, &startedAt, &endedAt,
		&statistics.InputFiles, &statistics.InputBytes, &statistics.OutputRows, &statistics.OutputBytes,
	); err != nil {
		return nil, err
	}
	if configurationVersion != 1 {
		return nil, fmt.Errorf("unsupported load job configuration version %d", configurationVersion)
	}
	var configuration loaddomain.LoadConfiguration
	if err := json.Unmarshal([]byte(configurationJSON), &configuration); err != nil {
		return nil, fmt.Errorf("decode load job configuration: %w", err)
	}
	created, err := decodeTime(createdAt)
	if err != nil {
		return nil, err
	}
	job, err := loaddomain.NewJob(loaddomain.JobReference{
		ProjectID: projectID, Location: location, JobID: jobID,
	}, configuration, created)
	if err != nil {
		return nil, fmt.Errorf("validate persisted load job: %w", err)
	}
	if job.ConfigurationDigest != digest {
		return nil, errors.New("persisted load configuration digest does not match its metadata")
	}
	job.State = loaddomain.JobState(state)
	job.Error = decodeLoadError(errorReason, errorMessage)
	job.Statistics = statistics
	job.StartedAt, err = decodeOptionalTime(startedAt)
	if err != nil {
		return nil, err
	}
	job.EndedAt, err = decodeOptionalTime(endedAt)
	if err != nil {
		return nil, err
	}
	return job, nil
}

func validateLoadJobReference(reference loaddomain.JobReference) error {
	if strings.TrimSpace(reference.ProjectID) == "" || strings.TrimSpace(reference.Location) == "" || strings.TrimSpace(reference.JobID) == "" {
		return fmt.Errorf("%w: projectId, location, and jobId are required", loaddomain.ErrInvalid)
	}
	return nil
}

func optionalLoadError(jobError *loaddomain.JobError) (any, any) {
	if jobError == nil {
		return nil, nil
	}
	return jobError.Reason, jobError.Message
}

func decodeLoadError(reason, message sql.NullString) *loaddomain.JobError {
	if !reason.Valid || !message.Valid {
		return nil
	}
	return &loaddomain.JobError{Reason: reason.String, Message: message.String}
}

func loadJobRepositoryError(ctx context.Context, operation string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return fmt.Errorf("SQLite load jobs %s: %v", operation, err)
}
