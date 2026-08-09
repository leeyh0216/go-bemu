package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/sqlite/sqlcgen"
	loaddomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
)

type loadJobRepository struct {
	queries *sqlcgen.Queries
}

func newLoadJobRepository(db *sql.DB) *loadJobRepository {
	return &loadJobRepository{queries: sqlcgen.New(db)}
}

var _ loadports.JobRepository = (*loadJobRepository)(nil)

func (r *loadJobRepository) CreateOrGet(ctx context.Context, job *loaddomain.Job) (*loaddomain.Job, bool, error) {
	values, err := encodeLoadJob(job)
	if err != nil {
		return nil, false, err
	}
	rowsAffected, err := r.queries.CreateLoadJob(ctx, values.createParams())
	if err != nil {
		return nil, false, loadJobRepositoryError(ctx, "create", err)
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
	rowsAffected, err := r.queries.UpdateLoadJob(ctx, values.updateParams())
	if err != nil {
		return loadJobRepositoryError(ctx, "update", err)
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
	row, err := r.queries.GetLoadJob(ctx, sqlcgen.GetLoadJobParams{
		ProjectID: reference.ProjectID, LocationKey: strings.ToUpper(reference.Location), JobID: reference.JobID,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: load job %s", loaddomain.ErrNotFound, reference.JobID)
	}
	if err != nil {
		return nil, loadJobRepositoryError(ctx, "get", err)
	}
	job, err := decodeGetLoadJob(row)
	if err != nil {
		return nil, loadJobRepositoryError(ctx, "get", err)
	}
	return job, nil
}

func (r *loadJobRepository) List(ctx context.Context, projectID, location string) ([]*loaddomain.Job, error) {
	if strings.TrimSpace(projectID) == "" {
		return nil, fmt.Errorf("%w: projectId is required", loaddomain.ErrInvalid)
	}
	if location == "" {
		rows, err := r.queries.ListLoadJobs(ctx, projectID)
		if err != nil {
			return nil, loadJobRepositoryError(ctx, "list", err)
		}
		return decodeListedLoadJobs(rows)
	}
	rows, err := r.queries.ListLoadJobsAtLocation(ctx, sqlcgen.ListLoadJobsAtLocationParams{
		ProjectID: projectID, LocationKey: strings.ToUpper(location),
	})
	if err != nil {
		return nil, loadJobRepositoryError(ctx, "list", err)
	}
	return decodeListedLoadJobsAtLocation(rows)
}

func (r *loadJobRepository) ListInterrupted(ctx context.Context) ([]*loaddomain.Job, error) {
	rows, err := r.queries.ListInterruptedLoadJobs(ctx)
	if err != nil {
		return nil, loadJobRepositoryError(ctx, "list interrupted", err)
	}
	jobs := make([]*loaddomain.Job, 0, len(rows))
	for _, row := range rows {
		job, decodeErr := decodeInterruptedLoadJob(row)
		if decodeErr != nil {
			return nil, loadJobRepositoryError(ctx, "list interrupted", decodeErr)
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

type loadJobPersistence struct {
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
	inputFiles           int64
	inputBytes           int64
	outputRows           int64
	outputBytes          int64
}

func encodeLoadJob(job *loaddomain.Job) (loadJobPersistence, error) {
	if job == nil {
		return loadJobPersistence{}, fmt.Errorf("%w: load job is required", loaddomain.ErrInvalid)
	}
	if err := validateLoadJobReference(job.Reference); err != nil {
		return loadJobPersistence{}, err
	}
	validated, err := loaddomain.NewJob(job.Reference, job.Configuration, job.CreatedAt)
	if err != nil {
		return loadJobPersistence{}, err
	}
	if validated.ConfigurationDigest != job.ConfigurationDigest {
		return loadJobPersistence{}, fmt.Errorf("%w: load configuration digest does not match job metadata", loaddomain.ErrInvalid)
	}
	configurationJSON, err := json.Marshal(job.Configuration)
	if err != nil {
		return loadJobPersistence{}, fmt.Errorf("%w: encode load job configuration: %v", loaddomain.ErrInvalid, err)
	}
	errorReason, errorMessage := optionalLoadError(job.Error)
	return loadJobPersistence{
		projectID: job.Reference.ProjectID, locationKey: strings.ToUpper(job.Reference.Location),
		location: job.Reference.Location, jobID: job.Reference.JobID, configurationVersion: 1,
		configurationJSON: string(configurationJSON), configurationDigest: job.ConfigurationDigest,
		state: string(job.State), errorReason: errorReason, errorMessage: errorMessage,
		createdAt: encodeTime(job.CreatedAt), startedAt: optionalLoadTime(job.StartedAt), endedAt: optionalLoadTime(job.EndedAt),
		inputFiles: job.Statistics.InputFiles, inputBytes: job.Statistics.InputBytes,
		outputRows: job.Statistics.OutputRows, outputBytes: job.Statistics.OutputBytes,
	}, nil
}

func (values loadJobPersistence) createParams() sqlcgen.CreateLoadJobParams {
	return sqlcgen.CreateLoadJobParams{
		ProjectID: values.projectID, LocationKey: values.locationKey, Location: values.location, JobID: values.jobID,
		ConfigurationVersion: values.configurationVersion, ConfigurationJson: values.configurationJSON,
		ConfigurationDigest: values.configurationDigest, State: values.state,
		ErrorReason: values.errorReason, ErrorMessage: values.errorMessage,
		CreatedAt: values.createdAt, StartedAt: values.startedAt, EndedAt: values.endedAt,
		InputFiles: values.inputFiles, InputBytes: values.inputBytes,
		OutputRows: values.outputRows, OutputBytes: values.outputBytes,
	}
}

func (values loadJobPersistence) updateParams() sqlcgen.UpdateLoadJobParams {
	return sqlcgen.UpdateLoadJobParams{
		Location: values.location, ConfigurationVersion: values.configurationVersion,
		ConfigurationJson: values.configurationJSON, ConfigurationDigest: values.configurationDigest,
		State: values.state, ErrorReason: values.errorReason, ErrorMessage: values.errorMessage,
		CreatedAt: values.createdAt, StartedAt: values.startedAt, EndedAt: values.endedAt,
		InputFiles: values.inputFiles, InputBytes: values.inputBytes,
		OutputRows: values.outputRows, OutputBytes: values.outputBytes,
		ProjectID: values.projectID, LocationKey: values.locationKey, JobID: values.jobID,
	}
}

func decodeGetLoadJob(row sqlcgen.GetLoadJobRow) (*loaddomain.Job, error) {
	return decodeLoadJob(
		row.ProjectID, row.Location, row.JobID, row.ConfigurationVersion,
		row.ConfigurationJson, row.ConfigurationDigest, row.State, row.ErrorReason, row.ErrorMessage,
		row.CreatedAt, row.StartedAt, row.EndedAt,
		row.InputFiles, row.InputBytes, row.OutputRows, row.OutputBytes,
	)
}

func decodeListedLoadJobs(rows []sqlcgen.ListLoadJobsRow) ([]*loaddomain.Job, error) {
	jobs := make([]*loaddomain.Job, 0, len(rows))
	for _, row := range rows {
		job, err := decodeLoadJob(
			row.ProjectID, row.Location, row.JobID, row.ConfigurationVersion,
			row.ConfigurationJson, row.ConfigurationDigest, row.State, row.ErrorReason, row.ErrorMessage,
			row.CreatedAt, row.StartedAt, row.EndedAt,
			row.InputFiles, row.InputBytes, row.OutputRows, row.OutputBytes,
		)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func decodeListedLoadJobsAtLocation(rows []sqlcgen.ListLoadJobsAtLocationRow) ([]*loaddomain.Job, error) {
	jobs := make([]*loaddomain.Job, 0, len(rows))
	for _, row := range rows {
		job, err := decodeLoadJob(
			row.ProjectID, row.Location, row.JobID, row.ConfigurationVersion,
			row.ConfigurationJson, row.ConfigurationDigest, row.State, row.ErrorReason, row.ErrorMessage,
			row.CreatedAt, row.StartedAt, row.EndedAt,
			row.InputFiles, row.InputBytes, row.OutputRows, row.OutputBytes,
		)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func decodeInterruptedLoadJob(row sqlcgen.ListInterruptedLoadJobsRow) (*loaddomain.Job, error) {
	return decodeLoadJob(
		row.ProjectID, row.Location, row.JobID, row.ConfigurationVersion,
		row.ConfigurationJson, row.ConfigurationDigest, row.State, row.ErrorReason, row.ErrorMessage,
		row.CreatedAt, row.StartedAt, row.EndedAt,
		row.InputFiles, row.InputBytes, row.OutputRows, row.OutputBytes,
	)
}

func decodeLoadJob(
	projectID, location, jobID string,
	configurationVersion int64,
	configurationJSON, digest, state string,
	errorReason, errorMessage sql.NullString,
	createdAt string,
	startedAt, endedAt sql.NullString,
	inputFiles, inputBytes, outputRows, outputBytes int64,
) (*loaddomain.Job, error) {
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
	job.Statistics = loaddomain.Statistics{
		InputFiles: inputFiles, InputBytes: inputBytes, OutputRows: outputRows, OutputBytes: outputBytes,
	}
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

func optionalLoadError(jobError *loaddomain.JobError) (sql.NullString, sql.NullString) {
	if jobError == nil {
		return sql.NullString{}, sql.NullString{}
	}
	return sql.NullString{String: jobError.Reason, Valid: true}, sql.NullString{String: jobError.Message, Valid: true}
}

func optionalLoadTime(value *time.Time) sql.NullString {
	if value == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: encodeTime(*value), Valid: true}
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
