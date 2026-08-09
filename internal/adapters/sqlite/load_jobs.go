package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	loaddomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
)

// LoadJobRepository persists load job resources in the shared job identity
// namespace. Object contents and loaded table rows remain outside SQLite.
type LoadJobRepository struct {
	store *Store
}

var _ loadports.JobRepository = (*LoadJobRepository)(nil)

func NewLoadJobRepository(store *Store) *LoadJobRepository {
	return &LoadJobRepository{store: store}
}

type persistedLoadJob struct {
	identity          persistedJobIdentity
	configurationJSON string
	statistics        loaddomain.Statistics
}

const loadJobSelect = `SELECT
	j.project_id, j.location, j.job_id, j.job_kind, j.configuration_digest, j.state,
	j.error_reason, j.error_message, j.created_at, j.started_at, j.ended_at,
	l.configuration_json, l.input_files, l.input_bytes, l.output_bytes, l.output_rows
FROM job_identities AS j
JOIN load_job_details AS l
	ON l.project_id = j.project_id AND l.location_key = j.location_key AND l.job_id = j.job_id`

func (r *LoadJobRepository) CreateOrGet(ctx context.Context, job *loaddomain.Job) (*loaddomain.Job, bool, error) {
	record, err := encodeLoadJob(job)
	if err != nil {
		return nil, false, err
	}
	if r == nil || r.store == nil || r.store.db == nil {
		return nil, false, fmt.Errorf("load job repository is not open")
	}
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, fmt.Errorf("begin load job insert: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	created, err := insertJobIdentity(ctx, tx, record.identity)
	if err != nil {
		return nil, false, fmt.Errorf("create load job: %w", err)
	}
	if !created {
		existing, lookupErr := getJobIdentity(ctx, tx, record.identity.projectID, record.identity.location, record.identity.jobID)
		if lookupErr != nil {
			return nil, false, fmt.Errorf("inspect existing load job: %w", lookupErr)
		}
		if existing.kind != loadJobKind {
			return nil, false, fmt.Errorf("%w: job ID %q is already used by a query job", loaddomain.ErrConflict, record.identity.jobID)
		}
		if err := tx.Rollback(); err != nil {
			return nil, false, fmt.Errorf("release existing load job lookup: %w", err)
		}
		loaded, err := r.Get(ctx, job.Reference)
		return loaded, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO load_job_details (
		project_id, location_key, job_id, configuration_json,
		input_files, input_bytes, output_bytes, output_rows
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		record.identity.projectID, jobLocationKey(record.identity.location), record.identity.jobID,
		record.configurationJSON, record.statistics.InputFiles, record.statistics.InputBytes,
		record.statistics.OutputBytes, record.statistics.OutputRows,
	); err != nil {
		return nil, false, fmt.Errorf("insert load job details: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, false, fmt.Errorf("commit load job insert: %w", err)
	}
	return job.Clone(), true, nil
}

func (r *LoadJobRepository) Update(ctx context.Context, job *loaddomain.Job) error {
	record, err := encodeLoadJob(job)
	if err != nil {
		return err
	}
	if r == nil || r.store == nil || r.store.db == nil {
		return fmt.Errorf("load job repository is not open")
	}
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin load job update: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := updateJobIdentity(ctx, tx, record.identity); err != nil {
		switch {
		case errors.Is(err, errJobIdentityNotFound), errors.Is(err, errJobKindConflict):
			return fmt.Errorf("%w: load job %s", loaddomain.ErrNotFound, record.identity.jobID)
		case errors.Is(err, errJobStateConflict):
			return fmt.Errorf("%w: load job %s state or configuration changed", loaddomain.ErrConflict, record.identity.jobID)
		default:
			return fmt.Errorf("update load job identity: %w", err)
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE load_job_details SET
		input_files = ?, input_bytes = ?, output_bytes = ?, output_rows = ?
	WHERE project_id = ? AND location_key = ? AND job_id = ?`,
		record.statistics.InputFiles, record.statistics.InputBytes,
		record.statistics.OutputBytes, record.statistics.OutputRows,
		record.identity.projectID, jobLocationKey(record.identity.location), record.identity.jobID,
	)
	if err != nil {
		return fmt.Errorf("update load job details: %w", err)
	}
	if err := requireOneJobDetail(result); err != nil {
		return fmt.Errorf("%w: load job %s details", loaddomain.ErrNotFound, record.identity.jobID)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit load job update: %w", err)
	}
	return nil
}

func (r *LoadJobRepository) Get(ctx context.Context, reference loaddomain.JobReference) (*loaddomain.Job, error) {
	if reference.ProjectID == "" || !validJobLocation(reference.Location) || !validJobID(reference.JobID) {
		return nil, fmt.Errorf("%w: invalid projectId, location, or jobId", loaddomain.ErrInvalid)
	}
	if r == nil || r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("load job repository is not open")
	}
	job, err := scanLoadJob(r.store.db.QueryRowContext(ctx, loadJobSelect+`
		WHERE j.project_id = ? AND j.location_key = ? AND j.job_id = ? AND j.job_kind = 'LOAD'`,
		reference.ProjectID, jobLocationKey(reference.Location), reference.JobID,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: load job %s", loaddomain.ErrNotFound, reference.JobID)
	}
	if err != nil {
		return nil, fmt.Errorf("get load job %s: %w", reference.JobID, err)
	}
	return job, nil
}

func (r *LoadJobRepository) List(ctx context.Context, projectID, location string) ([]*loaddomain.Job, error) {
	if projectID == "" || location != "" && !validJobLocation(location) {
		return nil, fmt.Errorf("%w: invalid projectId or location", loaddomain.ErrInvalid)
	}
	if r == nil || r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("load job repository is not open")
	}
	statement := loadJobSelect + ` WHERE j.project_id = ? AND j.job_kind = 'LOAD'`
	arguments := []any{projectID}
	if location != "" {
		statement += " AND j.location_key = ?"
		arguments = append(arguments, jobLocationKey(location))
	}
	statement += " ORDER BY j.created_at DESC, j.job_id, j.location_key"
	rows, err := r.store.db.QueryContext(ctx, statement, arguments...)
	if err != nil {
		return nil, fmt.Errorf("list load jobs: %w", err)
	}
	defer rows.Close()
	jobs := make([]*loaddomain.Job, 0)
	for rows.Next() {
		job, err := scanLoadJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan load job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate load jobs: %w", err)
	}
	return jobs, nil
}

func encodeLoadJob(job *loaddomain.Job) (persistedLoadJob, error) {
	if job == nil {
		return persistedLoadJob{}, fmt.Errorf("%w: load job is required", loaddomain.ErrInvalid)
	}
	if job.CreatedAt.IsZero() {
		return persistedLoadJob{}, fmt.Errorf("%w: load job creation time is required", loaddomain.ErrInvalid)
	}
	if job.Reference.ProjectID == "" || !validJobLocation(job.Reference.Location) || !validJobID(job.Reference.JobID) {
		return persistedLoadJob{}, fmt.Errorf("%w: invalid projectId, location, or jobId", loaddomain.ErrInvalid)
	}
	normalized, err := loaddomain.NewJob(job.Reference, job.Configuration, job.CreatedAt)
	if err != nil {
		return persistedLoadJob{}, err
	}
	if job.ConfigurationDigest != normalized.ConfigurationDigest {
		return persistedLoadJob{}, fmt.Errorf("%w: load configuration digest does not match configuration", loaddomain.ErrInvalid)
	}
	configurationJSON, err := json.Marshal(normalized.Configuration)
	if err != nil {
		return persistedLoadJob{}, fmt.Errorf("%w: encode load configuration", loaddomain.ErrInvalid)
	}
	record := persistedLoadJob{
		identity: persistedJobIdentity{
			projectID: job.Reference.ProjectID, location: job.Reference.Location, jobID: job.Reference.JobID,
			kind: loadJobKind, configurationDigest: job.ConfigurationDigest, state: string(job.State),
			createdAt: job.CreatedAt.UTC(), startedAt: cloneTimePointer(job.StartedAt), endedAt: cloneTimePointer(job.EndedAt),
		},
		configurationJSON: string(configurationJSON), statistics: job.Statistics,
	}
	if job.Error != nil {
		record.identity.errorReason = nullableString(job.Error.Reason)
		record.identity.errorMessage = nullableString(job.Error.Message)
	}
	if err := validateLoadJobPersistence(job); err != nil {
		return persistedLoadJob{}, err
	}
	return record, nil
}

func validateLoadJobPersistence(job *loaddomain.Job) error {
	if job.StartedAt != nil && job.StartedAt.Before(job.CreatedAt) {
		return fmt.Errorf("%w: load job starts before it was created", loaddomain.ErrInvalid)
	}
	if job.EndedAt != nil && (job.StartedAt == nil || job.EndedAt.Before(*job.StartedAt)) {
		return fmt.Errorf("%w: load job ends before it starts", loaddomain.ErrInvalid)
	}
	zeroStatistics := loaddomain.Statistics{}
	switch job.State {
	case loaddomain.JobPending:
		if job.StartedAt != nil || job.EndedAt != nil || job.Error != nil || job.Statistics != zeroStatistics {
			return fmt.Errorf("%w: pending load job contains execution state", loaddomain.ErrInvalid)
		}
	case loaddomain.JobRunning:
		if job.StartedAt == nil || job.EndedAt != nil || job.Error != nil || job.Statistics != zeroStatistics {
			return fmt.Errorf("%w: running load job has inconsistent execution state", loaddomain.ErrInvalid)
		}
	case loaddomain.JobDone:
		if job.StartedAt == nil || job.EndedAt == nil {
			return fmt.Errorf("%w: done load job has inconsistent terminal state", loaddomain.ErrInvalid)
		}
	default:
		return fmt.Errorf("%w: unknown load job state %q", loaddomain.ErrInvalid, job.State)
	}
	if job.Statistics.InputFiles < 0 || job.Statistics.InputBytes < 0 || job.Statistics.OutputBytes < 0 || job.Statistics.OutputRows < 0 {
		return fmt.Errorf("%w: load job statistics must not be negative", loaddomain.ErrInvalid)
	}
	return nil
}

func scanLoadJob(scanner rowScanner) (*loaddomain.Job, error) {
	var identity persistedJobIdentity
	var kind, createdAt string
	var startedAt, endedAt sql.NullString
	var configurationJSON string
	var statistics loaddomain.Statistics
	if err := scanner.Scan(
		&identity.projectID, &identity.location, &identity.jobID, &kind,
		&identity.configurationDigest, &identity.state, &identity.errorReason, &identity.errorMessage,
		&createdAt, &startedAt, &endedAt, &configurationJSON,
		&statistics.InputFiles, &statistics.InputBytes, &statistics.OutputBytes, &statistics.OutputRows,
	); err != nil {
		return nil, err
	}
	identity.kind = jobKind(kind)
	if identity.kind != loadJobKind {
		return nil, errors.New("persisted load job has the wrong kind")
	}
	var configuration loaddomain.LoadConfiguration
	if err := json.Unmarshal([]byte(configurationJSON), &configuration); err != nil {
		return nil, errors.New("decode persisted load configuration")
	}
	created, err := decodeJobTime(createdAt)
	if err != nil {
		return nil, errors.New("decode persisted load creation time")
	}
	job, err := loaddomain.NewJob(loaddomain.JobReference{
		ProjectID: identity.projectID, Location: identity.location, JobID: identity.jobID,
	}, configuration, created)
	if err != nil || job.ConfigurationDigest != identity.configurationDigest {
		return nil, errors.New("persisted load configuration is invalid")
	}
	started, err := decodeNullableJobTime(startedAt)
	if err != nil {
		return nil, errors.New("decode persisted load start time")
	}
	ended, err := decodeNullableJobTime(endedAt)
	if err != nil {
		return nil, errors.New("decode persisted load end time")
	}
	if identity.state != string(loaddomain.JobPending) {
		if started == nil || job.Start(*started) != nil {
			return nil, errors.New("persisted load running state is invalid")
		}
	}
	if identity.state == string(loaddomain.JobDone) {
		if ended == nil {
			return nil, errors.New("persisted load terminal time is invalid")
		}
		if identity.errorReason.Valid || identity.errorMessage.Valid {
			if !identity.errorReason.Valid || !identity.errorMessage.Valid || job.Fail(identity.errorReason.String, identity.errorMessage.String, statistics, *ended) != nil {
				return nil, errors.New("persisted load failure is invalid")
			}
		} else if job.Complete(statistics, *ended) != nil {
			return nil, errors.New("persisted load result metadata is invalid")
		}
	} else if identity.state != string(loaddomain.JobPending) && identity.state != string(loaddomain.JobRunning) {
		return nil, errors.New("persisted load state is invalid")
	}
	return job, nil
}
