package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

// QueryJobRepository stores query job resources in BQEMU's canonical SQLite
// database. Row payloads deliberately stay outside SQLite: the transient cache
// preserves getQueryResults behavior only for the lifetime of this process.
type QueryJobRepository struct {
	store *Store

	rowsMu        sync.RWMutex
	transientRows map[string][][]any
}

var _ ports.JobRepository = (*QueryJobRepository)(nil)

func NewQueryJobRepository(store *Store) *QueryJobRepository {
	return &QueryJobRepository{store: store, transientRows: make(map[string][][]any)}
}

type persistedQueryJob struct {
	identity          persistedJobIdentity
	configurationJSON string
	hasResult         bool
	columnsJSON       string
	affectedRows      int64
	totalRows         int64
}

const queryJobSelect = `SELECT
	j.project_id, j.location, j.job_id, j.job_kind, j.configuration_digest, j.state,
	j.error_reason, j.error_message, j.created_at, j.started_at, j.ended_at,
	q.configuration_json, q.has_result, q.result_columns_json, q.affected_rows, q.total_rows
FROM job_identities AS j
JOIN query_job_details AS q
	ON q.project_id = j.project_id AND q.location_key = j.location_key AND q.job_id = j.job_id`

func (r *QueryJobRepository) CreateOrGet(ctx context.Context, job *domain.Job) (*domain.Job, bool, error) {
	record, err := encodeQueryJob(job)
	if err != nil {
		return nil, false, err
	}
	if r == nil || r.store == nil || r.store.db == nil {
		return nil, false, fmt.Errorf("query job repository is not open")
	}
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, fmt.Errorf("begin query job insert: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	created, err := insertJobIdentity(ctx, tx, record.identity)
	if err != nil {
		return nil, false, fmt.Errorf("create query job: %w", err)
	}
	if !created {
		existing, lookupErr := getJobIdentity(ctx, tx, record.identity.projectID, record.identity.location, record.identity.jobID)
		if lookupErr != nil {
			return nil, false, fmt.Errorf("inspect existing query job: %w", lookupErr)
		}
		if existing.kind != queryJobKind {
			return nil, false, fmt.Errorf("%w: job ID %q is already used by a load job", domain.ErrConflict, record.identity.jobID)
		}
		if err := tx.Rollback(); err != nil {
			return nil, false, fmt.Errorf("release existing query job lookup: %w", err)
		}
		loaded, err := r.Get(ctx, job.Reference)
		return loaded, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO query_job_details (
		project_id, location_key, job_id, configuration_json, has_result,
		result_columns_json, affected_rows, total_rows
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		record.identity.projectID, jobLocationKey(record.identity.location), record.identity.jobID,
		record.configurationJSON, boolInt(record.hasResult), record.columnsJSON,
		record.affectedRows, record.totalRows,
	); err != nil {
		return nil, false, fmt.Errorf("insert query job details: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, false, fmt.Errorf("commit query job insert: %w", err)
	}
	r.cacheQueryRows(job)
	return cloneQueryJob(job), true, nil
}

func (r *QueryJobRepository) Update(ctx context.Context, job *domain.Job) error {
	record, err := encodeQueryJob(job)
	if err != nil {
		return err
	}
	if r == nil || r.store == nil || r.store.db == nil {
		return fmt.Errorf("query job repository is not open")
	}
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin query job update: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := updateJobIdentity(ctx, tx, record.identity); err != nil {
		switch {
		case errors.Is(err, errJobIdentityNotFound), errors.Is(err, errJobKindConflict):
			return fmt.Errorf("%w: query job %s", domain.ErrNotFound, record.identity.jobID)
		case errors.Is(err, errJobStateConflict):
			return fmt.Errorf("%w: query job %s state or configuration changed", domain.ErrConflict, record.identity.jobID)
		default:
			return fmt.Errorf("update query job identity: %w", err)
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE query_job_details SET
		has_result = ?, result_columns_json = ?, affected_rows = ?, total_rows = ?
	WHERE project_id = ? AND location_key = ? AND job_id = ?`,
		boolInt(record.hasResult), record.columnsJSON, record.affectedRows, record.totalRows,
		record.identity.projectID, jobLocationKey(record.identity.location), record.identity.jobID,
	)
	if err != nil {
		return fmt.Errorf("update query job details: %w", err)
	}
	if err := requireOneJobDetail(result); err != nil {
		return fmt.Errorf("%w: query job %s details", domain.ErrNotFound, record.identity.jobID)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit query job update: %w", err)
	}
	r.cacheQueryRows(job)
	return nil
}

func (r *QueryJobRepository) Get(ctx context.Context, reference domain.JobReference) (*domain.Job, error) {
	if err := reference.Validate(); err != nil {
		return nil, err
	}
	if r == nil || r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("query job repository is not open")
	}
	job, err := scanQueryJob(r.store.db.QueryRowContext(ctx, queryJobSelect+`
		WHERE j.project_id = ? AND j.location_key = ? AND j.job_id = ? AND j.job_kind = 'QUERY'`,
		reference.ProjectID, jobLocationKey(reference.Location), reference.JobID,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: query job %s", domain.ErrNotFound, reference.JobID)
	}
	if err != nil {
		return nil, fmt.Errorf("get query job %s: %w", reference.JobID, err)
	}
	r.restoreQueryRows(job)
	return job, nil
}

func (r *QueryJobRepository) List(ctx context.Context, projectID, location string) ([]*domain.Job, error) {
	if err := domain.ValidateJobListScope(projectID, location); err != nil {
		return nil, err
	}
	if r == nil || r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("query job repository is not open")
	}
	statement := queryJobSelect + ` WHERE j.project_id = ? AND j.job_kind = 'QUERY'`
	arguments := []any{projectID}
	if location != "" {
		statement += " AND j.location_key = ?"
		arguments = append(arguments, jobLocationKey(location))
	}
	statement += " ORDER BY j.created_at DESC, j.job_id, j.location_key"
	rows, err := r.store.db.QueryContext(ctx, statement, arguments...)
	if err != nil {
		return nil, fmt.Errorf("list query jobs: %w", err)
	}
	defer rows.Close()
	jobs := make([]*domain.Job, 0)
	for rows.Next() {
		job, err := scanQueryJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan query job: %w", err)
		}
		r.restoreQueryRows(job)
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate query jobs: %w", err)
	}
	return jobs, nil
}

func encodeQueryJob(job *domain.Job) (persistedQueryJob, error) {
	if job == nil {
		return persistedQueryJob{}, fmt.Errorf("%w: query job is required", domain.ErrInvalid)
	}
	if job.CreatedAt.IsZero() {
		return persistedQueryJob{}, fmt.Errorf("%w: query job creation time is required", domain.ErrInvalid)
	}
	configuration := job.Configuration
	if configuration.SQL == "" {
		configuration.SQL = job.Query
	}
	normalized, err := domain.NewConfiguredQueryJob(job.Reference, configuration, job.CreatedAt)
	if err != nil {
		return persistedQueryJob{}, err
	}
	if job.ConfigurationDigest != normalized.ConfigurationDigest {
		return persistedQueryJob{}, fmt.Errorf("%w: query configuration digest does not match configuration", domain.ErrInvalid)
	}
	configurationJSON, err := json.Marshal(normalized.Configuration)
	if err != nil {
		return persistedQueryJob{}, fmt.Errorf("%w: encode query configuration", domain.ErrInvalid)
	}
	record := persistedQueryJob{
		identity: persistedJobIdentity{
			projectID: job.Reference.ProjectID, location: job.Reference.Location, jobID: job.Reference.JobID,
			kind: queryJobKind, configurationDigest: job.ConfigurationDigest, state: string(job.State),
			createdAt: job.CreatedAt.UTC(), startedAt: cloneTimePointer(job.StartedAt), endedAt: cloneTimePointer(job.EndedAt),
		},
		configurationJSON: string(configurationJSON), columnsJSON: "[]",
	}
	if job.Error != nil {
		record.identity.errorReason = nullableString(job.Error.Reason)
		record.identity.errorMessage = nullableString(job.Error.Message)
	}
	if job.Result != nil {
		record.hasResult = true
		record.affectedRows = job.Result.AffectedRows
		record.totalRows = job.Result.TotalRows
		columnsJSON, err := json.Marshal(job.Result.Columns)
		if err != nil {
			return persistedQueryJob{}, fmt.Errorf("%w: encode query result schema", domain.ErrInvalid)
		}
		record.columnsJSON = string(columnsJSON)
	}
	if err := validateQueryJobPersistence(job); err != nil {
		return persistedQueryJob{}, err
	}
	return record, nil
}

func validateQueryJobPersistence(job *domain.Job) error {
	if job.StartedAt != nil && job.StartedAt.Before(job.CreatedAt) {
		return fmt.Errorf("%w: query job starts before it was created", domain.ErrInvalid)
	}
	if job.EndedAt != nil && (job.StartedAt == nil || job.EndedAt.Before(*job.StartedAt)) {
		return fmt.Errorf("%w: query job ends before it starts", domain.ErrInvalid)
	}
	switch job.State {
	case domain.JobPending:
		if job.StartedAt != nil || job.EndedAt != nil || job.Error != nil || job.Result != nil {
			return fmt.Errorf("%w: pending query job contains execution state", domain.ErrInvalid)
		}
	case domain.JobRunning:
		if job.StartedAt == nil || job.EndedAt != nil || job.Error != nil || job.Result != nil {
			return fmt.Errorf("%w: running query job has inconsistent execution state", domain.ErrInvalid)
		}
	case domain.JobDone:
		if job.StartedAt == nil || job.EndedAt == nil || (job.Error == nil) == (job.Result == nil) {
			return fmt.Errorf("%w: done query job has inconsistent terminal state", domain.ErrInvalid)
		}
	default:
		return fmt.Errorf("%w: unknown query job state %q", domain.ErrInvalid, job.State)
	}
	if job.Result != nil {
		if job.Result.AffectedRows < 0 {
			return fmt.Errorf("%w: query affected row count must not be negative", domain.ErrInvalid)
		}
		for _, row := range job.Result.Rows {
			if len(row) != len(job.Result.Columns) {
				return fmt.Errorf("%w: query result row does not match result schema", domain.ErrInvalid)
			}
		}
		if job.Result.TotalRows < 0 || (job.Result.RowsAvailable && job.Result.TotalRows != int64(len(job.Result.Rows))) {
			return fmt.Errorf("%w: query result total row count is inconsistent", domain.ErrInvalid)
		}
	}
	return nil
}

func scanQueryJob(scanner rowScanner) (*domain.Job, error) {
	var identity persistedJobIdentity
	var kind, createdAt string
	var startedAt, endedAt sql.NullString
	var configurationJSON, columnsJSON string
	var hasResult int
	var affectedRows, totalRows int64
	if err := scanner.Scan(
		&identity.projectID, &identity.location, &identity.jobID, &kind,
		&identity.configurationDigest, &identity.state, &identity.errorReason, &identity.errorMessage,
		&createdAt, &startedAt, &endedAt,
		&configurationJSON, &hasResult, &columnsJSON, &affectedRows, &totalRows,
	); err != nil {
		return nil, err
	}
	identity.kind = jobKind(kind)
	if identity.kind != queryJobKind {
		return nil, errors.New("persisted query job has the wrong kind")
	}
	var configuration domain.QueryConfiguration
	if err := json.Unmarshal([]byte(configurationJSON), &configuration); err != nil {
		return nil, errors.New("decode persisted query configuration")
	}
	created, err := decodeJobTime(createdAt)
	if err != nil {
		return nil, errors.New("decode persisted query creation time")
	}
	job, err := domain.NewConfiguredQueryJob(domain.JobReference{
		ProjectID: identity.projectID, Location: identity.location, JobID: identity.jobID,
	}, configuration, created)
	if err != nil || job.ConfigurationDigest != identity.configurationDigest {
		return nil, errors.New("persisted query configuration is invalid")
	}
	started, err := decodeNullableJobTime(startedAt)
	if err != nil {
		return nil, errors.New("decode persisted query start time")
	}
	ended, err := decodeNullableJobTime(endedAt)
	if err != nil {
		return nil, errors.New("decode persisted query end time")
	}
	if identity.state != string(domain.JobPending) {
		if started == nil || job.Start(*started) != nil {
			return nil, errors.New("persisted query running state is invalid")
		}
	}
	if identity.state == string(domain.JobDone) {
		if ended == nil {
			return nil, errors.New("persisted query terminal time is invalid")
		}
		if identity.errorReason.Valid || identity.errorMessage.Valid {
			if !identity.errorReason.Valid || !identity.errorMessage.Valid || hasResult != 0 || totalRows != 0 || affectedRows != 0 || columnsJSON != "[]" ||
				job.Fail(identity.errorReason.String, identity.errorMessage.String, *ended) != nil {
				return nil, errors.New("persisted query failure is invalid")
			}
		} else {
			result := domain.QueryResult{AffectedRows: affectedRows}
			if hasResult != 1 || totalRows < 0 || json.Unmarshal([]byte(columnsJSON), &result.Columns) != nil || job.Complete(result, *ended) != nil {
				return nil, errors.New("persisted query result metadata is invalid")
			}
			job.Result.TotalRows = totalRows
			job.Result.RowsAvailable = totalRows == 0
		}
	} else if identity.state != string(domain.JobPending) && identity.state != string(domain.JobRunning) {
		return nil, errors.New("persisted query state is invalid")
	}
	if identity.state != string(domain.JobDone) && (hasResult != 0 || totalRows != 0 || affectedRows != 0 || columnsJSON != "[]") {
		return nil, errors.New("nonterminal persisted query contains result metadata")
	}
	return job, nil
}

func (r *QueryJobRepository) cacheQueryRows(job *domain.Job) {
	key := queryRowsKey(job.Reference)
	r.rowsMu.Lock()
	defer r.rowsMu.Unlock()
	if job.Result == nil {
		delete(r.transientRows, key)
		return
	}
	r.transientRows[key] = cloneQueryRows(job.Result.Rows)
}

func (r *QueryJobRepository) restoreQueryRows(job *domain.Job) {
	if job == nil || job.Result == nil {
		return
	}
	r.rowsMu.RLock()
	rows, ok := r.transientRows[queryRowsKey(job.Reference)]
	r.rowsMu.RUnlock()
	if ok {
		job.Result.Rows = cloneQueryRows(rows)
		job.Result.RowsAvailable = true
	}
}

func queryRowsKey(reference domain.JobReference) string {
	return reference.ProjectID + "\x00" + jobLocationKey(reference.Location) + "\x00" + reference.JobID
}

func cloneQueryJob(job *domain.Job) *domain.Job {
	if job == nil {
		return nil
	}
	clone := *job
	clone.StartedAt = cloneTimePointer(job.StartedAt)
	clone.EndedAt = cloneTimePointer(job.EndedAt)
	if job.Error != nil {
		value := *job.Error
		clone.Error = &value
	}
	clone.Configuration = job.Configuration
	if job.Configuration.Destination != nil {
		value := *job.Configuration.Destination
		clone.Configuration.Destination = &value
	}
	if job.Configuration.Labels != nil {
		clone.Configuration.Labels = make(map[string]string, len(job.Configuration.Labels))
		for key, value := range job.Configuration.Labels {
			clone.Configuration.Labels[key] = value
		}
	}
	clone.Configuration.QueryParameters = append([]domain.QueryParameter(nil), job.Configuration.QueryParameters...)
	if job.Result != nil {
		result := *job.Result
		result.Columns = append([]domain.Column(nil), job.Result.Columns...)
		result.Rows = cloneQueryRows(job.Result.Rows)
		clone.Result = &result
	}
	return &clone
}

func cloneQueryRows(rows [][]any) [][]any {
	result := make([][]any, len(rows))
	for rowIndex, row := range rows {
		result[rowIndex] = make([]any, len(row))
		for valueIndex, value := range row {
			result[rowIndex][valueIndex] = cloneQueryValue(value)
		}
	}
	return result
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

func cloneTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	clone := value.UTC()
	return &clone
}

func requireOneJobDetail(result sql.Result) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect job detail update: %w", err)
	}
	if affected != 1 {
		return errJobDetailsNotFound
	}
	return nil
}
