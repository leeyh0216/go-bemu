package domain

// BigQuery Job and JobStatus state provenance:
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/Job
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/JobStatus
//
// DONE is terminal for both success and failure. A successful DONE job has a
// result and no errorResult; a failed DONE job has an errorResult. Clients must
// inspect both state and errorResult rather than treating DONE as success.

import (
	"fmt"
	"time"
)

type JobState string

const (
	JobPending JobState = "PENDING"
	JobRunning JobState = "RUNNING"
	JobDone    JobState = "DONE"
)

type JobReference struct {
	ProjectID string
	JobID     string
	Location  string
}

type JobError struct {
	Reason  string
	Message string
}

type Column struct {
	Name string
	Type string
}

type QueryResult struct {
	Columns      []Column
	Rows         [][]any
	AffectedRows int64
}

type Job struct {
	Reference JobReference
	Query     string
	State     JobState
	Error     *JobError
	Result    *QueryResult
	CreatedAt time.Time
	StartedAt *time.Time
	EndedAt   *time.Time
}

func NewQueryJob(reference JobReference, query string, now time.Time) (*Job, error) {
	if reference.ProjectID == "" || reference.JobID == "" || query == "" {
		return nil, fmt.Errorf("%w: job reference and query are required", ErrInvalid)
	}
	if reference.Location == "" {
		// US is BigQuery's documented default multi-region for callers that use
		// the domain constructor directly. Runtime composition supplies its
		// configured default before this boundary.
		// https://cloud.google.com/bigquery/docs/locations
		reference.Location = "US"
	}
	return &Job{Reference: reference, Query: query, State: JobPending, CreatedAt: now}, nil
}

func (j *Job) Start(now time.Time) error {
	if j.State != JobPending {
		return fmt.Errorf("%w: cannot start job in state %s", ErrConflict, j.State)
	}
	j.State = JobRunning
	j.StartedAt = &now
	return nil
}

func (j *Job) Complete(result QueryResult, now time.Time) error {
	if j.State != JobRunning {
		return fmt.Errorf("%w: cannot complete job in state %s", ErrConflict, j.State)
	}
	j.State = JobDone
	j.Result = &result
	j.EndedAt = &now
	return nil
}

// Fail intentionally transitions RUNNING to DONE. This mirrors the REST wire
// contract where status.state remains DONE and status.errorResult carries the
// terminal failure; there is no separate FAILED state.
func (j *Job) Fail(reason, message string, now time.Time) error {
	if j.State != JobRunning {
		return fmt.Errorf("%w: cannot fail job in state %s", ErrConflict, j.State)
	}
	j.State = JobDone
	j.Error = &JobError{Reason: reason, Message: message}
	j.EndedAt = &now
	return nil
}
