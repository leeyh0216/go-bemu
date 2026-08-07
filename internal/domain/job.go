package domain

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

func (j *Job) Fail(reason, message string, now time.Time) error {
	if j.State != JobRunning {
		return fmt.Errorf("%w: cannot fail job in state %s", ErrConflict, j.State)
	}
	j.State = JobDone
	j.Error = &JobError{Reason: reason, Message: message}
	j.EndedAt = &now
	return nil
}
