package domain

import (
	"errors"
	"testing"
	"time"
)

func TestJobStateMachine(t *testing.T) {
	now := time.Date(2026, 8, 8, 1, 2, 3, 0, time.UTC)
	job, err := NewQueryJob(JobReference{ProjectID: "test-project", JobID: "job-1"}, "SELECT 1", now)
	if err != nil {
		t.Fatal(err)
	}
	if job.State != JobPending || job.Reference.Location != "US" {
		t.Fatalf("unexpected new job: %#v", job)
	}
	if err := job.Start(now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	result := QueryResult{Rows: [][]any{{int64(1)}}}
	if err := job.Complete(result, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if job.State != JobDone || job.Result == nil || job.EndedAt == nil {
		t.Fatalf("unexpected completed job: %#v", job)
	}
	if err := job.Start(now); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected state conflict, got %v", err)
	}
}

func TestFailedJobIsTerminalDoneWithErrorResult(t *testing.T) {
	now := time.Date(2026, 8, 8, 1, 2, 3, 0, time.UTC)
	job, err := NewQueryJob(JobReference{ProjectID: "test-project", JobID: "job-failed"}, "SELECT missing", now)
	if err != nil {
		t.Fatal(err)
	}
	if err := job.Start(now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := job.Fail("invalidQuery", "column not found", now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if job.State != JobDone || job.Error == nil || job.Error.Reason != "invalidQuery" || job.Result != nil || job.EndedAt == nil {
		t.Fatalf("failed jobs must be terminal DONE with errorResult: %#v", job)
	}
}

func TestFieldValidationRejectsDuplicateAndInvalidTypes(t *testing.T) {
	table := Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events",
		Schema: []Field{{Name: "id", Type: "INT64"}, {Name: "ID", Type: "INT64"}},
	}
	if err := table.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected invalid duplicate field, got %v", err)
	}
	table.Schema = []Field{{Name: "payload", Type: "NOT_A_TYPE"}}
	if err := table.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected invalid field type, got %v", err)
	}
}
