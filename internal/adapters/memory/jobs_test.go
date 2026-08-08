package memory

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestJobRepositoryReturnsDefensiveDeepCopies(t *testing.T) {
	ctx, cancel := memoryQueryTestContext(t)
	defer cancel()
	repository := NewJobRepository()
	now := time.Date(2026, 8, 8, 2, 0, 0, 0, time.UTC)
	job, err := domain.NewConfiguredQueryJob(domain.JobReference{ProjectID: "test-project", JobID: "j"}, domain.QueryConfiguration{
		SQL: "SELECT 1", StatementType: "SELECT", Labels: map[string]string{"component": "query"},
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := job.Start(now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	result := domain.QueryResult{Rows: [][]any{{[]byte("bytes"), []any{"nested"}, map[string]any{"key": "value"}}}}
	if err := job.Complete(result, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, created, err := repository.CreateOrGet(ctx, job); err != nil {
		t.Fatal(err)
	} else if !created {
		t.Fatal("first job insert was not created")
	}

	loaded, err := repository.Get(ctx, job.Reference)
	if err != nil {
		t.Fatal(err)
	}
	*loaded.StartedAt = time.Time{}
	*loaded.EndedAt = time.Time{}
	loaded.Result.Rows[0][0].([]byte)[0] = 'X'
	loaded.Result.Rows[0][1].([]any)[0] = "changed"
	loaded.Result.Rows[0][2].(map[string]any)["key"] = "changed"
	loaded.Configuration.Labels["component"] = "changed"

	again, err := repository.Get(ctx, job.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if again.StartedAt.IsZero() || again.EndedAt.IsZero() ||
		string(again.Result.Rows[0][0].([]byte)) != "bytes" ||
		again.Result.Rows[0][1].([]any)[0] != "nested" ||
		again.Result.Rows[0][2].(map[string]any)["key"] != "value" ||
		again.Configuration.Labels["component"] != "query" {
		t.Fatalf("stored job was mutated through a returned clone: %#v", again)
	}
}

func TestJobRepositoryRejectsUnsafeCompositeKeyComponents(t *testing.T) {
	ctx, cancel := memoryQueryTestContext(t)
	defer cancel()
	repository := NewJobRepository()
	for name, reference := range map[string]domain.JobReference{
		"project control":  {ProjectID: "test\x00project", Location: "US", JobID: "job"},
		"location control": {ProjectID: "test-project", Location: "US\n", JobID: "job"},
		"job control":      {ProjectID: "test-project", Location: "US", JobID: "job\x00other"},
	} {
		t.Run(name, func(t *testing.T) {
			job := &domain.Job{Reference: reference, Query: "SELECT 1"}
			if _, _, err := repository.CreateOrGet(ctx, job); !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("CreateOrGet unsafe reference error = %v, want invalid", err)
			}
			if _, err := repository.Get(ctx, reference); !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("Get unsafe reference error = %v, want invalid", err)
			}
		})
	}
}

func TestJobRepositoryListUsesLocationAsFinalStableTieBreak(t *testing.T) {
	ctx, cancel := memoryQueryTestContext(t)
	defer cancel()
	repository := NewJobRepository()
	now := time.Date(2026, 8, 8, 2, 0, 0, 0, time.UTC)
	for _, location := range []string{"US", "EU"} {
		job, err := domain.NewConfiguredQueryJob(domain.JobReference{
			ProjectID: "test-project", Location: location, JobID: "same",
		}, domain.QueryConfiguration{SQL: "SELECT 1", StatementType: "SELECT"}, now)
		if err != nil {
			t.Fatal(err)
		}
		if _, created, err := repository.CreateOrGet(ctx, job); err != nil || !created {
			t.Fatalf("create %s job: created=%v err=%v", location, created, err)
		}
	}
	jobs, err := repository.List(ctx, "test-project", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 || jobs[0].Reference.Location != "EU" || jobs[1].Reference.Location != "US" {
		t.Fatalf("stable location order = %#v", jobs)
	}
}

func memoryQueryTestContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	timeout := 10 * time.Second
	if configured := os.Getenv("BQEMU_QUERY_TEST_TIMEOUT"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		if err != nil || parsed <= 0 {
			t.Fatalf("BQEMU_QUERY_TEST_TIMEOUT must be a positive Go duration: %q", configured)
		}
		timeout = parsed
	}
	return context.WithTimeout(context.Background(), timeout)
}
