package memory

import (
	"context"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestJobRepositoryReturnsDefensiveDeepCopies(t *testing.T) {
	ctx := context.Background()
	repository := NewJobRepository()
	now := time.Date(2026, 8, 8, 2, 0, 0, 0, time.UTC)
	job, err := domain.NewQueryJob(domain.JobReference{ProjectID: "p", JobID: "j"}, "SELECT 1", now)
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
	if err := repository.Create(ctx, job); err != nil {
		t.Fatal(err)
	}

	loaded, err := repository.Get(ctx, "p", "j")
	if err != nil {
		t.Fatal(err)
	}
	*loaded.StartedAt = time.Time{}
	*loaded.EndedAt = time.Time{}
	loaded.Result.Rows[0][0].([]byte)[0] = 'X'
	loaded.Result.Rows[0][1].([]any)[0] = "changed"
	loaded.Result.Rows[0][2].(map[string]any)["key"] = "changed"

	again, err := repository.Get(ctx, "p", "j")
	if err != nil {
		t.Fatal(err)
	}
	if again.StartedAt.IsZero() || again.EndedAt.IsZero() ||
		string(again.Result.Rows[0][0].([]byte)) != "bytes" ||
		again.Result.Rows[0][1].([]any)[0] != "nested" ||
		again.Result.Rows[0][2].(map[string]any)["key"] != "value" {
		t.Fatalf("stored job was mutated through a returned clone: %#v", again)
	}
}
