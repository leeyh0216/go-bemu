package application

import (
	"context"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
)

type fixedQueryID string

func (id fixedQueryID) NewID() string { return string(id) }

func TestQueryServiceUsesConfiguredDefaultLocation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	service := NewQueryService(
		memory.NewJobRepository(), &fakeWarehouse{},
		fixedClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}, fixedQueryID("one"),
		WithQueryDefaultLocation("EU"),
	)
	job, err := service.RunSync(ctx, QueryInput{ProjectID: "test-project", SQL: "SELECT 1"})
	if err != nil {
		t.Fatal(err)
	}
	if job.Reference.Location != "EU" {
		t.Fatalf("job location = %q, want EU", job.Reference.Location)
	}
}
