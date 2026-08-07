package application

// Catalog metadata must remain stable for the complete semantic DML operation,
// matching BigQuery's statement-level atomicity contract:
// https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestWithCanonicalTablesHoldsResourceMutationGateThroughOperation(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	service := NewCatalogService(memory.NewCatalogRepository(), &fakeWarehouse{}, fixedClock{now: time.Now()})
	if _, err := service.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "destination",
		Schema:           []domain.Field{{Name: "event_date", Type: "DATE"}},
		TimePartitioning: &domain.TimePartitioning{Type: "DAY", Field: "event_date"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "source",
		Schema: []domain.Field{{Name: "event_date", Type: "DATE"}},
	}); err != nil {
		t.Fatal(err)
	}
	sourceReference := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "source"}
	if _, err := service.WithCanonicalTables(ctx, domain.TableReference{
		ProjectID: "test-project", DatasetID: "analytics", TableID: "destination",
	}, sourceReference, nil); err == nil {
		t.Fatal("nil semantic operation callback must fail before taking the catalog gate")
	}
	callbackCalled := false
	if _, err := service.WithCanonicalTables(ctx, domain.TableReference{
		ProjectID: "test-project", DatasetID: "analytics", TableID: "destination",
	}, domain.TableReference{
		ProjectID: "test-project", DatasetID: "analytics", TableID: "missing-source",
	}, func(domain.Table, domain.Table) (domain.QueryResult, error) {
		callbackCalled = true
		return domain.QueryResult{}, nil
	}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing canonical source error = %v", err)
	}
	if callbackCalled {
		t.Fatal("semantic operation callback ran without canonical source metadata")
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	operationDone := make(chan error, 1)
	go func() {
		_, err := service.WithCanonicalTables(ctx, domain.TableReference{
			ProjectID: "test-project", DatasetID: "analytics", TableID: "destination",
		}, sourceReference, func(table, source domain.Table) (domain.QueryResult, error) {
			if table.TimePartitioning == nil || table.TimePartitioning.Field != "event_date" {
				return domain.QueryResult{}, domain.ErrPrecondition
			}
			if source.ID != "source" {
				return domain.QueryResult{}, domain.ErrPrecondition
			}
			close(entered)
			select {
			case <-release:
				return domain.QueryResult{}, nil
			case <-ctx.Done():
				return domain.QueryResult{}, ctx.Err()
			}
		})
		operationDone <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatalf("semantic operation did not enter catalog gate: %v", ctx.Err())
	}

	canceledMutation, cancelMutation := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelMutation()
	if err := service.DeleteTable(canceledMutation, "test-project", "analytics", "destination"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued mutation cancellation = %v, want deadline exceeded", err)
	}

	deleteStarted := make(chan struct{})
	deleteDone := make(chan error, 1)
	go func() {
		close(deleteStarted)
		deleteDone <- service.DeleteTable(ctx, "test-project", "analytics", "destination")
	}()
	<-deleteStarted
	timer := time.NewTimer(20 * time.Millisecond)
	defer timer.Stop()
	select {
	case err := <-deleteDone:
		t.Fatalf("table deletion crossed active semantic operation gate: %v", err)
	case <-timer.C:
	case <-ctx.Done():
		t.Fatalf("catalog gate assertion exceeded configurable query test timeout: %v", ctx.Err())
	}

	close(release)
	for name, done := range map[string]<-chan error{"operation": operationDone, "delete": deleteDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s failed: %v", name, err)
			}
		case <-ctx.Done():
			t.Fatalf("%s did not complete: %v", name, ctx.Err())
		}
	}
	if _, err := service.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "reuse_after_cancel",
		Schema: []domain.Field{{Name: "event_date", Type: "DATE"}},
	}); err != nil {
		t.Fatalf("resource mutation gate was not reusable after canceled waiter: %v", err)
	}
}
