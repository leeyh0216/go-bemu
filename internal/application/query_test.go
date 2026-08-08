package application

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

type fixedQueryID string

func (id fixedQueryID) NewID() string { return string(id) }

func TestQueryServiceRequiresAnalyzedStatementPortsAtComposition(t *testing.T) {
	clock := fixedClock{now: time.Unix(1, 0)}
	ids := fixedQueryID("composition")
	for name, options := range map[string][]QueryOption{
		"gateway":  {WithStatementExecutor(&countingStatementExecutor{})},
		"executor": {withTestQueryAnalysis(ports.QueryAnalysis{})},
	} {
		t.Run(name, func(t *testing.T) {
			service, err := NewQueryService(memory.NewJobRepository(), clock, ids, options...)
			if service != nil || !errors.Is(err, domain.ErrPrecondition) {
				t.Fatalf("NewQueryService() = (%v, %v), want nil precondition error", service, err)
			}
		})
	}
}

func TestQueryServiceUsesConfiguredDefaultLocation(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	service := newTestQueryService(
		memory.NewJobRepository(), &countingStatementExecutor{},
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

func TestQueryJobLogsLabelValuesWithShape(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previous)
	})

	service := newTestQueryService(
		memory.NewJobRepository(), &countingStatementExecutor{}, fixedClock{now: time.Unix(1, 0)}, fixedQueryID("safe-log"),
	)
	const secretValue = "secret-label-value-42"
	if _, err := service.RunSync(ctx, QueryInput{
		ProjectID: "test-project", SQL: "SELECT 1", Labels: map[string]string{"component": secretValue},
	}); err != nil {
		t.Fatal(err)
	}
	output := logs.String()
	if !strings.Contains(output, secretValue) {
		t.Fatalf("label value omitted from query logs: %s", output)
	}
	for _, field := range []string{"label_count", "label_keys_fingerprint", "configuration_fingerprint"} {
		if !strings.Contains(output, field) {
			t.Fatalf("query log is missing %q: %s", field, output)
		}
	}
}

type countingStatementExecutor struct{ calls atomic.Int64 }

func (executor *countingStatementExecutor) ExecuteStatement(context.Context, semantic.Statement) (domain.QueryResult, error) {
	executor.calls.Add(1)
	return domain.QueryResult{Columns: []domain.Column{{Name: "value", Type: "INTEGER"}}, Rows: [][]any{{int64(1)}}}, nil
}

type deadlineAwareStatementExecutor struct {
	deadlines chan time.Time
}

func (executor *deadlineAwareStatementExecutor) ExecuteStatement(ctx context.Context, _ semantic.Statement) (domain.QueryResult, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return domain.QueryResult{}, errors.New("query execution context has no deadline")
	}
	executor.deadlines <- deadline
	<-ctx.Done()
	return domain.QueryResult{}, ctx.Err()
}

type closeControlledStatementExecutor struct {
	started  chan struct{}
	canceled chan struct{}
	release  chan struct{}
	finished chan struct{}
}

func newCloseControlledStatementExecutor() *closeControlledStatementExecutor {
	return &closeControlledStatementExecutor{
		started: make(chan struct{}), canceled: make(chan struct{}),
		release: make(chan struct{}), finished: make(chan struct{}),
	}
}

func (executor *closeControlledStatementExecutor) ExecuteStatement(ctx context.Context, _ semantic.Statement) (domain.QueryResult, error) {
	close(executor.started)
	<-ctx.Done()
	close(executor.canceled)
	<-executor.release
	close(executor.finished)
	return domain.QueryResult{}, ctx.Err()
}

func TestQueryServiceCloseCancelsWaitsRejectsAndIsIdempotent(t *testing.T) {
	for _, asynchronous := range []bool{false, true} {
		name := "sync"
		if asynchronous {
			name = "async"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := queryApplicationTestContext(t)
			defer cancel()
			executor := newCloseControlledStatementExecutor()
			service := newTestQueryService(
				memory.NewJobRepository(), executor, fixedClock{now: time.Unix(1, 0)}, fixedQueryID(name),
				WithQueryOperationTimeout(time.Minute),
			)
			input := QueryInput{ProjectID: "test-project", JobID: name, SQL: "SELECT 1"}
			workResult := make(chan error, 1)
			if asynchronous {
				if _, err := service.Submit(ctx, input); err != nil {
					t.Fatal(err)
				}
			} else {
				go func() {
					_, err := service.RunSync(ctx, input)
					workResult <- err
				}()
			}
			waitForQueryLifecycleSignal(t, ctx, executor.started, "query start")

			closeResult := make(chan error, 1)
			go func() { closeResult <- service.Close(ctx) }()
			waitForQueryLifecycleSignal(t, ctx, executor.canceled, "query cancellation")
			select {
			case err := <-closeResult:
				t.Fatalf("Close returned before active query released: %v", err)
			default:
			}
			if _, err := service.Submit(ctx, QueryInput{ProjectID: "test-project", JobID: name + "-rejected", SQL: "SELECT 2"}); !errors.Is(err, ErrQueryServiceClosed) {
				t.Fatalf("Submit during close error = %v, want ErrQueryServiceClosed", err)
			}
			if _, err := service.RunSync(ctx, QueryInput{ProjectID: "test-project", JobID: name + "-sync-rejected", SQL: "SELECT 3"}); !errors.Is(err, ErrQueryServiceClosed) {
				t.Fatalf("RunSync during close error = %v, want ErrQueryServiceClosed", err)
			}
			canceledCtx, cancelClose := context.WithCancel(ctx)
			cancelClose()
			if err := service.Close(canceledCtx); !errors.Is(err, context.Canceled) {
				t.Fatalf("bounded Close error = %v, want context canceled", err)
			}

			close(executor.release)
			waitForQueryLifecycleSignal(t, ctx, executor.finished, "query finish")
			select {
			case err := <-closeResult:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if !asynchronous {
				select {
				case err := <-workResult:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if err := service.Close(ctx); err != nil {
				t.Fatalf("idempotent Close: %v", err)
			}
		})
	}
}

func waitForQueryLifecycleSignal(t *testing.T, ctx context.Context, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatalf("waiting for %s: %v", name, ctx.Err())
	}
}

func TestQueryOperationTimeoutBoundsSyncAndAsyncExecution(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	for _, asynchronous := range []bool{false, true} {
		name := "sync"
		if asynchronous {
			name = "async"
		}
		t.Run(name, func(t *testing.T) {
			executor := &deadlineAwareStatementExecutor{deadlines: make(chan time.Time, 1)}
			service := newTestQueryService(
				memory.NewJobRepository(), executor, fixedClock{now: time.Unix(1, 0)}, fixedQueryID(name),
				WithQueryOperationTimeout(20*time.Millisecond),
			)
			input := QueryInput{ProjectID: "test-project", JobID: name, SQL: "SELECT 1"}
			var job *domain.Job
			var err error
			if asynchronous {
				job, err = service.Submit(ctx, input)
			} else {
				job, err = service.RunSync(ctx, input)
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case deadline := <-executor.deadlines:
				if deadline.IsZero() {
					t.Fatal("query execution deadline is zero")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if asynchronous {
				job = waitForQueryJobDone(t, ctx, service, job.Reference)
			}
			if job.State != domain.JobDone || job.Error == nil {
				t.Fatalf("timed-out query job = %#v", job)
			}
		})
	}
}

func TestQueryJobIdentityIncludesLocationAndConfigurationFingerprint(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	repository := memory.NewJobRepository()
	executor := &countingStatementExecutor{}
	service := newTestQueryService(repository, executor, fixedClock{now: time.Unix(1, 0)}, fixedQueryID("generated"))
	input := QueryInput{ProjectID: "test-project", Location: "US", JobID: "stable", SQL: "SELECT 1"}

	const callers = 16
	var wait sync.WaitGroup
	errorsByCaller := make(chan error, callers)
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := service.Submit(ctx, input)
			errorsByCaller <- err
		}()
	}
	wait.Wait()
	close(errorsByCaller)
	successes, conflicts := 0, 0
	for err := range errorsByCaller {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, domain.ErrConflict):
			conflicts++
		default:
			t.Fatalf("concurrent submit error = %v", err)
		}
	}
	if successes != 1 || conflicts != callers-1 {
		t.Fatalf("concurrent duplicate results: successes=%d conflicts=%d", successes, conflicts)
	}
	waitForQueryJobDone(t, ctx, service, domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "stable"})
	if got := executor.calls.Load(); got != 1 {
		t.Fatalf("same identity/configuration executed %d times, want 1", got)
	}
	if _, err := service.Submit(ctx, input); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("same identity with same query error = %v, want conflict", err)
	}
	if _, err := service.Submit(ctx, QueryInput{
		ProjectID: "test-project", Location: "US", JobID: "stable", SQL: "SELECT 2",
	}); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("same identity with different query error = %v, want conflict", err)
	}
	if _, err := service.Submit(ctx, QueryInput{
		ProjectID: "test-project", Location: "EU", JobID: "stable", SQL: "SELECT 2",
	}); err != nil {
		t.Fatal(err)
	}
	waitForQueryJobDone(t, ctx, service, domain.JobReference{ProjectID: "test-project", Location: "EU", JobID: "stable"})
	if got := executor.calls.Load(); got != 2 {
		t.Fatalf("same jobId in a second location executed calls=%d, want 2", got)
	}
	if _, err := service.Get(ctx, domain.JobReference{ProjectID: "test-project", Location: "asia-northeast3", JobID: "stable"}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("wrong-location get error = %v, want not found", err)
	}
}

type failedPublicationCatalog struct{}

func (failedPublicationCatalog) GetDataset(context.Context, string, string) (domain.Dataset, error) {
	return domain.Dataset{ProjectID: "test-project", ID: "analytics", Location: "US"}, nil
}
func (failedPublicationCatalog) GetTable(context.Context, string, string, string) (domain.Table, error) {
	return domain.Table{}, fmt.Errorf("%w: destination", domain.ErrNotFound)
}
func (failedPublicationCatalog) EnsureAnonymousDataset(context.Context, string, string, string) (domain.Dataset, error) {
	return domain.Dataset{ProjectID: "test-project", ID: "_bqemu_anonymous", Location: "US", Hidden: true}, nil
}
func (failedPublicationCatalog) PublishMaterializedTable(context.Context, domain.Table) error {
	return fmt.Errorf("%w: injected metadata publication failure", domain.ErrConflict)
}

type compensatingMaterializer struct{ drops atomic.Int64 }

func (*compensatingMaterializer) MaterializeAnalyzedStatement(context.Context, semantic.Statement, ports.StatementMaterializationRequest) (ports.StatementMaterializationResult, error) {
	return ports.StatementMaterializationResult{
		QueryResult:        domain.QueryResult{Columns: []domain.Column{{Name: "id", Type: "INTEGER"}}, Rows: [][]any{{int64(1)}}},
		DestinationCreated: true,
	}, nil
}

type deadlineCompensatingMaterializer struct {
	compensatingMaterializer
	deadlineSeen atomic.Bool
}

type complexResultMaterializer struct{ drops atomic.Int64 }

func (*complexResultMaterializer) MaterializeAnalyzedStatement(context.Context, semantic.Statement, ports.StatementMaterializationRequest) (ports.StatementMaterializationResult, error) {
	return ports.StatementMaterializationResult{
		QueryResult: domain.QueryResult{
			Columns: []domain.Column{{Name: "values", Type: "ARRAY"}}, Rows: [][]any{{[]any{int64(1)}}},
		},
		DestinationCreated: true,
	}, nil
}

func (materializer *complexResultMaterializer) DropMaterializedDestination(context.Context, domain.TableReference) error {
	materializer.drops.Add(1)
	return nil
}

func (materializer *deadlineCompensatingMaterializer) DropMaterializedDestination(ctx context.Context, _ domain.TableReference) error {
	if _, ok := ctx.Deadline(); ok {
		materializer.deadlineSeen.Store(true)
	}
	materializer.drops.Add(1)
	<-ctx.Done()
	return ctx.Err()
}
func (materializer *compensatingMaterializer) DropMaterializedDestination(context.Context, domain.TableReference) error {
	materializer.drops.Add(1)
	return nil
}

func TestMaterializedTablePublicationFailureIsCompensated(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	materializer := &compensatingMaterializer{}
	service := newTestQueryService(
		memory.NewJobRepository(), &countingStatementExecutor{}, fixedClock{now: time.Unix(1, 0)}, fixedQueryID("generated"),
		WithStatementMaterializer(materializer), WithQueryDestinationCatalog(failedPublicationCatalog{}),
	)
	job, err := service.RunSync(ctx, QueryInput{
		ProjectID: "test-project", Location: "US", JobID: "publish-fails", SQL: "SELECT 1 AS id",
		Destination: &domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "materialized"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if job.State != domain.JobDone || job.Error == nil || job.Error.Reason != "duplicate" {
		t.Fatalf("publication failure job = %#v", job)
	}
	if got := materializer.drops.Load(); got != 1 {
		t.Fatalf("compensating drops = %d, want 1", got)
	}
}

func TestMaterializedTableCompensationHasDetachedDeadline(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	materializer := &deadlineCompensatingMaterializer{}
	service := newTestQueryService(
		memory.NewJobRepository(), &countingStatementExecutor{}, fixedClock{now: time.Unix(1, 0)}, fixedQueryID("bounded-cleanup"),
		WithStatementMaterializer(materializer), WithQueryDestinationCatalog(failedPublicationCatalog{}),
		WithQueryCompensationTimeout(20*time.Millisecond),
	)
	job, err := service.RunSync(ctx, QueryInput{
		ProjectID: "test-project", Location: "US", JobID: "bounded-cleanup", SQL: "SELECT 1 AS id",
		Destination: &domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "materialized"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if job.State != domain.JobDone || job.Error == nil || !materializer.deadlineSeen.Load() || materializer.drops.Load() != 1 {
		t.Fatalf("bounded compensation job=%#v deadline_seen=%v drops=%d", job, materializer.deadlineSeen.Load(), materializer.drops.Load())
	}
}

func TestComplexAnonymousResultFailsWithStableGapAndCompensates(t *testing.T) {
	ctx, cancel := queryApplicationTestContext(t)
	defer cancel()
	materializer := &complexResultMaterializer{}
	service := newTestQueryService(
		memory.NewJobRepository(), &countingStatementExecutor{}, fixedClock{now: time.Unix(1, 0)}, fixedQueryID("complex-result"),
		withTestQueryAnalysis(ports.QueryAnalysis{ProducesRows: true}),
		WithStatementMaterializer(materializer), WithQueryDestinationCatalog(failedPublicationCatalog{}),
	)
	job, err := service.RunSync(ctx, QueryInput{ProjectID: "test-project", JobID: "complex-result", SQL: "SELECT [1] AS values"})
	if err != nil {
		t.Fatal(err)
	}
	if job.State != domain.JobDone || job.Error == nil || !strings.Contains(job.Error.Message, domain.GapQueryComplexResultSchemaV1) {
		t.Fatalf("complex result job = %#v", job)
	}
	if materializer.drops.Load() != 1 {
		t.Fatalf("complex result compensating drops = %d, want one", materializer.drops.Load())
	}
}

func waitForQueryJobDone(t *testing.T, ctx context.Context, service *QueryService, reference domain.JobReference) *domain.Job {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		job, err := service.Get(ctx, reference)
		if err != nil {
			t.Fatal(err)
		}
		if job.State == domain.JobDone {
			return job
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
}

func queryApplicationTestContext(t *testing.T) (context.Context, context.CancelFunc) {
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
