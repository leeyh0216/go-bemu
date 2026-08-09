package sqlite

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	querydomain "github.com/leeyh0216/go-bemu/internal/domain"
	loaddomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
)

func TestSQLiteQueryJobMetadataSurvivesRestartWithoutRowPayloads(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	store := openJobTestStore(t, path)
	now := time.Date(2026, 8, 8, 3, 0, 0, 123, time.UTC)
	createJobTestProject(t, store, now)
	repository := NewQueryJobRepository(store)
	job, err := querydomain.NewConfiguredQueryJob(querydomain.JobReference{
		ProjectID: "test-project", Location: "us", JobID: "query-restart",
	}, querydomain.QueryConfiguration{
		SQL: "SELECT payload FROM source", DefaultProjectID: "test-project", DefaultDataset: "source",
		Destination:      &querydomain.TableReference{ProjectID: "result-project", DatasetID: "results", TableID: "result"},
		WriteDisposition: querydomain.WriteEmpty, CreateDisposition: querydomain.CreateIfNeeded,
		Priority: querydomain.QueryPriorityBatch, Labels: map[string]string{"purpose": "restart"},
		ManagedDestination: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := repository.CreateOrGet(ctx, job); err != nil || !created {
		t.Fatalf("create query job: created=%v err=%v", created, err)
	}
	if err := job.Start(now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repository.Update(ctx, job); err != nil {
		t.Fatal(err)
	}
	rowPayload := "ROW_PAYLOAD_MUST_NOT_ENTER_SQLITE"
	if err := job.Complete(querydomain.QueryResult{
		Columns: []querydomain.Column{{Name: "payload", Type: "STRING"}},
		Rows:    [][]any{{rowPayload}, {"second"}}, AffectedRows: 7,
	}, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repository.Update(ctx, job); err != nil {
		t.Fatal(err)
	}
	live, err := repository.Get(ctx, querydomain.JobReference{ProjectID: "test-project", Location: "US", JobID: "query-restart"})
	if err != nil {
		t.Fatal(err)
	}
	if len(live.Result.Rows) != 2 || live.Result.Rows[0][0] != rowPayload {
		t.Fatalf("live query rows = %#v", live.Result.Rows)
	}
	var persistedTotal int64
	if err := store.db.QueryRowContext(ctx, `SELECT total_rows FROM query_job_details
		WHERE project_id = ? AND location_key = ? AND job_id = ?`,
		"test-project", "US", "query-restart").Scan(&persistedTotal); err != nil {
		t.Fatal(err)
	}
	if persistedTotal != 2 {
		t.Fatalf("persisted total rows = %d", persistedTotal)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	for _, candidate := range []string{path, path + "-wal"} {
		payload, err := os.ReadFile(candidate)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if bytes.Contains(payload, []byte(rowPayload)) {
			t.Fatalf("query row payload was persisted in %s", filepath.Base(candidate))
		}
	}

	reopened := openJobTestStore(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	restarted, err := NewQueryJobRepository(reopened).Get(ctx, querydomain.JobReference{
		ProjectID: "test-project", Location: "US", JobID: "query-restart",
	})
	if err != nil {
		t.Fatal(err)
	}
	if restarted.State != querydomain.JobDone || restarted.Error != nil || restarted.Result == nil ||
		len(restarted.Result.Columns) != 1 || restarted.Result.Columns[0].Name != "payload" ||
		restarted.Result.AffectedRows != 7 || len(restarted.Result.Rows) != 0 {
		t.Fatalf("restarted query job = %#v", restarted)
	}
	if restarted.Configuration.Destination == nil || restarted.Configuration.AnonymousDestination || !restarted.Configuration.ManagedDestination ||
		restarted.Configuration.Labels["purpose"] != "restart" || restarted.Configuration.SQL != "SELECT payload FROM source" {
		t.Fatalf("restarted query configuration = %#v", restarted.Configuration)
	}
}

func TestSQLiteLoadJobTerminalMetadataSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	store := openJobTestStore(t, path)
	now := time.Date(2026, 8, 8, 4, 0, 0, 0, time.UTC)
	createJobTestProject(t, store, now)
	repository := NewLoadJobRepository(store)
	job, err := loaddomain.NewJob(loaddomain.JobReference{
		ProjectID: "test-project", Location: "EU", JobID: "load-restart",
	}, testLoadJobConfiguration(), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := repository.CreateOrGet(ctx, job); err != nil || !created {
		t.Fatalf("create load job: created=%v err=%v", created, err)
	}
	if err := job.Start(now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repository.Update(ctx, job); err != nil {
		t.Fatal(err)
	}
	statistics := loaddomain.Statistics{InputFiles: 2, InputBytes: 1234, OutputBytes: 1200, OutputRows: 8}
	if err := job.Complete(statistics, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repository.Update(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openJobTestStore(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	restarted, err := NewLoadJobRepository(reopened).Get(ctx, job.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.State != loaddomain.JobDone || restarted.Error != nil || restarted.Statistics != statistics {
		t.Fatalf("restarted load job = %#v", restarted)
	}
	if len(restarted.Configuration.SourceURIs) != 1 || restarted.Configuration.SourceURIs[0] != "file:///input.parquet" ||
		len(restarted.Configuration.Schema) != 1 || restarted.Configuration.Schema[0].Fields[0].Name != "value" {
		t.Fatalf("restarted load configuration = %#v", restarted.Configuration)
	}
}

func TestSQLiteSharedJobIdentityAllowsOnlyOneConcurrentKind(t *testing.T) {
	ctx := context.Background()
	store := openJobTestStore(t, ":memory:")
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, 8, 8, 5, 0, 0, 0, time.UTC)
	createJobTestProject(t, store, now)
	queryRepository := NewQueryJobRepository(store)
	loadRepository := NewLoadJobRepository(store)
	queryJob, err := querydomain.NewQueryJob(querydomain.JobReference{
		ProjectID: "test-project", Location: "us", JobID: "shared-id",
	}, "SELECT 1", now)
	if err != nil {
		t.Fatal(err)
	}
	loadJob, err := loaddomain.NewJob(loaddomain.JobReference{
		ProjectID: "test-project", Location: "US", JobID: "shared-id",
	}, testLoadJobConfiguration(), now)
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	type outcome struct {
		kind    string
		created bool
		err     error
	}
	outcomes := make(chan outcome, 2)
	var group sync.WaitGroup
	group.Add(2)
	go func() {
		defer group.Done()
		<-start
		_, created, err := queryRepository.CreateOrGet(ctx, queryJob)
		outcomes <- outcome{kind: "QUERY", created: created, err: err}
	}()
	go func() {
		defer group.Done()
		<-start
		_, created, err := loadRepository.CreateOrGet(ctx, loadJob)
		outcomes <- outcome{kind: "LOAD", created: created, err: err}
	}()
	close(start)
	group.Wait()
	close(outcomes)

	createdCount, conflictCount := 0, 0
	for result := range outcomes {
		if result.created && result.err == nil {
			createdCount++
			continue
		}
		if !result.created && (errors.Is(result.err, querydomain.ErrConflict) || errors.Is(result.err, loaddomain.ErrConflict)) {
			conflictCount++
			continue
		}
		t.Fatalf("unexpected %s outcome: created=%v err=%v", result.kind, result.created, result.err)
	}
	if createdCount != 1 || conflictCount != 1 {
		t.Fatalf("created=%d conflicts=%d", createdCount, conflictCount)
	}
	var identityCount int
	if err := store.db.QueryRowContext(ctx, `SELECT count(*) FROM job_identities
		WHERE project_id = 'test-project' AND location_key = 'US' AND job_id = 'shared-id'`).Scan(&identityCount); err != nil {
		t.Fatal(err)
	}
	if identityCount != 1 {
		t.Fatalf("shared identities = %d", identityCount)
	}
}

func TestSQLiteInterruptedJobsReconcileToStableTerminalFailures(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	store := openJobTestStore(t, path)
	now := time.Date(2026, 8, 8, 5, 30, 0, 0, time.UTC)
	createJobTestProject(t, store, now)
	queryRepository := NewQueryJobRepository(store)
	queryJob, err := querydomain.NewQueryJob(querydomain.JobReference{
		ProjectID: "test-project", Location: "US", JobID: "pending-query",
	}, "SELECT 1", now)
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := queryRepository.CreateOrGet(ctx, queryJob); err != nil || !created {
		t.Fatalf("create interrupted query: created=%v err=%v", created, err)
	}
	loadRepository := NewLoadJobRepository(store)
	loadJob, err := loaddomain.NewJob(loaddomain.JobReference{
		ProjectID: "test-project", Location: "US", JobID: "running-load",
	}, testLoadJobConfiguration(), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := loadRepository.CreateOrGet(ctx, loadJob); err != nil || !created {
		t.Fatalf("create interrupted load: created=%v err=%v", created, err)
	}
	if err := loadJob.Start(now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := loadRepository.Update(ctx, loadJob); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openJobTestStore(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	reconciledAt := now.Add(time.Hour)
	affected, err := reopened.ReconcileInterruptedJobs(ctx, reconciledAt)
	if err != nil {
		t.Fatal(err)
	}
	if affected != 2 {
		t.Fatalf("reconciled jobs = %d, want 2", affected)
	}
	if again, err := reopened.ReconcileInterruptedJobs(ctx, reconciledAt.Add(time.Second)); err != nil || again != 0 {
		t.Fatalf("second reconciliation = %d, %v", again, err)
	}

	restartedQuery, err := NewQueryJobRepository(reopened).Get(ctx, queryJob.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if restartedQuery.State != querydomain.JobDone || restartedQuery.Result != nil || restartedQuery.Error == nil ||
		restartedQuery.Error.Reason != "backendError" || restartedQuery.Error.Message != interruptedJobMessage ||
		restartedQuery.StartedAt == nil || restartedQuery.EndedAt == nil || !restartedQuery.EndedAt.Equal(reconciledAt) {
		t.Fatalf("reconciled query = %#v", restartedQuery)
	}
	restartedLoad, err := NewLoadJobRepository(reopened).Get(ctx, loadJob.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if restartedLoad.State != loaddomain.JobDone || restartedLoad.Error == nil ||
		restartedLoad.Error.Reason != "backendError" || restartedLoad.Error.Message != interruptedJobMessage ||
		restartedLoad.Statistics != (loaddomain.Statistics{}) || restartedLoad.StartedAt == nil || restartedLoad.EndedAt == nil {
		t.Fatalf("reconciled load = %#v", restartedLoad)
	}
}

func TestSQLiteQueryJobListOrderIsStableAcrossRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	store := openJobTestStore(t, path)
	now := time.Date(2026, 8, 8, 6, 0, 0, 0, time.UTC)
	createJobTestProject(t, store, now)
	repository := NewQueryJobRepository(store)
	for _, input := range []struct {
		id       string
		location string
		created  time.Time
	}{
		{id: "older", location: "US", created: now},
		{id: "fraction", location: "US", created: now.Add(500 * time.Millisecond)},
		{id: "same", location: "US", created: now.Add(time.Second)},
		{id: "same", location: "EU", created: now.Add(time.Second)},
	} {
		job, err := querydomain.NewQueryJob(querydomain.JobReference{
			ProjectID: "test-project", Location: input.location, JobID: input.id,
		}, "SELECT 1", input.created)
		if err != nil {
			t.Fatal(err)
		}
		if _, created, err := repository.CreateOrGet(ctx, job); err != nil || !created {
			t.Fatalf("create list job: created=%v err=%v", created, err)
		}
	}
	assertQueryJobOrder(t, repository, []string{"same/EU", "same/US", "fraction/US", "older/US"})
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openJobTestStore(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	assertQueryJobOrder(t, NewQueryJobRepository(reopened), []string{"same/EU", "same/US", "fraction/US", "older/US"})
}

func assertQueryJobOrder(t *testing.T, repository *QueryJobRepository, expected []string) {
	t.Helper()
	jobs, err := repository.List(context.Background(), "test-project", "")
	if err != nil {
		t.Fatal(err)
	}
	actual := make([]string, len(jobs))
	for index, job := range jobs {
		actual[index] = job.Reference.JobID + "/" + job.Reference.Location
	}
	if len(actual) != len(expected) {
		t.Fatalf("job order = %v, want %v", actual, expected)
	}
	for index := range expected {
		if actual[index] != expected[index] {
			t.Fatalf("job order = %v, want %v", actual, expected)
		}
	}
}

func openJobTestStore(t *testing.T, dataSourceName string) *Store {
	t.Helper()
	config := DefaultConfig(dataSourceName)
	config.Synchronous = "FULL"
	store, err := Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func createJobTestProject(t *testing.T, store *Store, now time.Time) {
	t.Helper()
	if err := store.CreateProject(context.Background(), querydomain.Project{
		ID: "test-project", FriendlyName: "Test", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
}

func testLoadJobConfiguration() loaddomain.LoadConfiguration {
	return loaddomain.LoadConfiguration{
		SourceURIs:   []string{"file:///input.parquet"},
		Destination:  loaddomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "table"},
		SourceFormat: loaddomain.FormatParquet, WriteDisposition: loaddomain.WriteAppend,
		CreateDisposition: loaddomain.CreateIfNeeded,
		Labels:            map[string]string{"consumer": "python"},
		Schema:            []loaddomain.Field{{Name: "record", Type: "RECORD", Mode: "NULLABLE", Fields: []loaddomain.Field{{Name: "value", Type: "STRING"}}}},
	}
}
