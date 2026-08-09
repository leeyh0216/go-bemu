package sqlite

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	loaddomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
)

func TestJobMetadataSurvivesRepositoryRestartWithoutQueryRows(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bqemu-state.sqlite")
	repositories, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}

	created := time.Date(2026, 8, 8, 4, 5, 6, 789000000, time.UTC)
	precision, scale := int64(38), int64(9)
	queryJob, err := domain.NewConfiguredQueryJob(domain.JobReference{
		ProjectID: "test-project", Location: "asia-northeast3", JobID: "query-restart",
	}, domain.QueryConfiguration{
		SQL: "SELECT 1 AS value", StatementType: "SELECT", DefaultProjectID: "test-project", DefaultDataset: "analytics",
		Destination:      &domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "query_output"},
		WriteDisposition: domain.WriteEmpty, CreateDisposition: domain.CreateIfNeeded,
		Priority: domain.QueryPriorityBatch, Labels: map[string]string{"purpose": "restart"},
		AnonymousDestination: true,
	}, created)
	if err != nil {
		t.Fatal(err)
	}
	if _, createdJob, err := repositories.QueryJobs().CreateOrGet(ctx, queryJob); err != nil || !createdJob {
		t.Fatalf("create query job = %v, created=%v", err, createdJob)
	}
	if err := queryJob.Start(created.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repositories.QueryJobs().Update(ctx, queryJob); err != nil {
		t.Fatal(err)
	}
	queryResult := domain.QueryResult{
		Columns: []domain.Field{{
			Name: "value", Type: "BIGNUMERIC", Precision: &precision, Scale: &scale,
			RoundingMode: domain.RoundingModeHalfEven,
		}},
		Rows: [][]any{{"row-payload-sentinel"}}, AffectedRows: 7, TotalRows: 1,
	}
	if err := queryJob.Complete(queryResult, created.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repositories.QueryJobs().Update(ctx, queryJob); err != nil {
		t.Fatal(err)
	}
	loadedBeforeRestart, err := repositories.QueryJobs().Get(ctx, queryJob.Reference)
	if err != nil || !reflect.DeepEqual(loadedBeforeRestart.Result.Rows, queryResult.Rows) || loadedBeforeRestart.Result.RowsUnavailable {
		t.Fatalf("live query payload = %#v, %v", loadedBeforeRestart, err)
	}

	loadJob, err := loaddomain.NewJob(loaddomain.JobReference{
		ProjectID: "test-project", Location: "asia-northeast3", JobID: "load-restart",
	}, loaddomain.LoadConfiguration{
		SourceURIs:   []string{"gs://restart-fixtures/events-*.parquet"},
		Destination:  loaddomain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"},
		SourceFormat: loaddomain.FormatParquet, WriteDisposition: loaddomain.WriteAppend,
		CreateDisposition: loaddomain.CreateNever,
		Schema:            []loaddomain.Field{{Name: "amount", Type: "BIGNUMERIC", Precision: &precision, Scale: &scale}},
	}, created.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, createdJob, err := repositories.LoadJobs().CreateOrGet(ctx, loadJob); err != nil || !createdJob {
		t.Fatalf("create load job = %v, created=%v", err, createdJob)
	}
	if err := loadJob.Start(created.Add(4 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repositories.LoadJobs().Update(ctx, loadJob); err != nil {
		t.Fatal(err)
	}
	wantLoadStatistics := loaddomain.Statistics{InputFiles: 3, InputBytes: 1024, OutputRows: 9, OutputBytes: 768}
	if err := loadJob.Complete(wantLoadStatistics, created.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repositories.LoadJobs().Update(ctx, loadJob); err != nil {
		t.Fatal(err)
	}

	var storedConfiguration, storedSchema string
	if err := repositories.db.QueryRowContext(ctx, `SELECT configuration_json, result_schema_json
FROM bqemu_query_jobs WHERE job_id = ?`, queryJob.Reference.JobID).Scan(&storedConfiguration, &storedSchema); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(storedConfiguration, "row-payload-sentinel") || strings.Contains(storedSchema, "row-payload-sentinel") {
		t.Fatal("query row payload was written to SQLite job metadata")
	}
	if err := repositories.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	restartedQuery, err := restarted.QueryJobs().Get(ctx, queryJob.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restartedQuery.Configuration, queryJob.Configuration) || restartedQuery.State != domain.JobDone ||
		restartedQuery.Error != nil || restartedQuery.Result == nil || !restartedQuery.Result.RowsUnavailable ||
		restartedQuery.Result.TotalRows != 1 || restartedQuery.Result.AffectedRows != 7 ||
		len(restartedQuery.Result.Rows) != 0 || !reflect.DeepEqual(restartedQuery.Result.Columns, queryResult.Columns) {
		t.Fatalf("restarted query metadata = %#v", restartedQuery)
	}
	restartedLoad, err := restarted.LoadJobs().Get(ctx, loadJob.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restartedLoad, loadJob) {
		t.Fatalf("restarted load metadata = %#v, want %#v", restartedLoad, loadJob)
	}
}

func TestRepositoryStartupTerminatesOnlyInterruptedQueryJobs(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bqemu-state.sqlite")
	repositories, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	created := time.Date(2026, 8, 8, 1, 0, 0, 0, time.UTC)

	pendingQuery, err := domain.NewQueryJob(domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "pending-query"}, "SELECT 1", created)
	if err != nil {
		t.Fatal(err)
	}
	runningQuery, err := domain.NewQueryJob(domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "running-query"}, "SELECT 2", created)
	if err != nil {
		t.Fatal(err)
	}
	if err := runningQuery.Start(created.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	for _, job := range []*domain.Job{pendingQuery, runningQuery} {
		if _, createdJob, err := repositories.QueryJobs().CreateOrGet(ctx, job); err != nil || !createdJob {
			t.Fatalf("create interrupted query = %v, created=%v", err, createdJob)
		}
	}

	pendingLoad := newInterruptedLoadJob(t, "pending-load", created)
	runningLoad := newInterruptedLoadJob(t, "running-load", created)
	if err := runningLoad.Start(created.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	for _, job := range []*loaddomain.Job{pendingLoad, runningLoad} {
		if _, createdJob, err := repositories.LoadJobs().CreateOrGet(ctx, job); err != nil || !createdJob {
			t.Fatalf("create interrupted load = %v, created=%v", err, createdJob)
		}
	}
	if err := repositories.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	for _, reference := range []domain.JobReference{pendingQuery.Reference, runningQuery.Reference} {
		job, err := restarted.QueryJobs().Get(ctx, reference)
		if err != nil || job.State != domain.JobDone || job.Error == nil || job.Error.Reason != "stopped" || job.EndedAt == nil {
			t.Fatalf("reconciled query job = %#v, %v", job, err)
		}
	}
	for _, want := range []*loaddomain.Job{pendingLoad, runningLoad} {
		job, err := restarted.LoadJobs().Get(ctx, want.Reference)
		if err != nil || job.State != want.State || job.Error != nil ||
			(job.StartedAt == nil) != (want.StartedAt == nil) || job.EndedAt != nil {
			t.Fatalf("load job changed before mutation recovery = %#v, %v", job, err)
		}
	}
}

func TestLoadJobRepositoryListsStaticScopesAndInterruptedJobs(t *testing.T) {
	ctx := context.Background()
	repositories, err := Open(ctx, filepath.Join(t.TempDir(), "bqemu-state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer repositories.Close()
	created := time.Date(2026, 8, 9, 1, 0, 0, 0, time.UTC)
	newJob := func(jobID, location string, createdAt time.Time) *loaddomain.Job {
		t.Helper()
		job, createErr := loaddomain.NewJob(loaddomain.JobReference{
			ProjectID: "test-project", Location: location, JobID: jobID,
		}, loaddomain.LoadConfiguration{
			SourceURIs: []string{"gs://fixtures/input.parquet"}, SourceFormat: loaddomain.FormatParquet,
			Destination: loaddomain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"},
		}, createdAt)
		if createErr != nil {
			t.Fatal(createErr)
		}
		return job
	}
	earlier := newJob("earlier", "US", created)
	later := newJob("later", "EU", created.Add(time.Second))
	if err := later.Start(created.Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for _, job := range []*loaddomain.Job{later, earlier} {
		if _, createdJob, createErr := repositories.LoadJobs().CreateOrGet(ctx, job); createErr != nil || !createdJob {
			t.Fatalf("create load job %s = %v, created=%v", job.Reference.JobID, createErr, createdJob)
		}
	}
	all, err := repositories.LoadJobs().List(ctx, "test-project", "")
	if err != nil || len(all) != 2 || all[0].Reference.JobID != "earlier" || all[1].Reference.JobID != "later" {
		t.Fatalf("all load jobs = %#v, %v", all, err)
	}
	us, err := repositories.LoadJobs().List(ctx, "test-project", "us")
	if err != nil || len(us) != 1 || us[0].Reference.JobID != "earlier" {
		t.Fatalf("US load jobs = %#v, %v", us, err)
	}
	interrupted, err := repositories.LoadJobs().ListInterrupted(ctx)
	if err != nil || len(interrupted) != 2 || interrupted[0].Reference.JobID != "earlier" || interrupted[1].Reference.JobID != "later" {
		t.Fatalf("interrupted load jobs = %#v, %v", interrupted, err)
	}
}

func newInterruptedLoadJob(t *testing.T, jobID string, created time.Time) *loaddomain.Job {
	t.Helper()
	job, err := loaddomain.NewJob(loaddomain.JobReference{
		ProjectID: "test-project", Location: "US", JobID: jobID,
	}, loaddomain.LoadConfiguration{
		SourceURIs: []string{"gs://fixtures/input.parquet"}, SourceFormat: loaddomain.FormatParquet,
		Destination: loaddomain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"},
	}, created)
	if err != nil {
		t.Fatal(err)
	}
	return job
}
