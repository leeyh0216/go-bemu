package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	"github.com/leeyh0216/go-bemu/internal/loadjob/ports"
)

func TestLoadMutationJournalSurvivesRestartAndEnforcesReceipts(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "load-mutations.sqlite")
	repositories, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	created := time.Date(2026, 8, 9, 1, 2, 3, 0, time.UTC)
	table := domain.Table{
		Reference: domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"},
		Location: "US", Schema: []domain.Field{{Name: "id", Type: "INT64", Mode: "REQUIRED"}},
	}
	job, err := domain.NewJob(domain.JobReference{
		ProjectID: "test-project", Location: "US", JobID: "load-recovery",
	}, domain.LoadConfiguration{
		SourceURIs: []string{"gs://fixtures/events.parquet"}, Destination: table.Reference,
		SourceFormat: domain.FormatParquet, WriteDisposition: domain.WriteAppend,
		CreateDisposition: domain.CreateIfNeeded, Schema: table.Schema,
	}, created)
	if err != nil {
		t.Fatal(err)
	}
	if _, createdJob, err := repositories.LoadJobs().CreateOrGet(ctx, job); err != nil || !createdJob {
		t.Fatalf("create load job = %v, created=%v", err, createdJob)
	}
	if err := job.Start(created.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repositories.LoadJobs().Update(ctx, job); err != nil {
		t.Fatal(err)
	}
	mutationID, err := domain.LoadMutationID(job.Reference, job.ConfigurationDigest)
	if err != nil {
		t.Fatal(err)
	}
	record := domain.MutationRecord{
		ID: mutationID, Job: job.Reference, ConfigurationDigest: job.ConfigurationDigest,
		PlanFingerprint: strings.Repeat("a", 64), Destination: table,
		Publication: domain.MutationPublishCreate, Phase: domain.MutationPrepared,
		InputFiles: 2, InputBytes: 4096,
	}
	journal := repositories.LoadMutations()
	if err := journal.Prepare(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := journal.Prepare(ctx, record.Clone()); err != nil {
		t.Fatalf("exact prepare retry = %v", err)
	}
	conflict := record.Clone()
	conflict.PlanFingerprint = strings.Repeat("b", 64)
	if err := journal.Prepare(ctx, conflict); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("conflicting prepare = %v", err)
	}
	if err := journal.MarkPhysical(ctx, mutationID, record.PlanFingerprint, ports.LoadResult{
		OutputRows: 3, CreatedDestination: false,
	}); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("invalid physical receipt = %v", err)
	}
	wantResult := ports.LoadResult{OutputRows: 3, CreatedDestination: true}
	if err := journal.MarkPhysical(ctx, mutationID, record.PlanFingerprint, wantResult); err != nil {
		t.Fatal(err)
	}
	if err := repositories.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	recoverable, err := restarted.LoadMutations().ListRecoverable(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(recoverable) != 1 || recoverable[0].Phase != domain.MutationPhysical ||
		recoverable[0].Result == nil || recoverable[0].Result.OutputRows != wantResult.OutputRows ||
		!reflect.DeepEqual(recoverable[0].Destination, table) {
		t.Fatalf("recovered load mutation = %#v", recoverable)
	}
	if err := restarted.LoadMutations().MarkPhysical(ctx, mutationID, record.PlanFingerprint, wantResult); err != nil {
		t.Fatalf("exact physical retry = %v", err)
	}
	if err := restarted.LoadMutations().MarkApplied(ctx, mutationID); err != nil {
		t.Fatal(err)
	}
	if err := job.Complete(domain.Statistics{InputFiles: 2, InputBytes: 4096, OutputRows: 3, OutputBytes: 4096}, created.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := restarted.LoadJobs().Update(ctx, job); err != nil {
		t.Fatal(err)
	}
	recoverable, err = restarted.LoadMutations().ListRecoverable(ctx)
	if err != nil || len(recoverable) != 0 {
		t.Fatalf("completed recovery set = %#v, %v", recoverable, err)
	}
}
