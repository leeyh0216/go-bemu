package application

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/sqlite"
	"github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
	"github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
)

func TestDurableStorageWriteOffsetAndReceiptSurviveRestart(t *testing.T) {
	ctx, cancel := storageWriteTestContext(t)
	defer cancel()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	clock := &fakeClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	coordinator := newFakeCoordinator()
	firstRepositories, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	first := newDurableTestService(t, ctx, coordinator, clock, &sequenceIDs{}, firstRepositories.WriteState())
	stream, err := first.CreateStream(ctx, domain.CreateStreamRequest{Parent: testParent(), Type: domain.StreamTypePending})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Append(ctx, appendRequest(stream.Name, 0)); err != nil {
		t.Fatal(err)
	}
	if err := firstRepositories.Close(); err != nil {
		t.Fatal(err)
	}

	restartedRepositories, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer restartedRepositories.Close()
	restarted := newDurableTestService(t, ctx, coordinator, clock, &sequenceIDs{}, restartedRepositories.WriteState())
	stored, err := restarted.GetStream(ctx, stream.Name)
	if err != nil || stored.NextOffset != 1 || stored.RowCount != 1 {
		t.Fatalf("restarted stream = %#v, %v", stored, err)
	}
	coordinator.mu.Lock()
	stagedBefore := len(coordinator.staged[stream.Name])
	coordinator.mu.Unlock()
	if _, err := restarted.Append(ctx, appendRequest(stream.Name, 0)); domain.CodeOf(err) != domain.ErrorAlreadyExists {
		t.Fatalf("duplicate restarted offset = %v (%s), want ALREADY_EXISTS", err, domain.CodeOf(err))
	}
	coordinator.mu.Lock()
	stagedAfter := len(coordinator.staged[stream.Name])
	coordinator.mu.Unlock()
	if stagedBefore != 1 || stagedAfter != stagedBefore {
		t.Fatalf("duplicate restart changed staged batches from %d to %d", stagedBefore, stagedAfter)
	}
}

func TestUnresolvedAppendAndCommitAreExcludedFromTTLAndRetryAfterRestart(t *testing.T) {
	ctx, cancel := storageWriteTestContext(t)
	defer cancel()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	clock := &fakeClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	coordinator := newFakeCoordinator()
	repositories, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	service := newDurableTestService(t, ctx, coordinator, clock, &sequenceIDs{}, repositories.WriteState())
	appendStream, err := service.CreateStream(ctx, domain.CreateStreamRequest{Parent: testParent(), Type: domain.StreamTypePending})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.stageErr = ports.ErrOperationTimeout
	if _, err := service.Append(ctx, appendRequest(appendStream.Name, 0)); domain.CodeOf(err) != domain.ErrorDeadlineExceeded {
		t.Fatalf("ambiguous append = %v (%s)", err, domain.CodeOf(err))
	}
	clock.Advance(2 * time.Minute)
	if err := service.SweepOrphans(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetStream(ctx, appendStream.Name); err != nil {
		t.Fatalf("TTL removed unresolved append: %v", err)
	}
	coordinator.stageErr = nil
	if _, err := service.Append(ctx, appendRequest(appendStream.Name, 0)); err != nil {
		t.Fatalf("exact unresolved append retry: %v", err)
	}
	if _, err := service.Finalize(ctx, appendStream.Name); err != nil {
		t.Fatal(err)
	}
	coordinator.commitErr = ports.ErrOperationTimeout
	if _, err := service.BatchCommit(ctx, testParent(), []string{appendStream.Name}); domain.CodeOf(err) != domain.ErrorDeadlineExceeded {
		t.Fatalf("ambiguous commit = %v (%s)", err, domain.CodeOf(err))
	}
	clock.Advance(2 * time.Minute)
	if err := service.SweepOrphans(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetStream(ctx, appendStream.Name); err != nil {
		t.Fatalf("TTL removed unresolved commit: %v", err)
	}
	if err := repositories.Close(); err != nil {
		t.Fatal(err)
	}

	restartedRepositories, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer restartedRepositories.Close()
	coordinator.commitErr = nil
	restarted := newDurableTestService(t, ctx, coordinator, clock, &sequenceIDs{}, restartedRepositories.WriteState())
	result, err := restarted.BatchCommit(ctx, testParent(), []string{appendStream.Name})
	if err != nil || result.CommitTime == nil {
		t.Fatalf("exact unresolved commit retry = %#v, %v", result, err)
	}
	committed, err := restarted.GetStream(ctx, appendStream.Name)
	if err != nil || committed.State != domain.StreamStateCommitted {
		t.Fatalf("reconciled committed stream = %#v, %v", committed, err)
	}
}

func TestDeterministicDefaultAppendErrorClearsPreparedState(t *testing.T) {
	ctx, cancel := storageWriteTestContext(t)
	defer cancel()
	repositories, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer repositories.Close()
	clock := &fakeClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	coordinator := newFakeCoordinator()
	coordinator.defaultErr = ports.ErrInvalidRows
	service := newDurableTestService(t, ctx, coordinator, clock, &sequenceIDs{}, repositories.WriteState())
	name := testParent().Name() + "/streams/_default"
	request := appendRequest(name, 0)
	request.Offset = nil
	if _, err := service.Append(ctx, request); domain.CodeOf(err) != domain.ErrorInvalidArgument {
		t.Fatalf("default invalid rows = %v (%s), want INVALID_ARGUMENT", err, domain.CodeOf(err))
	}
	record, err := repositories.WriteState().GetStream(ctx, name)
	if err != nil || record.Operation != domain.OperationNone || record.OperationPhase != domain.OperationPhaseNone {
		t.Fatalf("default stream after deterministic no-effect = %#v, %v", record, err)
	}
	snapshot, err := repositories.WriteState().ReconcileStartup(ctx, clock.Now())
	if err != nil || len(snapshot.Receipts) != 0 {
		t.Fatalf("default receipt after deterministic no-effect = %#v, %v", snapshot.Receipts, err)
	}
}

func newDurableTestService(
	t *testing.T,
	ctx context.Context,
	coordinator ports.Coordinator,
	clock ports.Clock,
	ids ports.IDGenerator,
	repository ports.StateRepository,
) *Service {
	t.Helper()
	service, err := New(Config{
		Location: "US", ProtocolModelVersion: "google.cloud.bigquery.storage.v1@test",
		MaxStreams: 8, MaxAppendBytes: 1024 * 1024, MaxAppendEnvelopeBytes: 64 * 1024,
		MaxConcurrentAppendRequests: 4, OrphanTTL: time.Minute, CleanupInterval: time.Second,
	}, coordinator, clock, ids, slog.New(slog.NewTextHandler(io.Discard, nil)), WithStateRepository(repository))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.ReconcilePersistedState(ctx); err != nil {
		t.Fatal(err)
	}
	return service
}
