package application

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	writememory "github.com/leeyh0216/go-bemu/internal/storagewrite/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
	"github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
)

type cleanupTestCoordinator struct {
	*fakeCoordinator
	discard func(context.Context, string) error
}

func (c *cleanupTestCoordinator) DiscardPending(ctx context.Context, name string) error {
	if c.discard != nil {
		return c.discard(ctx, name)
	}
	return c.fakeCoordinator.DiscardPending(ctx, name)
}

func newCleanupTestService(
	t *testing.T,
	maxStreams int,
	coordinator ports.Coordinator,
	logger *slog.Logger,
) (*Service, *fakeClock) {
	t.Helper()
	clock := &fakeClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	service, err := New(Config{
		Location: "US", ProtocolModelVersion: "spark-0.44.2",
		MaxStreams: maxStreams, MaxAppendBytes: 1024 * 1024, MaxAppendEnvelopeBytes: 64 * 1024, MaxConcurrentAppendRequests: 4,
		OrphanTTL: time.Minute, CleanupInterval: time.Second,
	}, coordinator.(ports.DurableCoordinator), writememory.NewRepository(), clock, &sequenceIDs{}, logger)
	if err != nil {
		t.Fatal(err)
	}
	return service, clock
}

func createStalePendingStream(
	t *testing.T,
	ctx context.Context,
	service *Service,
	clock *fakeClock,
) domain.WriteStream {
	t.Helper()
	stream, err := service.CreateStream(ctx, domain.CreateStreamRequest{Parent: testParent(), Type: domain.StreamTypePending})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Append(ctx, appendRequest(stream.Name, 0)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Minute)
	return stream
}

func TestSweepOrphansRetainsTombstoneUntilDiscardSucceeds(t *testing.T) {
	ctx, cancel := storageWriteTestContext(t)
	defer cancel()

	base := newFakeCoordinator()
	var attempts atomic.Int64
	coordinator := &cleanupTestCoordinator{fakeCoordinator: base}
	coordinator.discard = func(ctx context.Context, name string) error {
		if attempts.Add(1) == 1 {
			return errors.New("injected transient discard fault")
		}
		return base.DiscardPending(ctx, name)
	}
	var logs bytes.Buffer
	service, clock := newCleanupTestService(t, 1, coordinator, slog.New(slog.NewJSONHandler(&logs, nil)))
	stream := createStalePendingStream(t, ctx, service, clock)

	if err := service.SweepOrphans(ctx); err == nil {
		t.Fatal("first orphan sweep must expose the injected discard failure")
	}
	if pending := storageWritePendingCount(t, ctx, service); attempts.Load() != 1 || pending != 1 {
		t.Fatalf("failed cleanup attempts/pending = %d/%d, want 1/1", attempts.Load(), pending)
	}
	record, recordErr := service.repository.GetWriteStream(ctx, stream.Name)
	if recordErr != nil {
		t.Fatal("failed discard removed the cleanup tombstone")
	}
	if record.CleanupState != domain.CleanupStatePending || record.CleanupAttempts != 1 {
		t.Fatalf("cleanup state/attempts = %s/%d, want %s/1", record.CleanupState, record.CleanupAttempts, domain.CleanupStatePending)
	}
	if _, err := service.GetStream(ctx, stream.Name); domain.CodeOf(err) != domain.ErrorNotFound {
		t.Fatalf("cleanup-pending lookup = %v (%s), want NOT_FOUND", err, domain.CodeOf(err))
	}
	if _, err := service.Append(ctx, appendRequest(stream.Name, 1)); domain.CodeOf(err) != domain.ErrorNotFound {
		t.Fatalf("cleanup-pending append = %v (%s), want NOT_FOUND", err, domain.CodeOf(err))
	}
	if _, err := service.CreateStream(ctx, domain.CreateStreamRequest{Parent: testParent(), Type: domain.StreamTypePending}); domain.CodeOf(err) != domain.ErrorResourceExhausted {
		t.Fatalf("cleanup-pending capacity = %v (%s), want RESOURCE_EXHAUSTED", err, domain.CodeOf(err))
	}
	base.mu.Lock()
	_, stagedBeforeRetry := base.staged[stream.Name]
	base.mu.Unlock()
	if !stagedBeforeRetry {
		t.Fatal("failed discard made staged rows unreachable")
	}

	if err := service.SweepOrphans(ctx); err != nil {
		t.Fatalf("retry orphan sweep: %v", err)
	}
	if pending := storageWritePendingCount(t, ctx, service); attempts.Load() != 2 || pending != 0 {
		t.Fatalf("successful retry attempts/pending = %d/%d, want 2/0", attempts.Load(), pending)
	}
	if _, err := service.repository.GetWriteStream(ctx, stream.Name); !errors.Is(err, ports.ErrStreamNotFound) {
		t.Fatal("successful discard retained the application ledger")
	}
	base.mu.Lock()
	_, stagedAfterRetry := base.staged[stream.Name]
	discardCount := len(base.discarded)
	base.mu.Unlock()
	if stagedAfterRetry || discardCount != 1 {
		t.Fatalf("staging/discard count after retry = %t/%d, want false/1", stagedAfterRetry, discardCount)
	}
	if _, err := service.CreateStream(ctx, domain.CreateStreamRequest{Parent: testParent(), Type: domain.StreamTypePending}); err != nil {
		t.Fatalf("successful discard did not release stream capacity: %v", err)
	}

	output := logs.String()
	if strings.Contains(output, stream.Name) {
		t.Fatalf("cleanup logs exposed a raw stream resource: %s", output)
	}
	for _, required := range []string{
		`"stream_fingerprint":"` + digest([]byte(stream.Name)) + `"`,
		`"state_before":"active"`,
		`"state_after":"cleanup_pending"`,
		`"state_after":"discarded"`,
		`"retry_count":1`,
	} {
		if !strings.Contains(output, required) {
			t.Fatalf("cleanup logs omit %s: %s", required, output)
		}
	}
}

func TestConcurrentOrphanSweepsIssueOneDiscard(t *testing.T) {
	ctx, cancel := storageWriteTestContext(t)
	defer cancel()

	base := newFakeCoordinator()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var attempts atomic.Int64
	var active atomic.Int64
	var maxActive atomic.Int64
	coordinator := &cleanupTestCoordinator{fakeCoordinator: base}
	coordinator.discard = func(ctx context.Context, name string) error {
		attempts.Add(1)
		current := active.Add(1)
		defer active.Add(-1)
		for observed := maxActive.Load(); current > observed && !maxActive.CompareAndSwap(observed, current); observed = maxActive.Load() {
		}
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return base.DiscardPending(ctx, name)
		}
	}
	service, clock := newCleanupTestService(t, 1, coordinator, slog.New(slog.NewTextHandler(io.Discard, nil)))
	createStalePendingStream(t, ctx, service, clock)

	results := make(chan error, 2)
	go func() { results <- service.SweepOrphans(ctx) }()
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-entered:
	}
	go func() { results <- service.SweepOrphans(ctx) }()
	close(release)
	for range 2 {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if attempts.Load() != 1 || maxActive.Load() != 1 {
		t.Fatalf("discard attempts/max concurrency = %d/%d, want 1/1", attempts.Load(), maxActive.Load())
	}
	if pending := storageWritePendingCount(t, ctx, service); pending != 0 {
		t.Fatalf("successful concurrent sweep retained pending capacity: %d", pending)
	}
}

func TestSweepOrphansLeavesDefaultStreamWritable(t *testing.T) {
	ctx, cancel := storageWriteTestContext(t)
	defer cancel()

	base := newFakeCoordinator()
	var discardAttempts atomic.Int64
	coordinator := &cleanupTestCoordinator{fakeCoordinator: base}
	coordinator.discard = func(ctx context.Context, name string) error {
		discardAttempts.Add(1)
		return base.DiscardPending(ctx, name)
	}
	service, clock := newCleanupTestService(t, 1, coordinator, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defaultName := testParent().Name() + "/streams/_default"
	first := appendRequest(defaultName, 0)
	first.Offset = nil
	if _, err := service.Append(ctx, first); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Minute)
	if err := service.SweepOrphans(ctx); err != nil {
		t.Fatal(err)
	}
	second := appendRequest(defaultName, 0)
	second.Offset = nil
	second.Rows = [][]byte{{2}}
	second.PayloadDigest = rowsDigest(second.Rows)
	if _, err := service.Append(ctx, second); err != nil {
		t.Fatalf("default append after orphan sweep: %v", err)
	}
	stream, err := service.GetStream(ctx, defaultName)
	if err != nil || stream.RowCount != 2 {
		t.Fatalf("default stream after orphan sweep: %#v, %v", stream, err)
	}
	if pending := storageWritePendingCount(t, ctx, service); discardAttempts.Load() != 0 || pending != 0 {
		t.Fatalf("default cleanup attempts/pending = %d/%d, want 0/0", discardAttempts.Load(), pending)
	}
}

func TestClosePreservesPendingStreamForRestart(t *testing.T) {
	ctx, cancel := storageWriteTestContext(t)
	defer cancel()

	base := newFakeCoordinator()
	var attempts atomic.Int64
	coordinator := &cleanupTestCoordinator{fakeCoordinator: base}
	coordinator.discard = func(ctx context.Context, name string) error {
		attempts.Add(1)
		return base.DiscardPending(ctx, name)
	}
	service, _ := newCleanupTestService(t, 1, coordinator, slog.New(slog.NewTextHandler(io.Discard, nil)))
	stream, err := service.CreateStream(ctx, domain.CreateStreamRequest{Parent: testParent(), Type: domain.StreamTypePending})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Append(ctx, appendRequest(stream.Name, 0)); err != nil {
		t.Fatal(err)
	}
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if !service.closed.Load() || storageWritePendingCount(t, ctx, service) != 1 {
		t.Fatal("Close must stop admission without deleting canonical pending state")
	}
	if _, err := service.repository.GetWriteStream(ctx, stream.Name); err != nil {
		t.Fatalf("Close removed the pending stream record: %v", err)
	}
	base.mu.Lock()
	_, staged := base.staged[stream.Name]
	base.mu.Unlock()
	if !staged {
		t.Fatal("Close lost staged rows")
	}
	if _, err := service.Append(ctx, appendRequest(stream.Name, 1)); domain.CodeOf(err) != domain.ErrorFailedPrecondition {
		t.Fatalf("append after Close = %v (%s), want FAILED_PRECONDITION", err, domain.CodeOf(err))
	}
	if attempts.Load() != 0 {
		t.Fatalf("Close discarded durable pending rows %d times", attempts.Load())
	}
}

func storageWritePendingCount(t *testing.T, ctx context.Context, service *Service) int64 {
	t.Helper()
	count, err := service.repository.CountActivePendingStreams(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return count
}

var _ ports.Coordinator = (*cleanupTestCoordinator)(nil)
