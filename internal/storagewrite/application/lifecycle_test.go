package application

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
	}, coordinator, clock, &sequenceIDs{}, logger)
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
	if attempts.Load() != 1 || service.pending.Load() != 1 {
		t.Fatalf("failed cleanup attempts/pending = %d/%d, want 1/1", attempts.Load(), service.pending.Load())
	}
	service.mu.RLock()
	state := service.streams[stream.Name]
	service.mu.RUnlock()
	if state == nil {
		t.Fatal("failed discard removed the cleanup tombstone")
	}
	state.mu.Lock()
	phase, cleanupAttempts := state.cleanupPhase, state.cleanupAttempts
	state.mu.Unlock()
	if phase != cleanupPhasePending || cleanupAttempts != 1 {
		t.Fatalf("cleanup state/attempts = %s/%d, want %s/1", phase, cleanupAttempts, cleanupPhasePending)
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
	if attempts.Load() != 2 || service.pending.Load() != 0 {
		t.Fatalf("successful retry attempts/pending = %d/%d, want 2/0", attempts.Load(), service.pending.Load())
	}
	service.mu.RLock()
	_, retained := service.streams[stream.Name]
	service.mu.RUnlock()
	if retained {
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
	if service.pending.Load() != 0 {
		t.Fatalf("successful concurrent sweep retained pending capacity: %d", service.pending.Load())
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
	if discardAttempts.Load() != 0 || service.pending.Load() != 0 {
		t.Fatalf("default cleanup attempts/pending = %d/%d, want 0/0", discardAttempts.Load(), service.pending.Load())
	}
}

func TestCloseTimeoutRetainsCleanupForRetry(t *testing.T) {
	ctx, cancel := storageWriteTestContext(t)
	defer cancel()

	base := newFakeCoordinator()
	entered := make(chan struct{}, 1)
	var attempts atomic.Int64
	coordinator := &cleanupTestCoordinator{fakeCoordinator: base}
	coordinator.discard = func(ctx context.Context, name string) error {
		if attempts.Add(1) == 1 {
			entered <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}
		return base.DiscardPending(ctx, name)
	}
	service, clock := newCleanupTestService(t, 1, coordinator, slog.New(slog.NewTextHandler(io.Discard, nil)))
	stream := createStalePendingStream(t, ctx, service, clock)

	closeCtx, closeCancel := context.WithTimeout(ctx, storageWriteCleanupAttemptTimeout(t))
	defer closeCancel()
	result := make(chan error, 1)
	go func() { result <- service.Close(closeCtx) }()
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-entered:
	}
	var closeErr error
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case closeErr = <-result:
	}
	if !errors.Is(closeErr, context.DeadlineExceeded) {
		t.Fatalf("Close timeout = %v, want context deadline exceeded", closeErr)
	}
	if service.pending.Load() != 1 {
		t.Fatalf("timed-out Close released pending capacity: %d", service.pending.Load())
	}
	service.mu.RLock()
	state := service.streams[stream.Name]
	closed := service.closed
	service.mu.RUnlock()
	if !closed || state == nil {
		t.Fatalf("timed-out Close closed/tombstone = %t/%t, want true/true", closed, state != nil)
	}
	state.mu.Lock()
	phase := state.cleanupPhase
	state.mu.Unlock()
	if phase != cleanupPhasePending {
		t.Fatalf("timed-out Close cleanup phase = %s, want %s", phase, cleanupPhasePending)
	}
	base.mu.Lock()
	_, staged := base.staged[stream.Name]
	base.mu.Unlock()
	if !staged {
		t.Fatal("timed-out Close lost staged state")
	}
	if _, err := service.Append(ctx, appendRequest(stream.Name, 1)); domain.CodeOf(err) != domain.ErrorFailedPrecondition {
		t.Fatalf("append after Close = %v (%s), want FAILED_PRECONDITION", err, domain.CodeOf(err))
	}

	if err := service.Close(ctx); err != nil {
		t.Fatalf("Close retry: %v", err)
	}
	if attempts.Load() != 2 || service.pending.Load() != 0 {
		t.Fatalf("Close retry attempts/pending = %d/%d, want 2/0", attempts.Load(), service.pending.Load())
	}
	service.mu.RLock()
	_, retained := service.streams[stream.Name]
	service.mu.RUnlock()
	if retained {
		t.Fatal("successful Close retry retained cleanup tombstone")
	}
}

func storageWriteCleanupAttemptTimeout(t *testing.T) time.Duration {
	t.Helper()
	timeout := 100 * time.Millisecond
	if configured := os.Getenv("BQEMU_STORAGE_WRITE_CLEANUP_ATTEMPT_TIMEOUT"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		if err != nil {
			t.Fatalf("BQEMU_STORAGE_WRITE_CLEANUP_ATTEMPT_TIMEOUT: %v", err)
		}
		if parsed <= 0 {
			t.Fatal("BQEMU_STORAGE_WRITE_CLEANUP_ATTEMPT_TIMEOUT must be positive")
		}
		timeout = parsed
	}
	return timeout
}

var _ ports.Coordinator = (*cleanupTestCoordinator)(nil)
