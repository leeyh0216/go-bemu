package application

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/storageread/domain"
	"github.com/leeyh0216/go-bemu/internal/storageread/ports"
)

func TestConcurrentCreateSessionAdmissionBoundsMaterialization(t *testing.T) {
	for _, testCase := range []struct {
		name             string
		maxSessions      int
		maxSnapshotBytes int64
		maxTotalBytes    int64
		wantAdmitted     int
	}{
		{name: "session slots", maxSessions: 2, maxSnapshotBytes: 100, maxTotalBytes: 1_000, wantAdmitted: 2},
		{name: "global bytes", maxSessions: 8, maxSnapshotBytes: 100, maxTotalBytes: 200, wantAdmitted: 2},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := testContext(t)
			defer cancel()
			materializer := newBlockingMaterializer(40)
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(materializer.release) }) })
			service := newAdmissionTestService(t, materializer, Config{
				MaxSessions:           testCase.maxSessions,
				MaxSnapshotBytes:      testCase.maxSnapshotBytes,
				MaxTotalSnapshotBytes: testCase.maxTotalBytes,
			})

			const requestCount = 8
			results := make(chan error, requestCount)
			for range requestCount {
				go func() {
					_, err := service.CreateSession(ctx, createRequest(domain.FormatArrow, 1))
					results <- err
				}()
			}
			for range testCase.wantAdmitted {
				select {
				case <-materializer.started:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			for range requestCount - testCase.wantAdmitted {
				select {
				case err := <-results:
					if domain.CodeOf(err) != domain.ErrorResourceExhausted {
						t.Fatalf("rejected create code = %s, want RESOURCE_EXHAUSTED: %v", domain.CodeOf(err), err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if calls, active := materializer.counts(); calls != testCase.wantAdmitted || active != testCase.wantAdmitted {
				t.Fatalf("materializer calls/active = %d/%d, want %d/%d", calls, active, testCase.wantAdmitted, testCase.wantAdmitted)
			}
			if got := materializer.maximumConcurrency(); got != testCase.wantAdmitted {
				t.Fatalf("maximum materialization concurrency = %d, want %d", got, testCase.wantAdmitted)
			}

			releaseOnce.Do(func() { close(materializer.release) })
			for range testCase.wantAdmitted {
				select {
				case err := <-results:
					if err != nil {
						t.Fatalf("admitted create failed: %v", err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if err := service.Close(ctx); err != nil {
				t.Fatal(err)
			}
			assertAdmissionEmpty(t, service)
		})
	}
}

func TestCreateSessionRejectsPreCanceledContextBeforeAdmission(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		newContext func(context.Context) (context.Context, context.CancelFunc)
		wantCode   domain.ErrorCode
		wantCause  error
	}{
		{
			name: "canceled",
			newContext: func(parent context.Context) (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(parent)
				cancel()
				return ctx, func() {}
			},
			wantCode: domain.ErrorCanceled, wantCause: context.Canceled,
		},
		{
			name: "deadline exceeded",
			newContext: func(parent context.Context) (context.Context, context.CancelFunc) {
				return context.WithDeadline(parent, time.Now().Add(-time.Second))
			},
			wantCode: domain.ErrorDeadlineExceeded, wantCause: context.DeadlineExceeded,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := testContext(t)
			defer cancel()
			requestContext, requestCancel := testCase.newContext(ctx)
			defer requestCancel()
			materializer := &fakeMaterializer{snapshot: newFakeSnapshot(domain.FormatArrow, 1)}
			service := newAdmissionTestService(t, materializer, Config{})
			var logs bytes.Buffer
			service.logger = slog.New(slog.NewJSONHandler(&logs, nil))

			_, err := service.CreateSession(requestContext, createRequest(domain.FormatArrow, 1))
			if domain.CodeOf(err) != testCase.wantCode || !errors.Is(err, testCase.wantCause) {
				t.Fatalf("pre-admission context error = %v/%s, want %s", err, domain.CodeOf(err), testCase.wantCode)
			}
			if !strings.Contains(err.Error(), testCase.wantCause.Error()) {
				t.Fatalf("public application error omitted context cause: %v", err)
			}
			if materializer.callCount() != 0 {
				t.Fatalf("materializer calls = %d, want 0", materializer.callCount())
			}
			assertAdmissionEmpty(t, service)
			for _, marker := range []string{"read session admission reserved", "read session admission released"} {
				if strings.Contains(logs.String(), marker) {
					t.Fatalf("pre-admission context failure logged %q: %s", marker, logs.String())
				}
			}
			if err := service.Close(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCreateSessionReleasesAdmissionOnFailureAndOversizeSnapshot(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	valid := newFakeSnapshot(domain.FormatArrow, 4)
	valid.metadata.RetainedBytes = 40
	oversize := newFakeSnapshot(domain.FormatArrow, 4)
	oversize.metadata.RetainedBytes = 101
	materializer := &scriptedMaterializer{outcomes: []materializeOutcome{
		{err: errors.New("backend unavailable")},
		{snapshot: oversize},
		{snapshot: valid},
	}}
	service := newAdmissionTestService(t, materializer, Config{
		MaxSessions: 1, MaxSnapshotBytes: 100, MaxTotalSnapshotBytes: 100,
	})

	if _, err := service.CreateSession(ctx, createRequest(domain.FormatArrow, 1)); domain.CodeOf(err) != domain.ErrorInternal {
		t.Fatalf("materialize failure code = %s, want INTERNAL: %v", domain.CodeOf(err), err)
	}
	assertAdmissionEmpty(t, service)
	if _, err := service.CreateSession(ctx, createRequest(domain.FormatArrow, 1)); domain.CodeOf(err) != domain.ErrorResourceExhausted {
		t.Fatalf("oversize snapshot code = %s, want RESOURCE_EXHAUSTED: %v", domain.CodeOf(err), err)
	}
	if oversize.closeCount() != 1 {
		t.Fatalf("oversize snapshot close calls = %d, want 1", oversize.closeCount())
	}
	assertAdmissionEmpty(t, service)
	if _, err := service.CreateSession(ctx, createRequest(domain.FormatArrow, 1)); err != nil {
		t.Fatal(err)
	}
	if got := retainedAdmissionBytes(service); got != 40 {
		t.Fatalf("committed retained bytes = %d, want 40", got)
	}
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if valid.closeCount() != 1 {
		t.Fatalf("valid snapshot close calls = %d, want 1", valid.closeCount())
	}
	assertAdmissionEmpty(t, service)
}

func TestSnapshotBudgetReleasedOnTTLAndClose(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	clock := newFakeClock()
	first := newFakeSnapshot(domain.FormatArrow, 4)
	first.metadata.RetainedBytes = 48
	second := newFakeSnapshot(domain.FormatArrow, 4)
	second.metadata.RetainedBytes = 32
	materializer := &scriptedMaterializer{outcomes: []materializeOutcome{{snapshot: first}, {snapshot: second}}}
	service := newAdmissionTestService(t, materializer, Config{
		SessionTTL: 10 * time.Minute, MaxSessions: 1, MaxSnapshotBytes: 64, MaxTotalSnapshotBytes: 64,
	})
	service.clock = clock

	if _, err := service.CreateSession(ctx, createRequest(domain.FormatArrow, 1)); err != nil {
		t.Fatal(err)
	}
	if got := retainedAdmissionBytes(service); got != 48 {
		t.Fatalf("retained bytes before TTL = %d, want 48", got)
	}
	clock.Advance(11 * time.Minute)
	if err := service.SweepExpired(ctx); err != nil {
		t.Fatal(err)
	}
	assertAdmissionEmpty(t, service)
	if first.closeCount() != 1 {
		t.Fatalf("expired snapshot close calls = %d, want 1", first.closeCount())
	}

	if _, err := service.CreateSession(ctx, createRequest(domain.FormatArrow, 1)); err != nil {
		t.Fatal(err)
	}
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	assertAdmissionEmpty(t, service)
	if second.closeCount() != 1 {
		t.Fatalf("shutdown snapshot close calls = %d, want 1", second.closeCount())
	}
}

func TestCloseCancelsInflightMaterializationAndReturnsReservation(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	materializer := newBlockingMaterializer(8)
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(materializer.release) })
	service := newAdmissionTestService(t, materializer, Config{
		MaxSessions: 1, MaxSnapshotBytes: 64, MaxTotalSnapshotBytes: 64,
	})
	var logs bytes.Buffer
	service.logger = slog.New(slog.NewJSONHandler(&logs, nil))
	createResult := make(chan error, 1)
	go func() {
		_, err := service.CreateSession(ctx, createRequest(domain.FormatArrow, 1))
		createResult <- err
	}()
	select {
	case <-materializer.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-createResult:
		if domain.CodeOf(err) != domain.ErrorFailedPrecondition {
			t.Fatalf("inflight create code = %s, want FAILED_PRECONDITION: %v", domain.CodeOf(err), err)
		}
		if !strings.Contains(err.Error(), "context canceled") {
			t.Fatalf("shutdown error omitted cancellation cause: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	assertAdmissionEmpty(t, service)
	assertOneReservationReleaseLog(t, logs.String())
}

func TestCallerCancellationIsCanceledAndReleasesReservationOnce(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	requestContext, requestCancel := context.WithCancel(ctx)
	materializer := newBlockingMaterializer(8)
	defer close(materializer.release)
	service := newAdmissionTestService(t, materializer, Config{
		MaxSessions: 1, MaxSnapshotBytes: 64, MaxTotalSnapshotBytes: 64,
	})
	var logs bytes.Buffer
	service.logger = slog.New(slog.NewJSONHandler(&logs, nil))
	result := make(chan error, 1)
	go func() {
		_, err := service.CreateSession(requestContext, createRequest(domain.FormatArrow, 1))
		result <- err
	}()
	select {
	case <-materializer.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	requestCancel()
	select {
	case err := <-result:
		if domain.CodeOf(err) != domain.ErrorCanceled || !errors.Is(err, context.Canceled) {
			t.Fatalf("caller cancellation = %v/%s, want typed CANCELED", err, domain.CodeOf(err))
		}
		if !strings.Contains(err.Error(), "context canceled") {
			t.Fatalf("public application error omitted cancellation cause: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	assertAdmissionEmpty(t, service)
	assertOneReservationReleaseLog(t, logs.String())
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCloseRejectsCommitAfterReservationWasReleased(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	snapshot := newFakeSnapshot(domain.FormatArrow, 1)
	materializer := newStubbornMaterializer(snapshot)
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(materializer.release) })
	service := newAdmissionTestService(t, materializer, Config{
		MaxSessions: 1, MaxSnapshotBytes: 64, MaxTotalSnapshotBytes: 64,
	})
	var logs bytes.Buffer
	service.logger = slog.New(slog.NewJSONHandler(&logs, nil))
	createResult := make(chan error, 1)
	go func() {
		_, err := service.CreateSession(ctx, createRequest(domain.FormatArrow, 1))
		createResult <- err
	}()
	select {
	case <-materializer.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	closeResult := make(chan error, 1)
	go func() { closeResult <- service.Close(ctx) }()
	select {
	case <-materializer.canceled:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	releaseOnce.Do(func() { close(materializer.release) })
	select {
	case err := <-createResult:
		if domain.CodeOf(err) != domain.ErrorFailedPrecondition {
			t.Fatalf("post-shutdown commit code = %s, want FAILED_PRECONDITION: %v", domain.CodeOf(err), err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if snapshot.closeCount() != 1 {
		t.Fatalf("uncommitted snapshot close calls = %d, want 1", snapshot.closeCount())
	}
	assertAdmissionEmpty(t, service)
	assertOneReservationReleaseLog(t, logs.String())
}

func TestCloseKnownGapStalledReadCanOutliveShutdownDeadline(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	snapshot := newFakeSnapshot(domain.FormatArrow, 1)
	service := newAdmissionTestService(t, &fakeMaterializer{snapshot: snapshot}, Config{})
	session, err := service.CreateSession(ctx, createRequest(domain.FormatArrow, 1))
	if err != nil {
		t.Fatal(err)
	}
	sendStarted := make(chan struct{})
	releaseSend := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseSend) })
	readResult := make(chan error, 1)
	go func() {
		readResult <- service.ReadRows(ctx, domain.ReadRowsRequest{StreamName: session.Streams[0].Name}, func(domain.ReadChunk) error {
			close(sendStarted)
			<-releaseSend
			return nil
		})
	}()
	select {
	case <-sendStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	// Known issue #6 gap: ReadRows holds the per-session read lock while the
	// transport send callback is stalled. sync.RWMutex has no context-aware
	// Lock, so Close cannot honor its deadline until that callback returns.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer shutdownCancel()
	closeStarted := make(chan struct{})
	closeResult := make(chan error, 1)
	go func() {
		close(closeStarted)
		closeResult <- service.Close(shutdownCtx)
	}()
	<-closeStarted
	<-shutdownCtx.Done()
	select {
	case err := <-closeResult:
		t.Fatalf("known stalled-read gap changed; Close returned before send release: %v", err)
	default:
	}

	releaseOnce.Do(func() { close(releaseSend) })
	select {
	case err := <-readResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-closeResult:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	assertAdmissionEmpty(t, service)
}

func newAdmissionTestService(t *testing.T, materializer ports.SnapshotMaterializer, overrides Config) *Service {
	t.Helper()
	config := Config{
		Location: "test-location", ProtocolModelVersion: "google.cloud.bigquery.storage.v1@admission-test",
		MaxStreams: 16, DefaultStreamCount: 4,
		SessionTTL: 30 * time.Minute, CleanupInterval: time.Minute,
		MaxRowsPerResponse: 2, MaxSessions: 32,
		MaxSnapshotBytes: 1 << 20, MaxTotalSnapshotBytes: 32 << 20,
	}
	if overrides.SessionTTL != 0 {
		config.SessionTTL = overrides.SessionTTL
	}
	if overrides.MaxSessions != 0 {
		config.MaxSessions = overrides.MaxSessions
	}
	if overrides.MaxSnapshotBytes != 0 {
		config.MaxSnapshotBytes = overrides.MaxSnapshotBytes
	}
	if overrides.MaxTotalSnapshotBytes != 0 {
		config.MaxTotalSnapshotBytes = overrides.MaxTotalSnapshotBytes
	}
	service, err := New(config, materializer, acceptingRowRestrictionParser{}, newFakeClock(), &fakeIDs{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func assertAdmissionEmpty(t *testing.T, service *Service) {
	t.Helper()
	service.mu.RLock()
	defer service.mu.RUnlock()
	if service.retainedSnapshotBytes != 0 || len(service.reservations) != 0 || len(service.sessions) != 0 {
		t.Fatalf("admission not empty: retained=%d reservations=%d sessions=%d",
			service.retainedSnapshotBytes, len(service.reservations), len(service.sessions))
	}
}

func retainedAdmissionBytes(service *Service) int64 {
	service.mu.RLock()
	defer service.mu.RUnlock()
	return service.retainedSnapshotBytes
}

func assertOneReservationReleaseLog(t *testing.T, logs string) {
	t.Helper()
	if got := strings.Count(logs, `"msg":"read session admission released"`); got != 1 {
		t.Fatalf("reservation release log count = %d, want 1: %s", got, logs)
	}
}

type blockingMaterializer struct {
	mu        sync.Mutex
	calls     int
	active    int
	maxActive int
	retained  int64
	started   chan struct{}
	release   chan struct{}
}

func newBlockingMaterializer(retained int64) *blockingMaterializer {
	return &blockingMaterializer{
		retained: retained,
		started:  make(chan struct{}, 32),
		release:  make(chan struct{}),
	}
}

func (m *blockingMaterializer) Materialize(ctx context.Context, _ ports.MaterializeRequest) (ports.ReadSnapshot, error) {
	m.mu.Lock()
	m.calls++
	m.active++
	if m.active > m.maxActive {
		m.maxActive = m.active
	}
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.active--
		m.mu.Unlock()
	}()
	select {
	case m.started <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-m.release:
		snapshot := newFakeSnapshot(domain.FormatArrow, 1)
		snapshot.metadata.RetainedBytes = m.retained
		return snapshot, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *blockingMaterializer) counts() (int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls, m.active
}

func (m *blockingMaterializer) maximumConcurrency() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.maxActive
}

type materializeOutcome struct {
	snapshot ports.ReadSnapshot
	err      error
}

type stubbornMaterializer struct {
	snapshot ports.ReadSnapshot
	started  chan struct{}
	canceled chan struct{}
	release  chan struct{}
}

func newStubbornMaterializer(snapshot ports.ReadSnapshot) *stubbornMaterializer {
	return &stubbornMaterializer{
		snapshot: snapshot,
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
		release:  make(chan struct{}),
	}
}

func (m *stubbornMaterializer) Materialize(ctx context.Context, _ ports.MaterializeRequest) (ports.ReadSnapshot, error) {
	close(m.started)
	<-ctx.Done()
	close(m.canceled)
	<-m.release
	return m.snapshot, nil
}

type scriptedMaterializer struct {
	mu       sync.Mutex
	outcomes []materializeOutcome
	next     int
}

func (m *scriptedMaterializer) Materialize(context.Context, ports.MaterializeRequest) (ports.ReadSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.next >= len(m.outcomes) {
		return nil, errors.New("unexpected materializer call")
	}
	outcome := m.outcomes[m.next]
	m.next++
	return outcome.snapshot, outcome.err
}

var _ ports.SnapshotMaterializer = (*blockingMaterializer)(nil)
var _ ports.SnapshotMaterializer = (*scriptedMaterializer)(nil)
var _ ports.SnapshotMaterializer = (*stubbornMaterializer)(nil)
