package application

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
	"github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

type sequenceIDs struct{ next atomic.Int64 }

func (g *sequenceIDs) NewID() string { return fmt.Sprintf("stream-%d", g.next.Add(1)) }

type fakeCoordinator struct {
	mu          sync.Mutex
	staged      map[string][]ports.AppendBatch
	visibleRows int
	discarded   []string
	commits     int
	failCommit  bool
	commitHook  func()
	stageErr    error
	defaultErr  error
	commitErr   error
	describeErr error
}

func newFakeCoordinator() *fakeCoordinator {
	return &fakeCoordinator{staged: make(map[string][]ports.AppendBatch)}
}

func (c *fakeCoordinator) DescribeTable(_ context.Context, _ domain.TableReference) (domain.TableSchema, error) {
	if c.describeErr != nil {
		return domain.TableSchema{}, c.describeErr
	}
	return domain.TableSchema{Fields: []domain.Field{{Name: "id", Type: "INT64", Mode: "NULLABLE"}}}, nil
}

func (c *fakeCoordinator) AppendDefault(_ context.Context, batch ports.AppendBatch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.defaultErr != nil {
		return c.defaultErr
	}
	c.visibleRows += len(batch.Rows)
	return nil
}

func (c *fakeCoordinator) StagePending(_ context.Context, batch ports.AppendBatch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stageErr != nil {
		return c.stageErr
	}
	c.staged[batch.StreamName] = append(c.staged[batch.StreamName], batch)
	return nil
}

func (c *fakeCoordinator) CommitPending(_ context.Context, request ports.CommitRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commits++
	if c.commitErr != nil {
		return c.commitErr
	}
	if c.failCommit {
		return errors.New("injected commit fault")
	}
	for _, name := range request.StreamNames {
		for _, batch := range c.staged[name] {
			c.visibleRows += len(batch.Rows)
		}
	}
	for _, name := range request.StreamNames {
		delete(c.staged, name)
	}
	if c.commitHook != nil {
		c.commitHook()
	}
	return nil
}

func (c *fakeCoordinator) DiscardPending(_ context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.staged, name)
	c.discarded = append(c.discarded, name)
	return nil
}

func newTestService(t *testing.T, maxStreams int) (*Service, *fakeCoordinator, *fakeClock) {
	t.Helper()
	coordinator := newFakeCoordinator()
	clock := &fakeClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	service, err := New(Config{
		Location: "US", ProtocolModelVersion: "spark-0.44.2",
		MaxStreams: maxStreams, MaxAppendBytes: 1024 * 1024, MaxAppendEnvelopeBytes: 64 * 1024, MaxConcurrentAppendRequests: 4,
		OrphanTTL: time.Minute, CleanupInterval: time.Second,
	}, coordinator, clock, &sequenceIDs{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return service, coordinator, clock
}

func TestConfigAcceptsPinnedConnectorClientRequestLimit(t *testing.T) {
	config := Config{
		Location: "US", ProtocolModelVersion: "spark-0.44.2",
		MaxStreams: 1, MaxAppendBytes: ProtocolMaxAppendBytes, MaxAppendEnvelopeBytes: 64 * 1024, MaxConcurrentAppendRequests: 1,
		OrphanTTL: time.Minute, CleanupInterval: time.Second,
	}
	if err := validateConfig(config); err != nil {
		t.Fatalf("20 MiB client limit must be accepted: %v", err)
	}
	config.MaxAppendBytes++
	if err := validateConfig(config); err == nil {
		t.Fatal("request limit above the pinned client maximum must be rejected")
	}
}

func TestCreateStreamClassifiesUnsupportedDestinationSchema(t *testing.T) {
	service, coordinator, _ := newTestService(t, 1)
	coordinator.describeErr = ports.ErrUnsupportedSchema
	_, err := service.CreateStream(context.Background(), domain.CreateStreamRequest{
		Parent: domain.TableReference{ProjectID: "p", DatasetID: "d", TableID: "t"}, Type: domain.StreamTypePending,
	})
	if code := domain.CodeOf(err); code != domain.ErrorUnimplemented {
		t.Fatalf("CreateStream code = %s, error = %v", code, err)
	}
}

func TestAppendLimitMeasuresProtoDataNotRequestEnvelope(t *testing.T) {
	ctx, cancel := storageWriteTestContext(t)
	defer cancel()
	service, _, _ := newTestService(t, 3)
	stream, err := service.CreateStream(ctx, domain.CreateStreamRequest{Parent: testParent(), Type: domain.StreamTypePending})
	if err != nil {
		t.Fatal(err)
	}
	request := appendRequest(stream.Name, 0)
	request.PayloadBytes = 1024 * 1024
	request.WireBytes = request.PayloadBytes + 4096
	if _, err := service.Append(ctx, request); err != nil {
		t.Fatalf("client-valid ProtoData with request envelope: %v", err)
	}

	second, err := service.CreateStream(ctx, domain.CreateStreamRequest{Parent: testParent(), Type: domain.StreamTypePending})
	if err != nil {
		t.Fatal(err)
	}
	request = appendRequest(second.Name, 0)
	request.PayloadBytes = 1024*1024 + 1
	request.WireBytes = request.PayloadBytes + 4096
	if _, err := service.Append(ctx, request); domain.CodeOf(err) != domain.ErrorInvalidArgument {
		t.Fatalf("oversize ProtoData = %v (%s), want INVALID_ARGUMENT", err, domain.CodeOf(err))
	}

	third, err := service.CreateStream(ctx, domain.CreateStreamRequest{Parent: testParent(), Type: domain.StreamTypePending})
	if err != nil {
		t.Fatal(err)
	}
	request = appendRequest(third.Name, 0)
	request.PayloadBytes = 1024
	request.WireBytes = request.PayloadBytes + 64*1024 + 1
	if _, err := service.Append(ctx, request); domain.CodeOf(err) != domain.ErrorInvalidArgument {
		t.Fatalf("oversize envelope = %v (%s), want INVALID_ARGUMENT", err, domain.CodeOf(err))
	}
}

func testParent() domain.TableReference {
	return domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}
}

func appendRequest(name string, offset int64) domain.AppendRequest {
	return domain.AppendRequest{
		StreamName: name, Offset: &offset, Descriptor: []byte("descriptor"),
		Rows: [][]byte{{byte(offset)}}, PayloadBytes: 90, WireBytes: 100,
		SchemaFingerprint: digest([]byte("descriptor")), PayloadDigest: rowsDigest([][]byte{{byte(offset)}}),
	}
}

func TestPendingStreamsScaleAndCommitAtomically(t *testing.T) {
	for _, streamCount := range []int{2, 8, 16} {
		t.Run(fmt.Sprintf("streams_%d", streamCount), func(t *testing.T) {
			ctx, cancel := storageWriteTestContext(t)
			defer cancel()
			service, coordinator, _ := newTestService(t, streamCount)
			streams := make([]domain.WriteStream, streamCount)
			var wait sync.WaitGroup
			errorsByIndex := make([]error, streamCount)
			for index := range streams {
				wait.Add(1)
				go func(index int) {
					defer wait.Done()
					stream, err := service.CreateStream(ctx, domain.CreateStreamRequest{Parent: testParent(), Type: domain.StreamTypePending})
					if err == nil {
						_, err = service.Append(ctx, appendRequest(stream.Name, 0))
					}
					if err == nil {
						_, err = service.Finalize(ctx, stream.Name)
					}
					streams[index], errorsByIndex[index] = stream, err
				}(index)
			}
			wait.Wait()
			for _, err := range errorsByIndex {
				if err != nil {
					t.Fatal(err)
				}
			}
			names := make([]string, streamCount)
			for index, stream := range streams {
				names[index] = stream.Name
			}
			result, err := service.BatchCommit(ctx, testParent(), names)
			if err != nil {
				t.Fatal(err)
			}
			if result.CommitTime == nil || len(result.StreamErrors) != 0 {
				t.Fatalf("unexpected commit result: %#v", result)
			}
			coordinator.mu.Lock()
			visibleRows, commits := coordinator.visibleRows, coordinator.commits
			coordinator.mu.Unlock()
			if visibleRows != streamCount || commits != 1 {
				t.Fatalf("got visible=%d commits=%d, want %d/1", visibleRows, commits, streamCount)
			}
		})
	}
}

func TestAppendOffsetErrorsDoNotAdvanceLedger(t *testing.T) {
	ctx, cancel := storageWriteTestContext(t)
	defer cancel()
	service, _, _ := newTestService(t, 2)
	stream, err := service.CreateStream(ctx, domain.CreateStreamRequest{Parent: testParent(), Type: domain.StreamTypePending})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Append(ctx, appendRequest(stream.Name, 0)); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		offset int64
		code   domain.ErrorCode
	}{{0, domain.ErrorAlreadyExists}, {2, domain.ErrorOutOfRange}} {
		_, err := service.Append(ctx, appendRequest(stream.Name, test.offset))
		if domain.CodeOf(err) != test.code {
			t.Fatalf("offset %d: got %v (%s), want %s", test.offset, err, domain.CodeOf(err), test.code)
		}
	}
	result, err := service.Append(ctx, appendRequest(stream.Name, 1))
	if err != nil {
		t.Fatal(err)
	}
	if result.StartOffset != 1 {
		t.Fatalf("got start offset %d, want 1", result.StartOffset)
	}
}

func TestAppendPreservesCoordinatorResourceExhausted(t *testing.T) {
	ctx, cancel := storageWriteTestContext(t)
	defer cancel()
	service, coordinator, _ := newTestService(t, 1)
	stream, err := service.CreateStream(ctx, domain.CreateStreamRequest{Parent: testParent(), Type: domain.StreamTypePending})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.mu.Lock()
	coordinator.stageErr = fmt.Errorf("%w: injected byte ceiling", ports.ErrResourceExhausted)
	coordinator.mu.Unlock()
	if _, err := service.Append(ctx, appendRequest(stream.Name, 0)); domain.CodeOf(err) != domain.ErrorResourceExhausted {
		t.Fatalf("append admission error = %v (%s), want RESOURCE_EXHAUSTED", err, domain.CodeOf(err))
	}
	got, err := service.GetStream(ctx, stream.Name)
	if err != nil || got.NextOffset != 0 || got.RowCount != 0 {
		t.Fatalf("rejected append advanced ledger: %#v, %v", got, err)
	}
}

func TestAppendClassifiesCoordinatorTimeoutsWithoutAdvancingLedger(t *testing.T) {
	for name, injected := range map[string]struct {
		err  error
		code domain.ErrorCode
	}{
		"queue wait": {err: ports.ErrQueueWaitTimeout, code: domain.ErrorResourceExhausted},
		"operation":  {err: ports.ErrOperationTimeout, code: domain.ErrorDeadlineExceeded},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := storageWriteTestContext(t)
			defer cancel()
			service, coordinator, _ := newTestService(t, 1)
			stream, err := service.CreateStream(ctx, domain.CreateStreamRequest{Parent: testParent(), Type: domain.StreamTypePending})
			if err != nil {
				t.Fatal(err)
			}
			coordinator.mu.Lock()
			coordinator.stageErr = injected.err
			coordinator.mu.Unlock()
			if _, err := service.Append(ctx, appendRequest(stream.Name, 0)); domain.CodeOf(err) != injected.code {
				t.Fatalf("append error = %v (%s), want %s", err, domain.CodeOf(err), injected.code)
			}
			got, err := service.GetStream(ctx, stream.Name)
			if err != nil || got.NextOffset != 0 || got.RowCount != 0 {
				t.Fatalf("timed-out append advanced ledger: %#v, %v", got, err)
			}
		})
	}
}

func TestPendingAppendMustReconcileAmbiguousAcknowledgementBeforeFinalize(t *testing.T) {
	ctx, cancel := storageWriteTestContext(t)
	defer cancel()
	service, coordinator, _ := newTestService(t, 1)
	stream, err := service.CreateStream(ctx, domain.CreateStreamRequest{Parent: testParent(), Type: domain.StreamTypePending})
	if err != nil {
		t.Fatal(err)
	}
	original := appendRequest(stream.Name, 0)
	coordinator.mu.Lock()
	coordinator.stageErr = ports.ErrOperationTimeout
	coordinator.mu.Unlock()
	if _, err := service.Append(ctx, original); domain.CodeOf(err) != domain.ErrorDeadlineExceeded {
		t.Fatalf("ambiguous append = %v (%s), want DEADLINE_EXCEEDED", err, domain.CodeOf(err))
	}
	if _, err := service.Finalize(ctx, stream.Name); domain.CodeOf(err) != domain.ErrorFailedPrecondition {
		t.Fatalf("finalize before reconciliation = %v (%s), want FAILED_PRECONDITION", err, domain.CodeOf(err))
	}
	different := appendRequest(stream.Name, 0)
	different.Rows = [][]byte{{99}}
	different.PayloadDigest = rowsDigest(different.Rows)
	if _, err := service.Append(ctx, different); domain.CodeOf(err) != domain.ErrorFailedPrecondition {
		t.Fatalf("different retry = %v (%s), want FAILED_PRECONDITION", err, domain.CodeOf(err))
	}
	coordinator.mu.Lock()
	coordinator.stageErr = nil
	coordinator.mu.Unlock()
	if _, err := service.Append(ctx, original); err != nil {
		t.Fatalf("identical receipt retry: %v", err)
	}
	if rows, err := service.Finalize(ctx, stream.Name); err != nil || rows != 1 {
		t.Fatalf("finalize after reconciliation rows=%d err=%v", rows, err)
	}
}

func TestAppendPreservesCallerCancellationCode(t *testing.T) {
	ctx, cancel := storageWriteTestContext(t)
	defer cancel()
	service, coordinator, _ := newTestService(t, 1)
	stream, err := service.CreateStream(ctx, domain.CreateStreamRequest{Parent: testParent(), Type: domain.StreamTypePending})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.mu.Lock()
	coordinator.stageErr = context.Canceled
	coordinator.mu.Unlock()
	if _, err := service.Append(ctx, appendRequest(stream.Name, 0)); domain.CodeOf(err) != domain.ErrorCanceled {
		t.Fatalf("canceled append = %v (%s), want CANCELED", err, domain.CodeOf(err))
	}
}

func TestStorageWriteLogsFingerprintWithoutRawStreamOrRows(t *testing.T) {
	ctx, cancel := storageWriteTestContext(t)
	defer cancel()
	coordinator := newFakeCoordinator()
	clock := &fakeClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	var output bytes.Buffer
	service, err := New(Config{
		Location: "US", ProtocolModelVersion: "spark-0.44.2",
		MaxStreams: 1, MaxAppendBytes: 1024 * 1024, MaxAppendEnvelopeBytes: 64 * 1024, MaxConcurrentAppendRequests: 1,
		OrphanTTL: time.Minute, CleanupInterval: time.Second,
	}, coordinator, clock, &sequenceIDs{}, slog.New(slog.NewJSONHandler(&output, nil)))
	if err != nil {
		t.Fatal(err)
	}
	stream, err := service.CreateStream(ctx, domain.CreateStreamRequest{Parent: testParent(), Type: domain.StreamTypePending})
	if err != nil {
		t.Fatal(err)
	}
	request := appendRequest(stream.Name, 0)
	request.Rows = [][]byte{[]byte("raw-row-sentinel")}
	request.PayloadDigest = rowsDigest(request.Rows)
	if _, err := service.Append(ctx, request); err != nil {
		t.Fatal(err)
	}
	logs := output.String()
	if strings.Contains(logs, stream.Name) || strings.Contains(logs, "raw-row-sentinel") {
		t.Fatalf("Storage Write logs exposed raw stream or row payload: %s", logs)
	}
	if !strings.Contains(logs, `"stream_fingerprint":"`+digest([]byte(stream.Name))+`"`) {
		t.Fatalf("Storage Write logs omit stream fingerprint: %s", logs)
	}
}

func TestCommitFaultLeavesEveryStreamRetryable(t *testing.T) {
	ctx, cancel := storageWriteTestContext(t)
	defer cancel()
	service, coordinator, _ := newTestService(t, 2)
	names := make([]string, 2)
	for index := range names {
		stream, err := service.CreateStream(ctx, domain.CreateStreamRequest{Parent: testParent(), Type: domain.StreamTypePending})
		if err != nil {
			t.Fatal(err)
		}
		names[index] = stream.Name
		if _, err := service.Append(ctx, appendRequest(stream.Name, 0)); err != nil {
			t.Fatal(err)
		}
		if _, err := service.Finalize(ctx, stream.Name); err != nil {
			t.Fatal(err)
		}
	}
	coordinator.failCommit = true
	if _, err := service.BatchCommit(ctx, testParent(), names); domain.CodeOf(err) != domain.ErrorInternal {
		t.Fatalf("got %v, want internal commit fault", err)
	}
	for _, name := range names {
		stream, err := service.GetStream(ctx, name)
		if err != nil || stream.State != domain.StreamStateFinalized {
			t.Fatalf("stream after failed commit: %#v, %v", stream, err)
		}
	}
	coordinator.mu.Lock()
	if coordinator.visibleRows != 0 {
		t.Fatalf("failed commit exposed %d rows", coordinator.visibleRows)
	}
	coordinator.failCommit = false
	coordinator.mu.Unlock()
	result, err := service.BatchCommit(ctx, testParent(), names)
	if err != nil || result.CommitTime == nil {
		t.Fatalf("retry commit: %#v, %v", result, err)
	}
}

func TestBatchCommitTimeIsCapturedAfterBackendVisibility(t *testing.T) {
	ctx, cancel := storageWriteTestContext(t)
	defer cancel()
	service, coordinator, clock := newTestService(t, 1)
	stream, err := service.CreateStream(ctx, domain.CreateStreamRequest{Parent: testParent(), Type: domain.StreamTypePending})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Append(ctx, appendRequest(stream.Name, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Finalize(ctx, stream.Name); err != nil {
		t.Fatal(err)
	}
	coordinator.commitHook = func() { clock.Advance(time.Minute) }
	expected := clock.Now().Add(time.Minute)
	result, err := service.BatchCommit(ctx, testParent(), []string{stream.Name})
	if err != nil || result.CommitTime == nil || !result.CommitTime.Equal(expected) {
		t.Fatalf("commit time = %v, err=%v, want backend-visible time %s", result.CommitTime, err, expected)
	}
}

func TestBatchValidationMakesZeroCoordinatorCalls(t *testing.T) {
	ctx, cancel := storageWriteTestContext(t)
	defer cancel()
	service, coordinator, _ := newTestService(t, 2)
	stream, err := service.CreateStream(ctx, domain.CreateStreamRequest{Parent: testParent(), Type: domain.StreamTypePending})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.BatchCommit(ctx, testParent(), []string{stream.Name})
	if err != nil || len(result.StreamErrors) != 1 || result.StreamErrors[0].Code != domain.InvalidStreamState {
		t.Fatalf("unexpected validation result: %#v, %v", result, err)
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.commits != 0 {
		t.Fatalf("coordinator was called %d times", coordinator.commits)
	}
}

func TestDefaultAliasesShareImmediateStream(t *testing.T) {
	ctx, cancel := storageWriteTestContext(t)
	defer cancel()
	service, coordinator, _ := newTestService(t, 2)
	parent := testParent().Name()
	for _, alias := range []string{parent + "/_default", parent + "/streams/_default"} {
		request := appendRequest(alias, 0)
		request.Offset = nil
		result, err := service.Append(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if result.HasOffset {
			t.Fatal("default append must not return an offset")
		}
	}
	stream, err := service.GetStream(ctx, parent+"/_default")
	if err != nil {
		t.Fatal(err)
	}
	if stream.Name != parent+"/streams/_default" || stream.RowCount != 2 {
		t.Fatalf("unexpected default stream: %#v", stream)
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.visibleRows != 2 {
		t.Fatalf("got %d visible rows, want 2", coordinator.visibleRows)
	}
}

func TestSweepOrphansDiscardsStaging(t *testing.T) {
	ctx, cancel := storageWriteTestContext(t)
	defer cancel()
	service, coordinator, clock := newTestService(t, 2)
	stream, err := service.CreateStream(ctx, domain.CreateStreamRequest{Parent: testParent(), Type: domain.StreamTypePending})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Append(ctx, appendRequest(stream.Name, 0)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Minute)
	if err := service.SweepOrphans(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetStream(ctx, stream.Name); domain.CodeOf(err) != domain.ErrorNotFound {
		t.Fatalf("got %v, want not found", err)
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if len(coordinator.discarded) != 1 || coordinator.discarded[0] != stream.Name {
		t.Fatalf("unexpected discarded streams: %#v", coordinator.discarded)
	}
}

var _ ports.Coordinator = (*fakeCoordinator)(nil)

func storageWriteTestContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	timeout := 10 * time.Second
	if configured := os.Getenv("BQEMU_STORAGE_WRITE_TEST_TIMEOUT"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		if err != nil {
			t.Fatalf("BQEMU_STORAGE_WRITE_TEST_TIMEOUT: %v", err)
		}
		timeout = parsed
	}
	return context.WithTimeout(context.Background(), timeout)
}
