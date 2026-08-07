package application

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/storageread/domain"
	"github.com/leeyh0216/go-bemu/internal/storageread/ports"
)

func TestCreateSessionMaterializesOnceAndPartitionsConfiguredStreamMatrix(t *testing.T) {
	for _, streamCount := range []int32{1, 2, 4, 16} {
		streamCount := streamCount
		t.Run(strconv.Itoa(int(streamCount)), func(t *testing.T) {
			ctx, cancel := testContext(t)
			defer cancel()
			snapshot := newFakeSnapshot(domain.FormatArrow, 32)
			materializer := &fakeMaterializer{snapshot: snapshot}
			service := newTestService(t, materializer, newFakeClock())

			session, err := service.CreateSession(ctx, createRequest(domain.FormatArrow, streamCount))
			if err != nil {
				t.Fatal(err)
			}
			if materializer.callCount() != 1 {
				t.Fatalf("materialize calls = %d, want 1", materializer.callCount())
			}
			if len(session.Streams) != int(streamCount) {
				t.Fatalf("streams = %d, want %d", len(session.Streams), streamCount)
			}
			cursor := int64(0)
			for _, stream := range session.Streams {
				if stream.StartOffset != cursor {
					t.Fatalf("stream starts at %d, want contiguous offset %d", stream.StartOffset, cursor)
				}
				if stream.EndOffset < stream.StartOffset {
					t.Fatalf("negative stream range: %+v", stream)
				}
				cursor = stream.EndOffset
			}
			if cursor != 32 {
				t.Fatalf("stream union ends at %d, want 32", cursor)
			}
		})
	}
}

func TestReadRowsResumesAtStreamRelativeOffsetAndPreservesExactCounts(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	snapshot := newFakeSnapshot(domain.FormatArrow, 10)
	materializer := &fakeMaterializer{snapshot: snapshot}
	service := newTestService(t, materializer, newFakeClock())
	session, err := service.CreateSession(ctx, createRequest(domain.FormatArrow, 2))
	if err != nil {
		t.Fatal(err)
	}

	var chunks []domain.ReadChunk
	err = service.ReadRows(ctx, domain.ReadRowsRequest{
		StreamName: session.Streams[1].Name,
		Offset:     2,
	}, func(chunk domain.ReadChunk) error {
		chunks = append(chunks, chunk)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := snapshot.lastRange(), (readRange{start: 7, end: 10, maxBatch: 2}); got != want {
		t.Fatalf("snapshot range = %+v, want %+v", got, want)
	}
	if len(chunks) != 2 || chunks[0].Batch.RowCount != 2 || chunks[1].Batch.RowCount != 1 {
		t.Fatalf("unexpected chunks: %+v", chunks)
	}
	if chunks[0].Schema == nil || chunks[1].Schema != nil {
		t.Fatalf("reference schema must appear only on the first response")
	}
	var total int64
	for _, chunk := range chunks {
		total += chunk.Batch.RowCount
	}
	if total != 3 {
		t.Fatalf("row_count sum = %d, want 3", total)
	}
	if chunks[0].ProgressStart != 0.4 || chunks[1].ProgressEnd != 1 {
		t.Fatalf("unexpected progress: start=%f end=%f", chunks[0].ProgressStart, chunks[1].ProgressEnd)
	}
}

func TestPreferredStreamCountCanRaiseUnboundedDefault(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	service := newTestService(t, &fakeMaterializer{snapshot: newFakeSnapshot(domain.FormatArrow, 32)}, newFakeClock())
	request := createRequest(domain.FormatArrow, 0)
	request.PreferredMinStreamCount = 16
	session, err := service.CreateSession(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(session.Streams) != 16 {
		t.Fatalf("streams = %d, want preferred configured count 16", len(session.Streams))
	}
}

func TestStreamNegotiationUsesClientPreferenceAndConfiguredCeiling(t *testing.T) {
	service := newTestService(t, &fakeMaterializer{snapshot: newFakeSnapshot(domain.FormatArrow, 32)}, newFakeClock())
	for _, testCase := range []struct {
		name      string
		maximum   int32
		preferred int32
		want      int32
	}{
		{name: "explicit maximum", maximum: 3, want: 3},
		{name: "unbounded default", want: 4},
		{name: "preferred", maximum: 100, preferred: 10, want: 10},
		{name: "preferred over ceiling", maximum: 100, preferred: 100, want: 16},
		{name: "default exceeds small preference", maximum: 100, preferred: 2, want: 4},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := service.negotiateStreamCount(testCase.maximum, testCase.preferred)
			if err != nil {
				t.Fatal(err)
			}
			if got != testCase.want {
				t.Fatalf("stream count = %d, want %d", got, testCase.want)
			}
		})
	}
}

func TestReadRowsRejectsSnapshotGapInsteadOfReturningSilentDataLoss(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	snapshot := newFakeSnapshot(domain.FormatArrow, 4)
	snapshot.batchOffsetDelta = 1
	service := newTestService(t, &fakeMaterializer{snapshot: snapshot}, newFakeClock())
	session, err := service.CreateSession(ctx, createRequest(domain.FormatArrow, 1))
	if err != nil {
		t.Fatal(err)
	}
	err = service.ReadRows(ctx, domain.ReadRowsRequest{StreamName: session.Streams[0].Name}, func(domain.ReadChunk) error {
		return nil
	})
	if domain.CodeOf(err) != domain.ErrorInternal {
		t.Fatalf("error = %v, code = %s; want INTERNAL", err, domain.CodeOf(err))
	}
}

func TestExpiredSessionCleanupWaitsForLifecycleAndClosesSnapshot(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	clock := newFakeClock()
	snapshot := newFakeSnapshot(domain.FormatAvro, 1)
	service := newTestService(t, &fakeMaterializer{snapshot: snapshot}, clock)
	session, err := service.CreateSession(ctx, createRequest(domain.FormatAvro, 1))
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(31 * time.Minute)
	if err := service.SweepExpired(ctx); err != nil {
		t.Fatal(err)
	}
	if snapshot.closeCount() != 1 {
		t.Fatalf("snapshot close calls = %d, want 1", snapshot.closeCount())
	}
	err = service.ReadRows(ctx, domain.ReadRowsRequest{StreamName: session.Streams[0].Name}, func(domain.ReadChunk) error {
		return nil
	})
	if domain.CodeOf(err) != domain.ErrorNotFound {
		t.Fatalf("expired stream code = %s, want NOT_FOUND", domain.CodeOf(err))
	}
}

func TestClosePreventsAConcurrentLifecycleFromCreatingNewSessions(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	service := newTestService(t, &fakeMaterializer{snapshot: newFakeSnapshot(domain.FormatArrow, 1)}, newFakeClock())
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	_, err := service.CreateSession(ctx, createRequest(domain.FormatArrow, 1))
	if domain.CodeOf(err) != domain.ErrorFailedPrecondition {
		t.Fatalf("create after close code = %s, want FAILED_PRECONDITION", domain.CodeOf(err))
	}
}

func TestStructuredLogsKeepRestrictionAndRowPayloadOpaque(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	snapshot := newFakeSnapshot(domain.FormatArrow, 1)
	snapshot.payloadPrefix = "raw-row-secret"
	service := newTestServiceWithLogger(t, &fakeMaterializer{snapshot: snapshot}, newFakeClock(), logger)
	request := createRequest(domain.FormatArrow, 1)
	request.RowRestriction = "credential_value = 'restriction-secret'"
	session, err := service.CreateSession(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.ReadRows(ctx, domain.ReadRowsRequest{StreamName: session.Streams[0].Name}, func(domain.ReadChunk) error { return nil }); err != nil {
		t.Fatal(err)
	}
	logs := output.String()
	for _, secret := range []string{"restriction-secret", "raw-row-secret"} {
		if strings.Contains(logs, secret) {
			t.Fatalf("logs contain raw value %q: %s", secret, logs)
		}
	}
	for _, field := range []string{"model_version", "schema_fingerprint", "payload_digest", "row_count", "retained_snapshot_bytes", "reservation_bytes", "side_effect.before", "side_effect.after"} {
		if !strings.Contains(logs, field) {
			t.Fatalf("logs do not contain %q: %s", field, logs)
		}
	}
}

func TestAvroReferenceSchemaMustBeJSON(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	snapshot := newFakeSnapshot(domain.FormatAvro, 1)
	snapshot.metadata.Schema.Serialized = []byte("not-json")
	service := newTestService(t, &fakeMaterializer{snapshot: snapshot}, newFakeClock())
	_, err := service.CreateSession(ctx, createRequest(domain.FormatAvro, 1))
	if domain.CodeOf(err) != domain.ErrorInternal {
		t.Fatalf("error = %v, want invalid backend schema to fail INTERNAL", err)
	}
	if snapshot.closeCount() != 1 {
		t.Fatalf("invalid snapshot close calls = %d, want 1", snapshot.closeCount())
	}
}

func TestCreateSessionPreservesClassifiedMaterializerErrors(t *testing.T) {
	for _, testCase := range []struct {
		name string
		err  error
		want domain.ErrorCode
	}{
		{
			name: "invalid projection",
			err:  domain.NewError(domain.ErrorInvalidArgument, "adapter.materialize", fmt.Errorf("secret selected field")),
			want: domain.ErrorInvalidArgument,
		},
		{
			name: "missing table",
			err:  domain.NewError(domain.ErrorNotFound, "adapter.materialize", fmt.Errorf("secret catalog key")),
			want: domain.ErrorNotFound,
		},
		{
			name: "unsupported snapshot",
			err:  domain.NewError(domain.ErrorUnimplemented, "adapter.materialize", fmt.Errorf("secret option")),
			want: domain.ErrorUnimplemented,
		},
		{
			name: "backend failure",
			err:  fmt.Errorf("secret backend query"),
			want: domain.ErrorInternal,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := testContext(t)
			defer cancel()
			service := newTestService(t, &fakeMaterializer{err: testCase.err}, newFakeClock())
			_, err := service.CreateSession(ctx, createRequest(domain.FormatArrow, 1))
			if domain.CodeOf(err) != testCase.want {
				t.Fatalf("materializer error code = %s, want %s: %v", domain.CodeOf(err), testCase.want, err)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("public application error leaked adapter cause: %v", err)
			}
		})
	}
}

func createRequest(format domain.Format, streams int32) domain.CreateSessionRequest {
	return domain.CreateSessionRequest{
		Parent:         "projects/reader-project",
		Table:          "projects/data-project/datasets/analytics/tables/events",
		Format:         format,
		MaxStreamCount: streams,
		TraceID:        "spark-stage-7",
	}
}

func newTestService(t *testing.T, materializer ports.SnapshotMaterializer, clock ports.Clock) *Service {
	t.Helper()
	return newTestServiceWithLogger(t, materializer, clock, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func newTestServiceWithLogger(t *testing.T, materializer ports.SnapshotMaterializer, clock ports.Clock, logger *slog.Logger) *Service {
	t.Helper()
	service, err := New(Config{
		Location:              "test-location",
		ProtocolModelVersion:  "google.cloud.bigquery.storage.v1@test",
		MaxStreams:            16,
		DefaultStreamCount:    4,
		SessionTTL:            30 * time.Minute,
		CleanupInterval:       time.Minute,
		MaxRowsPerResponse:    2,
		MaxSessions:           32,
		MaxSnapshotBytes:      1 << 20,
		MaxTotalSnapshotBytes: 32 << 20,
	}, materializer, clock, &fakeIDs{}, logger)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func testContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	timeout := 5 * time.Second
	if configured := os.Getenv("BQEMU_STORAGE_READ_TEST_TIMEOUT"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		if err != nil {
			t.Fatalf("BQEMU_STORAGE_READ_TEST_TIMEOUT: %v", err)
		}
		timeout = parsed
	}
	return context.WithTimeout(context.Background(), timeout)
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}

type fakeIDs struct {
	mu   sync.Mutex
	next int
}

func (g *fakeIDs) NewID() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.next++
	return fmt.Sprintf("session-%d", g.next)
}

type fakeMaterializer struct {
	mu       sync.Mutex
	calls    int
	snapshot *fakeSnapshot
	err      error
}

func (m *fakeMaterializer) Materialize(context.Context, domain.MaterializeRequest) (ports.ReadSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	return m.snapshot, m.err
}

func (m *fakeMaterializer) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

type readRange struct {
	start    int64
	end      int64
	maxBatch int64
}

type fakeSnapshot struct {
	mu               sync.Mutex
	metadata         domain.SnapshotMetadata
	ranges           []readRange
	closes           int
	batchOffsetDelta int64
	payloadPrefix    string
}

func newFakeSnapshot(format domain.Format, rows int64) *fakeSnapshot {
	schema := []byte("arrow-schema-message")
	if format == domain.FormatAvro {
		schema = []byte(`{"type":"record","name":"row","fields":[{"name":"id","type":"long"}]}`)
	}
	return &fakeSnapshot{
		metadata: domain.SnapshotMetadata{
			Schema:         domain.ReferenceSchema{Format: format, Serialized: schema},
			RowCount:       rows,
			EstimatedBytes: rows * 8,
			RetainedBytes:  rows * 8,
		},
		payloadPrefix: "batch",
	}
}

func (s *fakeSnapshot) Metadata() domain.SnapshotMetadata { return s.metadata }

func (s *fakeSnapshot) OpenRange(_ context.Context, start, end, maxBatch int64) (ports.BatchIterator, error) {
	s.mu.Lock()
	s.ranges = append(s.ranges, readRange{start: start, end: end, maxBatch: maxBatch})
	delta := s.batchOffsetDelta
	prefix := s.payloadPrefix
	s.mu.Unlock()
	return &fakeIterator{next: start, end: end, maxBatch: maxBatch, offsetDelta: delta, payloadPrefix: prefix}, nil
}

func (s *fakeSnapshot) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closes++
	return nil
}

func (s *fakeSnapshot) lastRange() readRange {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ranges[len(s.ranges)-1]
}

func (s *fakeSnapshot) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closes
}

type fakeIterator struct {
	next          int64
	end           int64
	maxBatch      int64
	offsetDelta   int64
	payloadPrefix string
}

func (i *fakeIterator) Next(context.Context) (domain.EncodedBatch, error) {
	if i.next >= i.end {
		return domain.EncodedBatch{}, io.EOF
	}
	rows := min(i.maxBatch, i.end-i.next)
	batch := domain.EncodedBatch{
		Offset:         i.next + i.offsetDelta,
		RowCount:       rows,
		SerializedRows: []byte(fmt.Sprintf("%s:%d:%d", i.payloadPrefix, i.next, rows)),
	}
	i.next += rows
	return batch, nil
}

func (*fakeIterator) Close() error { return nil }

var _ ports.SnapshotMaterializer = (*fakeMaterializer)(nil)
var _ ports.ReadSnapshot = (*fakeSnapshot)(nil)
var _ ports.BatchIterator = (*fakeIterator)(nil)
