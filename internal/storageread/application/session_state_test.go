package application

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/storageread/domain"
	"github.com/leeyh0216/go-bemu/internal/storageread/ports"
)

func TestPersistedReadStreamsAreNotRematerializedAfterRestart(t *testing.T) {
	ctx := context.Background()
	repository := newReadStateRecorder()
	clock := newFakeClock()
	repository.seed(readStateRecord("future", clock.Now().Add(-time.Minute), clock.Now().Add(time.Hour)))
	repository.seed(readStateRecord("expired", clock.Now().Add(-time.Hour), clock.Now().Add(-time.Minute)))
	materializer := &fakeMaterializer{snapshot: newFakeSnapshot(domain.FormatArrow, 1)}
	service, err := New(Config{
		Location: "test-location", ProtocolModelVersion: "storage-read-state-test",
		MaxStreams: 16, DefaultStreamCount: 4, SessionTTL: time.Hour,
		CleanupInterval: time.Minute, MaxRowsPerResponse: 2, MaxSessions: 32,
		MaxSnapshotBytes: 1 << 20, MaxTotalSnapshotBytes: 32 << 20,
	}, materializer, clock, &fakeIDs{}, slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithSessionStateRepository(repository))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateSession(ctx, createRequest(domain.FormatArrow, 1)); domain.CodeOf(err) != domain.ErrorFailedPrecondition {
		t.Fatalf("create before reconcile = %s, %v", domain.CodeOf(err), err)
	}
	if err := service.ReconcilePersistedSessions(ctx); err != nil {
		t.Fatal(err)
	}
	for _, testCase := range []struct {
		stream string
		want   domain.ErrorCode
	}{
		{readStateStream("future"), domain.ErrorUnavailable},
		{readStateStream("expired"), domain.ErrorNotFound},
		{readStateStream("missing"), domain.ErrorNotFound},
	} {
		err := service.ReadRows(ctx, domain.ReadRowsRequest{StreamName: testCase.stream}, func(domain.ReadChunk) error { return nil })
		if domain.CodeOf(err) != testCase.want {
			t.Fatalf("ReadRows(%s) = %s, want %s", testCase.stream, domain.CodeOf(err), testCase.want)
		}
	}
	if materializer.callCount() != 0 {
		t.Fatalf("persisted stream caused %d materializations", materializer.callCount())
	}
}

type readStateRecorder struct {
	mu      sync.Mutex
	records map[string]domain.SessionRecord
	streams map[string]string
}

func newReadStateRecorder() *readStateRecorder {
	return &readStateRecorder{records: make(map[string]domain.SessionRecord), streams: make(map[string]string)}
}

func (r *readStateRecorder) CreateSession(_ context.Context, record domain.SessionRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.records[record.Name]; exists {
		return ports.ErrSessionStateConflict
	}
	r.put(record)
	return nil
}

func (r *readStateRecorder) TransitionSessions(_ context.Context, names []string, from, to domain.SessionLifecycle, at time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, name := range names {
		record, exists := r.records[name]
		if !exists {
			return ports.ErrSessionStateNotFound
		}
		if record.Lifecycle != from && record.Lifecycle != to {
			return ports.ErrSessionStateConflict
		}
	}
	for _, name := range names {
		record := r.records[name]
		record.Lifecycle, record.LifecycleUpdatedAt = to, at
		r.records[name] = record
	}
	return nil
}

func (r *readStateRecorder) ReconcileActive(_ context.Context, now time.Time) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var count int64
	for name, record := range r.records {
		if record.Lifecycle != domain.SessionActive {
			continue
		}
		if record.ExpireTime.After(now) {
			record.Lifecycle = domain.SessionUnavailable
		} else {
			record.Lifecycle = domain.SessionExpired
		}
		record.LifecycleUpdatedAt = now
		r.records[name] = record
		count++
	}
	return count, nil
}

func (r *readStateRecorder) GetStream(_ context.Context, name string) (domain.PersistedStream, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	session, exists := r.streams[name]
	if !exists {
		return domain.PersistedStream{}, ports.ErrSessionStateNotFound
	}
	record := r.records[session]
	return domain.PersistedStream{Name: name, Session: session, Lifecycle: record.Lifecycle, ExpiresAt: record.ExpireTime}, nil
}

func (r *readStateRecorder) seed(record domain.SessionRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.put(record)
}

func (r *readStateRecorder) put(record domain.SessionRecord) {
	record = cloneSessionRecord(record)
	r.records[record.Name] = record
	for _, stream := range record.Streams {
		r.streams[stream.Name] = record.Name
	}
}

func readStateRecord(id string, createdAt, expiresAt time.Time) domain.SessionRecord {
	name := readStateSession(id)
	return domain.SessionRecord{
		Name: name, Table: "projects/data/datasets/analytics/tables/events", Format: domain.FormatArrow,
		Streams:   []domain.Stream{{Name: readStateStream(id), EndOffset: 1}},
		CreatedAt: createdAt, ExpireTime: expiresAt, RetainedRowCount: 1,
		RowRestrictionDigest: digest(nil), SchemaFingerprint: digest([]byte("schema")),
		Lifecycle: domain.SessionActive, LifecycleUpdatedAt: createdAt,
	}
}

func readStateSession(id string) string {
	return "projects/reader/locations/test-location/sessions/" + id
}

func readStateStream(id string) string { return readStateSession(id) + "/streams/0" }

var _ ports.SessionStateRepository = (*readStateRecorder)(nil)
