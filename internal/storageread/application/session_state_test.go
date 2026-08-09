package application

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/storageread/domain"
	"github.com/leeyh0216/go-bemu/internal/storageread/ports"
)

func TestCreateSessionPersistsSanitizedCanonicalLifecycleMetadata(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	repository := newRecordingSessionRepository()
	clock := newFakeClock()
	snapshot := newFakeSnapshot(domain.FormatArrow, 4)
	snapshot.metadata.SelectedFields = []string{"EventID", "Payload"}
	snapshot.metadata.FilterShape = domain.FilterShape{PredicateCount: 2, LogicalOperatorCount: 1}
	service := newDurableTestService(t, &fakeMaterializer{snapshot: snapshot}, clock, repository)
	if _, err := service.CreateSession(ctx, createRequest(domain.FormatArrow, 2)); domain.CodeOf(err) != domain.ErrorFailedPrecondition {
		t.Fatalf("create before reconciliation code = %s, want FAILED_PRECONDITION", domain.CodeOf(err))
	}
	if err := service.ReconcilePersistedSessions(ctx); err != nil {
		t.Fatal(err)
	}

	request := createRequest(domain.FormatArrow, 2)
	request.SelectedFields = []string{"eventid", "payload"}
	request.RowRestriction = "credential = 'restriction-secret' AND active = TRUE"
	session, err := service.CreateSession(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	record := repository.record(session.Name)
	if !slices.Equal(record.SelectedFields, []string{"EventID", "Payload"}) {
		t.Fatalf("canonical selected fields = %v", record.SelectedFields)
	}
	if record.RowRestrictionDigest != digest([]byte(request.RowRestriction)) ||
		record.RowRestrictionDigest == request.RowRestriction || record.RowRestrictionBytes != len(request.RowRestriction) {
		t.Fatalf("sanitized restriction metadata = %#v", record)
	}
	if record.FilterShape != snapshot.metadata.FilterShape || record.RetainedRowCount != 4 ||
		record.RetainedBytes != snapshot.metadata.RetainedBytes || len(record.Streams) != 2 {
		t.Fatalf("persisted lifecycle metadata = %#v", record)
	}

	if err := service.ReadRows(ctx, domain.ReadRowsRequest{StreamName: session.Streams[0].Name}, func(domain.ReadChunk) error { return nil }); err != nil {
		t.Fatalf("current-process ReadRows changed: %v", err)
	}
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if got := repository.record(session.Name).Lifecycle; got != domain.SessionUnavailable {
		t.Fatalf("lifecycle after close = %s, want UNAVAILABLE", got)
	}
	if !repository.allCallsBounded() {
		t.Fatal("a lifecycle repository call had no context deadline")
	}
}

func TestReconcilePersistedSessionsReturnsStableOldStreamStatuses(t *testing.T) {
	ctx, cancel := testContext(t)
	defer cancel()
	repository := newRecordingSessionRepository()
	now := newFakeClock().Now()
	repository.seed(sessionRecordForApplicationTest("future", now.Add(-time.Minute), now.Add(time.Hour)))
	repository.seed(sessionRecordForApplicationTest("expired", now.Add(-time.Hour), now.Add(-time.Minute)))
	materializer := &fakeMaterializer{snapshot: newFakeSnapshot(domain.FormatArrow, 1)}
	service := newDurableTestService(t, materializer, &fakeClock{now: now}, repository)
	if err := service.ReconcilePersistedSessions(ctx); err != nil {
		t.Fatal(err)
	}

	for _, testCase := range []struct {
		stream string
		want   domain.ErrorCode
	}{
		{stream: persistedTestStream("future"), want: domain.ErrorUnavailable},
		{stream: persistedTestStream("expired"), want: domain.ErrorNotFound},
		{stream: persistedTestStream("missing"), want: domain.ErrorNotFound},
	} {
		err := service.ReadRows(ctx, domain.ReadRowsRequest{StreamName: testCase.stream}, func(domain.ReadChunk) error { return nil })
		if domain.CodeOf(err) != testCase.want {
			t.Fatalf("ReadRows(%s) code = %s, want %s: %v", testCase.stream, domain.CodeOf(err), testCase.want, err)
		}
	}
	if materializer.callCount() != 0 {
		t.Fatalf("old streams caused %d rematerializations", materializer.callCount())
	}
	if got := repository.record(persistedTestSession("future")).Lifecycle; got != domain.SessionUnavailable {
		t.Fatalf("future lifecycle = %s", got)
	}
	if got := repository.record(persistedTestSession("expired")).Lifecycle; got != domain.SessionExpired {
		t.Fatalf("expired lifecycle = %s", got)
	}
	if !repository.allCallsBounded() {
		t.Fatal("a reconciliation or lookup call had no context deadline")
	}
}

func newDurableTestService(
	t *testing.T,
	materializer ports.SnapshotMaterializer,
	clock ports.Clock,
	repository ports.SessionStateRepository,
) *Service {
	t.Helper()
	service, err := New(Config{
		Location: "test-location", ProtocolModelVersion: "google.cloud.bigquery.storage.v1@test",
		MaxStreams: 16, DefaultStreamCount: 4, SessionTTL: 30 * time.Minute,
		CleanupInterval: time.Minute, MaxRowsPerResponse: 2, MaxSessions: 32,
		MaxSnapshotBytes: 1 << 20, MaxTotalSnapshotBytes: 32 << 20,
		StateOperationTimeout: time.Second,
	}, materializer, clock, &fakeIDs{}, slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithSessionStateRepository(repository))
	if err != nil {
		t.Fatal(err)
	}
	return service
}

type recordingSessionRepository struct {
	mu             sync.Mutex
	records        map[string]domain.SessionRecord
	streamSessions map[string]string
	bounded        []bool
}

func newRecordingSessionRepository() *recordingSessionRepository {
	return &recordingSessionRepository{
		records: make(map[string]domain.SessionRecord), streamSessions: make(map[string]string),
	}
}

func (r *recordingSessionRepository) CreateSession(ctx context.Context, record domain.SessionRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.noteBounded(ctx)
	if _, found := r.records[record.Name]; found {
		return ports.ErrSessionStateConflict
	}
	r.put(record)
	return nil
}

func (r *recordingSessionRepository) TransitionSessions(
	ctx context.Context,
	names []string,
	from domain.SessionLifecycle,
	to domain.SessionLifecycle,
	at time.Time,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.noteBounded(ctx)
	for _, name := range names {
		record, found := r.records[name]
		if !found {
			return ports.ErrSessionStateNotFound
		}
		if record.Lifecycle != from && record.Lifecycle != to {
			return ports.ErrSessionStateConflict
		}
	}
	for _, name := range names {
		record := r.records[name]
		record.Lifecycle = to
		record.LifecycleUpdatedAt = at
		r.records[name] = record
	}
	return nil
}

func (r *recordingSessionRepository) ReconcileActive(ctx context.Context, now time.Time) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.noteBounded(ctx)
	var count int64
	for name, record := range r.records {
		if record.Lifecycle != domain.SessionActive {
			continue
		}
		if !record.ExpireTime.After(now) {
			record.Lifecycle = domain.SessionExpired
		} else {
			record.Lifecycle = domain.SessionUnavailable
		}
		record.LifecycleUpdatedAt = now
		r.records[name] = record
		count++
	}
	return count, nil
}

func (r *recordingSessionRepository) GetStream(ctx context.Context, streamName string) (domain.PersistedStream, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.noteBounded(ctx)
	sessionName, found := r.streamSessions[streamName]
	if !found {
		return domain.PersistedStream{}, ports.ErrSessionStateNotFound
	}
	record := r.records[sessionName]
	return domain.PersistedStream{
		Name: streamName, Session: sessionName, Lifecycle: record.Lifecycle, ExpiresAt: record.ExpireTime,
	}, nil
}

func (r *recordingSessionRepository) seed(record domain.SessionRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.put(record)
}

func (r *recordingSessionRepository) put(record domain.SessionRecord) {
	record = cloneSessionRecord(record)
	r.records[record.Name] = record
	for _, stream := range record.Streams {
		r.streamSessions[stream.Name] = record.Name
	}
}

func (r *recordingSessionRepository) record(name string) domain.SessionRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneSessionRecord(r.records[name])
}

func (r *recordingSessionRepository) noteBounded(ctx context.Context) {
	_, ok := ctx.Deadline()
	r.bounded = append(r.bounded, ok)
}

func (r *recordingSessionRepository) allCallsBounded() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.bounded) > 0 && !slices.Contains(r.bounded, false)
}

func sessionRecordForApplicationTest(id string, createdAt, expiresAt time.Time) domain.SessionRecord {
	name := persistedTestSession(id)
	return domain.SessionRecord{
		Name: name, Table: "projects/data-project/datasets/analytics/tables/events",
		Format: domain.FormatArrow, Streams: []domain.Stream{{Name: persistedTestStream(id), EndOffset: 1}},
		CreatedAt: createdAt, ExpireTime: expiresAt, RetainedRowCount: 1,
		Lifecycle: domain.SessionActive, LifecycleUpdatedAt: createdAt,
	}
}

func persistedTestSession(id string) string {
	return "projects/reader-project/locations/test-location/sessions/" + id
}

func persistedTestStream(id string) string { return persistedTestSession(id) + "/streams/0" }

var _ ports.SessionStateRepository = (*recordingSessionRepository)(nil)
