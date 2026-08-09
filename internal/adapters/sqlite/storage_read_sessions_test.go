package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	readdomain "github.com/leeyh0216/go-bemu/internal/storageread/domain"
	readports "github.com/leeyh0216/go-bemu/internal/storageread/ports"
)

func TestStorageReadSessionStatePersistsWithoutRestrictionText(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	store := openTestStore(t, path)
	record := storageReadRecord("session-one", time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC), time.Hour)
	if err := store.CreateSession(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateSession(ctx, record); !errors.Is(err, readports.ErrSessionStateConflict) {
		t.Fatalf("duplicate create error = %v, want conflict", err)
	}

	stream, err := store.GetStream(ctx, record.Streams[1].Name)
	if err != nil {
		t.Fatal(err)
	}
	if stream.Session != record.Name || stream.Lifecycle != readdomain.SessionActive || !stream.ExpiresAt.Equal(record.ExpireTime) {
		t.Fatalf("persisted stream = %#v", stream)
	}
	var selectedJSON, digest string
	if err := store.db.QueryRowContext(ctx, `SELECT selected_fields_json, row_restriction_digest
        FROM storage_read_sessions WHERE session_name = ?`, record.Name).Scan(&selectedJSON, &digest); err != nil {
		t.Fatal(err)
	}
	if selectedJSON != `["EventID","Payload"]` || digest != record.RowRestrictionDigest {
		t.Fatalf("persisted safe request metadata = %s / %s", selectedJSON, digest)
	}
	var rawMatches int
	if err := store.db.QueryRowContext(ctx, `SELECT count(*) FROM storage_read_sessions
        WHERE instr(selected_fields_json, 'restriction-secret') > 0
           OR instr(row_restriction_digest, 'restriction-secret') > 0`).Scan(&rawMatches); err != nil {
		t.Fatal(err)
	}
	if rawMatches != 0 {
		t.Fatal("raw row restriction was persisted")
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openTestStore(t, path)
	stream, err = store.GetStream(ctx, record.Streams[1].Name)
	if err != nil || stream.Lifecycle != readdomain.SessionActive {
		t.Fatalf("stream after restart = %#v, %v", stream, err)
	}
}

func TestStorageReadLifecycleTransitionIsAtomicAndTerminal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "state.sqlite"))
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	record := storageReadRecord("atomic-session", now, time.Hour)
	duplicateStreamRecord := storageReadRecord("duplicate-stream-session", now, time.Hour)
	duplicateStreamRecord.Streams[1].Name = duplicateStreamRecord.Streams[0].Name
	if err := store.CreateSession(ctx, duplicateStreamRecord); !errors.Is(err, readports.ErrSessionStateConflict) {
		t.Fatalf("duplicate stream create error = %v, want conflict", err)
	}
	var partiallyCreated int
	if err := store.db.QueryRowContext(ctx,
		"SELECT count(*) FROM storage_read_sessions WHERE session_name = ?", duplicateStreamRecord.Name,
	).Scan(&partiallyCreated); err != nil {
		t.Fatal(err)
	}
	if partiallyCreated != 0 {
		t.Fatal("session insert survived a stream insert rollback")
	}
	if err := store.CreateSession(ctx, record); err != nil {
		t.Fatal(err)
	}

	err := store.TransitionSessions(ctx,
		[]string{record.Name, "missing-session"},
		readdomain.SessionActive, readdomain.SessionUnavailable, now.Add(time.Minute),
	)
	if !errors.Is(err, readports.ErrSessionStateNotFound) {
		t.Fatalf("partial transition error = %v, want not found", err)
	}
	stream, err := store.GetStream(ctx, record.Streams[0].Name)
	if err != nil {
		t.Fatal(err)
	}
	if stream.Lifecycle != readdomain.SessionActive {
		t.Fatalf("partially committed lifecycle = %s", stream.Lifecycle)
	}

	if err := store.TransitionSessions(ctx, []string{record.Name},
		readdomain.SessionActive, readdomain.SessionUnavailable, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	// A retry to the same terminal state is idempotent.
	if err := store.TransitionSessions(ctx, []string{record.Name},
		readdomain.SessionActive, readdomain.SessionUnavailable, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.TransitionSessions(ctx, []string{record.Name},
		readdomain.SessionActive, readdomain.SessionExpired, now.Add(2*time.Minute)); !errors.Is(err, readports.ErrSessionStateConflict) {
		t.Fatalf("terminal rewrite error = %v, want conflict", err)
	}
}

func TestStorageReadReconcileClassifiesActiveSessionsAcrossRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	store := openTestStore(t, path)
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	future := storageReadRecord("future-session", now.Add(-time.Minute), time.Hour)
	expired := storageReadRecord("expired-session", now.Add(-2*time.Hour), time.Hour)
	for _, record := range []readdomain.SessionRecord{future, expired} {
		if err := store.CreateSession(ctx, record); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openTestStore(t, path)

	count, err := store.ReconcileActive(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("reconciled sessions = %d, want 2", count)
	}
	want := map[string]readdomain.SessionLifecycle{
		future.Streams[0].Name:  readdomain.SessionUnavailable,
		expired.Streams[0].Name: readdomain.SessionExpired,
	}
	for streamName, lifecycle := range want {
		stream, err := store.GetStream(ctx, streamName)
		if err != nil {
			t.Fatal(err)
		}
		if stream.Lifecycle != lifecycle {
			t.Fatalf("stream %s lifecycle = %s, want %s", streamName, stream.Lifecycle, lifecycle)
		}
	}
	count, err = store.ReconcileActive(ctx, now.Add(time.Minute))
	if err != nil || count != 0 {
		t.Fatalf("second reconciliation = %d, %v; want no-op", count, err)
	}
}

func TestStorageReadSessionMigrationShape(t *testing.T) {
	t.Parallel()
	migrations, err := readMigrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) < 5 || migrations[4].name != "005_storage_read_sessions.sql" {
		t.Fatalf("migration 005 = %#v", migrations)
	}
	store := openTestStore(t, filepath.Join(t.TempDir(), "state.sqlite"))
	rows, err := store.db.Query("PRAGMA table_info(storage_read_sessions)")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns := make([]string, 0)
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, dataType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, name)
	}
	for _, required := range []string{
		"selected_fields_json", "row_restriction_digest", "filter_predicate_count",
		"stream_count", "retained_row_count", "retained_bytes", "lifecycle_state",
	} {
		if !slices.Contains(columns, required) {
			t.Fatalf("migration columns = %v, missing %s", columns, required)
		}
	}
	if len(columns) == 0 {
		t.Fatal("Storage Read migration created no columns")
	}
}

func storageReadRecord(id string, createdAt time.Time, ttl time.Duration) readdomain.SessionRecord {
	sessionName := "projects/reader-project/locations/US/sessions/" + id
	return readdomain.SessionRecord{
		Name: sessionName, Table: "projects/data-project/datasets/analytics/tables/events",
		Format: readdomain.FormatArrow, SelectedFields: []string{"EventID", "Payload"},
		RowRestrictionDigest: "sha256:" + strings.Repeat("a", 64), RowRestrictionBytes: 39,
		FilterShape: readdomain.FilterShape{PredicateCount: 2, LogicalOperatorCount: 1},
		Streams: []readdomain.Stream{
			{Name: sessionName + "/streams/0", StartOffset: 0, EndOffset: 2},
			{Name: sessionName + "/streams/1", StartOffset: 2, EndOffset: 4},
		},
		CreatedAt: createdAt, ExpireTime: createdAt.Add(ttl), RetainedRowCount: 4,
		RetainedBytes: 128, EstimatedBytesScanned: 96,
		SchemaFingerprint: "sha256:" + strings.Repeat("b", 64),
		Lifecycle:         readdomain.SessionActive, LifecycleUpdatedAt: createdAt,
	}
}
