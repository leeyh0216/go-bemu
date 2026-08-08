package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	readdomain "github.com/leeyh0216/go-bemu/internal/storageread/domain"
	readports "github.com/leeyh0216/go-bemu/internal/storageread/ports"
)

func TestStorageReadSessionLifecycleSurvivesRestartWithoutPayload(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	repositories, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	future := readSessionRecord("future", now.Add(-time.Minute), now.Add(time.Hour))
	expired := readSessionRecord("expired", now.Add(-2*time.Hour), now.Add(-time.Hour))
	for _, record := range []readdomain.SessionRecord{future, expired} {
		if err := repositories.ReadSessions().CreateSession(ctx, record); err != nil {
			t.Fatal(err)
		}
	}
	if err := repositories.ReadSessions().CreateSession(ctx, future); !errors.Is(err, readports.ErrSessionStateConflict) {
		t.Fatalf("duplicate create error = %v", err)
	}
	var rawMatches int
	if err := repositories.db.QueryRowContext(ctx, `SELECT count(*) FROM bqemu_read_sessions
WHERE instr(selected_fields_json, 'restriction-secret') > 0
   OR instr(row_restriction_digest, 'restriction-secret') > 0`).Scan(&rawMatches); err != nil {
		t.Fatal(err)
	}
	if rawMatches != 0 {
		t.Fatal("raw row restriction was persisted")
	}
	if err := repositories.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	count, err := restarted.ReadSessions().ReconcileActive(ctx, now)
	if err != nil || count != 2 {
		t.Fatalf("reconciled sessions = %d, %v", count, err)
	}
	for _, testCase := range []struct {
		stream    string
		lifecycle readdomain.SessionLifecycle
	}{
		{future.Streams[0].Name, readdomain.SessionUnavailable},
		{expired.Streams[0].Name, readdomain.SessionExpired},
	} {
		stream, err := restarted.ReadSessions().GetStream(ctx, testCase.stream)
		if err != nil || stream.Lifecycle != testCase.lifecycle {
			t.Fatalf("stream %s = %#v, %v", testCase.stream, stream, err)
		}
	}
}

func TestStorageReadSessionCreateRollsBackInvalidStreamSet(t *testing.T) {
	ctx := context.Background()
	repositories, err := Open(ctx, filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer repositories.Close()
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	record := readSessionRecord("invalid", now, now.Add(time.Hour))
	record.Streams[1].Name = record.Streams[0].Name
	if err := repositories.ReadSessions().CreateSession(ctx, record); !errors.Is(err, readports.ErrSessionStateConflict) {
		t.Fatalf("invalid create error = %v", err)
	}
	var count int
	if err := repositories.db.QueryRowContext(ctx,
		"SELECT count(*) FROM bqemu_read_sessions WHERE session_name = ?", record.Name,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("invalid session was partially persisted")
	}
}

func readSessionRecord(id string, createdAt, expiresAt time.Time) readdomain.SessionRecord {
	name := "projects/reader/locations/US/sessions/" + id
	return readdomain.SessionRecord{
		Name: name, Table: "projects/data/datasets/analytics/tables/events",
		Format: readdomain.FormatArrow, SelectedFields: []string{"event_id", "payload"},
		RowRestrictionDigest: "sha256:" + strings.Repeat("a", 64), RowRestrictionBytes: 39,
		Streams: []readdomain.Stream{
			{Name: name + "/streams/0", StartOffset: 0, EndOffset: 2},
			{Name: name + "/streams/1", StartOffset: 2, EndOffset: 4},
		},
		CreatedAt: createdAt, ExpireTime: expiresAt, RetainedRowCount: 4,
		RetainedBytes: 128, EstimatedBytesScanned: 96,
		SchemaFingerprint: "sha256:" + strings.Repeat("b", 64),
		Lifecycle:         readdomain.SessionActive, LifecycleUpdatedAt: createdAt,
	}
}
