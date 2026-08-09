package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/state"
)

func TestMutationJournalBeginIsIdempotentAndDetectsConflict(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "state.sqlite"))
	intent := testMutation("mutation-001", "resource/one", time.Date(2026, 8, 8, 1, 0, 0, 0, time.UTC))

	first, err := store.Begin(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	retry := intent
	retry.PreparedAt = intent.PreparedAt.Add(time.Hour)
	second, err := store.Begin(ctx, retry)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("idempotent Begin mismatch:\nfirst=%#v\nsecond=%#v", first, second)
	}

	conflict := intent
	conflict.AfterPhysicalFingerprint = state.Fingerprint([]byte("different-after"))
	_, err = store.Begin(ctx, conflict)
	if !errors.Is(err, state.ErrConflict) {
		t.Fatalf("conflicting Begin error = %v, want ErrConflict", err)
	}
	if strings.Contains(err.Error(), intent.ID) || strings.Contains(err.Error(), intent.ResourceKey) {
		t.Fatalf("conflict error disclosed identifiers: %v", err)
	}
}

func TestMutationJournalPendingOrderAndCASTransitions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "state.sqlite"))
	base := time.Date(2026, 8, 8, 2, 0, 0, 0, time.UTC)
	for _, intent := range []state.BeginMutation{
		testMutation("mutation-c", "resource/c", base.Add(time.Second)),
		testMutation("mutation-b", "resource/b", base),
		testMutation("mutation-a", "resource/a", base),
	} {
		if _, err := store.Begin(ctx, intent); err != nil {
			t.Fatal(err)
		}
	}
	pending, err := store.ListPending(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{pending[0].ID, pending[1].ID}; !reflect.DeepEqual(got, []string{"mutation-a", "mutation-b"}) {
		t.Fatalf("pending order = %v", got)
	}

	var wait sync.WaitGroup
	wait.Add(2)
	results := make(chan error, 2)
	go func() {
		defer wait.Done()
		_, err := store.MarkFailed(ctx, "mutation-a", state.Failure{
			Code: "physical.apply_failed", Digest: state.Fingerprint([]byte("failure-class-a")),
		}, base.Add(2*time.Second))
		results <- err
	}()
	go func() {
		defer wait.Done()
		_, err := store.MarkFailed(ctx, "mutation-a", state.Failure{
			Code: "physical.apply_failed", Digest: state.Fingerprint([]byte("failure-class-b")),
		}, base.Add(2*time.Second))
		results <- err
	}()
	wait.Wait()
	close(results)
	var successes, transitionFailures int
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, state.ErrInvalidTransition):
			transitionFailures++
		default:
			t.Fatalf("CAS transition error = %v", err)
		}
	}
	if successes != 1 || transitionFailures != 1 {
		t.Fatalf("CAS results: successes=%d transition failures=%d", successes, transitionFailures)
	}
	terminal, err := store.Get(ctx, "mutation-a")
	if err != nil {
		t.Fatal(err)
	}
	if terminal.State != state.MutationFailed {
		t.Fatalf("terminal state = %s", terminal.State)
	}
	if _, err := store.MarkFailed(ctx, "mutation-a", state.Failure{
		Code: "physical.apply_failed", Digest: state.Fingerprint([]byte("retry")),
	}, base.Add(3*time.Second)); !errors.Is(err, state.ErrInvalidTransition) {
		t.Fatalf("second terminal transition error = %v", err)
	}

	pending, err = store.ListPending(ctx, state.MaxPendingList)
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{pending[0].ID, pending[1].ID}; !reflect.DeepEqual(got, []string{"mutation-b", "mutation-c"}) {
		t.Fatalf("remaining pending order = %v", got)
	}
}

func TestMutationJournalPersistsAcrossRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	store, err := Open(ctx, DefaultConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 8, 3, 0, 0, 0, time.UTC)
	pendingIntent := testMutation("mutation-pending", "resource/pending", base)
	failedIntent := testMutation("mutation-failed", "resource/failed", base.Add(time.Second))
	if _, err := store.Begin(ctx, pendingIntent); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Begin(ctx, failedIntent); err != nil {
		t.Fatal(err)
	}
	failed, err := store.MarkFailed(ctx, failedIntent.ID, state.Failure{
		Code: "physical.apply_failed", Digest: state.Fingerprint([]byte("persistent-failure")),
	}, base.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(ctx, DefaultConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	reloaded, err := store.Get(ctx, failedIntent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(reloaded, failed) {
		t.Fatalf("failed mutation after restart mismatch:\n got=%#v\nwant=%#v", reloaded, failed)
	}
	pending, err := store.ListPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].ID != pendingIntent.ID {
		t.Fatalf("pending after restart = %#v", pending)
	}
}

func TestCommitTableChangeAtomicallyPublishesCatalogAndJournal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "state.sqlite"))
	base := time.Date(2026, 8, 8, 3, 30, 0, 0, time.UTC)
	intent := testMutation("mutation-atomic", "resource/atomic", base)
	before := intent.TableChange.Before
	if err := store.CreateProject(ctx, domain.Project{ID: before.ProjectID, CreatedAt: base, UpdatedAt: base}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateDataset(ctx, domain.Dataset{
		ProjectID: before.ProjectID, ID: before.DatasetID, Location: "US",
		CreatedAt: base, UpdatedAt: base,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTable(ctx, before); err != nil {
		t.Fatal(err)
	}
	storedBefore, err := store.GetTable(ctx, before.ProjectID, before.DatasetID, before.ID)
	if err != nil {
		t.Fatal(err)
	}
	storedAfter := storedBefore
	storedAfter.Schema = []domain.Field{{Name: "value", Type: "INT64", Fields: []domain.Field{}}}
	storedAfter.UpdatedAt = storedBefore.UpdatedAt.Add(time.Second)
	intent.ResourceKey = state.TableResourceKey(storedBefore)
	intent.ExpectedCanonicalRevision = state.TableRevision(storedBefore)
	intent.TableChange = state.TableChange{Before: storedBefore, After: storedAfter}
	record, err := store.Begin(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`CREATE TRIGGER reject_test_mutation_apply
        BEFORE UPDATE OF state ON mutation_journal
        WHEN NEW.state = 'APPLIED'
        BEGIN SELECT RAISE(ABORT, 'injected journal failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitTableChange(ctx, record.ID, base.Add(2*time.Second)); err == nil ||
		!strings.Contains(err.Error(), "injected journal failure") {
		t.Fatalf("injected commit error = %v", err)
	}
	canonical, err := store.GetTable(ctx, before.ProjectID, before.DatasetID, before.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(canonical, storedBefore) {
		t.Fatalf("catalog changed despite rolled back journal transition:\n got=%#v\nwant=%#v", canonical, storedBefore)
	}
	pending, err := store.Get(ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if pending.State != state.MutationPrepared {
		t.Fatalf("journal state after rollback = %s", pending.State)
	}
	if _, err := store.db.Exec(`DROP TRIGGER reject_test_mutation_apply`); err != nil {
		t.Fatal(err)
	}
	applied, err := store.CommitTableChange(ctx, record.ID, base.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if applied.State != state.MutationApplied {
		t.Fatalf("journal state after retry = %s", applied.State)
	}
	canonical, err = store.GetTable(ctx, before.ProjectID, before.DatasetID, before.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(canonical, intent.TableChange.After) {
		t.Fatalf("catalog after atomic commit:\n got=%#v\nwant=%#v", canonical, intent.TableChange.After)
	}
}

func TestMutationJournalMigrationChecksumAndDatabaseGuards(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	store, err := Open(ctx, DefaultConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	migrations, err := readMigrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) < 3 || migrations[1].name != "002_mutation_journal.sql" ||
		migrations[2].name != "003_canonical_table_mutations.sql" {
		t.Fatalf("migrations = %#v", migrations)
	}
	var name, checksum string
	if err := store.db.QueryRow(
		"SELECT name, checksum FROM schema_migrations WHERE version = 2",
	).Scan(&name, &checksum); err != nil {
		t.Fatal(err)
	}
	if name != migrations[1].name || checksum != migrations[1].checksum {
		t.Fatalf("migration ledger = %s/%s, want %s/%s", name, checksum, migrations[1].name, migrations[1].checksum)
	}
	rows, err := store.db.Query("PRAGMA table_info(mutation_journal)")
	if err != nil {
		t.Fatal(err)
	}
	var columns []string
	for rows.Next() {
		var columnID, notNull, primaryKey int
		var columnName, dataType string
		var defaultValue any
		if err := rows.Scan(&columnID, &columnName, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		columns = append(columns, columnName)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	expectedColumns := []string{
		"mutation_id", "resource_key", "mutation_kind", "expected_canonical_revision",
		"before_physical_fingerprint", "after_physical_fingerprint", "state",
		"failure_code", "failure_digest", "prepared_at", "updated_at", "completed_at",
		"canonical_before_json", "canonical_after_json",
	}
	if !reflect.DeepEqual(columns, expectedColumns) {
		t.Fatalf("mutation journal columns = %v", columns)
	}

	intent := testMutation("mutation-guard", "resource/guard", time.Date(2026, 8, 8, 4, 0, 0, 0, time.UTC))
	if _, err := store.Begin(ctx, intent); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(
		"UPDATE mutation_journal SET resource_key = 'changed' WHERE mutation_id = ?",
		intent.ID,
	); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("immutable intent update error = %v", err)
	}
	if _, err := store.MarkFailed(ctx, intent.ID, state.Failure{
		Code: "physical.apply_failed", Digest: state.Fingerprint([]byte("failure-class")),
	}, intent.PreparedAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(
		"UPDATE mutation_journal SET failure_code = 'changed' WHERE mutation_id = ?",
		intent.ID,
	); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("terminal record update error = %v", err)
	}
	if _, err := store.db.Exec(
		"DELETE FROM mutation_journal WHERE mutation_id = ?",
		intent.ID,
	); err == nil || !strings.Contains(err.Error(), "cannot be deleted") {
		t.Fatalf("journal delete error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE schema_migrations SET checksum = 'changed' WHERE version = 2"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = Open(ctx, DefaultConfig(path))
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("Open error = %v, want checksum mismatch", err)
	}
}

func TestMutationJournalRejectsUnsafeFailureAndInvalidBounds(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "state.sqlite"))
	base := time.Date(2026, 8, 8, 5, 0, 0, 0, time.UTC)
	intent := testMutation("mutation-safe-error", "sensitive/project/dataset/table", base)
	if _, err := store.Begin(ctx, intent); err != nil {
		t.Fatal(err)
	}
	unsafeFailure := state.Failure{
		Code: "duckdb error: SELECT sensitive_value", Digest: state.Fingerprint([]byte("safe-class")),
	}
	_, err := store.MarkFailed(ctx, intent.ID, unsafeFailure, base.Add(time.Second))
	if !errors.Is(err, state.ErrInvalid) {
		t.Fatalf("unsafe failure error = %v", err)
	}
	if strings.Contains(err.Error(), unsafeFailure.Code) || strings.Contains(err.Error(), intent.ResourceKey) {
		t.Fatalf("failure validation disclosed unsafe values: %v", err)
	}
	if _, err := store.ListPending(ctx, 0); !errors.Is(err, state.ErrInvalid) {
		t.Fatalf("zero pending limit error = %v", err)
	}
	if _, err := store.ListPending(ctx, state.MaxPendingList+1); !errors.Is(err, state.ErrInvalid) {
		t.Fatalf("large pending limit error = %v", err)
	}
}

func testMutation(id, _ string, preparedAt time.Time) state.BeginMutation {
	tableID := strings.ReplaceAll(id, "-", "_")
	before := domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: tableID, Type: "TABLE",
		Schema:    []domain.Field{{Name: "value", Type: "STRING"}},
		CreatedAt: preparedAt, UpdatedAt: preparedAt,
	}
	after := before
	after.Schema = []domain.Field{{Name: "value", Type: "INT64"}}
	after.UpdatedAt = preparedAt.Add(time.Second)
	return state.BeginMutation{
		ID: id, ResourceKey: state.TableResourceKey(before), Kind: state.MutationKindTableSchema,
		ExpectedCanonicalRevision: state.TableRevision(before),
		BeforePhysicalFingerprint: state.Fingerprint([]byte("before:" + id)),
		AfterPhysicalFingerprint:  state.Fingerprint([]byte("after:" + id)),
		TableChange:               state.TableChange{Before: before, After: after},
		PreparedAt:                preparedAt,
	}
}
