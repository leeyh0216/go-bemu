package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
	"github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
)

func TestStorageWritePreparedAppendReconcilesAcrossSQLiteRestartWithoutPayload(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	repositories, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	record := writeStreamRecord("restart", now)
	if err := repositories.WriteState().CreateStream(ctx, record); err != nil {
		t.Fatal(err)
	}
	receipt := writeReceipt(record.Stream.Name, 0, "a", now)
	prepared := record
	prepared.Operation = domain.OperationAppend
	prepared.OperationPhase = domain.OperationPhasePrepared
	prepared.OperationToken = "0"
	prepared.Revision++
	if err := repositories.WriteState().PrepareAppend(ctx, record.Revision, prepared, receipt); err != nil {
		t.Fatal(err)
	}

	var forbiddenColumns int
	if err := repositories.db.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info('bqemu_write_streams')
WHERE lower(name) LIKE '%descriptor%' OR lower(name) LIKE '%proto%' OR lower(name) LIKE '%row_payload%'`).Scan(&forbiddenColumns); err != nil {
		t.Fatal(err)
	}
	if forbiddenColumns != 0 {
		t.Fatalf("Storage Write ledger exposes %d raw payload columns", forbiddenColumns)
	}
	var rawMatches int
	if err := repositories.db.QueryRowContext(ctx, `SELECT count(*) FROM bqemu_write_append_receipts
WHERE instr(schema_fingerprint, 'raw-protorow-sentinel') > 0
   OR instr(payload_digest, 'raw-protorow-sentinel') > 0`).Scan(&rawMatches); err != nil {
		t.Fatal(err)
	}
	if rawMatches != 0 {
		t.Fatal("raw ProtoRows were persisted")
	}
	if err := repositories.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	snapshot, err := restarted.WriteState().ReconcileStartup(ctx, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Streams) != 1 || len(snapshot.Receipts) != 1 {
		t.Fatalf("startup snapshot streams/receipts = %d/%d", len(snapshot.Streams), len(snapshot.Receipts))
	}
	reconciled := snapshot.Streams[0]
	if reconciled.Operation != domain.OperationAppend || reconciled.OperationPhase != domain.OperationPhaseUnresolved ||
		reconciled.Revision != prepared.Revision+1 || snapshot.Receipts[0].Phase != domain.ReceiptUnresolved {
		t.Fatalf("reconciled stream/receipt = %#v / %#v", reconciled, snapshot.Receipts[0])
	}
	amount := reconciled.Stream.Schema.Fields[0].Fields[0]
	if amount.Type != "BIGNUMERIC" || amount.Precision == nil || *amount.Precision != 38 ||
		amount.Scale == nil || *amount.Scale != 18 || amount.RoundingMode != catalogdomain.RoundingModeHalfEven {
		t.Fatalf("reconciled canonical decimal schema = %#v", amount)
	}
}

func TestStorageWriteAppendReceiptExactDuplicateCAS(t *testing.T) {
	ctx := context.Background()
	repositories, err := Open(ctx, filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer repositories.Close()
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	record := writeStreamRecord("receipt", now)
	if err := repositories.WriteState().CreateStream(ctx, record); err != nil {
		t.Fatal(err)
	}
	receipt := writeReceipt(record.Stream.Name, 0, "b", now)
	prepared := record
	prepared.Operation = domain.OperationAppend
	prepared.OperationPhase = domain.OperationPhasePrepared
	prepared.OperationToken = "0"
	prepared.Revision++
	if err := repositories.WriteState().PrepareAppend(ctx, record.Revision, prepared, receipt); err != nil {
		t.Fatal(err)
	}
	if err := repositories.WriteState().PrepareAppend(ctx, record.Revision, prepared, receipt); !errors.Is(err, ports.ErrExactReceipt) {
		t.Fatalf("exact duplicate receipt error = %v", err)
	}
	conflict := receipt
	conflict.PayloadDigest = "sha256:" + strings.Repeat("c", 64)
	if err := repositories.WriteState().PrepareAppend(ctx, record.Revision, prepared, conflict); !errors.Is(err, ports.ErrReceiptConflict) {
		t.Fatalf("conflicting duplicate receipt error = %v", err)
	}
	stored, err := repositories.WriteState().GetStream(ctx, record.Stream.Name)
	if err != nil || stored.Revision != prepared.Revision || stored.OperationPhase != domain.OperationPhasePrepared {
		t.Fatalf("stream after duplicate CAS = %#v, %v", stored, err)
	}
	var count int
	if err := repositories.db.QueryRowContext(ctx, `SELECT count(*) FROM bqemu_write_append_receipts
WHERE stream_name = ?`, record.Stream.Name).Scan(&count); err != nil || count != 1 {
		t.Fatalf("receipt count = %d, %v", count, err)
	}
}

func TestStorageWriteCommitPreparationRollsBackAndReconcilesExactGroup(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	repositories, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	base := []domain.StreamRecord{writeStreamRecord("commit-a", now), writeStreamRecord("commit-b", now)}
	for index := range base {
		base[index].Stream.State = domain.StreamStateFinalized
		if err := repositories.WriteState().CreateStream(ctx, base[index]); err != nil {
			t.Fatal(err)
		}
	}
	group, prepared, expected := writeCommitPreparation(base, now)
	badExpected := map[string]int64{base[0].Stream.Name: base[0].Revision, base[1].Stream.Name: 99}
	if err := repositories.WriteState().PrepareCommit(ctx, badExpected, prepared, group); !errors.Is(err, ports.ErrStateConflict) {
		t.Fatalf("invalid commit preparation error = %v", err)
	}
	var groups int
	if err := repositories.db.QueryRowContext(ctx, "SELECT count(*) FROM bqemu_write_commit_groups").Scan(&groups); err != nil || groups != 0 {
		t.Fatalf("rolled-back commit group count = %d, %v", groups, err)
	}
	for _, record := range base {
		stored, err := repositories.WriteState().GetStream(ctx, record.Stream.Name)
		if err != nil || stored.Operation != domain.OperationNone || stored.Revision != record.Revision {
			t.Fatalf("stream after rolled-back group = %#v, %v", stored, err)
		}
	}
	if err := repositories.WriteState().PrepareCommit(ctx, expected, prepared, group); err != nil {
		t.Fatal(err)
	}
	if err := repositories.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	snapshot, err := restarted.WriteState().ReconcileStartup(ctx, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.CommitGroups) != 1 || snapshot.CommitGroups[0].Phase != domain.CommitUnresolved ||
		snapshot.CommitGroups[0].ExpectedRowCount != 0 || len(snapshot.CommitGroups[0].Members) != 2 {
		t.Fatalf("reconciled commit group = %#v", snapshot.CommitGroups)
	}
	for _, record := range snapshot.Streams {
		if record.Operation != domain.OperationCommit || record.OperationPhase != domain.OperationPhaseUnresolved ||
			record.OperationToken != group.ID || record.Revision != 3 {
			t.Fatalf("reconciled commit stream = %#v", record)
		}
	}
}

func writeStreamRecord(id string, now time.Time) domain.StreamRecord {
	precision, scale := int64(38), int64(18)
	parent := domain.TableReference{ProjectID: "test-project", DatasetID: "analytics", TableID: "events"}
	return domain.StreamRecord{
		Stream: domain.WriteStream{
			Name: parent.Name() + "/streams/" + id, Parent: parent,
			Type: domain.StreamTypePending, State: domain.StreamStateOpen,
			CreateTime: now, Location: "US", LastActivity: now,
			Schema: domain.TableSchema{Fields: []domain.Field{{
				Name: "payload", Type: "STRUCT", Mode: "NULLABLE", Fields: []domain.Field{{
					Name: "amount", Type: "BIGNUMERIC", Mode: "NULLABLE",
					Precision: &precision, Scale: &scale, RoundingMode: catalogdomain.RoundingModeHalfEven,
				}},
			}}},
		},
		Operation: domain.OperationNone, OperationPhase: domain.OperationPhaseNone,
		CleanupPhase: domain.CleanupActive, Revision: 1,
	}
}

func writeReceipt(stream string, offset int64, digestByte string, now time.Time) domain.AppendReceipt {
	return domain.AppendReceipt{
		StreamName: stream, StartOffset: offset, RowCount: 1,
		SchemaFingerprint: "sha256:" + strings.Repeat("d", 64),
		PayloadDigest:     "sha256:" + strings.Repeat(digestByte, 64),
		Phase:             domain.ReceiptPrepared, CreatedAt: now, UpdatedAt: now,
	}
}

func writeCommitPreparation(base []domain.StreamRecord, now time.Time) (domain.CommitGroup, []domain.StreamRecord, map[string]int64) {
	group := domain.CommitGroup{
		ID: "sha256:" + strings.Repeat("e", 64), Parent: base[0].Stream.Parent,
		Phase: domain.CommitPrepared, CreatedAt: now, UpdatedAt: now,
		Members: make([]domain.CommitMember, len(base)),
	}
	prepared := make([]domain.StreamRecord, len(base))
	expected := make(map[string]int64, len(base))
	for index, record := range base {
		expected[record.Stream.Name] = record.Revision
		prepared[index] = record
		prepared[index].Operation = domain.OperationCommit
		prepared[index].OperationPhase = domain.OperationPhasePrepared
		prepared[index].OperationToken = group.ID
		prepared[index].Revision++
		group.Members[index] = domain.CommitMember{StreamName: record.Stream.Name, ExpectedRowCount: record.Stream.RowCount}
		group.ExpectedRowCount += record.Stream.RowCount
	}
	return group, prepared, expected
}
