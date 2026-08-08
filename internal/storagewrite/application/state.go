package application

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
	"github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
)

type Option func(*Service) error

func WithStateRepository(repository ports.StateRepository) Option {
	return func(service *Service) error {
		if repository == nil {
			return errors.New("Storage Write state repository must not be nil")
		}
		service.state = repository
		service.stateReconciled = false
		return nil
	}
}

// ReconcilePersistedState classifies every prior PREPARED side effect before
// installing the durable ledger in memory. No request or TTL sweep is admitted
// until this method succeeds.
func (s *Service) ReconcilePersistedState(ctx context.Context) error {
	const operation = "storage_write.reconcile_state"
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("Storage Write service is closed"))
	}
	if s.stateReconciled {
		return nil
	}
	if len(s.streams) != 0 {
		return domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("Storage Write state reconciliation must run before requests"))
	}
	snapshot, err := s.state.ReconcileStartup(ctx, s.clock.Now())
	if err != nil {
		return s.classifyStateError(operation, err)
	}
	receipts := make(map[string]domain.AppendReceipt)
	for _, receipt := range snapshot.Receipts {
		if receipt.Phase == domain.ReceiptUnresolved {
			if _, duplicate := receipts[receipt.StreamName]; duplicate {
				return domain.NewError(domain.ErrorInternal, operation, errors.New("multiple unresolved receipts for one stream"))
			}
			receipts[receipt.StreamName] = receipt
		}
	}
	for _, group := range snapshot.CommitGroups {
		s.commitGroups[group.ID] = cloneCommitGroup(group)
	}
	for _, record := range snapshot.Streams {
		state := &streamState{
			stream: cloneStream(record.Stream), cleanupPhase: cleanupPhaseFromRecord(record.CleanupPhase),
			cleanupAttempts: record.CleanupAttempts, operation: record.Operation,
			operationPhase: record.OperationPhase, operationToken: record.OperationToken,
			revision: record.Revision,
		}
		if record.Operation == domain.OperationAppend {
			receipt, exists := receipts[record.Stream.Name]
			offset, parseErr := strconv.ParseInt(record.OperationToken, 10, 64)
			if !exists || parseErr != nil || receipt.StartOffset != offset {
				return domain.NewError(domain.ErrorInternal, operation, errors.New("unresolved append receipt is inconsistent"))
			}
			state.unacknowledged = &appendReceipt{
				startOffset: receipt.StartOffset, rowCount: receipt.RowCount,
				schemaFingerprint: receipt.SchemaFingerprint, payloadDigest: receipt.PayloadDigest,
				phase: receipt.Phase, createdAt: receipt.CreatedAt, updatedAt: receipt.UpdatedAt,
			}
		}
		s.streams[record.Stream.Name] = state
		if record.Stream.Type == domain.StreamTypePending && record.Stream.State != domain.StreamStateCommitted {
			s.pending.Add(1)
		}
	}
	s.stateReconciled = true
	s.logger.InfoContext(ctx, "reconciled persisted write state",
		"event", "domain.transition", "operation", operation,
		"model_version", s.config.ProtocolModelVersion,
		"stream_count", len(snapshot.Streams), "receipt_count", len(snapshot.Receipts),
		"commit_group_count", len(snapshot.CommitGroups))
	return nil
}

func (s *Service) ensureStateReconciled(operation string) error {
	s.mu.RLock()
	reconciled := s.stateReconciled
	s.mu.RUnlock()
	if !reconciled {
		return domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("Storage Write startup reconciliation is incomplete"))
	}
	return nil
}

func (s *Service) classifyStateError(operation string, err error) error {
	if errors.Is(err, context.Canceled) {
		return domain.NewError(domain.ErrorCanceled, operation, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return domain.NewError(domain.ErrorDeadlineExceeded, operation, err)
	}
	if errors.Is(err, ports.ErrStateConflict) || errors.Is(err, ports.ErrReceiptConflict) ||
		errors.Is(err, ports.ErrCommitGroupConflict) {
		return domain.NewError(domain.ErrorFailedPrecondition, operation, err)
	}
	return domain.NewError(domain.ErrorInternal, operation, err)
}

func streamRecord(state *streamState) domain.StreamRecord {
	return domain.StreamRecord{
		Stream: cloneStream(state.stream), Operation: state.operation,
		OperationPhase: state.operationPhase, OperationToken: state.operationToken,
		CleanupPhase:    cleanupPhaseRecordValue(state.cleanupPhase),
		CleanupAttempts: state.cleanupAttempts, Revision: state.revision,
	}
}

func appendReceiptRecord(stream string, receipt appendReceipt, phase domain.ReceiptPhase, createdAt, updatedAt time.Time) domain.AppendReceipt {
	return domain.AppendReceipt{
		StreamName: stream, StartOffset: receipt.startOffset, RowCount: receipt.rowCount,
		SchemaFingerprint: receipt.schemaFingerprint, PayloadDigest: receipt.payloadDigest,
		Phase: phase, CreatedAt: createdAt, UpdatedAt: updatedAt,
	}
}

func appendReceiptsMatch(left, right appendReceipt) bool {
	return left.startOffset == right.startOffset && left.rowCount == right.rowCount &&
		left.schemaFingerprint == right.schemaFingerprint && left.payloadDigest == right.payloadDigest
}

func cleanupPhaseRecordValue(phase cleanupPhase) domain.CleanupPhase {
	if phase == cleanupPhasePending {
		return domain.CleanupPending
	}
	return domain.CleanupActive
}

func cleanupPhaseFromRecord(phase domain.CleanupPhase) cleanupPhase {
	if phase == domain.CleanupPending {
		return cleanupPhasePending
	}
	return cleanupPhaseActive
}

func cloneCommitGroup(group domain.CommitGroup) domain.CommitGroup {
	group.Members = append([]domain.CommitMember(nil), group.Members...)
	group.CommitTime = cloneTime(group.CommitTime)
	return group
}

func commitGroupMatches(group domain.CommitGroup, parent domain.TableReference, names []string, rows map[string]int64) bool {
	if group.ID == "" || group.Parent != parent || len(group.Members) != len(names) {
		return false
	}
	var total int64
	for index, member := range group.Members {
		if member.StreamName != names[index] || member.ExpectedRowCount != rows[member.StreamName] {
			return false
		}
		total += member.ExpectedRowCount
	}
	return total == group.ExpectedRowCount
}

type transientStateRepository struct{}

func (transientStateRepository) ReconcileStartup(context.Context, time.Time) (domain.StartupSnapshot, error) {
	return domain.StartupSnapshot{}, nil
}
func (transientStateRepository) CreateStream(context.Context, domain.StreamRecord) error { return nil }
func (transientStateRepository) GetStream(context.Context, string) (domain.StreamRecord, error) {
	return domain.StreamRecord{}, ports.ErrStateNotFound
}
func (transientStateRepository) UpdateStream(context.Context, int64, domain.StreamRecord) error {
	return nil
}
func (transientStateRepository) DeleteStream(context.Context, string, int64) error { return nil }
func (transientStateRepository) PrepareAppend(context.Context, int64, domain.StreamRecord, domain.AppendReceipt) error {
	return nil
}
func (transientStateRepository) MarkAppendUnresolved(context.Context, int64, domain.StreamRecord, domain.AppendReceipt) error {
	return nil
}
func (transientStateRepository) CompleteAppend(context.Context, int64, domain.StreamRecord, domain.AppendReceipt) error {
	return nil
}
func (transientStateRepository) AbortAppend(context.Context, int64, domain.StreamRecord, domain.AppendReceipt) error {
	return nil
}
func (transientStateRepository) PrepareCommit(context.Context, map[string]int64, []domain.StreamRecord, domain.CommitGroup) error {
	return nil
}
func (transientStateRepository) MarkCommitUnresolved(context.Context, map[string]int64, []domain.StreamRecord, domain.CommitGroup) error {
	return nil
}
func (transientStateRepository) CompleteCommit(context.Context, map[string]int64, []domain.StreamRecord, domain.CommitGroup) error {
	return nil
}
func (transientStateRepository) AbortCommit(context.Context, map[string]int64, []domain.StreamRecord, domain.CommitGroup) error {
	return nil
}

var _ ports.StateRepository = transientStateRepository{}
