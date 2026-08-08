package application

// Orphan cleanup models server-side disposal of uncommitted PENDING streams.
// BigQuery does not expose a delete-stream RPC; finalized-but-uncommitted data
// remains invisible and is eventually reclaimed by the service. BQEMU therefore
// keeps an application tombstone until its idempotent adapter discard succeeds.
// A failed google.rpc.Status must leave that tombstone retryable.
//
// Official PENDING lifecycle and status contracts:
//   - https://cloud.google.com/bigquery/docs/write-api-batch
//   - https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.WriteStream
//   - https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.rpc#status

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
)

func (s *Service) SweepOrphans(ctx context.Context) error {
	if err := s.ensureStateReconciled("storage_write.sweep_orphans"); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.cleanupGate:
	}
	defer func() { s.cleanupGate <- struct{}{} }()
	if err := ctx.Err(); err != nil {
		return err
	}

	cutoff := s.clock.Now().Add(-s.config.OrphanTTL)
	type orphan struct {
		name  string
		state *streamState
	}
	orphans := make([]orphan, 0)
	s.mu.Lock()
	for name, state := range s.streams {
		state.mu.Lock()
		eligible := state.stream.Type == domain.StreamTypePending && state.stream.State != domain.StreamStateCommitted &&
			state.operation == domain.OperationNone &&
			(state.cleanupPhase == cleanupPhasePending ||
				(state.cleanupPhase == cleanupPhaseActive && !state.stream.LastActivity.After(cutoff)))
		if eligible {
			orphans = append(orphans, orphan{name: name, state: state})
		}
		state.mu.Unlock()
	}
	s.mu.Unlock()
	sort.Slice(orphans, func(i, j int) bool { return orphans[i].name < orphans[j].name })

	var result error
	for _, item := range orphans {
		if err := ctx.Err(); err != nil {
			result = errors.Join(result, err)
			break
		}
		item.state.mu.Lock()
		stateBefore := item.state.cleanupPhase
		retryCount := item.state.cleanupAttempts
		prepared := streamRecord(item.state)
		prepared.CleanupPhase = domain.CleanupPending
		prepared.CleanupAttempts++
		prepared.Revision++
		if err := s.state.UpdateStream(ctx, item.state.revision, prepared); err != nil {
			item.state.mu.Unlock()
			result = errors.Join(result, fmt.Errorf("prepare orphan cleanup: %w", err))
			continue
		}
		item.state.cleanupPhase = cleanupPhasePending
		item.state.cleanupAttempts = prepared.CleanupAttempts
		item.state.revision = prepared.Revision
		item.state.mu.Unlock()
		if stateBefore == cleanupPhaseActive {
			s.logger.InfoContext(ctx, "pending write stream entered cleanup",
				"event", "domain.transition", "operation", "storage_write.sweep_orphans",
				"model_version", s.config.ProtocolModelVersion,
				"stream_fingerprint", digest([]byte(item.name)),
				"state_before", cleanupPhaseActive, "state_after", cleanupPhasePending,
				"retry_count", retryCount)
		}
		s.logger.InfoContext(ctx, "discarding orphaned write stream",
			"event", "side_effect.before", "side_effect", "coordinator.discard_pending",
			"operation", "storage_write.sweep_orphans", "model_version", s.config.ProtocolModelVersion,
			"stream_fingerprint", digest([]byte(item.name)),
			"state_before", cleanupPhasePending, "state_after", cleanupPhasePending,
			"retry_count", retryCount)
		err := s.coordinator.DiscardPending(ctx, item.name)
		stateAfter := cleanupPhasePending
		if err == nil {
			s.mu.Lock()
			item.state.mu.Lock()
			if s.streams[item.name] == item.state && item.state.cleanupPhase == cleanupPhasePending {
				if deleteErr := s.state.DeleteStream(ctx, item.name, item.state.revision); deleteErr != nil {
					err = deleteErr
				} else {
					item.state.cleanupPhase = cleanupPhaseDiscarded
					delete(s.streams, item.name)
					s.pending.Add(-1)
					stateAfter = cleanupPhaseDiscarded
				}
			}
			item.state.mu.Unlock()
			s.mu.Unlock()
		}
		attrs := []any{
			"event", "side_effect.after", "side_effect", "coordinator.discard_pending",
			"operation", "storage_write.sweep_orphans", "model_version", s.config.ProtocolModelVersion,
			"stream_fingerprint", digest([]byte(item.name)), "success", err == nil,
			"state_before", cleanupPhasePending, "state_after", stateAfter,
			"retry_count", retryCount,
		}
		if err != nil {
			attrs = append(attrs, errorLogAttrs(err)...)
			result = errors.Join(result, fmt.Errorf("discard pending stream %s: %w", digest([]byte(item.name)), err))
		}
		s.logger.InfoContext(ctx, "orphaned write stream discard completed", attrs...)
	}
	return result
}

func (s *Service) RunCleanup(ctx context.Context) error {
	ticker := time.NewTicker(s.config.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.SweepOrphans(ctx); err != nil {
				s.logger.ErrorContext(ctx, "Storage Write orphan sweep failed",
					"event", "side_effect.error", "operation", "storage_write.sweep_orphans",
					"model_version", s.config.ProtocolModelVersion,
					"error_type", fmt.Sprintf("%T", err), "error_digest", digest([]byte(err.Error())))
			}
		}
	}
}

func (s *Service) Close(ctx context.Context) error {
	if err := s.ensureStateReconciled("storage_write.close"); err != nil {
		return err
	}
	var persistErr error
	s.mu.Lock()
	s.closed = true
	for _, state := range s.streams {
		state.mu.Lock()
		if state.stream.Type == domain.StreamTypePending && state.stream.State != domain.StreamStateCommitted &&
			state.operation == domain.OperationNone {
			updated := streamRecord(state)
			updated.Stream.LastActivity = time.Unix(0, 1).UTC()
			updated.Revision++
			if err := s.state.UpdateStream(ctx, state.revision, updated); err != nil {
				persistErr = errors.Join(persistErr, err)
			} else {
				state.stream.LastActivity = updated.Stream.LastActivity
				state.revision = updated.Revision
			}
		}
		state.mu.Unlock()
	}
	s.mu.Unlock()
	return errors.Join(persistErr, s.SweepOrphans(ctx))
}
