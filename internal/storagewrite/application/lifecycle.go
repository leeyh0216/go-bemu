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
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
)

func (s *Service) SweepOrphans(ctx context.Context) error {
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
	records, err := s.repository.ListWriteStreams(ctx)
	if err != nil {
		return fmt.Errorf("list Storage Write streams for orphan cleanup: %w", err)
	}
	orphans := make([]domain.StreamRecord, 0)
	for _, record := range records {
		eligible := record.Stream.Type == domain.StreamTypePending && record.Stream.State != domain.StreamStateCommitted &&
			(record.CleanupState == domain.CleanupStatePending ||
				(record.CleanupState == domain.CleanupStateActive && !record.Stream.LastActivity.After(cutoff)))
		if eligible {
			orphans = append(orphans, record)
		}
	}
	sort.Slice(orphans, func(i, j int) bool { return orphans[i].Stream.Name < orphans[j].Stream.Name })

	var result error
	for _, record := range orphans {
		if err := ctx.Err(); err != nil {
			result = errors.Join(result, err)
			break
		}
		stateBefore := record.CleanupState
		retryCount := record.CleanupAttempts
		expected := record.Revision
		record.CleanupState = domain.CleanupStatePending
		record.CleanupAttempts++
		record.Revision++
		if err := s.repository.SaveWriteStream(ctx, expected, record); err != nil {
			result = errors.Join(result, fmt.Errorf("prepare orphan cleanup: %w", err))
			continue
		}
		if stateBefore == domain.CleanupStateActive {
			s.logger.InfoContext(ctx, "pending write stream entered cleanup",
				"event", "domain.transition", "operation", "storage_write.sweep_orphans",
				"model_version", s.config.ProtocolModelVersion,
				"stream_fingerprint", digest([]byte(record.Stream.Name)),
				"state_before", cleanupStateLogValue(domain.CleanupStateActive), "state_after", cleanupStateLogValue(domain.CleanupStatePending),
				"retry_count", retryCount)
		}
		s.logger.InfoContext(ctx, "discarding orphaned write stream",
			"event", "side_effect.before", "side_effect", "coordinator.discard_pending",
			"operation", "storage_write.sweep_orphans", "model_version", s.config.ProtocolModelVersion,
			"stream_fingerprint", digest([]byte(record.Stream.Name)),
			"state_before", cleanupStateLogValue(domain.CleanupStatePending), "state_after", cleanupStateLogValue(domain.CleanupStatePending),
			"retry_count", retryCount)
		err := s.coordinator.DiscardPending(ctx, record.Stream.Name)
		stateAfter := cleanupStateLogValue(domain.CleanupStatePending)
		if err == nil {
			deleteErr := s.repository.DeleteWriteStream(ctx, record.Stream.Name, record.Revision)
			if deleteErr != nil {
				err = fmt.Errorf("delete discarded stream metadata: %w", deleteErr)
			} else {
				stateAfter = "discarded"
			}
		}
		attrs := []any{
			"event", "side_effect.after", "side_effect", "coordinator.discard_pending",
			"operation", "storage_write.sweep_orphans", "model_version", s.config.ProtocolModelVersion,
			"stream_fingerprint", digest([]byte(record.Stream.Name)), "success", err == nil,
			"state_before", cleanupStateLogValue(domain.CleanupStatePending), "state_after", stateAfter,
			"retry_count", retryCount,
		}
		if err != nil {
			attrs = append(attrs, errorLogAttrs(err)...)
			result = errors.Join(result, fmt.Errorf("discard pending stream %s: %w", digest([]byte(record.Stream.Name)), err))
		}
		s.logger.InfoContext(ctx, "orphaned write stream discard completed", attrs...)
	}
	return result
}

func cleanupStateLogValue(state domain.CleanupState) string {
	switch state {
	case domain.CleanupStateActive:
		return "active"
	case domain.CleanupStatePending:
		return "cleanup_pending"
	default:
		return strings.ToLower(string(state))
	}
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
	if err := ctx.Err(); err != nil {
		return err
	}
	s.closed.Store(true)
	return nil
}
