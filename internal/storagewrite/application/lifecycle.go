package application

// Orphan cleanup models server-side disposal of uncommitted PENDING streams.
// BigQuery does not expose a delete-stream RPC; finalized-but-uncommitted data
// remains invisible and is eventually reclaimed by the service.
// Source: https://cloud.google.com/bigquery/docs/write-api-streaming

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
)

func (s *Service) SweepOrphans(ctx context.Context) error {
	cutoff := s.clock.Now().Add(-s.config.OrphanTTL)
	type orphan struct {
		name  string
		state *streamState
	}
	orphans := make([]orphan, 0)
	s.mu.Lock()
	for name, state := range s.streams {
		state.mu.Lock()
		eligible := state.stream.Type == domain.StreamTypePending &&
			state.stream.State != domain.StreamStateCommitted &&
			!state.stream.LastActivity.After(cutoff)
		if eligible {
			state.orphaned = true
			delete(s.streams, name)
			s.pending.Add(-1)
			orphans = append(orphans, orphan{name: name, state: state})
		}
		state.mu.Unlock()
	}
	s.mu.Unlock()

	var result error
	for _, item := range orphans {
		s.logger.InfoContext(ctx, "discarding orphaned write stream",
			"event", "side_effect.before", "side_effect", "coordinator.discard_pending",
			"operation", "storage_write.sweep_orphans", "model_version", s.config.ProtocolModelVersion,
			"stream", item.name, "tx_state", "discarding")
		err := s.coordinator.DiscardPending(ctx, item.name)
		attrs := []any{
			"event", "side_effect.after", "side_effect", "coordinator.discard_pending",
			"operation", "storage_write.sweep_orphans", "model_version", s.config.ProtocolModelVersion,
			"stream", item.name, "success", err == nil, "tx_state", "discarded",
		}
		if err != nil {
			attrs = append(attrs, errorLogAttrs(err)...)
			result = errors.Join(result, fmt.Errorf("discard %s: %w", item.name, err))
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
	s.mu.Lock()
	s.closed = true
	for _, state := range s.streams {
		state.mu.Lock()
		if state.stream.Type == domain.StreamTypePending && state.stream.State != domain.StreamStateCommitted {
			state.stream.LastActivity = time.Time{}
		}
		state.mu.Unlock()
	}
	s.mu.Unlock()
	return s.SweepOrphans(ctx)
}
