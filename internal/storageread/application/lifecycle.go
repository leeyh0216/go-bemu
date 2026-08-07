package application

// Session expiry is server-owned and does not require a client close RPC.
// Source: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readsession

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/leeyh0216/go-bemu/internal/storageread/ports"
)

func (s *Service) SweepExpired(ctx context.Context) error {
	now := s.clock.Now()
	var expired []*sessionState
	s.mu.Lock()
	for name, state := range s.sessions {
		if now.Before(state.session.ExpireTime) {
			continue
		}
		delete(s.sessions, name)
		for _, stream := range state.session.Streams {
			delete(s.streams, stream.Name)
		}
		expired = append(expired, state)
	}
	s.mu.Unlock()
	return s.closeSnapshots(ctx, "storage_read.expire_sessions", expired)
}

func (s *Service) RunCleanup(ctx context.Context) error {
	ticker := time.NewTicker(s.config.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.SweepExpired(ctx); err != nil {
				s.logger.ErrorContext(ctx, "expired snapshot cleanup failed",
					"event", "side_effect.error", "side_effect", "snapshot.close",
					"operation", "storage_read.expire_sessions",
					"model_version", s.config.ProtocolModelVersion,
					"error_type", fmt.Sprintf("%T", err), "error_digest", digest([]byte(err.Error())),
				)
			}
		}
	}
}

func (s *Service) Close(ctx context.Context) error {
	s.mu.Lock()
	states := make([]*sessionState, 0, len(s.sessions))
	for _, state := range s.sessions {
		states = append(states, state)
	}
	s.sessions = make(map[string]*sessionState)
	s.streams = make(map[string]streamState)
	s.closed = true
	reservations := make([]*sessionReservation, 0, len(s.reservations))
	for _, reservation := range s.reservations {
		reservations = append(reservations, reservation)
	}
	s.mu.Unlock()
	for _, reservation := range reservations {
		// No reservation may commit once closed is visible. Return its byte and
		// session admission immediately, then wait below for the port call to
		// observe cancellation. A context-ignoring adapter cannot retain budget.
		s.releaseReservation(ctx, reservation, "service_shutdown")
	}
	closeErr := s.closeSnapshots(ctx, "storage_read.shutdown", states)
	waitErr := waitForReservations(ctx, reservations)
	return errors.Join(closeErr, waitErr)
}

func (s *Service) closeSnapshots(ctx context.Context, operation string, states []*sessionState) error {
	var result error
	for _, state := range states {
		state.mu.Lock()
		s.logger.InfoContext(ctx, "closing read snapshot",
			"event", "side_effect.before", "side_effect", "snapshot.close",
			"operation", operation, "model_version", s.config.ProtocolModelVersion,
			"session", state.session.Name,
		)
		err := state.snapshot.Close(ctx)
		state.mu.Unlock()
		budgetErr := s.releaseSnapshotBudget(ctx, operation, state)
		attrs := []any{
			"event", "side_effect.after", "side_effect", "snapshot.close",
			"operation", operation, "model_version", s.config.ProtocolModelVersion,
			"session", state.session.Name, "success", err == nil && budgetErr == nil,
		}
		if err != nil {
			attrs = append(attrs, "error_type", fmt.Sprintf("%T", err), "error_digest", digest([]byte(err.Error())))
		}
		if budgetErr != nil {
			attrs = append(attrs, "budget_error_type", fmt.Sprintf("%T", budgetErr), "budget_error_digest", digest([]byte(budgetErr.Error())))
		}
		s.logger.InfoContext(ctx, "read snapshot closed", attrs...)
		result = errors.Join(result, err, budgetErr)
	}
	return result
}

func (s *Service) closeUnstoredSnapshot(ctx context.Context, operation, reason string, snapshot ports.ReadSnapshot) error {
	s.logger.InfoContext(ctx, "closing unstored read snapshot",
		"event", "side_effect.before", "side_effect", "snapshot.close",
		"operation", operation, "model_version", s.config.ProtocolModelVersion,
		"reason", reason,
	)
	err := snapshot.Close(ctx)
	attrs := []any{
		"event", "side_effect.after", "side_effect", "snapshot.close",
		"operation", operation, "model_version", s.config.ProtocolModelVersion,
		"reason", reason, "success", err == nil,
	}
	if err != nil {
		attrs = append(attrs, "error_type", fmt.Sprintf("%T", err), "error_digest", digest([]byte(err.Error())))
	}
	s.logger.InfoContext(ctx, "unstored read snapshot closed", attrs...)
	return err
}
