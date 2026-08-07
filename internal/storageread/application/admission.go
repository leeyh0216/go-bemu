package application

// CreateReadSession may materialize a large immutable snapshot before the
// session becomes visible. Admission therefore reserves both a logical session
// slot and the configured per-session byte ceiling before crossing the
// SnapshotMaterializer port. A successful materialization settles that ceiling
// to Metadata().RetainedBytes; failure, expiry, and shutdown return it.
//
// Official contracts:
//   - sessions and streams: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#createreadsessionrequest
//   - automatic session expiry: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readsession
//   - Storage Read quotas: https://cloud.google.com/bigquery/quotas#storage_read_api_limits
//   - RESOURCE_EXHAUSTED: https://grpc.io/docs/guides/status-codes/

import (
	"context"
	"errors"
	"fmt"

	"github.com/leeyh0216/go-bemu/internal/storageread/domain"
)

type sessionReservation struct {
	id     uint64
	bytes  int64
	cancel context.CancelFunc
	done   chan struct{}
}

func (s *Service) reserveSession(ctx context.Context, operation string) (*sessionReservation, context.Context, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, domain.NewError(contextErrorCode(err), operation, err)
	}
	s.mu.Lock()
	sessionCount := len(s.sessions)
	inflightCount := len(s.reservations)
	retainedBefore := s.retainedSnapshotBytes
	reason := ""
	code := domainErrorCodeForAdmission("")
	if s.closed {
		reason = "service_closed"
		code = domainErrorCodeForAdmission(reason)
	} else if sessionCount+inflightCount >= s.config.MaxSessions {
		reason = "session_limit"
		code = domainErrorCodeForAdmission(reason)
	} else if s.config.MaxSnapshotBytes > s.config.MaxTotalSnapshotBytes-retainedBefore {
		reason = "snapshot_byte_budget"
		code = domainErrorCodeForAdmission(reason)
	}
	if reason != "" {
		s.mu.Unlock()
		s.logger.WarnContext(ctx, "read session admission rejected",
			"event", "domain.transition", "operation", operation,
			"model_version", s.config.ProtocolModelVersion,
			"state_from", "AVAILABLE", "state_to", "REJECTED", "reason", reason,
			"session_count", sessionCount, "materialization_count", inflightCount,
			"retained_snapshot_bytes", retainedBefore,
			"reservation_bytes", s.config.MaxSnapshotBytes,
			"max_sessions", s.config.MaxSessions,
			"max_total_snapshot_bytes", s.config.MaxTotalSnapshotBytes,
		)
		return nil, nil, domain.NewError(code, operation, errors.New(reason))
	}

	materializeContext, cancel := context.WithCancel(ctx)
	s.nextReservationID++
	reservation := &sessionReservation{
		id: s.nextReservationID, bytes: s.config.MaxSnapshotBytes,
		cancel: cancel, done: make(chan struct{}),
	}
	s.reservations[reservation.id] = reservation
	s.retainedSnapshotBytes += reservation.bytes
	retainedAfter := s.retainedSnapshotBytes
	inflightAfter := len(s.reservations)
	s.mu.Unlock()
	s.logger.InfoContext(ctx, "read session admission reserved",
		"event", "domain.transition", "operation", operation,
		"model_version", s.config.ProtocolModelVersion,
		"state_from", "AVAILABLE", "state_to", "RESERVED",
		"session_count", sessionCount, "materialization_count", inflightAfter,
		"retained_snapshot_bytes_before", retainedBefore,
		"retained_snapshot_bytes_after", retainedAfter,
		"reservation_bytes", reservation.bytes,
	)
	return reservation, materializeContext, nil
}

func (s *Service) commitReservedSession(ctx context.Context, operation string, reservation *sessionReservation, state *sessionState) error {
	s.mu.Lock()
	current, found := s.reservations[reservation.id]
	if !found {
		closed := s.closed
		s.mu.Unlock()
		if closed {
			return domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("Storage Read service closed before session commit"))
		}
		return domain.NewError(domain.ErrorInternal, operation, errors.New("session reservation was lost"))
	}
	if s.closed {
		s.mu.Unlock()
		return domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("Storage Read service is closed"))
	}
	if state.retainedBytes > current.bytes {
		s.mu.Unlock()
		return domain.NewError(domain.ErrorResourceExhausted, operation, errors.New("snapshot exceeds reserved byte limit"))
	}
	if _, duplicate := s.sessions[state.session.Name]; duplicate {
		s.mu.Unlock()
		return domain.NewError(domain.ErrorInternal, operation, errors.New("session ID already exists"))
	}
	retainedBefore := s.retainedSnapshotBytes
	s.retainedSnapshotBytes = retainedBefore - current.bytes + state.retainedBytes
	delete(s.reservations, current.id)
	s.sessions[state.session.Name] = state
	for _, stream := range state.session.Streams {
		s.streams[stream.Name] = streamState{session: state, stream: stream}
	}
	retainedAfter := s.retainedSnapshotBytes
	sessionCount := len(s.sessions)
	inflightCount := len(s.reservations)
	s.mu.Unlock()
	current.cancel()
	s.logger.InfoContext(ctx, "read session admission committed",
		"event", "domain.transition", "operation", operation,
		"model_version", s.config.ProtocolModelVersion,
		"state_from", "RESERVED", "state_to", "COMMITTED",
		"session_count", sessionCount, "materialization_count", inflightCount,
		"retained_snapshot_bytes_before", retainedBefore,
		"retained_snapshot_bytes_after", retainedAfter,
		"reserved_bytes", current.bytes, "committed_bytes", state.retainedBytes,
	)
	return nil
}

func (s *Service) releaseReservation(ctx context.Context, reservation *sessionReservation, reason string) {
	if reservation == nil {
		return
	}
	reservation.cancel()
	s.mu.Lock()
	current, found := s.reservations[reservation.id]
	if !found {
		s.mu.Unlock()
		return
	}
	retainedBefore := s.retainedSnapshotBytes
	if current.bytes > s.retainedSnapshotBytes {
		s.retainedSnapshotBytes = 0
	} else {
		s.retainedSnapshotBytes -= current.bytes
	}
	delete(s.reservations, current.id)
	retainedAfter := s.retainedSnapshotBytes
	inflightCount := len(s.reservations)
	s.mu.Unlock()
	s.logger.InfoContext(ctx, "read session admission released",
		"event", "domain.transition", "operation", "storage_read.release_reservation",
		"model_version", s.config.ProtocolModelVersion,
		"state_from", "RESERVED", "state_to", "RELEASED", "reason", reason,
		"materialization_count", inflightCount,
		"retained_snapshot_bytes_before", retainedBefore,
		"retained_snapshot_bytes_after", retainedAfter,
		"released_bytes", current.bytes,
	)
}

func (s *Service) releaseSnapshotBudget(ctx context.Context, operation string, state *sessionState) error {
	s.mu.Lock()
	retainedBefore := s.retainedSnapshotBytes
	releasedBytes := state.retainedBytes
	var invariantErr error
	if state.retainedBytes > s.retainedSnapshotBytes {
		invariantErr = fmt.Errorf("snapshot budget underflow: release %d from %d", state.retainedBytes, s.retainedSnapshotBytes)
		s.retainedSnapshotBytes = 0
	} else {
		s.retainedSnapshotBytes -= state.retainedBytes
	}
	retainedAfter := s.retainedSnapshotBytes
	state.retainedBytes = 0
	s.mu.Unlock()
	s.logger.InfoContext(ctx, "read snapshot budget released",
		"event", "domain.transition", "operation", operation,
		"model_version", s.config.ProtocolModelVersion,
		"state_from", "COMMITTED", "state_to", "RELEASED",
		"retained_snapshot_bytes_before", retainedBefore,
		"retained_snapshot_bytes_after", retainedAfter,
		"released_bytes", releasedBytes,
	)
	return invariantErr
}

func waitForReservations(ctx context.Context, reservations []*sessionReservation) error {
	for _, reservation := range reservations {
		select {
		case <-reservation.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func domainErrorCodeForAdmission(reason string) domain.ErrorCode {
	if reason == "service_closed" {
		return domain.ErrorFailedPrecondition
	}
	return domain.ErrorResourceExhausted
}

func contextErrorCode(err error) domain.ErrorCode {
	if errors.Is(err, context.DeadlineExceeded) {
		return domain.ErrorDeadlineExceeded
	}
	return domain.ErrorCanceled
}
