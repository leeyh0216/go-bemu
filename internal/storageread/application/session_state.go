package application

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/leeyh0216/go-bemu/internal/storageread/domain"
	"github.com/leeyh0216/go-bemu/internal/storageread/ports"
)

type Option func(*Service) error

func WithSessionStateRepository(repository ports.SessionStateRepository) Option {
	return func(service *Service) error {
		if repository == nil {
			return fmt.Errorf("Storage Read session state repository must not be nil")
		}
		service.stateRepository = repository
		service.stateReconciled = false
		return nil
	}
}

// ReconcilePersistedSessions terminates sessions left ACTIVE by an older
// process before this service admits a new request. Their snapshots cannot be
// reconstructed from lifecycle metadata alone.
func (s *Service) ReconcilePersistedSessions(ctx context.Context) error {
	const operation = "storage_read.reconcile_sessions"
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("Storage Read service is closed"))
	}
	if s.stateReconciled {
		return nil
	}
	now := s.clock.Now()
	count, err := s.stateRepository.ReconcileActive(ctx, now)
	if err != nil {
		return s.classifyStateFailure(operation, err)
	}
	s.stateReconciled = true
	s.logger.InfoContext(ctx, "reconciled persisted read sessions",
		"event", "domain.transition", "operation", operation,
		"model_version", s.config.ProtocolModelVersion,
		"reconciled_session_count", count, "reconciled_at", now,
	)
	return nil
}

func (s *Service) persistSession(ctx context.Context, record domain.SessionRecord) error {
	if err := s.stateRepository.CreateSession(ctx, cloneSessionRecord(record)); err != nil {
		return s.classifyStateFailure("storage_read.persist_session", err)
	}
	return nil
}

func (s *Service) transitionSessions(ctx context.Context, names []string, to domain.SessionLifecycle, at time.Time) error {
	if len(names) == 0 {
		return nil
	}
	if err := s.stateRepository.TransitionSessions(ctx, slices.Clone(names), domain.SessionActive, to, at); err != nil {
		return s.classifyStateFailure("storage_read.transition_sessions", err)
	}
	return nil
}

func (s *Service) persistedStreamError(ctx context.Context, streamName string) error {
	const operation = "storage_read.read_rows"
	stream, err := s.stateRepository.GetStream(ctx, streamName)
	if errors.Is(err, ports.ErrSessionStateNotFound) {
		return domain.NewError(domain.ErrorNotFound, operation, err)
	}
	if err != nil {
		return s.classifyStateFailure(operation, err)
	}
	switch stream.Lifecycle {
	case domain.SessionExpired:
		return domain.NewError(domain.ErrorNotFound, operation, errors.New("read session expired"))
	case domain.SessionActive, domain.SessionUnavailable:
		return domain.NewError(domain.ErrorUnavailable, operation, errors.New("read session snapshot is unavailable"))
	default:
		return domain.NewError(domain.ErrorInternal, operation, errors.New("persisted read session has an invalid lifecycle"))
	}
}

func (s *Service) classifyStateFailure(operation string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return domain.NewError(contextErrorCode(err), operation, err)
	}
	if errors.Is(err, ports.ErrSessionStateConflict) {
		return domain.NewError(domain.ErrorFailedPrecondition, operation, err)
	}
	return domain.NewError(domain.ErrorUnavailable, operation, err)
}

func cloneSessionRecord(record domain.SessionRecord) domain.SessionRecord {
	record.SelectedFields = slices.Clone(record.SelectedFields)
	record.Streams = slices.Clone(record.Streams)
	record.SnapshotTime = cloneTime(record.SnapshotTime)
	return record
}

type transientSessionStateRepository struct{}

func (transientSessionStateRepository) CreateSession(context.Context, domain.SessionRecord) error {
	return nil
}
func (transientSessionStateRepository) TransitionSessions(context.Context, []string, domain.SessionLifecycle, domain.SessionLifecycle, time.Time) error {
	return nil
}
func (transientSessionStateRepository) ReconcileActive(context.Context, time.Time) (int64, error) {
	return 0, nil
}
func (transientSessionStateRepository) GetStream(context.Context, string) (domain.PersistedStream, error) {
	return domain.PersistedStream{}, ports.ErrSessionStateNotFound
}

var _ ports.SessionStateRepository = transientSessionStateRepository{}
