package application

// Session creation follows CreateReadSession while encoded bytes remain behind
// the snapshot port.
//
// Protocol sources:
//   - CreateReadSession: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#createreadsessionrequest
//   - Stream count negotiation: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#createreadsessionrequest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"

	"github.com/leeyh0216/go-bemu/internal/storageread/domain"
	"github.com/leeyh0216/go-bemu/internal/storageread/ports"
)

type Service struct {
	config          Config
	materializer    ports.SnapshotMaterializer
	clock           ports.Clock
	ids             ports.IDGenerator
	logger          *slog.Logger
	stateRepository ports.SessionStateRepository

	mu                    sync.RWMutex
	sessions              map[string]*sessionState
	streams               map[string]streamState
	reservations          map[uint64]*sessionReservation
	nextReservationID     uint64
	retainedSnapshotBytes int64
	closed                bool
	stateReconciled       bool
}

type sessionState struct {
	mu            sync.RWMutex
	session       domain.Session
	snapshot      ports.ReadSnapshot
	retainedBytes int64
	record        domain.SessionRecord
}

type streamState struct {
	session *sessionState
	stream  domain.Stream
}

func New(config Config, materializer ports.SnapshotMaterializer, clock ports.Clock, ids ports.IDGenerator, logger *slog.Logger, options ...Option) (*Service, error) {
	if materializer == nil || clock == nil || ids == nil || logger == nil {
		return nil, fmt.Errorf("storage read dependencies must not be nil")
	}
	if err := validateConfig(&config); err != nil {
		return nil, err
	}
	service := &Service{
		config:          config,
		materializer:    materializer,
		clock:           clock,
		ids:             ids,
		logger:          logger,
		stateRepository: transientSessionStateRepository{},
		stateReconciled: true,
		sessions:        make(map[string]*sessionState),
		streams:         make(map[string]streamState),
		reservations:    make(map[uint64]*sessionReservation),
	}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("Storage Read option must not be nil")
		}
		if err := option(service); err != nil {
			return nil, err
		}
	}
	return service, nil
}

func (s *Service) CreateSession(ctx context.Context, request domain.CreateSessionRequest) (domain.Session, error) {
	const operation = "storage_read.create_session"
	if err := validateCreateRequest(request); err != nil {
		return domain.Session{}, domain.NewError(domain.ErrorInvalidArgument, operation, err)
	}
	streamCount, err := s.negotiateStreamCount(request.MaxStreamCount, request.PreferredMinStreamCount)
	if err != nil {
		return domain.Session{}, domain.NewError(domain.ErrorInvalidArgument, operation, err)
	}
	reservation, materializeContext, err := s.reserveSession(ctx, operation)
	if err != nil {
		return domain.Session{}, err
	}
	// The creator owns this completion signal. Shutdown can return admission
	// reservations immediately while still waiting, within its context, for an
	// outbound materializer to observe cancellation and leave the port call.
	defer close(reservation.done)
	reservationReason := "materialization_failed"
	committed := false
	defer func() {
		if !committed {
			s.releaseReservation(context.WithoutCancel(ctx), reservation, reservationReason)
		}
	}()

	materializeRequest := domain.MaterializeRequest{
		Table:          request.Table,
		Format:         request.Format,
		SelectedFields: slices.Clone(request.SelectedFields),
		RowRestriction: request.RowRestriction,
		SnapshotTime:   cloneTime(request.SnapshotTime),
	}
	s.logger.InfoContext(ctx, "materializing read snapshot",
		"event", "side_effect.before", "side_effect", "snapshot.materialize",
		"operation", operation, "model_version", s.config.ProtocolModelVersion,
		"table", request.Table, "format", request.Format.String(),
		"selected_field_count", len(request.SelectedFields),
		"row_restriction_bytes", len(request.RowRestriction),
		"row_restriction_digest", digest([]byte(request.RowRestriction)),
	)
	snapshot, err := s.materializer.Materialize(materializeContext, materializeRequest)
	if err != nil {
		s.logger.ErrorContext(ctx, "read snapshot materialization failed",
			"event", "side_effect.error", "side_effect", "snapshot.materialize",
			"operation", operation, "model_version", s.config.ProtocolModelVersion,
			"error_type", fmt.Sprintf("%T", err), "error_digest", digest([]byte(err.Error())),
		)
		return domain.Session{}, s.classifyMaterializationFailure(ctx, operation, err)
	}
	if snapshot == nil {
		reservationReason = "nil_snapshot"
		err := errors.New("materializer returned a nil snapshot")
		s.logger.ErrorContext(ctx, "read snapshot materialization returned no snapshot",
			"event", "side_effect.error", "side_effect", "snapshot.materialize",
			"operation", operation, "model_version", s.config.ProtocolModelVersion,
			"error_type", fmt.Sprintf("%T", err), "error_digest", digest([]byte(err.Error())),
		)
		return domain.Session{}, domain.NewError(domain.ErrorInternal, operation, err)
	}
	metadata := snapshot.Metadata()
	if err := validateSnapshotMetadata(request.Format, &metadata); err != nil {
		reservationReason = "invalid_metadata"
		s.logger.ErrorContext(ctx, "read snapshot metadata violated the port contract",
			"event", "side_effect.error", "side_effect", "snapshot.materialize",
			"operation", operation, "model_version", s.config.ProtocolModelVersion,
			"format", request.Format.String(), "schema_bytes", len(metadata.Schema.Serialized),
			"schema_fingerprint", digest(metadata.Schema.Serialized),
			"error_type", fmt.Sprintf("%T", err), "error_digest", digest([]byte(err.Error())),
		)
		closeErr := s.closeUnstoredSnapshot(ctx, operation, "invalid_metadata", snapshot)
		return domain.Session{}, domain.NewError(domain.ErrorInternal, operation, errors.Join(err, closeErr))
	}
	if metadata.RetainedBytes > s.config.MaxSnapshotBytes {
		reservationReason = "snapshot_byte_limit"
		err := fmt.Errorf("snapshot retained bytes %d exceed configured per-session limit %d", metadata.RetainedBytes, s.config.MaxSnapshotBytes)
		closeErr := s.closeUnstoredSnapshot(ctx, operation, reservationReason, snapshot)
		return domain.Session{}, domain.NewError(domain.ErrorResourceExhausted, operation, errors.Join(err, closeErr))
	}
	s.logger.InfoContext(ctx, "read snapshot materialized",
		"event", "side_effect.after", "side_effect", "snapshot.materialize",
		"operation", operation, "model_version", s.config.ProtocolModelVersion,
		"format", metadata.Schema.Format.String(), "row_count", metadata.RowCount,
		"estimated_bytes", metadata.EstimatedBytes, "retained_bytes", metadata.RetainedBytes,
		"schema_bytes", len(metadata.Schema.Serialized),
		"schema_fingerprint", metadata.Schema.Fingerprint,
	)

	sessionID := s.ids.NewID()
	if !validResourceSegment(sessionID) {
		closeErr := s.closeUnstoredSnapshot(ctx, operation, "invalid_session_id", snapshot)
		return domain.Session{}, domain.NewError(domain.ErrorInternal, operation, errors.Join(errors.New("ID generator returned an invalid resource segment"), closeErr))
	}
	project := strings.TrimPrefix(request.Parent, "projects/")
	sessionName := fmt.Sprintf("projects/%s/locations/%s/sessions/%s", project, s.config.Location, sessionID)
	streams := partitionStreams(sessionName, metadata.RowCount, streamCount)
	now := s.clock.Now()
	session := domain.Session{
		Name:                  sessionName,
		Table:                 request.Table,
		Format:                request.Format,
		Schema:                cloneSchema(metadata.Schema),
		Streams:               slices.Clone(streams),
		ExpireTime:            now.Add(s.config.SessionTTL),
		EstimatedRowCount:     metadata.RowCount,
		EstimatedBytesScanned: metadata.EstimatedBytes,
		SelectedFields:        slices.Clone(request.SelectedFields),
		RowRestriction:        request.RowRestriction,
		SnapshotTime:          cloneTime(request.SnapshotTime),
		TraceID:               request.TraceID,
	}
	canonicalSelectedFields := slices.Clone(metadata.SelectedFields)
	if canonicalSelectedFields == nil {
		canonicalSelectedFields = slices.Clone(request.SelectedFields)
	}
	record := domain.SessionRecord{
		Name: session.Name, Table: session.Table, Format: session.Format,
		SelectedFields:       canonicalSelectedFields,
		RowRestrictionDigest: digest([]byte(request.RowRestriction)),
		RowRestrictionBytes:  len(request.RowRestriction), FilterShape: metadata.FilterShape,
		Streams: slices.Clone(streams), CreatedAt: now, ExpireTime: session.ExpireTime,
		SnapshotTime: cloneTime(request.SnapshotTime), RetainedRowCount: metadata.RowCount,
		RetainedBytes: metadata.RetainedBytes, EstimatedBytesScanned: metadata.EstimatedBytes,
		SchemaFingerprint: metadata.Schema.Fingerprint, Lifecycle: domain.SessionActive,
		LifecycleUpdatedAt: now,
	}
	state := &sessionState{session: session, snapshot: snapshot, retainedBytes: metadata.RetainedBytes, record: record}
	if err := s.commitReservedSession(ctx, operation, reservation, state); err != nil {
		reservationReason = "commit_rejected"
		closeErr := s.closeUnstoredSnapshot(ctx, operation, reservationReason, snapshot)
		return domain.Session{}, domain.NewError(domain.CodeOf(err), operation, errors.Join(err, closeErr))
	}
	committed = true
	s.logger.InfoContext(ctx, "read session stored",
		"event", "domain.transition", "operation", operation,
		"model_version", s.config.ProtocolModelVersion, "session", session.Name,
		"stream_count", len(streams), "row_count", metadata.RowCount,
		"expires_at", session.ExpireTime,
	)
	return cloneSession(session), nil
}

// classifyMaterializationFailure separates caller cancellation from the
// service-owned cancellation used by Close. Both status categories are stable
// public protocol outcomes; the adapter cause remains available only through
// errors.Is/internal digests and is never rendered by domain.Error.Error.
// Source: https://grpc.io/docs/guides/status-codes/
func (s *Service) classifyMaterializationFailure(ctx context.Context, operation string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		if callerErr := ctx.Err(); callerErr != nil {
			return domain.NewError(contextErrorCode(callerErr), operation, callerErr)
		}
		s.mu.RLock()
		closed := s.closed
		s.mu.RUnlock()
		if closed {
			return domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("Storage Read service closed during materialization"))
		}
	}
	// Outbound adapters classify request, capability, and lookup failures.
	// Preserve that category across the application boundary while replacing
	// the adapter operation with this public use-case operation.
	var classified *domain.Error
	if errors.As(err, &classified) {
		return domain.NewError(classified.Code, operation, err)
	}
	return domain.NewError(domain.ErrorInternal, operation, err)
}
