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
	config       Config
	materializer ports.SnapshotMaterializer
	clock        ports.Clock
	ids          ports.IDGenerator
	logger       *slog.Logger

	mu       sync.RWMutex
	sessions map[string]*sessionState
	streams  map[string]streamState
	closed   bool
}

type sessionState struct {
	mu       sync.RWMutex
	session  domain.Session
	snapshot ports.ReadSnapshot
}

type streamState struct {
	session *sessionState
	stream  domain.Stream
}

func New(config Config, materializer ports.SnapshotMaterializer, clock ports.Clock, ids ports.IDGenerator, logger *slog.Logger) (*Service, error) {
	if materializer == nil || clock == nil || ids == nil || logger == nil {
		return nil, fmt.Errorf("storage read dependencies must not be nil")
	}
	if err := validateConfig(&config); err != nil {
		return nil, err
	}
	return &Service{
		config:       config,
		materializer: materializer,
		clock:        clock,
		ids:          ids,
		logger:       logger,
		sessions:     make(map[string]*sessionState),
		streams:      make(map[string]streamState),
	}, nil
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
	if err := s.admissionError(operation); err != nil {
		return domain.Session{}, err
	}

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
	snapshot, err := s.materializer.Materialize(ctx, materializeRequest)
	if err != nil {
		s.logger.ErrorContext(ctx, "read snapshot materialization failed",
			"event", "side_effect.error", "side_effect", "snapshot.materialize",
			"operation", operation, "model_version", s.config.ProtocolModelVersion,
			"error_type", fmt.Sprintf("%T", err), "error_digest", digest([]byte(err.Error())),
		)
		return domain.Session{}, domain.NewError(domain.ErrorInternal, operation, err)
	}
	if snapshot == nil {
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
	s.logger.InfoContext(ctx, "read snapshot materialized",
		"event", "side_effect.after", "side_effect", "snapshot.materialize",
		"operation", operation, "model_version", s.config.ProtocolModelVersion,
		"format", metadata.Schema.Format.String(), "row_count", metadata.RowCount,
		"estimated_bytes", metadata.EstimatedBytes,
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
	state := &sessionState{session: session, snapshot: snapshot}

	s.mu.Lock()
	closed := s.closed
	atCapacity := len(s.sessions) >= s.config.MaxSessions
	if closed || atCapacity {
		s.mu.Unlock()
		reason := "capacity_race"
		code := domain.ErrorResourceExhausted
		cause := errors.New("session capacity reached")
		if closed {
			reason = "service_closed"
			code = domain.ErrorFailedPrecondition
			cause = errors.New("Storage Read service is closed")
		}
		closeErr := s.closeUnstoredSnapshot(ctx, operation, reason, snapshot)
		return domain.Session{}, domain.NewError(code, operation, errors.Join(cause, closeErr))
	}
	s.sessions[session.Name] = state
	for _, stream := range streams {
		s.streams[stream.Name] = streamState{session: state, stream: stream}
	}
	s.mu.Unlock()
	s.logger.InfoContext(ctx, "read session stored",
		"event", "domain.transition", "operation", operation,
		"model_version", s.config.ProtocolModelVersion, "session", session.Name,
		"stream_count", len(streams), "row_count", metadata.RowCount,
		"expires_at", session.ExpireTime,
	)
	return cloneSession(session), nil
}
