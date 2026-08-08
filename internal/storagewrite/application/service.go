package application

// Stream state transitions implement CreateWriteStream, AppendRows,
// FinalizeWriteStream, and BatchCommitWriteStreams as specified by the official
// Storage Write API. PENDING rows remain behind the coordinator port until one
// atomic commit succeeds; DEFAULT rows are visible once append is acknowledged.
//
// Official RPC contract:
// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite
// Spark 0.44.2 direct writer lifecycle:
// https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryDirectDataWriterHelper.java

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
	"github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
)

type Service struct {
	config      Config
	coordinator ports.Coordinator
	clock       ports.Clock
	ids         ports.IDGenerator
	logger      *slog.Logger

	mu      sync.RWMutex
	streams map[string]*streamState
	closed  bool
	pending atomic.Int64

	// cleanupGate serializes orphan disposal without making a caller wait past
	// its context deadline. This keeps DiscardPending exactly-once per sweep even
	// when the periodic cleaner and shutdown race.
	cleanupGate chan struct{}
}

type streamState struct {
	mu              sync.Mutex
	stream          domain.WriteStream
	descriptor      []byte
	unacknowledged  *appendReceipt
	cleanupPhase    cleanupPhase
	cleanupAttempts uint64
}

// appendReceipt identifies a PENDING append whose backend outcome may have
// succeeded although the acknowledgement was lost. Finalize must not cross
// this boundary until an identical retry reconciles the application ledger.
// See the official offset retry contract:
// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#appendrowsrequest
type appendReceipt struct {
	startOffset       int64
	rowCount          int64
	schemaFingerprint string
	payloadDigest     string
}

type cleanupPhase string

const (
	cleanupPhaseActive    cleanupPhase = "active"
	cleanupPhasePending   cleanupPhase = "cleanup_pending"
	cleanupPhaseDiscarded cleanupPhase = "discarded"
)

func New(config Config, coordinator ports.Coordinator, clock ports.Clock, ids ports.IDGenerator, logger *slog.Logger) (*Service, error) {
	if coordinator == nil || clock == nil || ids == nil || logger == nil {
		return nil, fmt.Errorf("storage write dependencies must not be nil")
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	service := &Service{
		config: config, coordinator: coordinator, clock: clock, ids: ids,
		logger: logger, streams: make(map[string]*streamState), cleanupGate: make(chan struct{}, 1),
	}
	service.cleanupGate <- struct{}{}
	return service, nil
}

// MaxConcurrentAppendRequests exposes only the transport admission contract,
// not the full application configuration. The gRPC adapter acquires this gate
// before Recv decodes and clones a potentially large AppendRowsRequest.
func (s *Service) MaxConcurrentAppendRequests() int {
	if s == nil {
		return 0
	}
	return s.config.MaxConcurrentAppendRequests
}

func (s *Service) CreateStream(ctx context.Context, request domain.CreateStreamRequest) (domain.WriteStream, error) {
	const operation = "storage_write.create_stream"
	if request.Type != domain.StreamTypePending {
		return domain.WriteStream{}, domain.NewError(domain.ErrorUnimplemented, operation, errors.New("only PENDING streams can be created"))
	}
	if _, err := domain.ParseTableName(request.Parent.Name()); err != nil {
		return domain.WriteStream{}, domain.NewError(domain.ErrorInvalidArgument, operation, err)
	}
	if err := s.admissionError(operation); err != nil {
		return domain.WriteStream{}, err
	}
	schema, err := s.describeTable(ctx, operation, request.Parent)
	if err != nil {
		return domain.WriteStream{}, err
	}
	now := s.clock.Now()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return domain.WriteStream{}, domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("Storage Write service is closed"))
	}
	if s.pending.Load() >= int64(s.config.MaxStreams) {
		return domain.WriteStream{}, domain.NewError(domain.ErrorResourceExhausted, operation, errors.New("logical stream capacity reached"))
	}
	var name string
	for attempts := 0; attempts < 4; attempts++ {
		id := s.ids.NewID()
		candidate := request.Parent.Name() + "/streams/" + id
		if _, _, isDefault, parseErr := domain.ParseStreamName(candidate); parseErr != nil || isDefault {
			continue
		}
		if _, exists := s.streams[candidate]; !exists {
			name = candidate
			break
		}
	}
	if name == "" {
		return domain.WriteStream{}, domain.NewError(domain.ErrorInternal, operation, errors.New("ID generator did not produce a unique valid stream ID"))
	}
	stream := domain.WriteStream{
		Name: name, Parent: request.Parent, Type: domain.StreamTypePending,
		State: domain.StreamStateOpen, CreateTime: now, LastActivity: now,
		Location: s.config.Location, Schema: cloneSchema(schema),
	}
	s.streams[name] = &streamState{stream: stream, cleanupPhase: cleanupPhaseActive}
	s.pending.Add(1)
	s.logger.InfoContext(ctx, "pending write stream created",
		"event", "domain.transition", "operation", operation,
		"model_version", s.config.ProtocolModelVersion, "stream_fingerprint", digest([]byte(name)),
		"table", request.Parent.Name(), "stream_type", stream.Type,
		"stream_count", s.pending.Load())
	return cloneStream(stream), nil
}

func (s *Service) GetStream(ctx context.Context, name string) (domain.WriteStream, error) {
	const operation = "storage_write.get_stream"
	table, canonical, isDefault, err := domain.ParseStreamName(name)
	if err != nil {
		return domain.WriteStream{}, domain.NewError(domain.ErrorInvalidArgument, operation, err)
	}
	state, err := s.lookupOrCreateDefault(ctx, operation, table, canonical, isDefault)
	if err != nil {
		return domain.WriteStream{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.cleanupPhase != cleanupPhaseActive {
		return domain.WriteStream{}, domain.NewError(domain.ErrorNotFound, operation, errors.New("write stream was discarded"))
	}
	return cloneStream(state.stream), nil
}

// Append enforces the exactly-once offset ledger before invoking a backend side
// effect. Duplicate offsets are ALREADY_EXISTS and gaps are OUT_OF_RANGE; in
// either case the ledger and staged backend rows remain unchanged.
func (s *Service) Append(ctx context.Context, request domain.AppendRequest) (domain.AppendResult, error) {
	const operation = "storage_write.append"
	table, canonical, isDefault, err := domain.ParseStreamName(request.StreamName)
	if err != nil {
		return domain.AppendResult{}, domain.NewError(domain.ErrorInvalidArgument, operation, err)
	}
	if len(request.Rows) == 0 {
		return domain.AppendResult{}, domain.NewError(domain.ErrorInvalidArgument, operation, errors.New("at least one ProtoRow is required"))
	}
	if request.PayloadBytes <= 0 || request.PayloadBytes > s.config.MaxAppendBytes {
		return domain.AppendResult{}, domain.NewError(domain.ErrorInvalidArgument, operation, fmt.Errorf("append ProtoData size %d exceeds configured limit %d", request.PayloadBytes, s.config.MaxAppendBytes))
	}
	if request.WireBytes < request.PayloadBytes {
		return domain.AppendResult{}, domain.NewError(domain.ErrorInvalidArgument, operation, errors.New("append wire size is smaller than ProtoData size"))
	}
	if request.WireBytes-request.PayloadBytes > s.config.MaxAppendEnvelopeBytes {
		return domain.AppendResult{}, domain.NewError(domain.ErrorInvalidArgument, operation, fmt.Errorf("append envelope size %d exceeds configured limit %d", request.WireBytes-request.PayloadBytes, s.config.MaxAppendEnvelopeBytes))
	}
	state, err := s.lookupOrCreateDefault(ctx, operation, table, canonical, isDefault)
	if err != nil {
		return domain.AppendResult{}, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.cleanupPhase != cleanupPhaseActive {
		return domain.AppendResult{}, domain.NewError(domain.ErrorNotFound, operation, errors.New("write stream was discarded"))
	}
	if state.stream.Type == domain.StreamTypePending && state.stream.State != domain.StreamStateOpen {
		code := domain.ErrorFailedPrecondition
		cause := errors.New("write stream is finalized")
		if state.stream.State == domain.StreamStateCommitted {
			cause = errors.New("write stream is already committed")
		}
		return domain.AppendResult{}, domain.NewError(code, operation, cause)
	}
	if state.stream.Type == domain.StreamTypeDefault && request.Offset != nil {
		return domain.AppendResult{}, domain.NewError(domain.ErrorInvalidArgument, operation, errors.New("offset is not allowed for the default stream"))
	}
	if len(request.Descriptor) == 0 && len(state.descriptor) == 0 {
		return domain.AppendResult{}, domain.NewError(domain.ErrorInvalidArgument, operation, errors.New("writer schema is required on the first append"))
	}
	descriptor := request.Descriptor
	fingerprint := request.SchemaFingerprint
	if len(descriptor) == 0 {
		descriptor = state.descriptor
		fingerprint = state.stream.SchemaFingerprint
	} else {
		computedFingerprint := digest(descriptor)
		if fingerprint != "" && fingerprint != computedFingerprint {
			return domain.AppendResult{}, domain.NewError(domain.ErrorInvalidArgument, operation, errors.New("writer schema fingerprint does not match descriptor"))
		}
		fingerprint = computedFingerprint
		if state.stream.SchemaFingerprint != "" && fingerprint != state.stream.SchemaFingerprint {
			return domain.AppendResult{}, domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("writer schema changed for an existing stream"))
		}
	}
	computedPayloadDigest := rowsDigest(request.Rows)
	if request.PayloadDigest != "" && request.PayloadDigest != computedPayloadDigest {
		return domain.AppendResult{}, domain.NewError(domain.ErrorInvalidArgument, operation, errors.New("payload digest does not match ProtoRows"))
	}
	startOffset := state.stream.NextOffset
	receipt := appendReceipt{
		startOffset: startOffset, rowCount: int64(len(request.Rows)),
		schemaFingerprint: fingerprint, payloadDigest: computedPayloadDigest,
	}
	if state.unacknowledged != nil && *state.unacknowledged != receipt {
		return domain.AppendResult{}, domain.NewError(
			domain.ErrorFailedPrecondition, operation,
			errors.New("an unacknowledged append must be retried with the same offset, schema, and payload before the stream can advance"),
		)
	}
	if request.Offset != nil {
		switch {
		case *request.Offset < startOffset:
			return domain.AppendResult{}, domain.NewError(domain.ErrorAlreadyExists, operation, errors.New("append offset already exists"))
		case *request.Offset > startOffset:
			return domain.AppendResult{}, domain.NewError(domain.ErrorOutOfRange, operation, errors.New("append offset is beyond stream end"))
		}
	}
	batch := ports.AppendBatch{
		StreamName: canonical, Table: table, StartOffset: startOffset,
		WireBytes: int64(request.WireBytes), Descriptor: descriptor, Rows: request.Rows,
		SchemaFingerprint: fingerprint, PayloadDigest: request.PayloadDigest,
		TraceID: request.TraceID,
	}
	batch.PayloadDigest = computedPayloadDigest
	sideEffect := "coordinator.stage_pending"
	call := s.coordinator.StagePending
	if isDefault {
		sideEffect = "coordinator.append_default"
		call = s.coordinator.AppendDefault
	}
	s.logger.InfoContext(ctx, "submitting Storage Write batch",
		"event", "side_effect.before", "side_effect", sideEffect,
		"operation", operation, "model_version", s.config.ProtocolModelVersion,
		"stream_fingerprint", digest([]byte(canonical)), "table", table.Name(), "start_offset", startOffset,
		"row_count", len(request.Rows), "row_bytes", rowsBytes(request.Rows),
		"payload_bytes", request.PayloadBytes, "wire_bytes", request.WireBytes,
		"schema_fingerprint", fingerprint, "payload_digest", batch.PayloadDigest,
		"trace_id", safeTraceID(request.TraceID), "tx_state", pendingTxState(isDefault))
	err = call(ctx, batch)
	s.logSideEffectEnd(ctx, operation, sideEffect, canonical, table.Name(), startOffset, len(request.Rows), fingerprint, batch.PayloadDigest, err)
	if err != nil {
		if !isDefault && appendOutcomeIsAmbiguous(err) {
			state.unacknowledged = &receipt
			s.logger.WarnContext(ctx, "pending append acknowledgement is unresolved",
				"event", "domain.transition", "operation", operation,
				"model_version", s.config.ProtocolModelVersion,
				"stream_fingerprint", digest([]byte(canonical)), "start_offset", startOffset,
				"row_count", len(request.Rows), "schema_fingerprint", fingerprint,
				"payload_digest", batch.PayloadDigest, "state_after", "append_unacknowledged")
		}
		code := coordinatorErrorCode(err, domain.ErrorInternal)
		return domain.AppendResult{}, domain.NewError(code, operation, err)
	}
	wasUnacknowledged := state.unacknowledged != nil
	state.unacknowledged = nil
	if len(state.descriptor) == 0 {
		state.descriptor = slices.Clone(descriptor)
		state.stream.SchemaFingerprint = fingerprint
	}
	state.stream.RowCount += int64(len(request.Rows))
	state.stream.NextOffset += int64(len(request.Rows))
	state.stream.LastActivity = s.clock.Now()
	if wasUnacknowledged {
		s.logger.InfoContext(ctx, "pending append acknowledgement reconciled",
			"event", "domain.transition", "operation", operation,
			"model_version", s.config.ProtocolModelVersion,
			"stream_fingerprint", digest([]byte(canonical)), "start_offset", startOffset,
			"row_count", len(request.Rows), "schema_fingerprint", fingerprint,
			"payload_digest", batch.PayloadDigest, "state_after", "append_acknowledged")
	}
	return domain.AppendResult{
		StreamName: canonical, StartOffset: startOffset,
		HasOffset: !isDefault, RowCount: int64(len(request.Rows)),
	}, nil
}

// Finalize is idempotent for an already-finalized PENDING stream and returns
// the total accepted row count. DEFAULT streams cannot be finalized.
func (s *Service) Finalize(_ context.Context, name string) (int64, error) {
	const operation = "storage_write.finalize_stream"
	_, canonical, isDefault, err := domain.ParseStreamName(name)
	if err != nil {
		return 0, domain.NewError(domain.ErrorInvalidArgument, operation, err)
	}
	if isDefault {
		return 0, domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("the default stream cannot be finalized"))
	}
	state, err := s.lookup(canonical, operation)
	if err != nil {
		return 0, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.cleanupPhase != cleanupPhaseActive {
		return 0, domain.NewError(domain.ErrorNotFound, operation, errors.New("write stream was discarded"))
	}
	if state.stream.State == domain.StreamStateCommitted {
		return 0, domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("write stream is already committed"))
	}
	if state.unacknowledged != nil {
		return 0, domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("an unacknowledged append must be reconciled before finalizing the stream"))
	}
	state.stream.State = domain.StreamStateFinalized
	state.stream.LastActivity = s.clock.Now()
	return state.stream.RowCount, nil
}

// BatchCommit locks all streams in stable name order. Validation completes for
// the whole group before the coordinator is called, and state changes only
// after that single all-or-none backend transaction succeeds.
func (s *Service) BatchCommit(ctx context.Context, parent domain.TableReference, names []string) (domain.BatchCommitResult, error) {
	const operation = "storage_write.batch_commit"
	if _, err := domain.ParseTableName(parent.Name()); err != nil {
		return domain.BatchCommitResult{}, domain.NewError(domain.ErrorInvalidArgument, operation, err)
	}
	if len(names) == 0 {
		return domain.BatchCommitResult{}, domain.NewError(domain.ErrorInvalidArgument, operation, errors.New("at least one stream is required"))
	}
	type namedState struct {
		name  string
		state *streamState
	}
	unique := make(map[string]struct{}, len(names))
	states := make([]namedState, 0, len(names))
	streamErrors := make([]domain.StreamError, 0)
	for _, name := range names {
		table, canonical, isDefault, err := domain.ParseStreamName(name)
		if err != nil {
			streamErrors = append(streamErrors, domain.StreamError{Code: domain.StreamNotFound, Stream: name, Message: "invalid stream resource"})
			continue
		}
		if _, duplicate := unique[canonical]; duplicate {
			streamErrors = append(streamErrors, domain.StreamError{Code: domain.InvalidStreamState, Stream: canonical, Message: "duplicate stream in commit request"})
			continue
		}
		unique[canonical] = struct{}{}
		if isDefault {
			streamErrors = append(streamErrors, domain.StreamError{Code: domain.InvalidStreamType, Stream: canonical, Message: "default stream is not PENDING"})
			continue
		}
		if table != parent {
			streamErrors = append(streamErrors, domain.StreamError{Code: domain.InvalidStreamState, Stream: canonical, Message: "stream belongs to another table"})
			continue
		}
		state, lookupErr := s.lookup(canonical, operation)
		if lookupErr != nil {
			streamErrors = append(streamErrors, domain.StreamError{Code: domain.StreamNotFound, Stream: canonical, Message: "stream not found"})
			continue
		}
		states = append(states, namedState{name: canonical, state: state})
	}
	if len(streamErrors) > 0 {
		return domain.BatchCommitResult{StreamErrors: streamErrors}, nil
	}
	sort.Slice(states, func(i, j int) bool { return states[i].name < states[j].name })
	for _, item := range states {
		item.state.mu.Lock()
	}
	defer func() {
		for index := len(states) - 1; index >= 0; index-- {
			states[index].state.mu.Unlock()
		}
	}()
	for _, item := range states {
		switch {
		case item.state.cleanupPhase != cleanupPhaseActive:
			streamErrors = append(streamErrors, domain.StreamError{Code: domain.StreamNotFound, Stream: item.name, Message: "stream was discarded"})
		case item.state.stream.Type != domain.StreamTypePending:
			streamErrors = append(streamErrors, domain.StreamError{Code: domain.InvalidStreamType, Stream: item.name, Message: "stream is not PENDING"})
		case item.state.stream.State == domain.StreamStateCommitted:
			streamErrors = append(streamErrors, domain.StreamError{Code: domain.StreamAlreadyCommitted, Stream: item.name, Message: "stream is already committed"})
		case item.state.stream.State != domain.StreamStateFinalized:
			streamErrors = append(streamErrors, domain.StreamError{Code: domain.InvalidStreamState, Stream: item.name, Message: "stream must be finalized before commit"})
		}
	}
	if len(streamErrors) > 0 {
		return domain.BatchCommitResult{StreamErrors: streamErrors}, nil
	}
	canonicalNames := make([]string, len(states))
	var rowCount int64
	for index, item := range states {
		canonicalNames[index] = item.name
		rowCount += item.state.stream.RowCount
	}
	s.logger.InfoContext(ctx, "committing pending write streams",
		"event", "side_effect.before", "side_effect", "coordinator.commit_pending",
		"operation", operation, "model_version", s.config.ProtocolModelVersion,
		"table", parent.Name(), "stream_count", len(states), "row_count", rowCount,
		"stream_set_fingerprint", digest([]byte(strings.Join(canonicalNames, "\n"))),
		"tx_state", "begin")
	err := s.coordinator.CommitPending(ctx, ports.CommitRequest{Parent: parent, StreamNames: canonicalNames})
	s.logCommitEnd(ctx, operation, parent.Name(), canonicalNames, rowCount, err)
	if err != nil {
		return domain.BatchCommitResult{}, domain.NewError(coordinatorErrorCode(err, domain.ErrorInternal), operation, err)
	}
	// The response time represents successful atomic visibility, so capture it
	// only after the coordinator transaction acknowledges its commit.
	// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#batchcommitwritestreamsresponse
	commitTime := s.clock.Now()
	for _, item := range states {
		item.state.stream.State = domain.StreamStateCommitted
		item.state.stream.CommitTime = cloneTime(&commitTime)
		item.state.stream.LastActivity = commitTime
	}
	s.pending.Add(-int64(len(states)))
	return domain.BatchCommitResult{CommitTime: cloneTime(&commitTime)}, nil
}

func (s *Service) lookup(name, operation string) (*streamState, error) {
	s.mu.RLock()
	state := s.streams[name]
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil, domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("Storage Write service is closed"))
	}
	if state == nil {
		return nil, domain.NewError(domain.ErrorNotFound, operation, errors.New("write stream not found"))
	}
	return state, nil
}

func (s *Service) lookupOrCreateDefault(ctx context.Context, operation string, table domain.TableReference, canonical string, isDefault bool) (*streamState, error) {
	if !isDefault {
		return s.lookup(canonical, operation)
	}
	s.mu.RLock()
	state := s.streams[canonical]
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil, domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("Storage Write service is closed"))
	}
	if state != nil {
		return state, nil
	}
	schema, err := s.describeTable(ctx, operation, table)
	if err != nil {
		return nil, err
	}
	now := s.clock.Now()
	commitTime := now
	candidate := &streamState{stream: domain.WriteStream{
		Name: canonical, Parent: table, Type: domain.StreamTypeDefault,
		State: domain.StreamStateCommitted, CreateTime: now, CommitTime: &commitTime,
		LastActivity: now, Location: s.config.Location, Schema: cloneSchema(schema),
	}, cleanupPhase: cleanupPhaseActive}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("Storage Write service is closed"))
	}
	if existing := s.streams[canonical]; existing != nil {
		return existing, nil
	}
	s.streams[canonical] = candidate
	return candidate, nil
}

func (s *Service) describeTable(ctx context.Context, operation string, table domain.TableReference) (domain.TableSchema, error) {
	s.logger.InfoContext(ctx, "validating Storage Write destination",
		"event", "side_effect.before", "side_effect", "coordinator.describe_table",
		"operation", operation, "model_version", s.config.ProtocolModelVersion,
		"table", table.Name())
	schema, err := s.coordinator.DescribeTable(ctx, table)
	attrs := []any{
		"event", "side_effect.after", "side_effect", "coordinator.describe_table",
		"operation", operation, "model_version", s.config.ProtocolModelVersion,
		"table", table.Name(), "success", err == nil, "field_count", len(schema.Fields),
		"schema_fingerprint", digest([]byte(fmt.Sprintf("%v", schema.Fields))),
	}
	if err != nil {
		attrs = append(attrs, errorLogAttrs(err)...)
	}
	s.logger.InfoContext(ctx, "Storage Write destination validated", attrs...)
	if err != nil {
		code := coordinatorErrorCode(err, domain.ErrorInternal)
		if errors.Is(err, ports.ErrTableNotFound) {
			code = domain.ErrorNotFound
		} else if errors.Is(err, ports.ErrUnsupportedSchema) {
			code = domain.ErrorUnimplemented
		}
		return domain.TableSchema{}, domain.NewError(code, operation, err)
	}
	return schema, nil
}

func coordinatorErrorCode(err error, fallback domain.ErrorCode) domain.ErrorCode {
	switch {
	case errors.Is(err, context.Canceled):
		return domain.ErrorCanceled
	case errors.Is(err, ports.ErrQueueWaitTimeout), errors.Is(err, ports.ErrResourceExhausted):
		return domain.ErrorResourceExhausted
	case errors.Is(err, ports.ErrOperationTimeout), errors.Is(err, context.DeadlineExceeded):
		return domain.ErrorDeadlineExceeded
	default:
		return fallback
	}
}

func appendOutcomeIsAmbiguous(err error) bool {
	return errors.Is(err, ports.ErrOperationTimeout) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

func (s *Service) admissionError(operation string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("Storage Write service is closed"))
	}
	if s.pending.Load() >= int64(s.config.MaxStreams) {
		return domain.NewError(domain.ErrorResourceExhausted, operation, errors.New("logical stream capacity reached"))
	}
	return nil
}

func (s *Service) logSideEffectEnd(ctx context.Context, operation, sideEffect, stream, table string, offset int64, rowCount int, schemaFingerprint, payloadDigest string, err error) {
	attrs := []any{
		"event", "side_effect.after", "side_effect", sideEffect,
		"operation", operation, "model_version", s.config.ProtocolModelVersion,
		"stream_fingerprint", digest([]byte(stream)), "table", table, "start_offset", offset,
		"row_count", rowCount, "schema_fingerprint", schemaFingerprint,
		"payload_digest", payloadDigest, "success", err == nil,
	}
	if err != nil {
		attrs = append(attrs, errorLogAttrs(err)...)
	}
	s.logger.InfoContext(ctx, "Storage Write batch completed", attrs...)
}

func (s *Service) logCommitEnd(ctx context.Context, operation, table string, streams []string, rowCount int64, err error) {
	attrs := []any{
		"event", "side_effect.after", "side_effect", "coordinator.commit_pending",
		"operation", operation, "model_version", s.config.ProtocolModelVersion,
		"table", table, "stream_count", len(streams), "row_count", rowCount,
		"stream_set_fingerprint", digest([]byte(strings.Join(streams, "\n"))),
		"success", err == nil, "tx_state", map[bool]string{true: "committed", false: "rolled_back"}[err == nil],
	}
	if err != nil {
		attrs = append(attrs, errorLogAttrs(err)...)
	}
	s.logger.InfoContext(ctx, "pending write stream commit completed", attrs...)
}

func cloneStream(stream domain.WriteStream) domain.WriteStream {
	stream.Schema = cloneSchema(stream.Schema)
	stream.CommitTime = cloneTime(stream.CommitTime)
	return stream
}

func cloneSchema(schema domain.TableSchema) domain.TableSchema {
	return domain.TableSchema{Fields: cloneFields(schema.Fields)}
}

func cloneFields(fields []domain.Field) []domain.Field {
	result := make([]domain.Field, len(fields))
	for index, field := range fields {
		result[index] = field
		result[index].Fields = cloneFields(field.Fields)
	}
	return result
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func digest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func rowsDigest(rows [][]byte) string {
	hash := sha256.New()
	for _, row := range rows {
		_, _ = hash.Write(row)
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

func rowsBytes(rows [][]byte) int {
	total := 0
	for _, row := range rows {
		total += len(row)
	}
	return total
}

func safeTraceID(value string) string {
	if len(value) > 128 {
		return ""
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || strings.ContainsRune("-_./", character) {
			continue
		}
		return ""
	}
	return value
}

func pendingTxState(isDefault bool) string {
	if isDefault {
		return "autocommit"
	}
	return "staged"
}

func errorLogAttrs(err error) []any {
	return []any{"error_type", fmt.Sprintf("%T", err), "error_digest", digest([]byte(err.Error()))}
}
