package application

// Stream state transitions implement CreateWriteStream, AppendRows,
// FinalizeWriteStream, and BatchCommitWriteStreams as specified by the official
// Storage Write API. PENDING rows remain behind the coordinator port until one
// atomic commit succeeds; DEFAULT rows are visible once append is acknowledged.
//
// Official RPC contract:
// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
	"github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
)

type Service struct {
	config      Config
	coordinator ports.Coordinator
	clock       ports.Clock
	ids         ports.IDGenerator
	logger      *slog.Logger
	state       ports.StateRepository

	mu              sync.RWMutex
	streams         map[string]*streamState
	commitGroups    map[string]domain.CommitGroup
	closed          bool
	stateReconciled bool
	pending         atomic.Int64

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
	operation       domain.OperationKind
	operationPhase  domain.OperationPhase
	operationToken  string
	revision        int64
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
	phase             domain.ReceiptPhase
	createdAt         time.Time
	updatedAt         time.Time
}

type cleanupPhase string

const (
	cleanupPhaseActive    cleanupPhase = "active"
	cleanupPhasePending   cleanupPhase = "cleanup_pending"
	cleanupPhaseDiscarded cleanupPhase = "discarded"
)

func New(config Config, coordinator ports.Coordinator, clock ports.Clock, ids ports.IDGenerator, logger *slog.Logger, options ...Option) (*Service, error) {
	if coordinator == nil || clock == nil || ids == nil || logger == nil {
		return nil, fmt.Errorf("storage write dependencies must not be nil")
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	service := &Service{
		config: config, coordinator: coordinator, clock: clock, ids: ids,
		logger: logger, state: transientStateRepository{}, stateReconciled: true,
		streams: make(map[string]*streamState), commitGroups: make(map[string]domain.CommitGroup),
		cleanupGate: make(chan struct{}, 1),
	}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("Storage Write option must not be nil")
		}
		if err := option(service); err != nil {
			return nil, err
		}
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
	if err := s.ensureStateReconciled(operation); err != nil {
		return domain.WriteStream{}, err
	}
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
	var state *streamState
	for attempts := 0; attempts < 4; attempts++ {
		id := s.ids.NewID()
		candidate := request.Parent.Name() + "/streams/" + id
		if _, _, isDefault, parseErr := domain.ParseStreamName(candidate); parseErr != nil || isDefault {
			continue
		}
		if _, exists := s.streams[candidate]; exists {
			continue
		}
		candidateState := &streamState{stream: domain.WriteStream{
			Name: candidate, Parent: request.Parent, Type: domain.StreamTypePending,
			State: domain.StreamStateOpen, CreateTime: now, LastActivity: now,
			Location: s.config.Location, Schema: cloneSchema(schema),
		}, cleanupPhase: cleanupPhaseActive, operation: domain.OperationNone,
			operationPhase: domain.OperationPhaseNone, revision: 1}
		if err := s.state.CreateStream(ctx, streamRecord(candidateState)); err != nil {
			if errors.Is(err, ports.ErrStateConflict) {
				continue
			}
			return domain.WriteStream{}, s.classifyStateError(operation, err)
		}
		name = candidate
		state = candidateState
		break
	}
	if name == "" {
		return domain.WriteStream{}, domain.NewError(domain.ErrorInternal, operation, errors.New("ID generator did not produce a unique valid stream ID"))
	}
	stream := state.stream
	s.streams[name] = state
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
	if err := s.ensureStateReconciled(operation); err != nil {
		return domain.WriteStream{}, err
	}
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
	if err := s.ensureStateReconciled(operation); err != nil {
		return domain.AppendResult{}, err
	}
	table, canonical, isDefault, err := domain.ParseStreamName(request.StreamName)
	if err != nil {
		return domain.AppendResult{}, domain.NewError(domain.ErrorInvalidArgument, operation, err)
	}
	if request.CDC && !isDefault {
		return domain.AppendResult{}, domain.NewError(domain.ErrorInvalidArgument, operation, errors.New("CDC rows are supported on the default stream only"))
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
	now := s.clock.Now()
	receipt := appendReceipt{
		startOffset: startOffset, rowCount: int64(len(request.Rows)),
		schemaFingerprint: fingerprint, payloadDigest: computedPayloadDigest,
		phase: domain.ReceiptPrepared, createdAt: now, updatedAt: now,
	}
	if state.unacknowledged != nil && !appendReceiptsMatch(*state.unacknowledged, receipt) {
		return domain.AppendResult{}, domain.NewError(
			domain.ErrorFailedPrecondition, operation,
			errors.New("an unacknowledged append must be retried with the same offset, schema, and payload before the stream can advance"),
		)
	}
	wasUnacknowledged := state.unacknowledged != nil
	if wasUnacknowledged {
		receipt = *state.unacknowledged
	}
	if request.Offset != nil {
		switch {
		case *request.Offset < startOffset:
			return domain.AppendResult{}, domain.NewError(domain.ErrorAlreadyExists, operation, errors.New("append offset already exists"))
		case *request.Offset > startOffset:
			return domain.AppendResult{}, domain.NewError(domain.ErrorOutOfRange, operation, errors.New("append offset is beyond stream end"))
		}
	}
	if !wasUnacknowledged {
		expectedRevision := state.revision
		prepared := streamRecord(state)
		prepared.Operation = domain.OperationAppend
		prepared.OperationPhase = domain.OperationPhasePrepared
		prepared.OperationToken = strconv.FormatInt(startOffset, 10)
		prepared.Revision++
		persistedReceipt := appendReceiptRecord(canonical, receipt, domain.ReceiptPrepared, receipt.createdAt, receipt.updatedAt)
		if err := s.state.PrepareAppend(ctx, expectedRevision, prepared, persistedReceipt); err != nil {
			return domain.AppendResult{}, s.classifyStateError(operation, err)
		}
		state.operation = prepared.Operation
		state.operationPhase = prepared.OperationPhase
		state.operationToken = prepared.OperationToken
		state.revision = prepared.Revision
		state.unacknowledged = &receipt
	}
	batch := ports.AppendBatch{
		StreamName: canonical, Table: table, StartOffset: startOffset,
		WireBytes: int64(request.WireBytes), Descriptor: descriptor, Rows: request.Rows,
		SchemaFingerprint: fingerprint, PayloadDigest: request.PayloadDigest,
		TraceID: request.TraceID,
		CDC:     request.CDC,
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
		"stream", canonical, "stream_fingerprint", digest([]byte(canonical)), "table", table.Name(), "start_offset", startOffset,
		"row_count", len(request.Rows), "row_bytes", rowsBytes(request.Rows),
		"rows", request.Rows, "descriptor", descriptor,
		"payload_bytes", request.PayloadBytes, "wire_bytes", request.WireBytes,
		"schema_fingerprint", fingerprint, "payload_digest", batch.PayloadDigest,
		"trace_id", request.TraceID, "tx_state", pendingTxState(isDefault))
	err = call(ctx, batch)
	s.logSideEffectEnd(ctx, operation, sideEffect, canonical, table.Name(), startOffset, len(request.Rows), fingerprint, batch.PayloadDigest, err)
	if err != nil {
		if appendOutcomeIsAmbiguous(err) {
			if state.operationPhase == domain.OperationPhasePrepared {
				updatedAt := s.clock.Now()
				unresolved := streamRecord(state)
				unresolved.OperationPhase = domain.OperationPhaseUnresolved
				unresolved.Revision++
				receipt.phase = domain.ReceiptUnresolved
				receipt.updatedAt = updatedAt
				persistedReceipt := appendReceiptRecord(canonical, receipt, domain.ReceiptUnresolved, receipt.createdAt, updatedAt)
				if stateErr := s.state.MarkAppendUnresolved(ctx, state.revision, unresolved, persistedReceipt); stateErr != nil {
					return domain.AppendResult{}, s.classifyStateError(operation, errors.Join(err, stateErr))
				}
				state.operationPhase = unresolved.OperationPhase
				state.revision = unresolved.Revision
				state.unacknowledged = &receipt
			}
			s.logger.WarnContext(ctx, "pending append acknowledgement is unresolved",
				"event", "domain.transition", "operation", operation,
				"model_version", s.config.ProtocolModelVersion,
				"stream_fingerprint", digest([]byte(canonical)), "start_offset", startOffset,
				"row_count", len(request.Rows), "schema_fingerprint", fingerprint,
				"payload_digest", batch.PayloadDigest, "state_after", "append_unacknowledged")
		} else {
			aborted := streamRecord(state)
			aborted.Operation = domain.OperationNone
			aborted.OperationPhase = domain.OperationPhaseNone
			aborted.OperationToken = ""
			aborted.Revision++
			persistedReceipt := appendReceiptRecord(canonical, receipt, receipt.phase, receipt.createdAt, s.clock.Now())
			if stateErr := s.state.AbortAppend(ctx, state.revision, aborted, persistedReceipt); stateErr != nil {
				return domain.AppendResult{}, s.classifyStateError(operation, errors.Join(err, stateErr))
			}
			state.operation = domain.OperationNone
			state.operationPhase = domain.OperationPhaseNone
			state.operationToken = ""
			state.revision = aborted.Revision
			state.unacknowledged = nil
		}
		code := coordinatorErrorCode(err, domain.ErrorInternal)
		return domain.AppendResult{}, domain.NewError(code, operation, err)
	}
	completedAt := s.clock.Now()
	completed := streamRecord(state)
	completed.Operation = domain.OperationNone
	completed.OperationPhase = domain.OperationPhaseNone
	completed.OperationToken = ""
	completed.Stream.RowCount += int64(len(request.Rows))
	completed.Stream.NextOffset += int64(len(request.Rows))
	completed.Stream.LastActivity = completedAt
	if completed.Stream.SchemaFingerprint == "" {
		completed.Stream.SchemaFingerprint = fingerprint
	}
	completed.Revision++
	receipt.phase = domain.ReceiptApplied
	receipt.updatedAt = completedAt
	persistedReceipt := appendReceiptRecord(canonical, receipt, domain.ReceiptApplied, receipt.createdAt, completedAt)
	if stateErr := s.state.CompleteAppend(ctx, state.revision, completed, persistedReceipt); stateErr != nil {
		return domain.AppendResult{}, s.classifyStateError(operation, stateErr)
	}
	state.unacknowledged = nil
	state.operation = domain.OperationNone
	state.operationPhase = domain.OperationPhaseNone
	state.operationToken = ""
	state.revision = completed.Revision
	state.stream = cloneStream(completed.Stream)
	if len(state.descriptor) == 0 {
		state.descriptor = slices.Clone(descriptor)
	}
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
func (s *Service) Finalize(ctx context.Context, name string) (int64, error) {
	const operation = "storage_write.finalize_stream"
	if err := s.ensureStateReconciled(operation); err != nil {
		return 0, err
	}
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
	updated := streamRecord(state)
	updated.Stream.State = domain.StreamStateFinalized
	updated.Stream.LastActivity = s.clock.Now()
	updated.Revision++
	if err := s.state.UpdateStream(ctx, state.revision, updated); err != nil {
		return 0, s.classifyStateError(operation, err)
	}
	state.stream = cloneStream(updated.Stream)
	state.revision = updated.Revision
	return state.stream.RowCount, nil
}

// BatchCommit locks all streams in stable name order. Validation completes for
// the whole group before the coordinator is called, and state changes only
// after that single all-or-none backend transaction succeeds.
func (s *Service) BatchCommit(ctx context.Context, parent domain.TableReference, names []string) (domain.BatchCommitResult, error) {
	const operation = "storage_write.batch_commit"
	if err := s.ensureStateReconciled(operation); err != nil {
		return domain.BatchCommitResult{}, err
	}
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
	var existingGroupID string
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
		case item.state.operation == domain.OperationAppend:
			streamErrors = append(streamErrors, domain.StreamError{Code: domain.InvalidStreamState, Stream: item.name, Message: "stream has an unresolved append"})
		case item.state.operation == domain.OperationCommit:
			if existingGroupID == "" {
				existingGroupID = item.state.operationToken
			} else if existingGroupID != item.state.operationToken {
				streamErrors = append(streamErrors, domain.StreamError{Code: domain.InvalidStreamState, Stream: item.name, Message: "stream belongs to another unresolved commit"})
			}
		case item.state.operation != domain.OperationNone:
			streamErrors = append(streamErrors, domain.StreamError{Code: domain.InvalidStreamState, Stream: item.name, Message: "stream has an invalid durable operation"})
		}
	}
	if len(streamErrors) > 0 {
		return domain.BatchCommitResult{StreamErrors: streamErrors}, nil
	}
	canonicalNames := make([]string, len(states))
	expectedRows := make(map[string]int64, len(states))
	var rowCount int64
	for index, item := range states {
		canonicalNames[index] = item.name
		expectedRows[item.name] = item.state.stream.RowCount
		rowCount += item.state.stream.RowCount
	}
	var group domain.CommitGroup
	if existingGroupID != "" {
		for _, item := range states {
			if item.state.operation != domain.OperationCommit || item.state.operationToken != existingGroupID {
				return domain.BatchCommitResult{StreamErrors: []domain.StreamError{{
					Code: domain.InvalidStreamState, Stream: item.name,
					Message: "commit retry must contain the exact unresolved stream set",
				}}}, nil
			}
		}
		s.mu.RLock()
		group = cloneCommitGroup(s.commitGroups[existingGroupID])
		s.mu.RUnlock()
		if !commitGroupMatches(group, parent, canonicalNames, expectedRows) ||
			(group.Phase != domain.CommitPrepared && group.Phase != domain.CommitUnresolved) {
			return domain.BatchCommitResult{StreamErrors: []domain.StreamError{{
				Code: domain.InvalidStreamState, Stream: canonicalNames[0],
				Message: "commit retry does not match the unresolved commit group",
			}}}, nil
		}
	} else {
		now := s.clock.Now()
		group = domain.CommitGroup{
			ID:     digest([]byte(s.ids.NewID() + "\n" + strings.Join(canonicalNames, "\n"))),
			Parent: parent, ExpectedRowCount: rowCount, Phase: domain.CommitPrepared,
			CreatedAt: now, UpdatedAt: now, Members: make([]domain.CommitMember, len(states)),
		}
		expected := make(map[string]int64, len(states))
		prepared := make([]domain.StreamRecord, len(states))
		for index, item := range states {
			expected[item.name] = item.state.revision
			prepared[index] = streamRecord(item.state)
			prepared[index].Operation = domain.OperationCommit
			prepared[index].OperationPhase = domain.OperationPhasePrepared
			prepared[index].OperationToken = group.ID
			prepared[index].Revision++
			group.Members[index] = domain.CommitMember{StreamName: item.name, ExpectedRowCount: item.state.stream.RowCount}
		}
		if err := s.state.PrepareCommit(ctx, expected, prepared, group); err != nil {
			return domain.BatchCommitResult{}, s.classifyStateError(operation, err)
		}
		for index, item := range states {
			item.state.operation = domain.OperationCommit
			item.state.operationPhase = domain.OperationPhasePrepared
			item.state.operationToken = group.ID
			item.state.revision = prepared[index].Revision
		}
		s.mu.Lock()
		s.commitGroups[group.ID] = cloneCommitGroup(group)
		s.mu.Unlock()
	}
	s.logger.InfoContext(ctx, "committing pending write streams",
		"event", "side_effect.before", "side_effect", "coordinator.commit_pending",
		"operation", operation, "model_version", s.config.ProtocolModelVersion,
		"table", parent.Name(), "stream_count", len(states), "row_count", rowCount,
		"stream_set_fingerprint", digest([]byte(strings.Join(canonicalNames, "\n"))),
		"tx_state", "begin")
	err := s.coordinator.CommitPending(ctx, ports.CommitRequest{
		Parent: parent, StreamNames: canonicalNames, GroupID: group.ID, ExpectedRowCounts: expectedRows,
	})
	s.logCommitEnd(ctx, operation, parent.Name(), canonicalNames, rowCount, err)
	if err != nil {
		expected := make(map[string]int64, len(states))
		updated := make([]domain.StreamRecord, len(states))
		for index, item := range states {
			expected[item.name] = item.state.revision
			updated[index] = streamRecord(item.state)
			updated[index].Revision++
		}
		if appendOutcomeIsAmbiguous(err) {
			if group.Phase == domain.CommitPrepared {
				group.Phase = domain.CommitUnresolved
				group.UpdatedAt = s.clock.Now()
				for index := range updated {
					updated[index].OperationPhase = domain.OperationPhaseUnresolved
				}
				if stateErr := s.state.MarkCommitUnresolved(ctx, expected, updated, group); stateErr != nil {
					return domain.BatchCommitResult{}, s.classifyStateError(operation, errors.Join(err, stateErr))
				}
				for index, item := range states {
					item.state.operationPhase = domain.OperationPhaseUnresolved
					item.state.revision = updated[index].Revision
				}
				s.mu.Lock()
				s.commitGroups[group.ID] = cloneCommitGroup(group)
				s.mu.Unlock()
			}
		} else {
			group.Phase = domain.CommitAborted
			group.UpdatedAt = s.clock.Now()
			for index := range updated {
				updated[index].Operation = domain.OperationNone
				updated[index].OperationPhase = domain.OperationPhaseNone
				updated[index].OperationToken = ""
			}
			if stateErr := s.state.AbortCommit(ctx, expected, updated, group); stateErr != nil {
				return domain.BatchCommitResult{}, s.classifyStateError(operation, errors.Join(err, stateErr))
			}
			for index, item := range states {
				item.state.operation = domain.OperationNone
				item.state.operationPhase = domain.OperationPhaseNone
				item.state.operationToken = ""
				item.state.revision = updated[index].Revision
			}
			s.mu.Lock()
			s.commitGroups[group.ID] = cloneCommitGroup(group)
			s.mu.Unlock()
		}
		return domain.BatchCommitResult{}, domain.NewError(coordinatorErrorCode(err, domain.ErrorInternal), operation, err)
	}
	// The response time represents successful atomic visibility, so capture it
	// only after the coordinator transaction acknowledges its commit.
	// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#batchcommitwritestreamsresponse
	commitTime := s.clock.Now()
	expected := make(map[string]int64, len(states))
	completed := make([]domain.StreamRecord, len(states))
	for index, item := range states {
		expected[item.name] = item.state.revision
		completed[index] = streamRecord(item.state)
		completed[index].Stream.State = domain.StreamStateCommitted
		completed[index].Stream.CommitTime = cloneTime(&commitTime)
		completed[index].Stream.LastActivity = commitTime
		completed[index].Operation = domain.OperationNone
		completed[index].OperationPhase = domain.OperationPhaseNone
		completed[index].OperationToken = ""
		completed[index].Revision++
	}
	group.Phase = domain.CommitApplied
	group.UpdatedAt = commitTime
	group.CommitTime = cloneTime(&commitTime)
	if err := s.state.CompleteCommit(ctx, expected, completed, group); err != nil {
		return domain.BatchCommitResult{}, s.classifyStateError(operation, err)
	}
	for index, item := range states {
		item.state.stream = cloneStream(completed[index].Stream)
		item.state.operation = domain.OperationNone
		item.state.operationPhase = domain.OperationPhaseNone
		item.state.operationToken = ""
		item.state.revision = completed[index].Revision
	}
	s.mu.Lock()
	s.commitGroups[group.ID] = cloneCommitGroup(group)
	s.mu.Unlock()
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
	}, cleanupPhase: cleanupPhaseActive, operation: domain.OperationNone,
		operationPhase: domain.OperationPhaseNone, revision: 1}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("Storage Write service is closed"))
	}
	if existing := s.streams[canonical]; existing != nil {
		return existing, nil
	}
	if err := s.state.CreateStream(ctx, streamRecord(candidate)); err != nil {
		if errors.Is(err, ports.ErrStateConflict) {
			record, getErr := s.state.GetStream(ctx, canonical)
			if getErr != nil {
				return nil, s.classifyStateError(operation, errors.Join(err, getErr))
			}
			candidate = &streamState{
				stream: cloneStream(record.Stream), cleanupPhase: cleanupPhaseFromRecord(record.CleanupPhase),
				cleanupAttempts: record.CleanupAttempts, operation: record.Operation,
				operationPhase: record.OperationPhase, operationToken: record.OperationToken,
				revision: record.Revision,
			}
		} else {
			return nil, s.classifyStateError(operation, err)
		}
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
	case errors.Is(err, ports.ErrInvalidRows):
		return domain.ErrorInvalidArgument
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
	return catalogdomain.CloneFields(fields)
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
	return []any{"error", err, "error_type", fmt.Sprintf("%T", err), "error_digest", digest([]byte(err.Error()))}
}
