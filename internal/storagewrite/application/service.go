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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
	"github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
)

type Service struct {
	config      Config
	coordinator ports.DurableCoordinator
	repository  ports.StreamRepository
	clock       ports.Clock
	ids         ports.IDGenerator
	logger      *slog.Logger

	closed atomic.Bool

	// cleanupGate serializes orphan disposal without making a caller wait past
	// its context deadline. This keeps DiscardPending exactly-once per sweep even
	// when the periodic cleaner and shutdown race.
	cleanupGate chan struct{}
}

func New(config Config, coordinator ports.DurableCoordinator, repository ports.StreamRepository, clock ports.Clock, ids ports.IDGenerator, logger *slog.Logger) (*Service, error) {
	if coordinator == nil || repository == nil || clock == nil || ids == nil || logger == nil {
		return nil, fmt.Errorf("storage write dependencies must not be nil")
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	service := &Service{
		config: config, coordinator: coordinator, repository: repository, clock: clock, ids: ids,
		logger: logger, cleanupGate: make(chan struct{}, 1),
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
	if s.closed.Load() {
		return domain.WriteStream{}, domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("Storage Write service is closed"))
	}
	if err := ctx.Err(); err != nil {
		return domain.WriteStream{}, domain.NewError(coordinatorErrorCode(err, domain.ErrorInternal), operation, err)
	}
	count, err := s.repository.CountActivePendingStreams(ctx)
	if err != nil {
		return domain.WriteStream{}, domain.NewError(domain.ErrorInternal, operation, err)
	}
	if count >= int64(s.config.MaxStreams) {
		return domain.WriteStream{}, domain.NewError(domain.ErrorResourceExhausted, operation, errors.New("logical stream capacity reached"))
	}
	schema, err := s.describeTable(ctx, operation, request.Parent)
	if err != nil {
		return domain.WriteStream{}, err
	}
	now := s.clock.Now()

	var stream domain.WriteStream
	for attempts := 0; attempts < 4; attempts++ {
		id := s.ids.NewID()
		candidate := request.Parent.Name() + "/streams/" + id
		if _, _, isDefault, parseErr := domain.ParseStreamName(candidate); parseErr != nil || isDefault {
			continue
		}
		stream = domain.WriteStream{
			Name: candidate, Parent: request.Parent, Type: domain.StreamTypePending,
			State: domain.StreamStateOpen, CreateTime: now, LastActivity: now,
			Location: s.config.Location, Schema: cloneSchema(schema),
			TableFingerprint: schemaDigest(schema),
		}
		record := domain.StreamRecord{Stream: stream, CleanupState: domain.CleanupStateActive, Revision: 1}
		err = s.repository.CreateWriteStream(ctx, record, int64(s.config.MaxStreams))
		if err == nil {
			break
		}
		if errors.Is(err, ports.ErrResourceExhausted) {
			return domain.WriteStream{}, domain.NewError(domain.ErrorResourceExhausted, operation, errors.New("logical stream capacity reached"))
		}
		if !errors.Is(err, ports.ErrStreamExists) {
			return domain.WriteStream{}, domain.NewError(domain.ErrorInternal, operation, err)
		}
		stream = domain.WriteStream{}
	}
	if stream.Name == "" {
		return domain.WriteStream{}, domain.NewError(domain.ErrorInternal, operation, errors.New("ID generator did not produce a unique valid stream ID"))
	}
	s.logger.InfoContext(ctx, "pending write stream created",
		"event", "domain.transition", "operation", operation,
		"model_version", s.config.ProtocolModelVersion, "stream_fingerprint", digest([]byte(stream.Name)),
		"table", request.Parent.Name(), "stream_type", stream.Type,
		"stream_count", count+1)
	return cloneStream(stream), nil
}

func (s *Service) GetStream(ctx context.Context, name string) (domain.WriteStream, error) {
	const operation = "storage_write.get_stream"
	table, canonical, isDefault, err := domain.ParseStreamName(name)
	if err != nil {
		return domain.WriteStream{}, domain.NewError(domain.ErrorInvalidArgument, operation, err)
	}
	record, err := s.lookupOrCreateDefault(ctx, operation, table, canonical, isDefault)
	if err != nil {
		return domain.WriteStream{}, err
	}
	if record.CleanupState != domain.CleanupStateActive {
		return domain.WriteStream{}, domain.NewError(domain.ErrorNotFound, operation, errors.New("write stream was discarded"))
	}
	return cloneStream(record.Stream), nil
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
	record, err := s.lookupOrCreateDefault(ctx, operation, table, canonical, isDefault)
	if err != nil {
		return domain.AppendResult{}, err
	}
	if record.CleanupState != domain.CleanupStateActive {
		return domain.AppendResult{}, domain.NewError(domain.ErrorNotFound, operation, errors.New("write stream was discarded"))
	}
	if record.Stream.Type == domain.StreamTypePending && record.Stream.State != domain.StreamStateOpen {
		code := domain.ErrorFailedPrecondition
		cause := errors.New("write stream is finalized")
		if record.Stream.State == domain.StreamStateCommitted {
			cause = errors.New("write stream is already committed")
		}
		return domain.AppendResult{}, domain.NewError(code, operation, cause)
	}
	if record.Stream.Type == domain.StreamTypeDefault && request.Offset != nil {
		return domain.AppendResult{}, domain.NewError(domain.ErrorInvalidArgument, operation, errors.New("offset is not allowed for the default stream"))
	}
	if len(request.Descriptor) == 0 && len(record.WriterDescriptor) == 0 {
		return domain.AppendResult{}, domain.NewError(domain.ErrorInvalidArgument, operation, errors.New("writer schema is required on the first append"))
	}
	descriptor := request.Descriptor
	fingerprint := request.SchemaFingerprint
	if len(descriptor) == 0 {
		descriptor = record.WriterDescriptor
		fingerprint = record.Stream.SchemaFingerprint
	} else {
		computedFingerprint := digest(descriptor)
		if fingerprint != "" && fingerprint != computedFingerprint {
			return domain.AppendResult{}, domain.NewError(domain.ErrorInvalidArgument, operation, errors.New("writer schema fingerprint does not match descriptor"))
		}
		fingerprint = computedFingerprint
		if record.Stream.SchemaFingerprint != "" && fingerprint != record.Stream.SchemaFingerprint {
			return domain.AppendResult{}, domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("writer schema changed for an existing stream"))
		}
	}
	computedPayloadDigest := rowsDigest(request.Rows)
	if request.PayloadDigest != "" && request.PayloadDigest != computedPayloadDigest {
		return domain.AppendResult{}, domain.NewError(domain.ErrorInvalidArgument, operation, errors.New("payload digest does not match ProtoRows"))
	}
	startOffset := record.Stream.NextOffset
	now := s.clock.Now()
	receipt := domain.AppendReceipt{
		StreamName: canonical, StartOffset: startOffset, RowCount: int64(len(request.Rows)),
		StagedBytes:       int64(rowsBytes(request.Rows) + len(request.Rows)),
		SchemaFingerprint: fingerprint, PayloadDigest: computedPayloadDigest,
		State: domain.AppendReceiptPrepared, CreatedAt: now, UpdatedAt: now,
	}
	preparedRecord := record
	preparedHere := false
	if record.Operation == domain.StreamOperationAppend {
		preparedOffset, parseErr := strconv.ParseInt(record.OperationToken, 10, 64)
		if parseErr != nil {
			return domain.AppendResult{}, domain.NewError(domain.ErrorInternal, operation, errors.New("persisted append operation token is invalid"))
		}
		receipt, err = s.repository.GetWriteAppendReceipt(ctx, canonical, preparedOffset)
		if err != nil {
			return domain.AppendResult{}, domain.NewError(domain.ErrorInternal, operation, err)
		}
		if !receiptMatchesRequest(receipt, request.Offset, fingerprint, computedPayloadDigest, int64(len(request.Rows))) {
			return domain.AppendResult{}, domain.NewError(domain.ErrorFailedPrecondition, operation,
				errors.New("an unresolved append must be retried with the same offset, schema, and payload"))
		}
		startOffset = receipt.StartOffset
	} else {
		if record.Operation != domain.StreamOperationNone {
			return domain.AppendResult{}, domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("write stream has another operation in progress"))
		}
		if request.Offset != nil && *request.Offset < startOffset {
			return domain.AppendResult{}, domain.NewError(domain.ErrorAlreadyExists, operation, errors.New("append offset already exists"))
		}
		if request.Offset != nil && *request.Offset > startOffset {
			return domain.AppendResult{}, domain.NewError(domain.ErrorOutOfRange, operation, errors.New("append offset is beyond stream end"))
		}
		preparedRecord.Operation = domain.StreamOperationAppend
		preparedRecord.OperationToken = strconv.FormatInt(startOffset, 10)
		preparedRecord.WriterDescriptor = slices.Clone(descriptor)
		preparedRecord.Stream.SchemaFingerprint = fingerprint
		preparedRecord.Stream.LastActivity = now
		preparedRecord.Revision++
		if err := s.repository.PrepareAppend(ctx, record.Revision, preparedRecord, receipt); err != nil {
			if errors.Is(err, ports.ErrStreamConflict) || errors.Is(err, ports.ErrReceiptConflict) {
				return domain.AppendResult{}, domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("write stream changed concurrently; retry the append"))
			}
			return domain.AppendResult{}, domain.NewError(domain.ErrorInternal, operation, err)
		}
		preparedHere = true
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
		if appendOutcomeIsAmbiguous(err) {
			s.logger.WarnContext(ctx, "pending append acknowledgement is unresolved",
				"event", "domain.transition", "operation", operation,
				"model_version", s.config.ProtocolModelVersion,
				"stream_fingerprint", digest([]byte(canonical)), "start_offset", startOffset,
				"row_count", len(request.Rows), "schema_fingerprint", fingerprint,
				"payload_digest", batch.PayloadDigest, "state_after", "append_unacknowledged")
		} else if preparedHere && !isDefault {
			abort := record
			abort.Stream.LastActivity = s.clock.Now()
			abort.Revision = preparedRecord.Revision + 1
			abort.Operation = domain.StreamOperationNone
			abort.OperationToken = ""
			compensationContext, cancel := boundedCompensationContext(ctx)
			abortErr := s.repository.AbortAppend(compensationContext, preparedRecord.Revision, abort, receipt)
			cancel()
			if abortErr != nil {
				s.logger.ErrorContext(ctx, "failed to abort rejected Storage Write append", errorLogAttrs(abortErr)...)
			}
		}
		code := coordinatorErrorCode(err, domain.ErrorInternal)
		return domain.AppendResult{}, domain.NewError(code, operation, err)
	}
	completed := preparedRecord
	completed.Operation = domain.StreamOperationNone
	completed.OperationToken = ""
	completed.Stream.RowCount += receipt.RowCount
	completed.Stream.NextOffset += receipt.RowCount
	completed.Stream.LastActivity = s.clock.Now()
	completed.Revision++
	receipt.State = domain.AppendReceiptApplied
	receipt.UpdatedAt = completed.Stream.LastActivity
	if err := s.repository.CompleteAppend(ctx, preparedRecord.Revision, completed, receipt); err != nil {
		return domain.AppendResult{}, domain.NewError(domain.ErrorInternal, operation,
			fmt.Errorf("physical append succeeded but canonical receipt remains prepared: %w", err))
	}
	if isDefault {
		ackContext, cancel := boundedCompensationContext(ctx)
		ackErr := s.coordinator.AcknowledgeApplied(ackContext, []string{canonical})
		cancel()
		if ackErr != nil {
			s.logger.WarnContext(ctx, "deferred cleanup of acknowledged default stream rows", errorLogAttrs(ackErr)...)
		}
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
	_, canonical, isDefault, err := domain.ParseStreamName(name)
	if err != nil {
		return 0, domain.NewError(domain.ErrorInvalidArgument, operation, err)
	}
	if isDefault {
		return 0, domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("the default stream cannot be finalized"))
	}
	record, err := s.lookup(ctx, canonical, operation)
	if err != nil {
		return 0, err
	}
	if record.CleanupState != domain.CleanupStateActive {
		return 0, domain.NewError(domain.ErrorNotFound, operation, errors.New("write stream was discarded"))
	}
	if record.Stream.State == domain.StreamStateCommitted {
		return 0, domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("write stream is already committed"))
	}
	if record.Operation != domain.StreamOperationNone {
		return 0, domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("an unacknowledged append must be reconciled before finalizing the stream"))
	}
	if record.Stream.State == domain.StreamStateFinalized {
		return record.Stream.RowCount, nil
	}
	expected := record.Revision
	record.Stream.State = domain.StreamStateFinalized
	record.Stream.LastActivity = s.clock.Now()
	record.Revision++
	if err := s.repository.SaveWriteStream(ctx, expected, record); err != nil {
		if errors.Is(err, ports.ErrStreamConflict) {
			return 0, domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("write stream changed concurrently; retry finalization"))
		}
		return 0, domain.NewError(domain.ErrorInternal, operation, err)
	}
	return record.Stream.RowCount, nil
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
	unique := make(map[string]struct{}, len(names))
	records := make([]domain.StreamRecord, 0, len(names))
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
		record, lookupErr := s.lookup(ctx, canonical, operation)
		if lookupErr != nil {
			streamErrors = append(streamErrors, domain.StreamError{Code: domain.StreamNotFound, Stream: canonical, Message: "stream not found"})
			continue
		}
		records = append(records, record)
	}
	if len(streamErrors) > 0 {
		return domain.BatchCommitResult{StreamErrors: streamErrors}, nil
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Stream.Name < records[j].Stream.Name })
	canonicalNames := make([]string, len(records))
	for index, record := range records {
		canonicalNames[index] = record.Stream.Name
	}
	commitToken := digest([]byte(strings.Join(canonicalNames, "\n")))
	for _, record := range records {
		switch {
		case record.CleanupState != domain.CleanupStateActive:
			streamErrors = append(streamErrors, domain.StreamError{Code: domain.StreamNotFound, Stream: record.Stream.Name, Message: "stream was discarded"})
		case record.Stream.Type != domain.StreamTypePending:
			streamErrors = append(streamErrors, domain.StreamError{Code: domain.InvalidStreamType, Stream: record.Stream.Name, Message: "stream is not PENDING"})
		case record.Stream.State == domain.StreamStateCommitted:
			streamErrors = append(streamErrors, domain.StreamError{Code: domain.StreamAlreadyCommitted, Stream: record.Stream.Name, Message: "stream is already committed"})
		case record.Stream.State != domain.StreamStateFinalized:
			streamErrors = append(streamErrors, domain.StreamError{Code: domain.InvalidStreamState, Stream: record.Stream.Name, Message: "stream must be finalized before commit"})
		case record.Operation != domain.StreamOperationNone && (record.Operation != domain.StreamOperationCommit || record.OperationToken != commitToken):
			streamErrors = append(streamErrors, domain.StreamError{Code: domain.InvalidStreamState, Stream: record.Stream.Name, Message: "stream has another operation in progress"})
		}
	}
	if len(streamErrors) > 0 {
		return domain.BatchCommitResult{StreamErrors: streamErrors}, nil
	}
	var rowCount int64
	rowCounts := make(map[string]int64, len(records))
	prepared := make([]domain.StreamRecord, len(records))
	expected := make(map[string]int64, len(records))
	allPrepared := true
	for index, record := range records {
		rowCount += record.Stream.RowCount
		rowCounts[record.Stream.Name] = record.Stream.RowCount
		expected[record.Stream.Name] = record.Revision
		prepared[index] = record
		if record.Operation == domain.StreamOperationNone {
			allPrepared = false
			prepared[index].Operation = domain.StreamOperationCommit
			prepared[index].OperationToken = commitToken
			prepared[index].Stream.LastActivity = s.clock.Now()
			prepared[index].Revision++
		}
	}
	if !allPrepared {
		for _, record := range records {
			if record.Operation != domain.StreamOperationNone {
				return domain.BatchCommitResult{}, domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("commit preparation is only partially persisted"))
			}
		}
		if err := s.repository.SaveWriteStreams(ctx, expected, prepared); err != nil {
			if errors.Is(err, ports.ErrStreamConflict) {
				return domain.BatchCommitResult{}, domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("write streams changed concurrently; retry commit"))
			}
			return domain.BatchCommitResult{}, domain.NewError(domain.ErrorInternal, operation, err)
		}
	}
	s.logger.InfoContext(ctx, "committing pending write streams",
		"event", "side_effect.before", "side_effect", "coordinator.commit_pending",
		"operation", operation, "model_version", s.config.ProtocolModelVersion,
		"table", parent.Name(), "stream_count", len(records), "row_count", rowCount,
		"stream_set_fingerprint", digest([]byte(strings.Join(canonicalNames, "\n"))),
		"tx_state", "begin")
	err := s.coordinator.CommitPending(ctx, ports.CommitRequest{Parent: parent, StreamNames: canonicalNames, RowCounts: rowCounts})
	s.logCommitEnd(ctx, operation, parent.Name(), canonicalNames, rowCount, err)
	if err != nil {
		if !appendOutcomeIsAmbiguous(err) {
			rollbackExpected := make(map[string]int64, len(prepared))
			rollback := make([]domain.StreamRecord, len(prepared))
			for index, record := range prepared {
				rollbackExpected[record.Stream.Name] = record.Revision
				rollback[index] = record
				rollback[index].Operation = domain.StreamOperationNone
				rollback[index].OperationToken = ""
				rollback[index].Revision++
			}
			compensationContext, cancel := boundedCompensationContext(ctx)
			rollbackErr := s.repository.SaveWriteStreams(compensationContext, rollbackExpected, rollback)
			cancel()
			if rollbackErr != nil {
				s.logger.ErrorContext(ctx, "failed to abort rejected Storage Write commit", errorLogAttrs(rollbackErr)...)
			}
		}
		return domain.BatchCommitResult{}, domain.NewError(coordinatorErrorCode(err, domain.ErrorInternal), operation, err)
	}
	// The response time represents successful atomic visibility, so capture it
	// only after the coordinator transaction acknowledges its commit.
	// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#batchcommitwritestreamsresponse
	commitTime := s.clock.Now()
	completeExpected := make(map[string]int64, len(prepared))
	completed := make([]domain.StreamRecord, len(prepared))
	for index, record := range prepared {
		completeExpected[record.Stream.Name] = record.Revision
		completed[index] = record
		completed[index].Stream.State = domain.StreamStateCommitted
		completed[index].Stream.CommitTime = cloneTime(&commitTime)
		completed[index].Stream.LastActivity = commitTime
		completed[index].Operation = domain.StreamOperationNone
		completed[index].OperationToken = ""
		completed[index].Revision++
	}
	if err := s.repository.SaveWriteStreams(ctx, completeExpected, completed); err != nil {
		return domain.BatchCommitResult{}, domain.NewError(domain.ErrorInternal, operation,
			fmt.Errorf("physical commit succeeded but canonical streams remain prepared: %w", err))
	}
	ackContext, cancel := boundedCompensationContext(ctx)
	ackErr := s.coordinator.AcknowledgeApplied(ackContext, canonicalNames)
	cancel()
	if ackErr != nil {
		s.logger.WarnContext(ctx, "deferred cleanup of committed Storage Write rows", errorLogAttrs(ackErr)...)
	}
	return domain.BatchCommitResult{CommitTime: cloneTime(&commitTime)}, nil
}

func (s *Service) lookup(ctx context.Context, name, operation string) (domain.StreamRecord, error) {
	if s.closed.Load() {
		return domain.StreamRecord{}, domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("Storage Write service is closed"))
	}
	record, err := s.repository.GetWriteStream(ctx, name)
	if errors.Is(err, ports.ErrStreamNotFound) {
		return domain.StreamRecord{}, domain.NewError(domain.ErrorNotFound, operation, errors.New("write stream not found"))
	}
	if err != nil {
		return domain.StreamRecord{}, domain.NewError(domain.ErrorInternal, operation, err)
	}
	return record, nil
}

func (s *Service) lookupOrCreateDefault(ctx context.Context, operation string, table domain.TableReference, canonical string, isDefault bool) (domain.StreamRecord, error) {
	if !isDefault {
		return s.lookup(ctx, canonical, operation)
	}
	if s.closed.Load() {
		return domain.StreamRecord{}, domain.NewError(domain.ErrorFailedPrecondition, operation, errors.New("Storage Write service is closed"))
	}
	existing, err := s.repository.GetWriteStream(ctx, canonical)
	if err == nil {
		return existing, nil
	}
	if !errors.Is(err, ports.ErrStreamNotFound) {
		return domain.StreamRecord{}, domain.NewError(domain.ErrorInternal, operation, err)
	}
	schema, err := s.describeTable(ctx, operation, table)
	if err != nil {
		return domain.StreamRecord{}, err
	}
	now := s.clock.Now()
	commitTime := now
	candidate := domain.StreamRecord{Stream: domain.WriteStream{
		Name: canonical, Parent: table, Type: domain.StreamTypeDefault,
		State: domain.StreamStateCommitted, CreateTime: now, CommitTime: &commitTime,
		LastActivity: now, Location: s.config.Location, Schema: cloneSchema(schema),
		TableFingerprint: schemaDigest(schema),
	}, CleanupState: domain.CleanupStateActive, Revision: 1}
	if err := s.repository.CreateWriteStream(ctx, candidate, 0); err != nil {
		if errors.Is(err, ports.ErrStreamExists) {
			return s.repository.GetWriteStream(ctx, canonical)
		}
		return domain.StreamRecord{}, domain.NewError(domain.ErrorInternal, operation, err)
	}
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
		if field.Precision != nil {
			precision := *field.Precision
			result[index].Precision = &precision
		}
		if field.Scale != nil {
			scale := *field.Scale
			result[index].Scale = &scale
		}
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

func receiptMatchesRequest(receipt domain.AppendReceipt, requestedOffset *int64, schemaFingerprint, payloadDigest string, rowCount int64) bool {
	if requestedOffset != nil && *requestedOffset != receipt.StartOffset {
		return false
	}
	return receipt.RowCount == rowCount && receipt.SchemaFingerprint == schemaFingerprint && receipt.PayloadDigest == payloadDigest
}

func boundedCompensationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
}

func schemaDigest(schema domain.TableSchema) string {
	payload, err := json.Marshal(schema)
	if err != nil {
		return digest([]byte(fmt.Sprintf("%v", schema.Fields)))
	}
	return digest(payload)
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
