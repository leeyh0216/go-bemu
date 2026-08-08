package grpcserver

// This adapter binds the official generated BigQueryWrite service to the
// transport-neutral Storage Write application. AppendRows is connection-local:
// the first request supplies a stream and ProtoSchema, while later requests may
// inherit them. Per-request append failures are embedded in ordered responses so
// the bidi RPC remains usable for an adjusted offset retry.
//
// Official RPC/protobuf contract:
// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite
// Append inheritance and response ordering:
// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#appendrowsrequest
// Spark 0.44.2 ProtoRows writer:
// https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryDirectDataWriterHelper.java

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	writeapp "github.com/leeyh0216/go-bemu/internal/storagewrite/application"
	writedomain "github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
)

// StorageWriteServer is registered independently from the existing read
// server. Wiring may therefore enable Write health only after all outbound
// dependencies are ready.
type StorageWriteServer struct {
	storagepb.UnimplementedBigQueryWriteServer
	write *writeapp.Service
	// appendSlots is acquired before Recv so concurrent protobuf decode, clone,
	// and digest memory is bounded across long-lived bidi connections.
	appendSlots chan struct{}
}

var _ storagepb.BigQueryWriteServer = (*StorageWriteServer)(nil)

func NewStorageWriteServer(service *writeapp.Service) *StorageWriteServer {
	server := &StorageWriteServer{write: service}
	if service != nil {
		server.appendSlots = make(chan struct{}, service.MaxConcurrentAppendRequests())
	}
	return server
}

func (s *StorageWriteServer) CreateWriteStream(ctx context.Context, request *storagepb.CreateWriteStreamRequest) (*storagepb.WriteStream, error) {
	if s.write == nil {
		return nil, grpcstatus.Error(codes.Unimplemented, "Storage Write is not configured")
	}
	if request == nil || request.GetWriteStream() == nil {
		return nil, grpcstatus.Error(codes.InvalidArgument, "write_stream is required")
	}
	requested := request.GetWriteStream()
	if requested.GetType() != storagepb.WriteStream_PENDING {
		return nil, grpcstatus.Error(codes.Unimplemented, "only PENDING write streams are supported")
	}
	if requested.GetWriteMode() != storagepb.WriteStream_WRITE_MODE_UNSPECIFIED && requested.GetWriteMode() != storagepb.WriteStream_INSERT {
		return nil, grpcstatus.Error(codes.Unimplemented, "only INSERT write mode is supported; CDC is "+writedomain.GapCDC)
	}
	parent, err := writedomain.ParseTableName(request.GetParent())
	if err != nil {
		return nil, grpcstatus.Error(codes.InvalidArgument, "invalid parent table resource")
	}
	stream, err := s.write.CreateStream(ctx, writedomain.CreateStreamRequest{Parent: parent, Type: writedomain.StreamTypePending})
	if err != nil {
		return nil, storageWriteStatus(err)
	}
	return writeStreamToProto(stream, true), nil
}

func (s *StorageWriteServer) GetWriteStream(ctx context.Context, request *storagepb.GetWriteStreamRequest) (*storagepb.WriteStream, error) {
	if s.write == nil {
		return nil, grpcstatus.Error(codes.Unimplemented, "Storage Write is not configured")
	}
	if request == nil || request.GetName() == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "name is required")
	}
	if request.GetView() != storagepb.WriteStreamView_WRITE_STREAM_VIEW_UNSPECIFIED &&
		request.GetView() != storagepb.WriteStreamView_BASIC && request.GetView() != storagepb.WriteStreamView_FULL {
		return nil, grpcstatus.Error(codes.InvalidArgument, "unsupported write stream view")
	}
	stream, err := s.write.GetStream(ctx, request.GetName())
	if err != nil {
		return nil, storageWriteStatus(err)
	}
	return writeStreamToProto(stream, request.GetView() == storagepb.WriteStreamView_FULL), nil
}

func (s *StorageWriteServer) FinalizeWriteStream(ctx context.Context, request *storagepb.FinalizeWriteStreamRequest) (*storagepb.FinalizeWriteStreamResponse, error) {
	if s.write == nil {
		return nil, grpcstatus.Error(codes.Unimplemented, "Storage Write is not configured")
	}
	if request == nil || request.GetName() == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "name is required")
	}
	rowCount, err := s.write.Finalize(ctx, request.GetName())
	if err != nil {
		return nil, storageWriteStatus(err)
	}
	return &storagepb.FinalizeWriteStreamResponse{RowCount: rowCount}, nil
}

func (s *StorageWriteServer) BatchCommitWriteStreams(ctx context.Context, request *storagepb.BatchCommitWriteStreamsRequest) (*storagepb.BatchCommitWriteStreamsResponse, error) {
	if s.write == nil {
		return nil, grpcstatus.Error(codes.Unimplemented, "Storage Write is not configured")
	}
	if request == nil {
		return nil, grpcstatus.Error(codes.InvalidArgument, "request is required")
	}
	parent, err := writedomain.ParseTableName(request.GetParent())
	if err != nil {
		return nil, grpcstatus.Error(codes.InvalidArgument, "invalid parent table resource")
	}
	result, err := s.write.BatchCommit(ctx, parent, request.GetWriteStreams())
	if err != nil {
		return nil, storageWriteStatus(err)
	}
	response := &storagepb.BatchCommitWriteStreamsResponse{}
	if result.CommitTime != nil {
		response.CommitTime = timestamppb.New(*result.CommitTime)
	}
	for _, streamError := range result.StreamErrors {
		response.StreamErrors = append(response.StreamErrors, &storagepb.StorageError{
			Code: storageErrorCodeToProto(streamError.Code), Entity: streamError.Stream,
			ErrorMessage: streamError.Message,
		})
	}
	return response, nil
}

// AppendRows maintains only connection-scoped protocol state. Durable schema,
// offset, and stream state remain application-owned. Invalid requests receive
// exactly one ordered error response; EOF ends the bidi stream normally.
func (s *StorageWriteServer) AppendRows(stream storagepb.BigQueryWrite_AppendRowsServer) error {
	if s.write == nil {
		return grpcstatus.Error(codes.Unimplemented, "Storage Write is not configured")
	}
	var connection appendConnection
	for {
		release, err := s.acquireAppendSlot(stream.Context())
		if err != nil {
			return storageWriteStatus(err)
		}
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			release()
			return nil
		}
		if err != nil {
			release()
			return storageWriteStatus(err)
		}
		converted, next, conversionErr := connection.convert(request)
		if conversionErr != nil {
			sendErr := stream.Send(appendErrorResponse(connection.responseStream(request), conversionErr))
			release()
			if sendErr != nil {
				return sendErr
			}
			continue
		}
		// A structurally valid request establishes connection-local inheritance
		// even when its offset is rejected and the caller retries in-place.
		connection = next
		result, appendErr := s.write.Append(stream.Context(), converted)
		if appendErr != nil {
			sendErr := stream.Send(appendErrorResponse(connection.streamName, appendErr))
			release()
			if sendErr != nil {
				return sendErr
			}
			continue
		}
		response := &storagepb.AppendRowsResponse{
			WriteStream: result.StreamName,
			Response:    &storagepb.AppendRowsResponse_AppendResult_{AppendResult: &storagepb.AppendRowsResponse_AppendResult{}},
		}
		if result.HasOffset {
			response.GetAppendResult().Offset = wrapperspb.Int64(result.StartOffset)
		}
		err = stream.Send(response)
		release()
		if err != nil {
			return err
		}
	}
}

func (s *StorageWriteServer) acquireAppendSlot(ctx context.Context) (func(), error) {
	if s.appendSlots == nil {
		return func() {}, nil
	}
	select {
	case s.appendSlots <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-s.appendSlots }) }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type appendConnection struct {
	streamName string
	descriptor []byte
	traceID    string
}

func (c appendConnection) convert(request *storagepb.AppendRowsRequest) (writedomain.AppendRequest, appendConnection, error) {
	if request == nil {
		return writedomain.AppendRequest{}, c, writedomain.NewError(writedomain.ErrorInvalidArgument, "storage_write.append", errors.New("request is required"))
	}
	streamName := request.GetWriteStream()
	if streamName == "" {
		streamName = c.streamName
	}
	if streamName == "" {
		return writedomain.AppendRequest{}, c, writedomain.NewError(writedomain.ErrorInvalidArgument, "storage_write.append", errors.New("write_stream is required on the first request"))
	}
	_, canonical, _, err := writedomain.ParseStreamName(streamName)
	if err != nil {
		return writedomain.AppendRequest{}, c, writedomain.NewError(writedomain.ErrorInvalidArgument, "storage_write.append", err)
	}
	protoData := request.GetProtoRows()
	if protoData == nil {
		return writedomain.AppendRequest{}, c, writedomain.NewError(writedomain.ErrorUnimplemented, "storage_write.append", errors.New("only ProtoRows are supported"))
	}
	if protoData.GetRows() == nil || len(protoData.GetRows().GetSerializedRows()) == 0 {
		return writedomain.AppendRequest{}, c, writedomain.NewError(writedomain.ErrorInvalidArgument, "storage_write.append", errors.New("serialized ProtoRows are required"))
	}
	if len(request.GetMissingValueInterpretations()) != 0 || request.GetDefaultMissingValueInterpretation() != storagepb.AppendRowsRequest_MISSING_VALUE_INTERPRETATION_UNSPECIFIED {
		return writedomain.AppendRequest{}, c, writedomain.NewError(writedomain.ErrorUnimplemented, "storage_write.append", errors.New("missing-value default expressions are not implemented"))
	}
	destinationChanged := c.streamName != "" && canonical != c.streamName
	descriptor := c.descriptor
	if writerSchema := protoData.GetWriterSchema(); writerSchema != nil {
		if writerSchema.GetProtoDescriptor() == nil {
			return writedomain.AppendRequest{}, c, writedomain.NewError(writedomain.ErrorInvalidArgument, "storage_write.append", errors.New("proto_descriptor is required"))
		}
		descriptor, err = proto.Marshal(writerSchema.GetProtoDescriptor())
		if err != nil {
			return writedomain.AppendRequest{}, c, writedomain.NewError(writedomain.ErrorInvalidArgument, "storage_write.append", err)
		}
	} else if len(descriptor) == 0 || destinationChanged {
		return writedomain.AppendRequest{}, c, writedomain.NewError(writedomain.ErrorInvalidArgument, "storage_write.append", errors.New("writer_schema is required on the first request and after changing destination"))
	}
	traceID := c.traceID
	if c.streamName == "" {
		traceID = request.GetTraceId()
	}
	next := appendConnection{streamName: canonical, descriptor: append([]byte(nil), descriptor...), traceID: traceID}
	return writedomain.AppendRequest{
		StreamName: canonical, Offset: cloneOffset(request.GetOffset()),
		Descriptor: append([]byte(nil), descriptor...), Rows: cloneProtoRows(protoData.GetRows().GetSerializedRows()),
		PayloadBytes: proto.Size(request.GetProtoRows()), WireBytes: proto.Size(request),
		SchemaFingerprint: digestBytes(descriptor),
		PayloadDigest:     digestRows(protoData.GetRows().GetSerializedRows()), TraceID: traceID,
	}, next, nil
}

func (c appendConnection) responseStream(request *storagepb.AppendRowsRequest) string {
	if request != nil && request.GetWriteStream() != "" {
		if _, canonical, _, err := writedomain.ParseStreamName(request.GetWriteStream()); err == nil {
			return canonical
		}
		return request.GetWriteStream()
	}
	return c.streamName
}

func appendErrorResponse(stream string, err error) *storagepb.AppendRowsResponse {
	code := storageWriteCode(err)
	message := storageWriteMessage(err)
	statusMessage := &statuspb.Status{Code: int32(code), Message: message}
	if detail := appendStorageError(err, stream); detail != nil {
		if packed, packErr := anypb.New(detail); packErr == nil {
			statusMessage.Details = append(statusMessage.Details, packed)
		}
	}
	return &storagepb.AppendRowsResponse{
		WriteStream: stream,
		Response:    &storagepb.AppendRowsResponse_Error{Error: statusMessage},
	}
}

func appendStorageError(err error, stream string) *storagepb.StorageError {
	var code storagepb.StorageError_StorageErrorCode
	switch writedomain.CodeOf(err) {
	case writedomain.ErrorAlreadyExists:
		code = storagepb.StorageError_OFFSET_ALREADY_EXISTS
	case writedomain.ErrorOutOfRange:
		code = storagepb.StorageError_OFFSET_OUT_OF_RANGE
	case writedomain.ErrorNotFound:
		code = storagepb.StorageError_STREAM_NOT_FOUND
	case writedomain.ErrorFailedPrecondition:
		code = storagepb.StorageError_STREAM_FINALIZED
	default:
		return nil
	}
	return &storagepb.StorageError{Code: code, Entity: stream, ErrorMessage: storageWriteMessage(err)}
}

func writeStreamToProto(stream writedomain.WriteStream, includeSchema bool) *storagepb.WriteStream {
	result := &storagepb.WriteStream{
		Name: stream.Name, Type: streamTypeToProto(stream.Type),
		CreateTime: timestamppb.New(stream.CreateTime), Location: stream.Location,
		WriteMode: storagepb.WriteStream_INSERT,
	}
	if stream.CommitTime != nil {
		result.CommitTime = timestamppb.New(*stream.CommitTime)
	}
	if includeSchema {
		result.TableSchema = tableSchemaToProto(stream.Schema)
	}
	return result
}

func tableSchemaToProto(schema writedomain.TableSchema) *storagepb.TableSchema {
	result := &storagepb.TableSchema{Fields: make([]*storagepb.TableFieldSchema, 0, len(schema.Fields))}
	for _, field := range schema.Fields {
		result.Fields = append(result.Fields, fieldToProto(field))
	}
	return result
}

func fieldToProto(field writedomain.Field) *storagepb.TableFieldSchema {
	result := &storagepb.TableFieldSchema{
		Name: field.Name, Type: fieldTypeToProto(field.Type), Mode: fieldModeToProto(field.Mode),
		Description: field.Description,
	}
	if field.Precision != nil {
		result.Precision = *field.Precision
	}
	if field.Scale != nil {
		result.Scale = *field.Scale
	}
	for _, nested := range field.Fields {
		result.Fields = append(result.Fields, fieldToProto(nested))
	}
	return result
}

func fieldTypeToProto(value string) storagepb.TableFieldSchema_Type {
	switch strings.ToUpper(value) {
	case "BOOL", "BOOLEAN":
		return storagepb.TableFieldSchema_BOOL
	case "INT64", "INTEGER":
		return storagepb.TableFieldSchema_INT64
	case "FLOAT64", "FLOAT", "DOUBLE":
		return storagepb.TableFieldSchema_DOUBLE
	case "STRUCT", "RECORD":
		return storagepb.TableFieldSchema_STRUCT
	case "BYTES":
		return storagepb.TableFieldSchema_BYTES
	case "TIMESTAMP":
		return storagepb.TableFieldSchema_TIMESTAMP
	case "DATE":
		return storagepb.TableFieldSchema_DATE
	case "TIME":
		return storagepb.TableFieldSchema_TIME
	case "DATETIME":
		return storagepb.TableFieldSchema_DATETIME
	case "NUMERIC":
		return storagepb.TableFieldSchema_NUMERIC
	case "BIGNUMERIC":
		return storagepb.TableFieldSchema_BIGNUMERIC
	case "JSON":
		return storagepb.TableFieldSchema_JSON
	case "STRING":
		return storagepb.TableFieldSchema_STRING
	default:
		return storagepb.TableFieldSchema_TYPE_UNSPECIFIED
	}
}

func fieldModeToProto(value string) storagepb.TableFieldSchema_Mode {
	switch strings.ToUpper(value) {
	case "REQUIRED":
		return storagepb.TableFieldSchema_REQUIRED
	case "REPEATED":
		return storagepb.TableFieldSchema_REPEATED
	default:
		return storagepb.TableFieldSchema_NULLABLE
	}
}

func streamTypeToProto(value writedomain.StreamType) storagepb.WriteStream_Type {
	if value == writedomain.StreamTypePending {
		return storagepb.WriteStream_PENDING
	}
	return storagepb.WriteStream_COMMITTED
}

func storageErrorCodeToProto(value writedomain.StreamErrorCode) storagepb.StorageError_StorageErrorCode {
	switch value {
	case writedomain.StreamAlreadyCommitted:
		return storagepb.StorageError_STREAM_ALREADY_COMMITTED
	case writedomain.StreamNotFound:
		return storagepb.StorageError_STREAM_NOT_FOUND
	case writedomain.InvalidStreamType:
		return storagepb.StorageError_INVALID_STREAM_TYPE
	case writedomain.InvalidStreamState:
		return storagepb.StorageError_INVALID_STREAM_STATE
	case writedomain.StreamFinalized:
		return storagepb.StorageError_STREAM_FINALIZED
	case writedomain.OffsetAlreadyExists:
		return storagepb.StorageError_OFFSET_ALREADY_EXISTS
	case writedomain.OffsetOutOfRange:
		return storagepb.StorageError_OFFSET_OUT_OF_RANGE
	default:
		return storagepb.StorageError_STORAGE_ERROR_CODE_UNSPECIFIED
	}
}

func storageWriteStatus(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return grpcstatus.Error(codes.Canceled, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return grpcstatus.Error(codes.DeadlineExceeded, err.Error())
	}
	if _, ok := grpcstatus.FromError(err); ok {
		return err
	}
	return grpcstatus.Error(storageWriteCode(err), storageWriteMessage(err))
}

func storageWriteCode(err error) codes.Code {
	switch writedomain.CodeOf(err) {
	case writedomain.ErrorInvalidArgument:
		return codes.InvalidArgument
	case writedomain.ErrorNotFound:
		return codes.NotFound
	case writedomain.ErrorFailedPrecondition:
		return codes.FailedPrecondition
	case writedomain.ErrorResourceExhausted:
		return codes.ResourceExhausted
	case writedomain.ErrorCanceled:
		return codes.Canceled
	case writedomain.ErrorDeadlineExceeded:
		return codes.DeadlineExceeded
	case writedomain.ErrorAlreadyExists:
		return codes.AlreadyExists
	case writedomain.ErrorOutOfRange:
		return codes.OutOfRange
	case writedomain.ErrorUnimplemented:
		return codes.Unimplemented
	default:
		return codes.Internal
	}
}

func storageWriteMessage(err error) string {
	return err.Error()
}

func cloneOffset(value *wrapperspb.Int64Value) *int64 {
	if value == nil {
		return nil
	}
	result := value.GetValue()
	return &result
}

func cloneProtoRows(rows [][]byte) [][]byte {
	result := make([][]byte, len(rows))
	for index, row := range rows {
		result[index] = append([]byte(nil), row...)
	}
	return result
}

func digestBytes(value []byte) string {
	return fmt.Sprintf("sha256:%x", sha256Sum(value))
}

func digestRows(rows [][]byte) string {
	joined := make([]byte, 0)
	for _, row := range rows {
		joined = append(joined, row...)
	}
	return digestBytes(joined)
}

func sha256Sum(value []byte) [32]byte {
	return sha256.Sum256(value)
}
