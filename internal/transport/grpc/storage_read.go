package grpcserver

// The adapter uses the official generated Storage API service and message
// definitions. It converts protobuf at the boundary; application and snapshot
// packages remain independent of Google-generated types.
//
// Protocol sources:
//   - BigQueryRead RPC service: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryRead
//   - CreateReadSessionRequest: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#createreadsessionrequest
//   - ReadRowsResponse row_count/schema rules: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readrowsresponse

import (
	"context"
	"errors"
	"fmt"
	"time"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/leeyh0216/go-bemu/internal/storageread/domain"
)

// maxRowRestrictionBytes is a wire-contract limit, not an operational default.
// Source: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readsession.tablereadoptions
const maxRowRestrictionBytes = 1 << 20

const storageReadCreateOperation = "storage_read.create_session"

// The fields are part of the official TableReadOptions contract, so rejecting
// a recognized but unavailable feature is UNIMPLEMENTED rather than treating
// the request as malformed. INVALID_ARGUMENT is still used for inconsistent
// combinations such as Arrow options with AVRO output.
//
// Sources:
//   - options: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readsession.tablereadoptions
//   - UNIMPLEMENTED: https://grpc.io/docs/guides/status-codes/
func unsupportedStorageReadOption(message string) error {
	return domain.NewError(domain.ErrorUnimplemented, storageReadCreateOperation, errors.New(message))
}

func (s *StorageServer) CreateReadSession(ctx context.Context, request *storagepb.CreateReadSessionRequest) (*storagepb.ReadSession, error) {
	if s.read == nil {
		return nil, status.Error(codes.Unimplemented, "Storage Read is not configured")
	}
	domainRequest, err := createSessionRequestFromProto(request)
	if err != nil {
		var classified *domain.Error
		if errors.As(err, &classified) {
			return nil, storageReadStatus(err)
		}
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	session, err := s.read.CreateSession(ctx, domainRequest)
	if err != nil {
		return nil, storageReadStatus(err)
	}
	return readSessionToProto(session), nil
}

func (s *StorageServer) ReadRows(request *storagepb.ReadRowsRequest, stream storagepb.BigQueryRead_ReadRowsServer) error {
	if s.read == nil {
		return status.Error(codes.Unimplemented, "Storage Read is not configured")
	}
	if request == nil {
		return status.Error(codes.InvalidArgument, "ReadRows request is required")
	}
	err := s.read.ReadRows(stream.Context(), domain.ReadRowsRequest{
		StreamName: request.GetReadStream(),
		Offset:     request.GetOffset(),
	}, func(chunk domain.ReadChunk) error {
		return stream.Send(readChunkToProto(chunk))
	})
	return storageReadStatus(err)
}

func createSessionRequestFromProto(request *storagepb.CreateReadSessionRequest) (domain.CreateSessionRequest, error) {
	if request == nil || request.GetReadSession() == nil {
		return domain.CreateSessionRequest{}, errors.New("read_session is required")
	}
	readSession := request.GetReadSession()
	format, err := formatFromProto(readSession.GetDataFormat())
	if err != nil {
		return domain.CreateSessionRequest{}, err
	}
	options := readSession.GetReadOptions()
	if options != nil {
		if len(options.GetRowRestriction()) > maxRowRestrictionBytes {
			return domain.CreateSessionRequest{}, fmt.Errorf("row_restriction exceeds %d bytes", maxRowRestrictionBytes)
		}
		if options.SamplePercentage != nil {
			return domain.CreateSessionRequest{}, unsupportedStorageReadOption("sample_percentage is not implemented")
		}
		if options.GetResponseCompressionCodec() != storagepb.ReadSession_TableReadOptions_RESPONSE_COMPRESSION_CODEC_UNSPECIFIED {
			return domain.CreateSessionRequest{}, unsupportedStorageReadOption("response compression is not implemented")
		}
		if arrow := options.GetArrowSerializationOptions(); arrow != nil {
			if format != domain.FormatArrow {
				return domain.CreateSessionRequest{}, errors.New("Arrow serialization options require ARROW format")
			}
			if arrow.GetBufferCompression() != storagepb.ArrowSerializationOptions_COMPRESSION_UNSPECIFIED ||
				arrow.GetPicosTimestampPrecision() != storagepb.ArrowSerializationOptions_PICOS_TIMESTAMP_PRECISION_UNSPECIFIED {
				return domain.CreateSessionRequest{}, unsupportedStorageReadOption("non-default Arrow serialization options are not implemented")
			}
		}
		if avro := options.GetAvroSerializationOptions(); avro != nil {
			if format != domain.FormatAvro {
				return domain.CreateSessionRequest{}, errors.New("Avro serialization options require AVRO format")
			}
			if avro.GetEnableDisplayNameAttribute() ||
				avro.GetPicosTimestampPrecision() != storagepb.AvroSerializationOptions_PICOS_TIMESTAMP_PRECISION_UNSPECIFIED {
				return domain.CreateSessionRequest{}, unsupportedStorageReadOption("non-default Avro serialization options are not implemented")
			}
		}
	}

	var snapshotTime *time.Time
	if modifiers := readSession.GetTableModifiers(); modifiers != nil && modifiers.GetSnapshotTime() != nil {
		if err := modifiers.GetSnapshotTime().CheckValid(); err != nil {
			return domain.CreateSessionRequest{}, fmt.Errorf("invalid snapshot_time: %w", err)
		}
		value := modifiers.GetSnapshotTime().AsTime()
		snapshotTime = &value
	}
	domainRequest := domain.CreateSessionRequest{
		Parent:                  request.GetParent(),
		Table:                   readSession.GetTable(),
		Format:                  format,
		MaxStreamCount:          request.GetMaxStreamCount(),
		PreferredMinStreamCount: request.GetPreferredMinStreamCount(),
		TraceID:                 readSession.GetTraceId(),
	}
	if options != nil {
		domainRequest.SelectedFields = append([]string(nil), options.GetSelectedFields()...)
		domainRequest.RowRestriction = options.GetRowRestriction()
	}
	if snapshotTime != nil {
		domainRequest.SnapshotTime = snapshotTime
	}
	return domainRequest, nil
}

func formatFromProto(format storagepb.DataFormat) (domain.Format, error) {
	switch format {
	case storagepb.DataFormat_ARROW:
		return domain.FormatArrow, nil
	case storagepb.DataFormat_AVRO:
		return domain.FormatAvro, nil
	default:
		return domain.FormatUnspecified, errors.New("data_format must be ARROW or AVRO")
	}
}

func readSessionToProto(session domain.Session) *storagepb.ReadSession {
	response := &storagepb.ReadSession{
		Name:                       session.Name,
		ExpireTime:                 timestamppb.New(session.ExpireTime),
		DataFormat:                 formatToProto(session.Format),
		Table:                      session.Table,
		Streams:                    make([]*storagepb.ReadStream, 0, len(session.Streams)),
		EstimatedTotalBytesScanned: session.EstimatedBytesScanned,
		EstimatedRowCount:          session.EstimatedRowCount,
		TraceId:                    session.TraceID,
		ReadOptions: &storagepb.ReadSession_TableReadOptions{
			SelectedFields: append([]string(nil), session.SelectedFields...),
			RowRestriction: session.RowRestriction,
		},
	}
	for _, stream := range session.Streams {
		response.Streams = append(response.Streams, &storagepb.ReadStream{Name: stream.Name})
	}
	if session.SnapshotTime != nil {
		response.TableModifiers = &storagepb.ReadSession_TableModifiers{SnapshotTime: timestamppb.New(*session.SnapshotTime)}
	}
	setReadSessionSchema(response, session.Schema)
	return response
}

func readChunkToProto(chunk domain.ReadChunk) *storagepb.ReadRowsResponse {
	response := &storagepb.ReadRowsResponse{
		RowCount: chunk.Batch.RowCount,
		Stats: &storagepb.StreamStats{Progress: &storagepb.StreamStats_Progress{
			AtResponseStart: chunk.ProgressStart,
			AtResponseEnd:   chunk.ProgressEnd,
		}},
	}
	switch chunk.Format {
	case domain.FormatArrow:
		// SerializedRecordBatch is passed through unchanged. It is exactly one
		// IPC message supplied by the snapshot adapter, never a stream/file.
		response.Rows = &storagepb.ReadRowsResponse_ArrowRecordBatch{ArrowRecordBatch: &storagepb.ArrowRecordBatch{
			SerializedRecordBatch: chunk.Batch.SerializedRows,
		}}
	case domain.FormatAvro:
		// SerializedBinaryRows is concatenated raw datums, never an Avro object
		// container file (which would start with the Obj\x01 magic header).
		response.Rows = &storagepb.ReadRowsResponse_AvroRows{AvroRows: &storagepb.AvroRows{
			SerializedBinaryRows: chunk.Batch.SerializedRows,
		}}
	}
	if chunk.Schema != nil {
		setReadRowsSchema(response, *chunk.Schema)
	}
	return response
}

func formatToProto(format domain.Format) storagepb.DataFormat {
	switch format {
	case domain.FormatArrow:
		return storagepb.DataFormat_ARROW
	case domain.FormatAvro:
		return storagepb.DataFormat_AVRO
	default:
		return storagepb.DataFormat_DATA_FORMAT_UNSPECIFIED
	}
}

func setReadSessionSchema(session *storagepb.ReadSession, schema domain.ReferenceSchema) {
	switch schema.Format {
	case domain.FormatArrow:
		session.Schema = &storagepb.ReadSession_ArrowSchema{ArrowSchema: &storagepb.ArrowSchema{SerializedSchema: schema.Serialized}}
	case domain.FormatAvro:
		session.Schema = &storagepb.ReadSession_AvroSchema{AvroSchema: &storagepb.AvroSchema{Schema: string(schema.Serialized)}}
	}
}

func setReadRowsSchema(response *storagepb.ReadRowsResponse, schema domain.ReferenceSchema) {
	switch schema.Format {
	case domain.FormatArrow:
		response.Schema = &storagepb.ReadRowsResponse_ArrowSchema{ArrowSchema: &storagepb.ArrowSchema{SerializedSchema: schema.Serialized}}
	case domain.FormatAvro:
		response.Schema = &storagepb.ReadRowsResponse_AvroSchema{AvroSchema: &storagepb.AvroSchema{Schema: string(schema.Serialized)}}
	}
}

func storageReadStatus(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "Storage Read request canceled")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "Storage Read request deadline exceeded")
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch domain.CodeOf(err) {
	case domain.ErrorCanceled:
		return status.Error(codes.Canceled, err.Error())
	case domain.ErrorDeadlineExceeded:
		return status.Error(codes.DeadlineExceeded, err.Error())
	case domain.ErrorInvalidArgument:
		return status.Error(codes.InvalidArgument, err.Error())
	case domain.ErrorNotFound:
		return status.Error(codes.NotFound, err.Error())
	case domain.ErrorFailedPrecondition:
		return status.Error(codes.FailedPrecondition, err.Error())
	case domain.ErrorResourceExhausted:
		return status.Error(codes.ResourceExhausted, err.Error())
	case domain.ErrorUnimplemented:
		return status.Error(codes.Unimplemented, err.Error())
	default:
		return status.Error(codes.Internal, "Storage Read operation failed")
	}
}
