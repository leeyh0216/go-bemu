package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/leeyh0216/go-bemu/internal/contracttest"
	writeapp "github.com/leeyh0216/go-bemu/internal/storagewrite/application"
	writedomain "github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
	writeports "github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
)

func TestStorageWriteSparkProtoRowsLifecycleAndConnectionInheritance(t *testing.T) {
	contracttest.Operation(t, "grpc.bigquery-write.create-write-stream")
	contracttest.Operation(t, "grpc.bigquery-write.append-rows")
	contracttest.Operation(t, "grpc.bigquery-write.get-write-stream")
	contracttest.Operation(t, "grpc.bigquery-write.finalize-write-stream")
	contracttest.Operation(t, "grpc.bigquery-write.batch-commit-write-streams")
	ctx, cancel := grpcStorageWriteTestContext(t)
	defer cancel()
	coordinator := newWireWriteCoordinator()
	client := startWriteServer(t, newWireWriteService(t, coordinator))
	parent := "projects/test-project/datasets/analytics/tables/events"
	created, err := client.CreateWriteStream(ctx, &storagepb.CreateWriteStreamRequest{
		Parent: parent, WriteStream: &storagepb.WriteStream{Type: storagepb.WriteStream_PENDING},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.GetType() != storagepb.WriteStream_PENDING || created.GetTableSchema() == nil || len(created.GetTableSchema().GetFields()) != 1 {
		t.Fatalf("unexpected created stream: %#v", created)
	}
	loaded, err := client.GetWriteStream(ctx, &storagepb.GetWriteStreamRequest{Name: created.GetName(), View: storagepb.WriteStreamView_FULL})
	if err != nil || loaded.GetName() != created.GetName() || loaded.GetTableSchema() == nil {
		t.Fatalf("get write stream: %#v, %v", loaded, err)
	}
	descriptor, rows := wireProtoRows(t, 10, 20, 30)
	appendClient, err := client.AppendRows(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := appendClient.Send(&storagepb.AppendRowsRequest{
		WriteStream: created.GetName(), Offset: wrapperspb.Int64(0), TraceId: "spark-stage-7",
		Rows: &storagepb.AppendRowsRequest_ProtoRows{ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
			WriterSchema: &storagepb.ProtoSchema{ProtoDescriptor: descriptor},
			Rows:         &storagepb.ProtoRows{SerializedRows: [][]byte{rows[0]}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	first, err := appendClient.Recv()
	if err != nil || first.GetAppendResult().GetOffset().GetValue() != 0 {
		t.Fatalf("first append: %#v, %v", first, err)
	}
	// Stream and writer schema inherit from the first request on this bidi RPC.
	if err := appendClient.Send(&storagepb.AppendRowsRequest{
		Offset: wrapperspb.Int64(1),
		Rows: &storagepb.AppendRowsRequest_ProtoRows{ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
			Rows: &storagepb.ProtoRows{SerializedRows: [][]byte{rows[1]}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	second, err := appendClient.Recv()
	if err != nil || second.GetAppendResult().GetOffset().GetValue() != 1 {
		t.Fatalf("inherited append: %#v, %v", second, err)
	}
	// Offset errors are embedded responses, not terminal RPC statuses. Spark
	// 0.44.2 treats ALREADY_EXISTS as a successful retry signal.
	if err := appendClient.Send(&storagepb.AppendRowsRequest{
		Offset: wrapperspb.Int64(0),
		Rows: &storagepb.AppendRowsRequest_ProtoRows{ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
			Rows: &storagepb.ProtoRows{SerializedRows: [][]byte{rows[0]}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	duplicate, err := appendClient.Recv()
	if err != nil || codes.Code(duplicate.GetError().GetCode()) != codes.AlreadyExists {
		t.Fatalf("duplicate append: %#v, %v", duplicate, err)
	}
	if len(duplicate.GetError().GetDetails()) != 1 {
		t.Fatalf("duplicate response details = %d, want 1", len(duplicate.GetError().GetDetails()))
	}
	var storageError storagepb.StorageError
	if err := duplicate.GetError().GetDetails()[0].UnmarshalTo(&storageError); err != nil {
		t.Fatal(err)
	}
	if storageError.GetCode() != storagepb.StorageError_OFFSET_ALREADY_EXISTS || storageError.GetEntity() != created.GetName() {
		t.Fatalf("unexpected duplicate StorageError: %#v", &storageError)
	}
	if err := appendClient.Send(&storagepb.AppendRowsRequest{
		Offset: wrapperspb.Int64(3),
		Rows: &storagepb.AppendRowsRequest_ProtoRows{ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
			Rows: &storagepb.ProtoRows{SerializedRows: [][]byte{rows[2]}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	gap, err := appendClient.Recv()
	if err != nil || codes.Code(gap.GetError().GetCode()) != codes.OutOfRange {
		t.Fatalf("gap append: %#v, %v", gap, err)
	}
	if err := appendClient.Send(&storagepb.AppendRowsRequest{
		Offset: wrapperspb.Int64(2),
		Rows: &storagepb.AppendRowsRequest_ProtoRows{ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
			Rows: &storagepb.ProtoRows{SerializedRows: [][]byte{rows[2]}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	third, err := appendClient.Recv()
	if err != nil || third.GetAppendResult().GetOffset().GetValue() != 2 {
		t.Fatalf("adjusted append after errors: %#v, %v", third, err)
	}
	if err := appendClient.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := appendClient.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("append EOF = %v", err)
	}
	finalized, err := client.FinalizeWriteStream(ctx, &storagepb.FinalizeWriteStreamRequest{Name: created.GetName()})
	if err != nil || finalized.GetRowCount() != 3 {
		t.Fatalf("finalize: %#v, %v", finalized, err)
	}
	committed, err := client.BatchCommitWriteStreams(ctx, &storagepb.BatchCommitWriteStreamsRequest{
		Parent: parent, WriteStreams: []string{created.GetName()},
	})
	if err != nil || committed.GetCommitTime() == nil || len(committed.GetStreamErrors()) != 0 {
		t.Fatalf("commit: %#v, %v", committed, err)
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.visibleRows != 3 || coordinator.traceID != "spark-stage-7" {
		t.Fatalf("coordinator visible=%d trace=%q", coordinator.visibleRows, coordinator.traceID)
	}
}

func TestStorageWriteLegacyDefaultAliasHasNoResponseOffset(t *testing.T) {
	ctx, cancel := grpcStorageWriteTestContext(t)
	defer cancel()
	coordinator := newWireWriteCoordinator()
	client := startWriteServer(t, newWireWriteService(t, coordinator))
	descriptor, rows := wireProtoRows(t, 1, 2)
	parent := "projects/test-project/datasets/analytics/tables/events"
	appendClient, err := client.AppendRows(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := appendClient.Send(&storagepb.AppendRowsRequest{
		WriteStream: parent + "/_default",
		Rows: &storagepb.AppendRowsRequest_ProtoRows{ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
			WriterSchema: &storagepb.ProtoSchema{ProtoDescriptor: descriptor},
			Rows:         &storagepb.ProtoRows{SerializedRows: [][]byte{rows[0]}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := appendClient.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if response.GetWriteStream() != parent+"/streams/_default" || response.GetAppendResult().GetOffset() != nil {
		t.Fatalf("unexpected default append response: %#v", response)
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.visibleRows != 1 {
		t.Fatalf("got %d visible rows, want 1", coordinator.visibleRows)
	}
}

type wireWriteCoordinator struct {
	mu          sync.Mutex
	staged      map[string][]writeports.AppendBatch
	visibleRows int
	traceID     string
}

func newWireWriteCoordinator() *wireWriteCoordinator {
	return &wireWriteCoordinator{staged: make(map[string][]writeports.AppendBatch)}
}

func (c *wireWriteCoordinator) DescribeTable(context.Context, writedomain.TableReference) (writedomain.TableSchema, error) {
	return writedomain.TableSchema{Fields: []writedomain.Field{{Name: "id", Type: "INT64", Mode: "NULLABLE"}}}, nil
}

func (c *wireWriteCoordinator) AppendDefault(_ context.Context, batch writeports.AppendBatch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.visibleRows += len(batch.Rows)
	c.traceID = batch.TraceID
	return nil
}

func (c *wireWriteCoordinator) StagePending(_ context.Context, batch writeports.AppendBatch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.staged[batch.StreamName] = append(c.staged[batch.StreamName], batch)
	c.traceID = batch.TraceID
	return nil
}

func (c *wireWriteCoordinator) CommitPending(_ context.Context, request writeports.CommitRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, name := range request.StreamNames {
		for _, batch := range c.staged[name] {
			c.visibleRows += len(batch.Rows)
		}
		delete(c.staged, name)
	}
	return nil
}

func (c *wireWriteCoordinator) DiscardPending(_ context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.staged, name)
	return nil
}

func newWireWriteService(t *testing.T, coordinator writeports.Coordinator) *writeapp.Service {
	t.Helper()
	service, err := writeapp.New(writeapp.Config{
		Location: "US", ProtocolModelVersion: "spark-0.44.2",
		MaxStreams: 16, MaxAppendBytes: 9 * 1024 * 1024, MaxAppendEnvelopeBytes: 64 * 1024, MaxConcurrentAppendRequests: 4,
		OrphanTTL: time.Hour, CleanupInterval: time.Minute,
	}, coordinator, wireClock{}, &wireIDs{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func startWriteServer(t *testing.T, service *writeapp.Service) storagepb.BigQueryWriteClient {
	t.Helper()
	listener := bufconn.Listen(4 * 1024 * 1024)
	server := grpc.NewServer()
	storagepb.RegisterBigQueryWriteServer(server, NewStorageWriteServer(service))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient(
		"passthrough:///bufnet-write",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return storagepb.NewBigQueryWriteClient(connection)
}

func wireProtoRows(t *testing.T, values ...int64) (*descriptorpb.DescriptorProto, [][]byte) {
	t.Helper()
	messageName, fieldName := "Schema", "id"
	fieldNumber := int32(1)
	fieldType := descriptorpb.FieldDescriptorProto_TYPE_INT64
	fieldLabel := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	descriptor := &descriptorpb.DescriptorProto{
		Name: &messageName,
		Field: []*descriptorpb.FieldDescriptorProto{{
			Name: &fieldName, Number: &fieldNumber, Type: &fieldType, Label: &fieldLabel,
		}},
	}
	fileName, syntax := "spark.proto", "proto2"
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: &fileName, Syntax: &syntax, MessageType: []*descriptorpb.DescriptorProto{descriptor},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	messageDescriptor := file.Messages().Get(0)
	field := messageDescriptor.Fields().ByName(protoreflect.Name(fieldName))
	rows := make([][]byte, len(values))
	for index, value := range values {
		message := dynamicpb.NewMessage(messageDescriptor)
		message.Set(field, protoreflect.ValueOfInt64(value))
		rows[index], err = proto.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
	}
	return descriptor, rows
}

var _ writeports.Coordinator = (*wireWriteCoordinator)(nil)

func TestStorageWriteConfiguredDeadlineMapsToGRPCDeadlineExceeded(t *testing.T) {
	err := writedomain.NewError(writedomain.ErrorDeadlineExceeded, "storage_write.append", errors.New("opaque backend timeout"))
	if got := storageWriteCode(err); got != codes.DeadlineExceeded {
		t.Fatalf("storageWriteCode = %s, want DEADLINE_EXCEEDED", got)
	}
	if got := safeStorageWriteMessage(err); got != "Storage Write operation exceeded its configured deadline" {
		t.Fatalf("safe message = %q", got)
	}
}

func TestStorageWriteCanceledAppendMapsToEmbeddedCanceled(t *testing.T) {
	err := writedomain.NewError(writedomain.ErrorCanceled, "storage_write.append", context.Canceled)
	if got := storageWriteCode(err); got != codes.Canceled {
		t.Fatalf("storageWriteCode = %s, want CANCELED", got)
	}
	response := appendErrorResponse("projects/p/datasets/d/tables/t/streams/s", err)
	if got := codes.Code(response.GetError().GetCode()); got != codes.Canceled {
		t.Fatalf("embedded response code = %s, want CANCELED", got)
	}
	if got := response.GetError().GetMessage(); got != "Storage Write request canceled" {
		t.Fatalf("safe message = %q", got)
	}
}

func TestStorageWriteAppendReceiveAdmissionIsBoundedBeforeDecode(t *testing.T) {
	ctx, cancel := grpcStorageWriteTestContext(t)
	defer cancel()
	server := &StorageWriteServer{appendSlots: make(chan struct{}, 1)}
	release, err := server.acquireAppendSlot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	waitContext, cancelWait := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancelWait()
	if _, err := server.acquireAppendSlot(waitContext); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second pre-decode admission = %v, want deadline exceeded", err)
	}
	release()
	reacquired, err := server.acquireAppendSlot(ctx)
	if err != nil {
		t.Fatalf("reacquire after release: %v", err)
	}
	reacquired()
}

func TestAppendConnectionSeparatesProtoDataLimitFromWireAdmission(t *testing.T) {
	descriptor, rows := wireProtoRows(t, 1)
	protoData := &storagepb.AppendRowsRequest_ProtoData{
		WriterSchema: &storagepb.ProtoSchema{ProtoDescriptor: descriptor},
		Rows:         &storagepb.ProtoRows{SerializedRows: rows},
	}
	request := &storagepb.AppendRowsRequest{
		WriteStream: "projects/test-project/datasets/analytics/tables/events/streams/pending-a",
		TraceId:     "connector-trace-envelope",
		Rows:        &storagepb.AppendRowsRequest_ProtoRows{ProtoRows: protoData},
	}
	converted, _, err := (appendConnection{}).convert(request)
	if err != nil {
		t.Fatal(err)
	}
	if converted.PayloadBytes != proto.Size(protoData) {
		t.Fatalf("payload bytes = %d, want ProtoData size %d", converted.PayloadBytes, proto.Size(protoData))
	}
	if converted.WireBytes != proto.Size(request) || converted.WireBytes <= converted.PayloadBytes {
		t.Fatalf("wire bytes = %d, payload bytes = %d, request size = %d", converted.WireBytes, converted.PayloadBytes, proto.Size(request))
	}
}

func TestStorageWriteCDCGapIsStable(t *testing.T) {
	if writedomain.GapCDC != "GAP-STORAGE-WRITE-CDC-001" {
		t.Fatalf("unexpected CDC gap identifier %q", writedomain.GapCDC)
	}
	if got := fmt.Sprint(writedomain.GapCDC); got == "" {
		t.Fatal("CDC gap identifier must be externally reportable")
	}
}

func grpcStorageWriteTestContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	timeout := 10 * time.Second
	if configured := os.Getenv("BQEMU_STORAGE_WRITE_TEST_TIMEOUT"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		if err != nil {
			t.Fatalf("BQEMU_STORAGE_WRITE_TEST_TIMEOUT: %v", err)
		}
		timeout = parsed
	}
	return context.WithTimeout(context.Background(), timeout)
}
