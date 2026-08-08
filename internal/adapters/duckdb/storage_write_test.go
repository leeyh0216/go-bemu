package duckdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/leeyh0216/go-bemu/internal/domain"
	writeapp "github.com/leeyh0216/go-bemu/internal/storagewrite/application"
	writedomain "github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
	writeports "github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
	grpcserver "github.com/leeyh0216/go-bemu/internal/transport/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestStorageWritePendingAndDefaultVisibility(t *testing.T) {
	ctx, cancel := duckDBStorageWriteTestContext(t)
	defer cancel()
	warehouse, coordinator, table := newStorageWriteFixture(t, []domain.Field{
		{Name: "id", Type: "INT64"},
		{Name: "name", Type: "STRING"},
		{Name: "event_date", Type: "DATE"},
		{Name: "event_at", Type: "TIMESTAMP"},
		{Name: "amount", Type: "NUMERIC"},
		{Name: "large_amount", Type: "BIGNUMERIC"},
		{Name: "tags", Type: "STRING", Mode: "REPEATED"},
	})
	descriptor := storageWriteDescriptor(t,
		protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		protoField("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		protoField("event_date", 3, descriptorpb.FieldDescriptorProto_TYPE_INT32, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		protoField("event_at", 4, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		protoField("amount", 5, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		protoField("large_amount", 6, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		protoField("tags", 7, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_REPEATED),
	)
	row := storageWriteRow(t, descriptor, map[string]any{
		"id": int64(7), "name": "first", "event_date": int32(1),
		"event_at": int64(1_500_000), "amount": "12.340000000",
		"large_amount": "12345678901234567890.123456789012345678", "tags": []any{"alpha", "beta"},
	})
	pendingName := table.Name() + "/streams/pending-a"
	batch := writeports.AppendBatch{
		StreamName: pendingName, Table: table, Descriptor: descriptor, Rows: [][]byte{row},
		SchemaFingerprint: "schema-a", PayloadDigest: "payload-a",
	}
	if err := coordinator.StagePending(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if got := storageWriteRowCount(t, ctx, warehouse, table); got != 0 {
		t.Fatalf("PENDING rows were visible before commit: %d", got)
	}
	if err := coordinator.CommitPending(ctx, writeports.CommitRequest{
		Parent: table, StreamNames: []string{pendingName},
	}); err != nil {
		t.Fatal(err)
	}
	if got := storageWriteRowCount(t, ctx, warehouse, table); got != 1 {
		t.Fatalf("got %d committed rows, want 1", got)
	}

	defaultBatch := batch
	defaultBatch.StreamName = table.Name() + "/streams/_default"
	defaultBatch.StartOffset = 1
	defaultBatch.Rows = [][]byte{storageWriteRow(t, descriptor, map[string]any{"id": int64(8), "name": "default"})}
	if err := coordinator.AppendDefault(ctx, defaultBatch); err != nil {
		t.Fatal(err)
	}
	if got := storageWriteRowCount(t, ctx, warehouse, table); got != 2 {
		t.Fatalf("DEFAULT append was not immediately visible: %d", got)
	}

	query := `SELECT "name", CAST("event_date" AS VARCHAR), epoch_us("event_at"), CAST("amount" AS VARCHAR), CAST("large_amount" AS VARCHAR), "tags"[1] FROM ` +
		quoteIdentifier(physicalSchema(table.ProjectID, table.DatasetID)) + `.` + quoteIdentifier(table.TableID) + ` WHERE "id" = 7`
	var name, date, amount, largeAmount, firstTag string
	var timestampMicros int64
	if err := warehouse.db.QueryRowContext(ctx, query).Scan(&name, &date, &timestampMicros, &amount, &largeAmount, &firstTag); err != nil {
		t.Fatal(err)
	}
	if name != "first" || date != "1970-01-02" || timestampMicros != 1_500_000 || amount != "12.340000000" || largeAmount != "12345678901234567890.123456789012345678" || firstTag != "alpha" {
		t.Fatalf("unexpected converted row: %q %q %d %q %q %q", name, date, timestampMicros, amount, largeAmount, firstTag)
	}
}

func TestStorageWriteRejectsDecimalOverflowBeforeRowMutation(t *testing.T) {
	ctx, cancel := duckDBStorageWriteTestContext(t)
	defer cancel()
	precision, scale := int64(5), int64(2)
	warehouse, coordinator, table := newStorageWriteFixture(t, []domain.Field{{
		Name: "amount", Type: "BIGNUMERIC", Precision: &precision, Scale: &scale,
	}})
	descriptor := storageWriteDescriptor(t,
		protoField("amount", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	)
	row := storageWriteRow(t, descriptor, map[string]any{"amount": "1234.56"})
	err := coordinator.AppendDefault(ctx, writeports.AppendBatch{
		StreamName: table.Name() + "/streams/_default", Table: table,
		Descriptor: descriptor, Rows: [][]byte{row}, SchemaFingerprint: "decimal", PayloadDigest: "overflow",
	})
	if !errors.Is(err, writeports.ErrInvalidRows) || !strings.Contains(err.Error(), domain.DecimalValueOverflowV1) || strings.Contains(err.Error(), "1234.56") {
		t.Fatalf("decimal overflow error = %v", err)
	}
	if got := storageWriteRowCount(t, ctx, warehouse, table); got != 0 {
		t.Fatalf("decimal overflow inserted %d rows", got)
	}
}

func TestStorageWriteRejectsNonBigQueryDecimalGrammarBeforeRowMutation(t *testing.T) {
	for _, value := range []string{"secret-marker/2", "0xsecret-marker", "0bsecret-marker"} {
		t.Run(value, func(t *testing.T) {
			ctx, cancel := duckDBStorageWriteTestContext(t)
			defer cancel()
			warehouse, coordinator, table := newStorageWriteFixture(t, []domain.Field{{
				Name: "amount", Type: "NUMERIC",
			}})
			descriptor := storageWriteDescriptor(t,
				protoField("amount", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			)
			row := storageWriteRow(t, descriptor, map[string]any{"amount": value})
			err := coordinator.AppendDefault(ctx, writeports.AppendBatch{
				StreamName: table.Name() + "/streams/_default", Table: table,
				Descriptor: descriptor, Rows: [][]byte{row}, SchemaFingerprint: "decimal", PayloadDigest: value,
			})
			if !errors.Is(err, writeports.ErrInvalidRows) || !strings.Contains(err.Error(), domain.DecimalValueInvalidV1) || strings.Contains(err.Error(), value) || strings.Contains(err.Error(), "secret-marker") {
				t.Fatalf("decimal grammar error = %v", err)
			}
			if got := storageWriteRowCount(t, ctx, warehouse, table); got != 0 {
				t.Fatalf("invalid decimal inserted %d rows", got)
			}
		})
	}
}

func TestStorageWritePublicGRPCRedactsInvalidDecimalAndReturnsInvalidArgument(t *testing.T) {
	ctx, cancel := duckDBStorageWriteTestContext(t)
	defer cancel()
	warehouse, coordinator, table := newStorageWriteFixture(t, []domain.Field{{Name: "amount", Type: "NUMERIC"}})
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	previousLogger := slog.Default()
	slog.SetDefault(logger)
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	service, err := writeapp.New(writeapp.Config{
		Location: "US", ProtocolModelVersion: "spark-0.44.2",
		MaxStreams: 2, MaxAppendBytes: 1024 * 1024, MaxAppendEnvelopeBytes: 64 * 1024, MaxConcurrentAppendRequests: 2,
		OrphanTTL: time.Hour, CleanupInterval: time.Minute,
	}, coordinator, storageWriteRetryClock{}, storageWriteRetryIDs{}, logger)
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(4 * 1024 * 1024)
	server := grpc.NewServer()
	storagepb.RegisterBigQueryWriteServer(server, grpcserver.NewStorageWriteServer(service))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	descriptorBytes := storageWriteDescriptor(t,
		protoField("amount", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	)
	descriptor := new(descriptorpb.DescriptorProto)
	if err := proto.Unmarshal(descriptorBytes, descriptor); err != nil {
		t.Fatal(err)
	}
	const secret = "storage-write-secret-marker/2"
	row := storageWriteRow(t, descriptorBytes, map[string]any{"amount": secret})
	client := newDuckDBStorageWriteClient(t, listener)
	created, err := client.CreateWriteStream(ctx, &storagepb.CreateWriteStreamRequest{
		Parent: table.Name(), WriteStream: &storagepb.WriteStream{Type: storagepb.WriteStream_PENDING},
	})
	if err != nil {
		t.Fatal(err)
	}
	appendClient, err := client.AppendRows(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := appendClient.Send(&storagepb.AppendRowsRequest{
		WriteStream: created.GetName(), Offset: wrapperspb.Int64(0),
		Rows: &storagepb.AppendRowsRequest_ProtoRows{ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
			WriterSchema: &storagepb.ProtoSchema{ProtoDescriptor: descriptor},
			Rows:         &storagepb.ProtoRows{SerializedRows: [][]byte{row}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := appendClient.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if response.GetError().GetCode() != int32(codes.InvalidArgument) {
		t.Fatalf("AppendRows status = %#v, want INVALID_ARGUMENT", response.GetError())
	}
	if strings.Contains(response.String(), secret) || strings.Contains(response.String(), "secret-marker") {
		t.Fatalf("AppendRows response exposed the decimal payload: %s", response)
	}
	if strings.Contains(logs.String(), secret) || strings.Contains(logs.String(), "secret-marker") {
		t.Fatalf("Storage Write logs exposed the decimal payload: %s", logs.String())
	}
	if got := storageWriteRowCount(t, ctx, warehouse, table); got != 0 {
		t.Fatalf("invalid public ProtoRow inserted %d rows", got)
	}
	stream, err := service.GetStream(ctx, created.GetName())
	if err != nil {
		t.Fatal(err)
	}
	if stream.NextOffset != 0 || stream.RowCount != 0 || coordinator.stagedBytes.Load() != 0 {
		t.Fatalf("invalid public ProtoRow changed pending state: offset=%d rows=%d staged_bytes=%d", stream.NextOffset, stream.RowCount, coordinator.stagedBytes.Load())
	}
}

func TestStorageWriteRejectsUnsupportedCanonicalSchemaBeforePhysicalAccess(t *testing.T) {
	ctx, cancel := duckDBStorageWriteTestContext(t)
	defer cancel()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	table := domain.Table{
		ProjectID: "test-project", DatasetID: "dataset", ID: "items",
		Schema: []domain.Field{{Name: "location", Type: "GEOGRAPHY"}},
	}
	coordinator, err := NewStorageWriteCoordinator(ctx, warehouse, staticStorageWriteResolver{table: table}, storageWriteCoordinatorTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := duckDBStorageWriteTestContext(t)
		defer closeCancel()
		_ = coordinator.Close(closeContext)
		_ = warehouse.Close()
	})
	_, err = coordinator.DescribeTable(ctx, writedomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"})
	if !errors.Is(err, writeports.ErrUnsupportedSchema) {
		t.Fatalf("DescribeTable error = %v, want ErrUnsupportedSchema before physical access", err)
	}
}

func TestStorageWriteCommitFaultRollsBackAllStreams(t *testing.T) {
	ctx, cancel := duckDBStorageWriteTestContext(t)
	defer cancel()
	warehouse, coordinator, table := newStorageWriteFixture(t, []domain.Field{{Name: "id", Type: "INT64"}})
	descriptor := storageWriteDescriptor(t, protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL))
	streamNames := []string{table.Name() + "/streams/a", table.Name() + "/streams/b"}
	var expectedStagedBytes int64
	for index, streamName := range streamNames {
		batch := writeports.AppendBatch{
			StreamName: streamName, Table: table, Descriptor: descriptor,
			Rows:              [][]byte{storageWriteRow(t, descriptor, map[string]any{"id": int64(index + 1)})},
			SchemaFingerprint: "schema", PayloadDigest: fmt.Sprintf("row-%d", index),
		}
		expectedStagedBytes += batchStagedBytes(batch)
		if err := coordinator.StagePending(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	coordinator.beforeCommit = func() error { return errors.New("injected fault before commit") }
	request := writeports.CommitRequest{Parent: table, StreamNames: streamNames}
	if err := coordinator.CommitPending(ctx, request); err == nil {
		t.Fatal("expected injected commit fault")
	}
	if got := storageWriteRowCount(t, ctx, warehouse, table); got != 0 {
		t.Fatalf("failed atomic commit exposed %d rows", got)
	}
	if got := coordinator.stagedBytes.Load(); got != expectedStagedBytes {
		t.Fatalf("failed commit staged bytes = %d, want %d", got, expectedStagedBytes)
	}
	if got := storageWriteStagingTableCount(t, ctx, warehouse); got != 2 {
		t.Fatalf("failed commit retained %d staging tables, want 2", got)
	}
	for _, streamName := range streamNames {
		if got := storageWriteReceiptCount(t, ctx, warehouse, streamName); got != 1 {
			t.Fatalf("failed commit receipt count for stream = %d, want 1", got)
		}
	}
	coordinator.beforeCommit = nil
	if err := coordinator.CommitPending(ctx, request); err != nil {
		t.Fatal(err)
	}
	if got := storageWriteRowCount(t, ctx, warehouse, table); got != 2 {
		t.Fatalf("retry exposed %d rows, want 2", got)
	}
	if got := coordinator.stagedBytes.Load(); got != 0 {
		t.Fatalf("successful retry retained %d staged bytes", got)
	}
	if got := storageWriteStagingTableCount(t, ctx, warehouse); got != 0 {
		t.Fatalf("successful retry retained %d staging tables", got)
	}
}

func TestStorageWriteCommitRejectsExpectedRowCountMismatch(t *testing.T) {
	ctx, cancel := duckDBStorageWriteTestContext(t)
	defer cancel()
	warehouse, coordinator, table := newStorageWriteFixture(t, []domain.Field{{Name: "id", Type: "INT64"}})
	descriptor := storageWriteDescriptor(t, protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL))
	stream := table.Name() + "/streams/row-proof"
	if err := coordinator.StagePending(ctx, writeports.AppendBatch{
		StreamName: stream, Table: table, Descriptor: descriptor,
		Rows:              [][]byte{storageWriteRow(t, descriptor, map[string]any{"id": int64(1)})},
		SchemaFingerprint: "schema", PayloadDigest: "payload",
	}); err != nil {
		t.Fatal(err)
	}
	err := coordinator.CommitPending(ctx, writeports.CommitRequest{
		Parent: table, StreamNames: []string{stream}, GroupID: "group-row-proof",
		ExpectedRowCounts: map[string]int64{stream: 2},
	})
	if err == nil || !strings.Contains(err.Error(), "row-count proof mismatch") {
		t.Fatalf("commit row-count mismatch = %v", err)
	}
	if got := storageWriteRowCount(t, ctx, warehouse, table); got != 0 {
		t.Fatalf("mismatched commit exposed %d rows", got)
	}
	if got := storageWriteReceiptCount(t, ctx, warehouse, stream); got != 1 {
		t.Fatalf("mismatched commit retained %d receipts, want 1", got)
	}
}

func TestStorageWriteStagePendingReceiptIsIdempotentAndRejectsConflicts(t *testing.T) {
	ctx, cancel := duckDBStorageWriteTestContext(t)
	defer cancel()
	warehouse, coordinator, table := newStorageWriteFixture(t, []domain.Field{{Name: "id", Type: "INT64"}})
	descriptor := storageWriteDescriptor(t, protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL))
	row := storageWriteRow(t, descriptor, map[string]any{"id": int64(1)})
	batch := writeports.AppendBatch{
		StreamName: table.Name() + "/streams/retry", Table: table, Descriptor: descriptor,
		Rows: [][]byte{row}, SchemaFingerprint: "schema-a", PayloadDigest: "payload-a",
	}
	if err := coordinator.StagePending(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.StagePending(ctx, batch); err != nil {
		t.Fatalf("identical receipt retry: %v", err)
	}
	if got := storageWriteReceiptCount(t, ctx, warehouse, batch.StreamName); got != 1 {
		t.Fatalf("identical receipt created %d staged batches, want 1", got)
	}

	conflicts := map[string]writeports.AppendBatch{
		"row count":          batch,
		"schema fingerprint": batch,
		"payload digest":     batch,
	}
	rowCountConflict := conflicts["row count"]
	rowCountConflict.Rows = [][]byte{row, row}
	conflicts["row count"] = rowCountConflict
	schemaConflict := conflicts["schema fingerprint"]
	schemaConflict.SchemaFingerprint = "schema-b"
	conflicts["schema fingerprint"] = schemaConflict
	payloadConflict := conflicts["payload digest"]
	payloadConflict.PayloadDigest = "payload-b"
	conflicts["payload digest"] = payloadConflict
	for name, conflicting := range conflicts {
		t.Run(name, func(t *testing.T) {
			if err := coordinator.StagePending(ctx, conflicting); err == nil {
				t.Fatal("expected receipt conflict")
			}
			if got := storageWriteReceiptCount(t, ctx, warehouse, batch.StreamName); got != 1 {
				t.Fatalf("receipt conflict changed staged batches to %d", got)
			}
		})
	}
}

func TestStorageWriteStagesHundredSequentialBatchesWithinLowByteBudget(t *testing.T) {
	ctx, cancel := duckDBStorageWriteTestContext(t)
	defer cancel()
	descriptor := storageWriteDescriptor(t, protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL))
	row := storageWriteRow(t, descriptor, map[string]any{"id": int64(1)})
	unitBytes := batchStagedBytes(writeports.AppendBatch{Rows: [][]byte{row}})
	config := storageWriteCoordinatorTestConfig()
	config.MaxStagedBytes = unitBytes * 100
	config.MaxStagedBytesPerStream = unitBytes * 100
	warehouse, coordinator, table := newStorageWriteFixtureWithConfig(t, []domain.Field{{Name: "id", Type: "INT64"}}, config)
	stream := table.Name() + "/streams/hundred"
	for offset := int64(0); offset < 100; offset++ {
		if err := coordinator.StagePending(ctx, writeports.AppendBatch{
			StreamName: stream, Table: table, StartOffset: offset, WireBytes: 128,
			Descriptor: descriptor, Rows: [][]byte{row},
			SchemaFingerprint: "schema", PayloadDigest: fmt.Sprintf("payload-%d", offset),
		}); err != nil {
			t.Fatalf("stage offset %d: %v", offset, err)
		}
	}
	if got := storageWriteRowCount(t, ctx, warehouse, table); got != 0 {
		t.Fatalf("PENDING destination rows = %d before commit", got)
	}
	if got := storageWriteReceiptCount(t, ctx, warehouse, stream); got != 100 {
		t.Fatalf("receipt count = %d, want 100", got)
	}
	if got := storageWriteStagingTableCount(t, ctx, warehouse); got != 1 {
		t.Fatalf("staging table count = %d, want 1", got)
	}
	if got := coordinator.stagedBytes.Load(); got != unitBytes*100 {
		t.Fatalf("staged bytes = %d, want %d", got, unitBytes*100)
	}
	if err := coordinator.StagePending(ctx, writeports.AppendBatch{
		StreamName: stream, Table: table, StartOffset: 100, WireBytes: 128,
		Descriptor: descriptor, Rows: [][]byte{row}, SchemaFingerprint: "schema", PayloadDigest: "overflow",
	}); !errors.Is(err, writeports.ErrResourceExhausted) {
		t.Fatalf("batch above staged budget = %v, want resource exhausted", err)
	}
	if global, perStream := coordinator.admission.snapshot(stream); global != 0 || perStream != 0 {
		t.Fatalf("rejected staged batch retained in-flight bytes global=%d stream=%d", global, perStream)
	}
	if err := coordinator.CommitPending(ctx, writeports.CommitRequest{Parent: table, StreamNames: []string{stream}}); err != nil {
		t.Fatal(err)
	}
	if got := storageWriteRowCount(t, ctx, warehouse, table); got != 100 {
		t.Fatalf("committed rows = %d, want 100", got)
	}
	if got := coordinator.stagedBytes.Load(); got != 0 {
		t.Fatalf("commit retained %d staged bytes", got)
	}
}

func TestStorageWriteStagedBudgetIsolatedPerStream(t *testing.T) {
	ctx, cancel := duckDBStorageWriteTestContext(t)
	defer cancel()
	descriptor := storageWriteDescriptor(t, protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL))
	row := storageWriteRow(t, descriptor, map[string]any{"id": int64(1)})
	unitBytes := batchStagedBytes(writeports.AppendBatch{Rows: [][]byte{row}})
	config := storageWriteCoordinatorTestConfig()
	config.MaxStagedBytes = unitBytes * 2
	config.MaxStagedBytesPerStream = unitBytes
	_, coordinator, table := newStorageWriteFixtureWithConfig(t, []domain.Field{{Name: "id", Type: "INT64"}}, config)
	first := table.Name() + "/streams/first"
	second := table.Name() + "/streams/second"
	batch := func(stream string, offset int64) writeports.AppendBatch {
		return writeports.AppendBatch{
			StreamName: stream, Table: table, StartOffset: offset, WireBytes: 128,
			Descriptor: descriptor, Rows: [][]byte{row}, SchemaFingerprint: "schema", PayloadDigest: fmt.Sprintf("%s-%d", stream, offset),
		}
	}
	if err := coordinator.StagePending(ctx, batch(first, 0)); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.StagePending(ctx, batch(first, 1)); !errors.Is(err, writeports.ErrResourceExhausted) {
		t.Fatalf("same-stream overflow = %v, want resource exhausted", err)
	}
	if err := coordinator.StagePending(ctx, batch(second, 0)); err != nil {
		t.Fatalf("independent stream within global budget: %v", err)
	}
	if got := coordinator.stagedBytes.Load(); got != unitBytes*2 {
		t.Fatalf("global staged bytes = %d, want %d", got, unitBytes*2)
	}
}

func TestStorageWriteSixteenConcurrentPayloadsUseWeightedAdmission(t *testing.T) {
	ctx, cancel := duckDBStorageWriteTestContext(t)
	defer cancel()
	const weight int64 = 1024
	config := storageWriteCoordinatorTestConfig()
	config.MaxInFlightBytes = 4 * weight
	config.MaxInFlightBytesPerStream = weight
	_, coordinator, table := newStorageWriteFixtureWithConfig(t, []domain.Field{{Name: "id", Type: "INT64"}}, config)
	descriptor := storageWriteDescriptor(t, protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL))
	row := storageWriteRow(t, descriptor, map[string]any{"id": int64(1)})
	oversized := writeports.AppendBatch{
		StreamName: table.Name() + "/streams/oversized", Table: table, WireBytes: weight + 1,
		Descriptor: descriptor, Rows: [][]byte{row}, SchemaFingerprint: "schema", PayloadDigest: "oversized",
	}
	if err := coordinator.StagePending(ctx, oversized); !errors.Is(err, writeports.ErrResourceExhausted) {
		t.Fatalf("per-stream in-flight overflow = %v, want resource exhausted", err)
	}

	stageApplied := make(chan struct{})
	releaseWorker := make(chan struct{})
	var hookOnce sync.Once
	coordinator.afterStage = func() {
		hookOnce.Do(func() {
			close(stageApplied)
			<-releaseWorker
		})
	}
	type appendOutcome struct {
		stream string
		err    error
	}
	outcomes := make(chan appendOutcome, 16)
	start := make(chan struct{})
	for index := 0; index < 16; index++ {
		stream := fmt.Sprintf("%s/streams/concurrent-%d", table.Name(), index)
		go func(index int, stream string) {
			<-start
			outcomes <- appendOutcome{stream: stream, err: coordinator.StagePending(ctx, writeports.AppendBatch{
				StreamName: stream, Table: table, WireBytes: weight,
				Descriptor: descriptor, Rows: [][]byte{row}, SchemaFingerprint: "schema", PayloadDigest: fmt.Sprintf("payload-%d", index),
			})}
		}(index, stream)
	}
	close(start)
	select {
	case <-stageApplied:
	case <-ctx.Done():
		t.Fatalf("waiting for admitted staging operation: %v", ctx.Err())
	}
	rejected := 0
	for rejected < 12 {
		select {
		case outcome := <-outcomes:
			if !errors.Is(outcome.err, writeports.ErrResourceExhausted) {
				t.Fatalf("outcome before release for %s = %v", outcome.stream, outcome.err)
			}
			rejected++
		case <-ctx.Done():
			t.Fatalf("waiting for weighted rejections: %v", ctx.Err())
		}
	}
	close(releaseWorker)
	succeeded := 0
	for succeeded < 4 {
		select {
		case outcome := <-outcomes:
			if outcome.err != nil {
				t.Fatalf("admitted outcome for %s: %v", outcome.stream, outcome.err)
			}
			succeeded++
		case <-ctx.Done():
			t.Fatalf("waiting for admitted writes: %v", ctx.Err())
		}
	}
	coordinator.afterStage = nil
	if global, _ := coordinator.admission.snapshot(""); global != 0 {
		t.Fatalf("completed concurrent writes retained %d in-flight bytes", global)
	}
	if got := storageWriteTotalReceiptCount(t, ctx, coordinator.warehouse); got != 4 {
		t.Fatalf("weighted admission staged %d batches, want 4", got)
	}
	for index := 0; index < 16; index++ {
		stream := fmt.Sprintf("%s/streams/concurrent-%d", table.Name(), index)
		if err := coordinator.DiscardPending(ctx, stream); err != nil {
			t.Fatal(err)
		}
	}
	if got := coordinator.stagedBytes.Load(); got != 0 {
		t.Fatalf("discard retained %d staged bytes", got)
	}
}

func TestStorageWriteQueueWaitTimeoutIsBounded(t *testing.T) {
	ctx, cancel := duckDBStorageWriteTestContext(t)
	defer cancel()
	config := storageWriteCoordinatorTestConfig()
	config.QueueCapacity = 1
	config.QueueWaitTimeout = 25 * time.Millisecond
	config.OperationTimeout = time.Second
	_, coordinator, table := newStorageWriteFixtureWithConfig(t, []domain.Field{{Name: "id", Type: "INT64"}}, config)
	descriptor := storageWriteDescriptor(t, protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL))
	row := storageWriteRow(t, descriptor, map[string]any{"id": int64(1)})
	batch := func(name string) writeports.AppendBatch {
		return writeports.AppendBatch{
			StreamName: table.Name() + "/streams/" + name, Table: table, WireBytes: 128,
			Descriptor: descriptor, Rows: [][]byte{row}, SchemaFingerprint: "schema", PayloadDigest: "payload-" + name,
		}
	}

	workerEntered := make(chan struct{})
	releaseWorker := make(chan struct{})
	var once sync.Once
	coordinator.afterStage = func() {
		once.Do(func() {
			close(workerEntered)
			<-releaseWorker
		})
	}
	results := make(chan error, 2)
	go func() { results <- coordinator.StagePending(ctx, batch("active")) }()
	select {
	case <-workerEntered:
	case <-ctx.Done():
		t.Fatalf("waiting for active coordinator operation: %v", ctx.Err())
	}
	go func() { results <- coordinator.StagePending(ctx, batch("queued")) }()
	for len(coordinator.queue) != 1 {
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("waiting for full coordinator queue: %v", ctx.Err())
		}
	}

	started := time.Now()
	err := coordinator.StagePending(ctx, batch("rejected"))
	if !errors.Is(err, writeports.ErrQueueWaitTimeout) {
		t.Fatalf("full queue error = %v, want queue wait timeout", err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("queue wait took %s, configured %s", elapsed, config.QueueWaitTimeout)
	}
	if global, stream := coordinator.admission.snapshot(batch("rejected").StreamName); global == 0 || stream != 0 {
		t.Fatalf("queue rejection admission global=%d rejected_stream=%d", global, stream)
	}

	close(releaseWorker)
	for range 2 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatalf("waiting for accepted coordinator operations: %v", ctx.Err())
		}
	}
}

func TestStorageWriteOperationTimeoutAllowsPendingReceiptRetry(t *testing.T) {
	ctx, cancel := duckDBStorageWriteTestContext(t)
	defer cancel()
	config := storageWriteCoordinatorTestConfig()
	config.QueueWaitTimeout = 5 * time.Millisecond
	config.OperationTimeout = time.Hour
	_, coordinator, table := newStorageWriteFixtureWithConfig(t, []domain.Field{{Name: "id", Type: "INT64"}}, config)
	timeoutRequested := make(chan func(), 1)
	coordinator.scheduleTimeout = func(_ time.Duration, expire func()) func() {
		timeoutRequested <- expire
		return func() {}
	}
	descriptor := storageWriteDescriptor(t, protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL))
	batch := writeports.AppendBatch{
		StreamName: table.Name() + "/streams/timeout-retry", Table: table, WireBytes: 128,
		Descriptor: descriptor, Rows: [][]byte{storageWriteRow(t, descriptor, map[string]any{"id": int64(1)})},
		SchemaFingerprint: "schema", PayloadDigest: "payload",
	}
	workerEntered := make(chan struct{})
	releaseWorker := make(chan struct{})
	var once sync.Once
	coordinator.afterStage = func() {
		once.Do(func() {
			close(workerEntered)
			<-releaseWorker
		})
	}
	result := make(chan error, 1)
	go func() { result <- coordinator.StagePending(ctx, batch) }()
	select {
	case <-workerEntered:
	case <-ctx.Done():
		t.Fatalf("waiting for staged operation: %v", ctx.Err())
	}
	select {
	case expire := <-timeoutRequested:
		expire()
	case <-ctx.Done():
		t.Fatalf("waiting for operation timeout registration: %v", ctx.Err())
	}
	select {
	case err := <-result:
		if !errors.Is(err, writeports.ErrOperationTimeout) {
			t.Fatalf("operation timeout error = %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("waiting for configured operation timeout: %v", ctx.Err())
	}
	close(releaseWorker)
	if err := coordinator.StagePending(ctx, batch); err != nil {
		t.Fatalf("receipt-backed retry after acknowledgement timeout: %v", err)
	}
	if got := storageWriteReceiptCount(t, ctx, coordinator.warehouse, batch.StreamName); got != 1 {
		t.Fatalf("receipt-backed retry count = %d, want 1", got)
	}
}

func TestStorageWriteOperationBudgetStartsAfterQueueAdmission(t *testing.T) {
	ctx, cancel := duckDBStorageWriteTestContext(t)
	defer cancel()
	config := storageWriteCoordinatorTestConfig()
	config.QueueCapacity = 1
	config.QueueWaitTimeout = 250 * time.Millisecond
	config.OperationTimeout = 400 * time.Millisecond
	_, coordinator, _ := newStorageWriteFixtureWithConfig(t, []domain.Field{{Name: "id", Type: "INT64"}}, config)

	activeStarted := make(chan struct{})
	releaseActive := make(chan struct{})
	acceptedResults := make(chan error, 2)
	go func() {
		_, err := coordinator.submit(ctx, func(context.Context) (any, error) {
			close(activeStarted)
			<-releaseActive
			return nil, nil
		})
		acceptedResults <- err
	}()
	select {
	case <-activeStarted:
	case <-ctx.Done():
		t.Fatalf("waiting for active operation: %v", ctx.Err())
	}
	go func() {
		_, err := coordinator.submit(ctx, func(context.Context) (any, error) { return nil, nil })
		acceptedResults <- err
	}()
	for len(coordinator.queue) != 1 {
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("waiting for queued operation: %v", ctx.Err())
		}
	}

	candidate := make(chan error, 1)
	go func() {
		_, err := coordinator.submit(ctx, func(operationContext context.Context) (any, error) {
			select {
			case <-time.After(300 * time.Millisecond):
				return nil, nil
			case <-operationContext.Done():
				return nil, operationContext.Err()
			}
		})
		candidate <- err
	}()
	time.Sleep(150 * time.Millisecond)
	close(releaseActive)
	for range 2 {
		select {
		case err := <-acceptedResults:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatalf("waiting for accepted operations: %v", ctx.Err())
		}
	}
	select {
	case err := <-candidate:
		if err != nil {
			t.Fatalf("operation budget was consumed before queue admission: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("waiting for admitted operation: %v", ctx.Err())
	}
}

func TestStorageWriteDiscardAndCloseCleanHiddenStaging(t *testing.T) {
	t.Run("discard", func(t *testing.T) {
		ctx, cancel := duckDBStorageWriteTestContext(t)
		defer cancel()
		warehouse, coordinator, table := newStorageWriteFixture(t, []domain.Field{{Name: "id", Type: "INT64"}})
		descriptor := storageWriteDescriptor(t, protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL))
		stream := table.Name() + "/streams/discard"
		if err := coordinator.StagePending(ctx, writeports.AppendBatch{
			StreamName: stream, Table: table, WireBytes: 128, Descriptor: descriptor,
			Rows:              [][]byte{storageWriteRow(t, descriptor, map[string]any{"id": int64(1)})},
			SchemaFingerprint: "schema", PayloadDigest: "payload",
		}); err != nil {
			t.Fatal(err)
		}
		if err := coordinator.DiscardPending(ctx, stream); err != nil {
			t.Fatal(err)
		}
		if got := storageWriteReceiptCount(t, ctx, warehouse, stream); got != 0 {
			t.Fatalf("discard retained %d receipts", got)
		}
		if got := storageWriteStagingTableCount(t, ctx, warehouse); got != 0 {
			t.Fatalf("discard retained %d staging tables", got)
		}
		if got := coordinator.stagedBytes.Load(); got != 0 {
			t.Fatalf("discard retained %d staged bytes", got)
		}
	})

	t.Run("close", func(t *testing.T) {
		ctx, cancel := duckDBStorageWriteTestContext(t)
		defer cancel()
		warehouse, coordinator, table := newStorageWriteFixture(t, []domain.Field{{Name: "id", Type: "INT64"}})
		descriptor := storageWriteDescriptor(t, protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL))
		stream := table.Name() + "/streams/close"
		if err := coordinator.StagePending(ctx, writeports.AppendBatch{
			StreamName: stream, Table: table, WireBytes: 128, Descriptor: descriptor,
			Rows:              [][]byte{storageWriteRow(t, descriptor, map[string]any{"id": int64(1)})},
			SchemaFingerprint: "schema", PayloadDigest: "payload",
		}); err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if got := storageWriteInternalTableCount(t, ctx, warehouse); got != 0 {
			t.Fatalf("close retained %d internal tables", got)
		}
		if got := coordinator.stagedBytes.Load(); got != 0 {
			t.Fatalf("close retained %d staged bytes", got)
		}
		if global, perStream := coordinator.admission.snapshot(stream); global != 0 || perStream != 0 {
			t.Fatalf("close retained in-flight bytes global=%d stream=%d", global, perStream)
		}
	})
}

func TestStorageWriteCloseOrdersCleanupAfterAcceptedAppendAndRejectsLateAppend(t *testing.T) {
	ctx, cancel := duckDBStorageWriteTestContext(t)
	defer cancel()
	warehouse, coordinator, table := newStorageWriteFixture(t, []domain.Field{{Name: "id", Type: "INT64"}})
	descriptor := storageWriteDescriptor(t, protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL))
	row := storageWriteRow(t, descriptor, map[string]any{"id": int64(1)})
	stageApplied := make(chan struct{})
	releaseWorker := make(chan struct{})
	coordinator.afterStage = func() {
		close(stageApplied)
		<-releaseWorker
	}
	firstStream := table.Name() + "/streams/pre-close"
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- coordinator.StagePending(ctx, writeports.AppendBatch{
			StreamName: firstStream, Table: table, WireBytes: 128, Descriptor: descriptor,
			Rows: [][]byte{row}, SchemaFingerprint: "schema", PayloadDigest: "first",
		})
	}()
	select {
	case <-stageApplied:
	case <-ctx.Done():
		t.Fatalf("waiting for pre-close staging: %v", ctx.Err())
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- coordinator.Close(ctx) }()
	waitForStorageWriteClosed(t, ctx, coordinator)
	lateStream := table.Name() + "/streams/post-close"
	if err := coordinator.StagePending(ctx, writeports.AppendBatch{
		StreamName: lateStream, Table: table, WireBytes: 128, Descriptor: descriptor,
		Rows: [][]byte{row}, SchemaFingerprint: "schema", PayloadDigest: "late",
	}); !errors.Is(err, errStorageWriteCoordinatorClosed) {
		t.Fatalf("post-close append = %v, want coordinator closed", err)
	}
	close(releaseWorker)
	if err := <-firstDone; err != nil {
		t.Fatalf("accepted pre-close append: %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("close after accepted append: %v", err)
	}
	coordinator.afterStage = nil
	if got := storageWriteInternalTableCount(t, ctx, warehouse); got != 0 {
		t.Fatalf("ordered close retained %d internal tables", got)
	}
	if got := coordinator.stagedBytes.Load(); got != 0 {
		t.Fatalf("ordered close retained %d staged bytes", got)
	}
	if global, _ := coordinator.admission.snapshot(""); global != 0 {
		t.Fatalf("ordered close retained %d in-flight bytes", global)
	}
}

func TestStorageWriteCloseDeadlineStopsWorkerAndReleasesAdmission(t *testing.T) {
	ctx, cancel := duckDBStorageWriteTestContext(t)
	defer cancel()
	warehouse, coordinator, table := newStorageWriteFixture(t, []domain.Field{{Name: "id", Type: "INT64"}})
	descriptor := storageWriteDescriptor(t, protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL))
	row := storageWriteRow(t, descriptor, map[string]any{"id": int64(1)})
	stageApplied := make(chan struct{})
	releaseWorker := make(chan struct{})
	coordinator.afterStage = func() {
		close(stageApplied)
		<-releaseWorker
	}
	stream := table.Name() + "/streams/close-timeout"
	stageDone := make(chan error, 1)
	go func() {
		stageDone <- coordinator.StagePending(ctx, writeports.AppendBatch{
			StreamName: stream, Table: table, WireBytes: 128, Descriptor: descriptor,
			Rows: [][]byte{row}, SchemaFingerprint: "schema", PayloadDigest: "payload",
		})
	}()
	select {
	case <-stageApplied:
	case <-ctx.Done():
		t.Fatalf("waiting for blocked staging: %v", ctx.Err())
	}
	closeContext, cancelClose := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancelClose()
	closeDone := make(chan error, 1)
	go func() { closeDone <- coordinator.Close(closeContext) }()
	waitForStorageWriteClosed(t, ctx, coordinator)
	var closeErr error
	select {
	case closeErr = <-closeDone:
	case <-ctx.Done():
		t.Fatalf("waiting for close deadline: %v", ctx.Err())
	}
	if !errors.Is(closeErr, context.DeadlineExceeded) {
		t.Fatalf("close error = %v, want deadline exceeded", closeErr)
	}
	close(releaseWorker)
	if err := <-stageDone; err != nil {
		t.Fatalf("already-applied stage result: %v", err)
	}
	select {
	case <-coordinator.done:
	case <-ctx.Done():
		t.Fatalf("waiting for worker stop: %v", ctx.Err())
	}
	coordinator.afterStage = nil
	if global, perStream := coordinator.admission.snapshot(stream); global != 0 || perStream != 0 {
		t.Fatalf("deadline close retained in-flight bytes global=%d stream=%d", global, perStream)
	}
	// Cleanup did not complete before its deadline, so its error is paired with
	// an intact receipt/staging table that a later process initialization drops.
	if got := storageWriteReceiptCount(t, ctx, warehouse, stream); got != 1 {
		t.Fatalf("deadline close receipt count = %d, want 1", got)
	}
	if got := storageWriteStagingTableCount(t, ctx, warehouse); got != 1 {
		t.Fatalf("deadline close staging table count = %d, want 1", got)
	}
	restarted, err := NewStorageWriteCoordinator(ctx, warehouse, coordinator.resolver, storageWriteCoordinatorTestConfig())
	if err != nil {
		t.Fatalf("restart coordinator cleanup: %v", err)
	}
	if got := storageWriteTotalReceiptCount(t, ctx, warehouse); got != 0 {
		t.Fatalf("restart retained %d stale receipts", got)
	}
	if got := storageWriteStagingTableCount(t, ctx, warehouse); got != 0 {
		t.Fatalf("restart retained %d stale staging tables", got)
	}
	if got := restarted.stagedBytes.Load(); got != 0 {
		t.Fatalf("restart staged byte counter = %d, want 0", got)
	}
	if err := restarted.Close(ctx); err != nil {
		t.Fatalf("close restarted coordinator: %v", err)
	}
}

func TestStorageWriteLostStageAcknowledgementRecoversOnNewBidiCall(t *testing.T) {
	ctx, cancel := duckDBStorageWriteTestContext(t)
	defer cancel()
	warehouse, coordinator, table := newStorageWriteFixture(t, []domain.Field{{Name: "id", Type: "INT64"}})
	service, err := writeapp.New(writeapp.Config{
		Location: "US", ProtocolModelVersion: "spark-0.44.2",
		MaxStreams: 2, MaxAppendBytes: 1024 * 1024, MaxAppendEnvelopeBytes: 64 * 1024, MaxConcurrentAppendRequests: 2,
		OrphanTTL: time.Hour, CleanupInterval: time.Minute,
	}, coordinator, storageWriteRetryClock{}, storageWriteRetryIDs{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}

	listener := bufconn.Listen(4 * 1024 * 1024)
	server := grpc.NewServer()
	storagepb.RegisterBigQueryWriteServer(server, grpcserver.NewStorageWriteServer(service))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	client := newDuckDBStorageWriteClient(t, listener)
	created, err := client.CreateWriteStream(ctx, &storagepb.CreateWriteStreamRequest{
		Parent: table.Name(), WriteStream: &storagepb.WriteStream{Type: storagepb.WriteStream_PENDING},
	})
	if err != nil {
		t.Fatal(err)
	}

	descriptorBytes := storageWriteDescriptor(t, protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL))
	descriptor := new(descriptorpb.DescriptorProto)
	if err := proto.Unmarshal(descriptorBytes, descriptor); err != nil {
		t.Fatal(err)
	}
	row := storageWriteRow(t, descriptorBytes, map[string]any{"id": int64(42)})
	appendRequest := func() *storagepb.AppendRowsRequest {
		return &storagepb.AppendRowsRequest{
			WriteStream: created.GetName(), Offset: wrapperspb.Int64(0),
			Rows: &storagepb.AppendRowsRequest_ProtoRows{ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
				WriterSchema: &storagepb.ProtoSchema{ProtoDescriptor: descriptor},
				Rows:         &storagepb.ProtoRows{SerializedRows: [][]byte{row}},
			}},
		}
	}

	stageApplied := make(chan struct{})
	releaseWorker := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseWorker) }) }
	defer release()
	coordinator.afterStage = func() {
		close(stageApplied)
		<-releaseWorker
	}
	firstContext, cancelFirst := context.WithCancel(ctx)
	firstAppend, err := client.AppendRows(firstContext)
	if err != nil {
		t.Fatal(err)
	}
	if err := firstAppend.Send(appendRequest()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stageApplied:
	case <-ctx.Done():
		t.Fatalf("waiting for staged side effect: %v", ctx.Err())
	}
	cancelFirst()
	firstResponse, firstErr := firstAppend.Recv()
	if firstErr == nil && firstResponse.GetAppendResult() != nil {
		t.Fatalf("first call unexpectedly acknowledged staged rows: %#v", firstResponse)
	}
	if firstErr != nil && grpcstatus.Code(firstErr) != codes.Canceled {
		t.Fatalf("first call ended with %v, want cancellation or an embedded error", firstErr)
	}
	ledgerBeforeRetry, err := service.GetStream(ctx, created.GetName())
	if err != nil {
		t.Fatalf("wait for canceled application append: %v", err)
	}
	if ledgerBeforeRetry.NextOffset != 0 || ledgerBeforeRetry.RowCount != 0 {
		t.Fatalf("canceled application ledger advanced to offset=%d rows=%d", ledgerBeforeRetry.NextOffset, ledgerBeforeRetry.RowCount)
	}
	if _, err := client.FinalizeWriteStream(ctx, &storagepb.FinalizeWriteStreamRequest{Name: created.GetName()}); grpcstatus.Code(err) != codes.FailedPrecondition {
		t.Fatalf("finalize while staged acknowledgement is unresolved = %v, want FAILED_PRECONDITION", err)
	}
	release()
	// This queued operation is a barrier: it proves the worker published the
	// staging receipt after the canceled caller stopped waiting for its result.
	if _, err := coordinator.DescribeTable(ctx, table); err != nil {
		t.Fatalf("wait for coordinator receipt: %v", err)
	}
	coordinator.afterStage = nil
	expectedStagedBytes := batchStagedBytes(writeports.AppendBatch{Rows: [][]byte{row}})
	if got := coordinator.stagedBytes.Load(); got != expectedStagedBytes {
		t.Fatalf("lost acknowledgement staged bytes = %d, want %d", got, expectedStagedBytes)
	}
	if got := storageWriteReceiptCount(t, ctx, warehouse, created.GetName()); got != 1 {
		t.Fatalf("lost acknowledgement receipt count = %d, want 1", got)
	}

	// A new transport connection also creates a fresh bidi inheritance scope.
	// Repeating the original stream/schema/offset must reconcile the application
	// ledger with the existing coordinator receipt rather than stage another row.
	retryClient := newDuckDBStorageWriteClient(t, listener)
	retryAppend, err := retryClient.AppendRows(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := retryAppend.Send(appendRequest()); err != nil {
		t.Fatal(err)
	}
	retried, err := retryAppend.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if retried.GetAppendResult() == nil || retried.GetAppendResult().GetOffset().GetValue() != 0 {
		t.Fatalf("retry response did not acknowledge offset zero: %#v", retried)
	}
	if got := coordinator.stagedBytes.Load(); got != expectedStagedBytes {
		t.Fatalf("idempotent retry changed staged bytes to %d", got)
	}
	if err := retryAppend.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := retryAppend.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("retry append EOF = %v", err)
	}

	finalized, err := retryClient.FinalizeWriteStream(ctx, &storagepb.FinalizeWriteStreamRequest{Name: created.GetName()})
	if err != nil || finalized.GetRowCount() != 1 {
		t.Fatalf("finalize row count: %#v, %v", finalized, err)
	}
	committed, err := retryClient.BatchCommitWriteStreams(ctx, &storagepb.BatchCommitWriteStreamsRequest{
		Parent: table.Name(), WriteStreams: []string{created.GetName()},
	})
	if err != nil || committed.GetCommitTime() == nil || len(committed.GetStreamErrors()) != 0 {
		t.Fatalf("commit after retry: %#v, %v", committed, err)
	}
	if got := storageWriteRowCount(t, ctx, warehouse, table); got != 1 {
		t.Fatalf("committed row count = %d, want exactly 1", got)
	}
	if got := coordinator.stagedBytes.Load(); got != 0 {
		t.Fatalf("commit retained %d staged bytes", got)
	}
	if global, perStream := coordinator.admission.snapshot(created.GetName()); global != 0 || perStream != 0 {
		t.Fatalf("commit retained in-flight bytes global=%d stream=%d", global, perStream)
	}
}

func TestStorageWriteDecodesNestedAndRepeatedSparkProtoRows(t *testing.T) {
	ctx, cancel := duckDBStorageWriteTestContext(t)
	defer cancel()
	precision10, precision6, scale2 := int64(10), int64(6), int64(2)
	warehouse, coordinator, table := newStorageWriteFixture(t, []domain.Field{
		{Name: "payload", Type: "RECORD", Fields: []domain.Field{
			{Name: "code", Type: "INT64"}, {Name: "label", Type: "STRING"},
			{Name: "amount", Type: "BIGNUMERIC", Precision: &precision10, Scale: &scale2, RoundingMode: domain.RoundingModeHalfEven},
		}},
		{Name: "payloads", Type: "RECORD", Mode: "REPEATED", Fields: []domain.Field{
			{Name: "code", Type: "INT64"}, {Name: "amount", Type: "NUMERIC", Precision: &precision6, Scale: &scale2},
		}},
		{Name: "amounts", Type: "NUMERIC", Mode: "REPEATED", Precision: &precision6, Scale: &scale2, RoundingMode: domain.RoundingModeHalfEven},
	})
	optional, repeated := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL, descriptorpb.FieldDescriptorProto_LABEL_REPEATED
	messageType, intType, stringType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_TYPE_STRING
	rootName, payloadName, payloadsName := "Schema", "STRUCT1", "STRUCT2"
	payloadFieldName, payloadsFieldName := "payload", "payloads"
	payloadNumber, payloadsNumber := int32(1), int32(2)
	payloadTypeName, payloadsTypeName := "STRUCT1", "STRUCT2"
	descriptor := &descriptorpb.DescriptorProto{
		Name: &rootName,
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: &payloadFieldName, Number: &payloadNumber, Type: &messageType, Label: &optional, TypeName: &payloadTypeName},
			{Name: &payloadsFieldName, Number: &payloadsNumber, Type: &messageType, Label: &repeated, TypeName: &payloadsTypeName},
			protoField("amounts", 3, stringType, repeated),
		},
		NestedType: []*descriptorpb.DescriptorProto{
			{Name: &payloadName, Field: []*descriptorpb.FieldDescriptorProto{
				protoField("code", 1, intType, optional), protoField("label", 2, stringType, optional), protoField("amount", 3, stringType, optional),
			}},
			{Name: &payloadsName, Field: []*descriptorpb.FieldDescriptorProto{protoField("code", 1, intType, optional), protoField("amount", 2, stringType, optional)}},
		},
	}
	serializedDescriptor, err := proto.Marshal(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	message, err := messageDescriptor(serializedDescriptor)
	if err != nil {
		t.Fatal(err)
	}
	row := dynamicpb.NewMessage(message)
	payloadField := message.Fields().ByName("payload")
	payload := dynamicpb.NewMessage(payloadField.Message())
	payload.Set(payload.Descriptor().Fields().ByName("code"), protoreflect.ValueOfInt64(7))
	payload.Set(payload.Descriptor().Fields().ByName("label"), protoreflect.ValueOfString("primary"))
	payload.Set(payload.Descriptor().Fields().ByName("amount"), protoreflect.ValueOfString("12345678.905"))
	row.Set(payloadField, protoreflect.ValueOfMessage(payload))
	payloadsField := message.Fields().ByName("payloads")
	for index, code := range []int64{8, 9} {
		item := dynamicpb.NewMessage(payloadsField.Message())
		item.Set(item.Descriptor().Fields().ByName("code"), protoreflect.ValueOfInt64(code))
		item.Set(item.Descriptor().Fields().ByName("amount"), protoreflect.ValueOfString([]string{"12.345", "-56.785"}[index]))
		row.Mutable(payloadsField).List().Append(protoreflect.ValueOfMessage(item))
	}
	for _, amount := range []string{"1.025", "-1.035"} {
		row.Mutable(message.Fields().ByName("amounts")).List().Append(protoreflect.ValueOfString(amount))
	}
	serializedRow, err := proto.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.AppendDefault(ctx, writeports.AppendBatch{
		StreamName: table.Name() + "/streams/_default", Table: table,
		Descriptor: serializedDescriptor, Rows: [][]byte{serializedRow},
		SchemaFingerprint: "nested-schema", PayloadDigest: "nested-row",
	}); err != nil {
		t.Fatal(err)
	}
	schema, err := coordinator.DescribeTable(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	if schema.Fields[0].Fields[2].Type != "BIGNUMERIC" || schema.Fields[0].Fields[2].Precision == nil || *schema.Fields[0].Fields[2].Precision != 10 ||
		schema.Fields[0].Fields[2].RoundingMode != domain.RoundingModeHalfEven ||
		schema.Fields[1].Mode != "REPEATED" || schema.Fields[1].Fields[1].Type != "NUMERIC" ||
		schema.Fields[2].Mode != "REPEATED" || schema.Fields[2].RoundingMode != domain.RoundingModeHalfEven {
		t.Fatalf("canonical Storage Write schema lost recursive decimal identity: %#v", schema)
	}
	statement := `SELECT "payload"."code", "payload"."label", CAST("payload"."amount" AS VARCHAR), "payloads"[1]."code", "payloads"[2]."code", CAST("payloads"[2]."amount" AS VARCHAR), CAST("amounts"[1] AS VARCHAR), CAST("amounts"[2] AS VARCHAR) FROM ` +
		quoteIdentifier(physicalSchema(table.ProjectID, table.DatasetID)) + `.` + quoteIdentifier(table.TableID)
	var code, first, second int64
	var label, amount, repeatedAmount, firstScalarAmount, secondScalarAmount string
	if err := warehouse.db.QueryRowContext(ctx, statement).Scan(&code, &label, &amount, &first, &second, &repeatedAmount, &firstScalarAmount, &secondScalarAmount); err != nil {
		t.Fatal(err)
	}
	if code != 7 || label != "primary" || amount != "12345678.90" || first != 8 || second != 9 || repeatedAmount != "-56.79" || firstScalarAmount != "1.02" || secondScalarAmount != "-1.04" {
		t.Fatalf("unexpected nested row: %d %q %q %d %d %q %q %q", code, label, amount, first, second, repeatedAmount, firstScalarAmount, secondScalarAmount)
	}
	const invalidMarker = "nested-repeated-secret-marker/2"
	invalid := dynamicpb.NewMessage(message)
	invalid.Mutable(message.Fields().ByName("amounts")).List().Append(protoreflect.ValueOfString(invalidMarker))
	serializedInvalid, err := proto.Marshal(invalid)
	if err != nil {
		t.Fatal(err)
	}
	err = coordinator.AppendDefault(ctx, writeports.AppendBatch{
		StreamName: table.Name() + "/streams/_default", Table: table,
		Descriptor: serializedDescriptor, Rows: [][]byte{serializedInvalid},
		SchemaFingerprint: "nested-schema", PayloadDigest: "invalid-nested-row",
	})
	if !errors.Is(err, writeports.ErrInvalidRows) || !strings.Contains(err.Error(), domain.DecimalValueInvalidV1) || strings.Contains(err.Error(), invalidMarker) {
		t.Fatalf("invalid nested/repeated decimal error = %v", err)
	}
	if got := storageWriteRowCount(t, ctx, warehouse, table); got != 1 {
		t.Fatalf("invalid nested/repeated decimal changed row count to %d", got)
	}
}

func TestStorageWriteCoordinatorSerializesSixteenParallelRequests(t *testing.T) {
	ctx, cancel := duckDBStorageWriteTestContext(t)
	defer cancel()
	_, coordinator, table := newStorageWriteFixture(t, []domain.Field{{Name: "id", Type: "INT64"}})
	var wait sync.WaitGroup
	errorsByRequest := make([]error, 16)
	for index := range errorsByRequest {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			schema, err := coordinator.DescribeTable(ctx, table)
			if err == nil && (len(schema.Fields) != 1 || schema.Fields[0].Name != "id") {
				err = fmt.Errorf("unexpected schema: %#v", schema)
			}
			errorsByRequest[index] = err
		}(index)
	}
	wait.Wait()
	for _, err := range errorsByRequest {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDecodePackedDateTimeMicros(t *testing.T) {
	// 2026-08-08 12:34:56.123456 using CivilTimeEncoder's documented layout.
	seconds := int64(2026)<<26 | int64(8)<<22 | int64(8)<<17 | int64(12)<<12 | int64(34)<<6 | int64(56)
	value, err := decodePackedDateTimeMicros(seconds<<20 | 123456)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 8, 8, 12, 34, 56, 123456000, time.UTC)
	if !value.Equal(want) {
		t.Fatalf("got %s, want %s", value, want)
	}
}

func newStorageWriteFixture(t *testing.T, fields []domain.Field) (*Warehouse, *StorageWriteCoordinator, writedomain.TableReference) {
	return newStorageWriteFixtureWithConfig(t, fields, storageWriteCoordinatorTestConfig())
}

func newStorageWriteFixtureWithConfig(
	t *testing.T,
	fields []domain.Field,
	config writeports.CoordinatorConfig,
) (*Warehouse, *StorageWriteCoordinator, writedomain.TableReference) {
	t.Helper()
	warehouse, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := duckDBStorageWriteTestContext(t)
	defer cancel()
	if err := warehouse.CreateDataset(ctx, "test-project", "dataset"); err != nil {
		t.Fatal(err)
	}
	table := domain.Table{ProjectID: "test-project", DatasetID: "dataset", ID: "items", Schema: fields}
	if err := warehouse.CreateTable(ctx, table); err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewStorageWriteCoordinator(ctx, warehouse, staticStorageWriteResolver{table: table}, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := duckDBStorageWriteTestContext(t)
		defer closeCancel()
		_ = coordinator.Close(closeContext)
		_ = warehouse.Close()
	})
	return warehouse, coordinator, writedomain.TableReference{ProjectID: "test-project", DatasetID: "dataset", TableID: "items"}
}

type staticStorageWriteResolver struct {
	table domain.Table
}

func (resolver staticStorageWriteResolver) GetTable(_ context.Context, projectID, datasetID, tableID string) (domain.Table, error) {
	if resolver.table.ProjectID != projectID || resolver.table.DatasetID != datasetID || resolver.table.ID != tableID {
		return domain.Table{}, fmt.Errorf("%w: table", domain.ErrNotFound)
	}
	result := resolver.table
	result.Schema = domain.CloneFields(result.Schema)
	return result, nil
}

func storageWriteDescriptor(t *testing.T, fields ...*descriptorpb.FieldDescriptorProto) []byte {
	t.Helper()
	name := "Schema"
	descriptor := &descriptorpb.DescriptorProto{Name: &name, Field: fields}
	serialized, err := proto.Marshal(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	return serialized
}

func protoField(name string, number int32, fieldType descriptorpb.FieldDescriptorProto_Type, label descriptorpb.FieldDescriptorProto_Label) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{Name: &name, Number: &number, Type: &fieldType, Label: &label}
}

func storageWriteRow(t *testing.T, descriptor []byte, values map[string]any) []byte {
	t.Helper()
	messageDescriptor, err := messageDescriptor(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	message := dynamicpb.NewMessage(messageDescriptor)
	for name, value := range values {
		field := messageDescriptor.Fields().ByName(protoreflect.Name(name))
		if field == nil {
			t.Fatalf("descriptor has no field %q", name)
		}
		if field.IsList() {
			list := message.Mutable(field).List()
			for _, element := range value.([]any) {
				list.Append(protoreflect.ValueOf(element))
			}
			continue
		}
		message.Set(field, protoreflect.ValueOf(value))
	}
	serialized, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return serialized
}

func storageWriteRowCount(t *testing.T, ctx context.Context, warehouse *Warehouse, table writedomain.TableReference) int {
	t.Helper()
	statement := "SELECT count(*) FROM " + quoteIdentifier(physicalSchema(table.ProjectID, table.DatasetID)) + "." + quoteIdentifier(table.TableID)
	var count int
	if err := warehouse.db.QueryRowContext(ctx, statement).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func storageWriteReceiptCount(t *testing.T, ctx context.Context, warehouse *Warehouse, stream string) int {
	t.Helper()
	statement := "SELECT count(*) FROM " + quoteIdentifier(storageWriteInternalSchema) + "." + quoteIdentifier(storageWriteReceiptTable) + " WHERE stream_name = ?"
	var count int
	if err := warehouse.db.QueryRowContext(ctx, statement, stream).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func storageWriteTotalReceiptCount(t *testing.T, ctx context.Context, warehouse *Warehouse) int {
	t.Helper()
	statement := "SELECT count(*) FROM " + quoteIdentifier(storageWriteInternalSchema) + "." + quoteIdentifier(storageWriteReceiptTable)
	var count int
	if err := warehouse.db.QueryRowContext(ctx, statement).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func storageWriteStagingTableCount(t *testing.T, ctx context.Context, warehouse *Warehouse) int {
	t.Helper()
	var count int
	if err := warehouse.db.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.tables
		WHERE table_schema = ? AND table_name LIKE 'stream_%'`, storageWriteInternalSchema).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func storageWriteInternalTableCount(t *testing.T, ctx context.Context, warehouse *Warehouse) int {
	t.Helper()
	var count int
	if err := warehouse.db.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema = ?`, storageWriteInternalSchema).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func storageWriteCoordinatorTestConfig() writeports.CoordinatorConfig {
	return writeports.CoordinatorConfig{
		QueueCapacity: 32, QueueWaitTimeout: time.Second, OperationTimeout: 5 * time.Second,
		MaxInFlightBytes: 64 << 20, MaxInFlightBytesPerStream: 32 << 20,
		MaxStagedBytes: 1 << 30, MaxStagedBytesPerStream: 512 << 20,
	}
}

func waitForStorageWriteClosed(t *testing.T, ctx context.Context, coordinator *StorageWriteCoordinator) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !coordinator.closed.Load() {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("waiting for coordinator close gate: %v", ctx.Err())
		}
	}
}

func newDuckDBStorageWriteClient(t *testing.T, listener *bufconn.Listener) storagepb.BigQueryWriteClient {
	t.Helper()
	connection, err := grpc.NewClient(
		"passthrough:///bufnet-duckdb-storage-write",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return storagepb.NewBigQueryWriteClient(connection)
}

type storageWriteRetryClock struct{}

func (storageWriteRetryClock) Now() time.Time {
	return time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
}

type storageWriteRetryIDs struct{}

func (storageWriteRetryIDs) NewID() string { return "retry-stream" }

func duckDBStorageWriteTestContext(t *testing.T) (context.Context, context.CancelFunc) {
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
