package duckdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	writedomain "github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
	writeports "github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
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
		{Name: "geo", Type: "GEOGRAPHY"},
		{Name: "tags", Type: "STRING", Mode: "REPEATED"},
	})
	descriptor := storageWriteDescriptor(t,
		protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		protoField("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		protoField("event_date", 3, descriptorpb.FieldDescriptorProto_TYPE_INT32, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		protoField("event_at", 4, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		protoField("amount", 5, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		protoField("geo", 6, descriptorpb.FieldDescriptorProto_TYPE_BYTES, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		protoField("tags", 7, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_REPEATED),
	)
	row := storageWriteRow(t, descriptor, map[string]any{
		"id": int64(7), "name": "first", "event_date": int32(1),
		"event_at": int64(1_500_000), "amount": "12.340000000",
		"geo": []byte("POINT(1 2)"), "tags": []any{"alpha", "beta"},
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
		Parent: table, StreamNames: []string{pendingName}, CommitTime: time.Now().UTC(),
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

	query := `SELECT "name", CAST("event_date" AS VARCHAR), epoch_us("event_at"), CAST("amount" AS VARCHAR), "geo", "tags"[1] FROM ` +
		quoteIdentifier(physicalSchema(table.ProjectID, table.DatasetID)) + `.` + quoteIdentifier(table.TableID) + ` WHERE "id" = 7`
	var name, date, amount, geography, firstTag string
	var timestampMicros int64
	if err := warehouse.db.QueryRowContext(ctx, query).Scan(&name, &date, &timestampMicros, &amount, &geography, &firstTag); err != nil {
		t.Fatal(err)
	}
	if name != "first" || date != "1970-01-02" || timestampMicros != 1_500_000 || amount != "12.340000000" || geography != "POINT(1 2)" || firstTag != "alpha" {
		t.Fatalf("unexpected converted row: %q %q %d %q %q %q", name, date, timestampMicros, amount, geography, firstTag)
	}
}

func TestStorageWriteCommitFaultRollsBackAllStreams(t *testing.T) {
	ctx, cancel := duckDBStorageWriteTestContext(t)
	defer cancel()
	warehouse, coordinator, table := newStorageWriteFixture(t, []domain.Field{{Name: "id", Type: "INT64"}})
	descriptor := storageWriteDescriptor(t, protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL))
	streamNames := []string{table.Name() + "/streams/a", table.Name() + "/streams/b"}
	for index, streamName := range streamNames {
		if err := coordinator.StagePending(ctx, writeports.AppendBatch{
			StreamName: streamName, Table: table, Descriptor: descriptor,
			Rows:              [][]byte{storageWriteRow(t, descriptor, map[string]any{"id": int64(index + 1)})},
			SchemaFingerprint: "schema", PayloadDigest: fmt.Sprintf("row-%d", index),
		}); err != nil {
			t.Fatal(err)
		}
	}
	coordinator.beforeCommit = func() error { return errors.New("injected fault before commit") }
	request := writeports.CommitRequest{Parent: table, StreamNames: streamNames, CommitTime: time.Now().UTC()}
	if err := coordinator.CommitPending(ctx, request); err == nil {
		t.Fatal("expected injected commit fault")
	}
	if got := storageWriteRowCount(t, ctx, warehouse, table); got != 0 {
		t.Fatalf("failed atomic commit exposed %d rows", got)
	}
	coordinator.beforeCommit = nil
	if err := coordinator.CommitPending(ctx, request); err != nil {
		t.Fatal(err)
	}
	if got := storageWriteRowCount(t, ctx, warehouse, table); got != 2 {
		t.Fatalf("retry exposed %d rows, want 2", got)
	}
}

func TestStorageWriteDecodesNestedAndRepeatedSparkProtoRows(t *testing.T) {
	ctx, cancel := duckDBStorageWriteTestContext(t)
	defer cancel()
	warehouse, coordinator, table := newStorageWriteFixture(t, []domain.Field{
		{Name: "payload", Type: "RECORD", Fields: []domain.Field{{Name: "code", Type: "INT64"}, {Name: "label", Type: "STRING"}}},
		{Name: "payloads", Type: "RECORD", Mode: "REPEATED", Fields: []domain.Field{{Name: "code", Type: "INT64"}}},
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
		},
		NestedType: []*descriptorpb.DescriptorProto{
			{Name: &payloadName, Field: []*descriptorpb.FieldDescriptorProto{
				protoField("code", 1, intType, optional), protoField("label", 2, stringType, optional),
			}},
			{Name: &payloadsName, Field: []*descriptorpb.FieldDescriptorProto{protoField("code", 1, intType, optional)}},
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
	row.Set(payloadField, protoreflect.ValueOfMessage(payload))
	payloadsField := message.Fields().ByName("payloads")
	for _, code := range []int64{8, 9} {
		item := dynamicpb.NewMessage(payloadsField.Message())
		item.Set(item.Descriptor().Fields().ByName("code"), protoreflect.ValueOfInt64(code))
		row.Mutable(payloadsField).List().Append(protoreflect.ValueOfMessage(item))
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
	statement := `SELECT "payload"."code", "payload"."label", "payloads"[1]."code", "payloads"[2]."code" FROM ` +
		quoteIdentifier(physicalSchema(table.ProjectID, table.DatasetID)) + `.` + quoteIdentifier(table.TableID)
	var code, first, second int64
	var label string
	if err := warehouse.db.QueryRowContext(ctx, statement).Scan(&code, &label, &first, &second); err != nil {
		t.Fatal(err)
	}
	if code != 7 || label != "primary" || first != 8 || second != 9 {
		t.Fatalf("unexpected nested row: %d %q %d %d", code, label, first, second)
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
	coordinator, err := NewStorageWriteCoordinator(warehouse, 32)
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
