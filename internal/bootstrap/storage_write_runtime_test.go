package bootstrap

// This test crosses configuration, runtime composition, the generated Storage
// Write client, and the DuckDB-backed coordinator without replacing any of
// those production boundaries.

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/adapters/system"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/config"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	grpcserver "github.com/leeyh0216/go-bemu/internal/transport/grpc"
)

func TestStorageWriteRuntimeCommitsPendingAndDefaultRowsThroughGeneratedClient(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	catalog := application.NewCatalogService(memory.NewCatalogRepository(), warehouse, system.Clock{}, application.WithTableDataReader(warehouse))
	if _, err := catalog.CreateProject(ctx, domain.Project{ID: "writer-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateDataset(ctx, domain.Dataset{ProjectID: "writer-project", ID: "analytics", Location: "US"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateTable(ctx, domain.Table{
		ProjectID: "writer-project", DatasetID: "analytics", ID: "events", Type: "TABLE",
		Schema: []domain.Field{{Name: "id", Type: "INT64", Mode: "NULLABLE"}},
	}); err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.Storage.Write.Enabled = true
	runtime, err := composeStorageWrite(ctx, cfg, warehouse, catalog, system.Clock{}, system.IDGenerator{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close(context.Background()) })

	listener := bufconn.Listen(4 << 20)
	server := grpcserver.NewWithServices(grpcserver.Services{Write: runtime.Service})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///storage-write-runtime",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := storagepb.NewBigQueryWriteClient(connection)
	parent := "projects/writer-project/datasets/analytics/tables/events"
	pending, err := client.CreateWriteStream(ctx, &storagepb.CreateWriteStreamRequest{
		Parent: parent, WriteStream: &storagepb.WriteStream{Type: storagepb.WriteStream_PENDING},
	})
	if err != nil {
		t.Fatal(err)
	}
	descriptor, row := storageWriteRuntimeRow(t, 11)
	appendRows, err := client.AppendRows(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := appendRows.Send(&storagepb.AppendRowsRequest{
		WriteStream: pending.GetName(), Offset: wrapperspb.Int64(0),
		Rows: &storagepb.AppendRowsRequest_ProtoRows{ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
			WriterSchema: &storagepb.ProtoSchema{ProtoDescriptor: descriptor},
			Rows:         &storagepb.ProtoRows{SerializedRows: [][]byte{row}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := appendRows.Recv()
	if err != nil || response.GetAppendResult().GetOffset().GetValue() != 0 {
		t.Fatalf("pending append = %#v, %v", response, err)
	}
	if err := appendRows.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.FinalizeWriteStream(ctx, &storagepb.FinalizeWriteStreamRequest{Name: pending.GetName()}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.BatchCommitWriteStreams(ctx, &storagepb.BatchCommitWriteStreamsRequest{
		Parent: parent, WriteStreams: []string{pending.GetName()},
	}); err != nil {
		t.Fatal(err)
	}

	defaultRows, err := client.AppendRows(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := defaultRows.Send(&storagepb.AppendRowsRequest{
		WriteStream: parent + "/streams/_default",
		Rows: &storagepb.AppendRowsRequest_ProtoRows{ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
			WriterSchema: &storagepb.ProtoSchema{ProtoDescriptor: descriptor},
			Rows:         &storagepb.ProtoRows{SerializedRows: [][]byte{storageWriteRuntimeMustRow(t, descriptor, 22)}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	response, err = defaultRows.Recv()
	if err != nil || response.GetError() != nil {
		t.Fatalf("default append = %#v, %v", response, err)
	}
	page, err := catalog.ListTableData(ctx, "writer-project", "analytics", "events", 0, ports.TableDataMaxResults{Value: 10, Present: true})
	if err != nil || page.TotalRows != 2 {
		t.Fatalf("visible Storage Write rows = %#v, %v", page, err)
	}
}

func storageWriteRuntimeRow(t *testing.T, value int64) (*descriptorpb.DescriptorProto, []byte) {
	t.Helper()
	name, fieldName, fileName, syntax := "RuntimeRow", "id", "runtime-row.proto", "proto2"
	fieldNumber := int32(1)
	fieldType := descriptorpb.FieldDescriptorProto_TYPE_INT64
	label := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	descriptor, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: &fileName, Syntax: &syntax, MessageType: []*descriptorpb.DescriptorProto{{
			Name: &name, Field: []*descriptorpb.FieldDescriptorProto{{
				Name: &fieldName, Number: &fieldNumber, Type: &fieldType, Label: &label,
			}},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	message := descriptor.Messages().Get(0)
	row := dynamicpb.NewMessage(message)
	row.Set(message.Fields().ByName(protoreflect.Name(fieldName)), protoreflect.ValueOfInt64(value))
	encoded, err := proto.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	return &descriptorpb.DescriptorProto{
		Name: &name, Field: []*descriptorpb.FieldDescriptorProto{{
			Name: &fieldName, Number: &fieldNumber, Type: &fieldType, Label: &label,
		}},
	}, encoded
}

func storageWriteRuntimeMustRow(t *testing.T, descriptor *descriptorpb.DescriptorProto, value int64) []byte {
	t.Helper()
	fileName, syntax := "runtime-row.proto", "proto2"
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{Name: &fileName, Syntax: &syntax, MessageType: []*descriptorpb.DescriptorProto{descriptor}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	message := file.Messages().Get(0)
	row := dynamicpb.NewMessage(message)
	row.Set(message.Fields().ByName("id"), protoreflect.ValueOfInt64(value))
	encoded, err := proto.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
