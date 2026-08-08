package grpcserver

import (
	"bytes"
	"context"
	"encoding/binary"
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
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/leeyh0216/go-bemu/internal/contracttest"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
	readapp "github.com/leeyh0216/go-bemu/internal/storageread/application"
	"github.com/leeyh0216/go-bemu/internal/storageread/domain"
	"github.com/leeyh0216/go-bemu/internal/storageread/ports"
)

func TestStorageReadWireFormatsAndOffsetResume(t *testing.T) {
	contracttest.Operation(t, "grpc.bigquery-read.create-read-session")
	contracttest.Operation(t, "grpc.bigquery-read.read-rows")
	for _, testCase := range []struct {
		name   string
		format domain.Format
		wire   storagepb.DataFormat
	}{
		{name: "arrow", format: domain.FormatArrow, wire: storagepb.DataFormat_ARROW},
		{name: "avro", format: domain.FormatAvro, wire: storagepb.DataFormat_AVRO},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := grpcTestContext(t)
			defer cancel()
			materializer := newWireMaterializer(t, testCase.format, 8)
			service := newWireReadService(t, materializer)
			client, healthClient := startReadServer(t, service)

			health, err := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: storageReadServiceName})
			if err != nil {
				t.Fatal(err)
			}
			if health.Status != grpc_health_v1.HealthCheckResponse_SERVING {
				t.Fatalf("Storage Read health = %s, want SERVING", health.Status)
			}

			session, err := client.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
				Parent:         "projects/reader-project",
				MaxStreamCount: 4,
				ReadSession: &storagepb.ReadSession{
					Table:      "projects/data-project/datasets/analytics/tables/events",
					DataFormat: testCase.wire,
					TraceId:    "reader-stage-42",
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if materializer.calls != 1 {
				t.Fatalf("materialize calls = %d, want 1", materializer.calls)
			}
			if len(session.GetStreams()) != 4 || session.GetEstimatedRowCount() != 8 {
				t.Fatalf("unexpected session streams/rows: %d/%d", len(session.GetStreams()), session.GetEstimatedRowCount())
			}
			assertReferenceSchemaWire(t, testCase.format, session)

			rows, err := client.ReadRows(ctx, &storagepb.ReadRowsRequest{
				ReadStream: session.GetStreams()[1].GetName(),
				Offset:     1,
			})
			if err != nil {
				t.Fatal(err)
			}
			response, err := rows.Recv()
			if err != nil {
				t.Fatal(err)
			}
			if response.GetRowCount() != 1 {
				t.Fatalf("response row_count = %d, want 1", response.GetRowCount())
			}
			if response.GetStats().GetProgress().GetAtResponseStart() != 0.5 ||
				response.GetStats().GetProgress().GetAtResponseEnd() != 1 {
				t.Fatalf("unexpected progress: %+v", response.GetStats().GetProgress())
			}
			assertRowsWire(t, testCase.format, response)
			if _, err := rows.Recv(); !errors.Is(err, io.EOF) {
				t.Fatalf("second Recv error = %v, want io.EOF", err)
			}
			if got := materializer.snapshot.lastRange; got != (wireRange{start: 3, end: 4, maxRows: 2}) {
				t.Fatalf("snapshot range = %+v, want [3,4) with max 2", got)
			}
		})
	}
}

func TestStorageReadRejectsUnsupportedCompressionExplicitly(t *testing.T) {
	ctx, cancel := grpcTestContext(t)
	defer cancel()
	materializer := newWireMaterializer(t, domain.FormatArrow, 1)
	client, _ := startReadServer(t, newWireReadService(t, materializer))
	_, err := client.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
		Parent: "projects/reader-project",
		ReadSession: &storagepb.ReadSession{
			Table:      "projects/data-project/datasets/analytics/tables/events",
			DataFormat: storagepb.DataFormat_ARROW,
			ReadOptions: &storagepb.ReadSession_TableReadOptions{
				ResponseCompressionCodec: storagepb.ReadSession_TableReadOptions_RESPONSE_COMPRESSION_CODEC_LZ4.Enum(),
			},
		},
	})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("compression status = %s, want UNIMPLEMENTED: %v", status.Code(err), err)
	}
	if !strings.Contains(err.Error(), "response compression is not implemented") {
		t.Fatalf("public status omitted internal option detail: %v", err)
	}
	if materializer.calls != 0 {
		t.Fatalf("materializer called for unsupported request")
	}
}

func TestStorageReadAvroAcceptsPresentDefaultArrowOptions(t *testing.T) {
	ctx, cancel := grpcTestContext(t)
	defer cancel()
	materializer := newWireMaterializer(t, domain.FormatAvro, 1)
	client, _ := startReadServer(t, newWireReadService(t, materializer))

	request := wireCreateSessionRequest(storagepb.DataFormat_AVRO)
	request.ReadSession.ReadOptions = &storagepb.ReadSession_TableReadOptions{
		OutputFormatSerializationOptions: &storagepb.ReadSession_TableReadOptions_ArrowSerializationOptions{
			ArrowSerializationOptions: &storagepb.ArrowSerializationOptions{},
		},
	}
	if _, err := client.CreateReadSession(ctx, request); err != nil {
		t.Fatalf("present default Arrow options with AVRO status = %s, want OK: %v", status.Code(err), err)
	}
	if materializer.calls != 1 {
		t.Fatalf("materializer calls = %d, want 1", materializer.calls)
	}
}

func TestStorageReadAvroRejectsNonDefaultArrowOptions(t *testing.T) {
	ctx, cancel := grpcTestContext(t)
	defer cancel()
	materializer := newWireMaterializer(t, domain.FormatAvro, 1)
	client, _ := startReadServer(t, newWireReadService(t, materializer))

	request := wireCreateSessionRequest(storagepb.DataFormat_AVRO)
	request.ReadSession.ReadOptions = &storagepb.ReadSession_TableReadOptions{
		OutputFormatSerializationOptions: &storagepb.ReadSession_TableReadOptions_ArrowSerializationOptions{
			ArrowSerializationOptions: &storagepb.ArrowSerializationOptions{
				BufferCompression: storagepb.ArrowSerializationOptions_LZ4_FRAME,
			},
		},
	}
	_, err := client.CreateReadSession(ctx, request)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("non-default Arrow options with AVRO status = %s, want INVALID_ARGUMENT: %v", status.Code(err), err)
	}
	if materializer.calls != 0 {
		t.Fatalf("materializer called for incompatible request")
	}
}

func TestStorageReadGeneratedClientReceivesClassifiedMaterializerStatus(t *testing.T) {
	for _, testCase := range []struct {
		name string
		err  error
		want codes.Code
	}{
		{
			name: "invalid argument",
			err:  domain.NewError(domain.ErrorInvalidArgument, "snapshot.materialize", errors.New("private selected field")),
			want: codes.InvalidArgument,
		},
		{
			name: "not found",
			err:  domain.NewError(domain.ErrorNotFound, "snapshot.materialize", errors.New("private catalog key")),
			want: codes.NotFound,
		},
		{
			name: "unimplemented",
			err:  domain.NewError(domain.ErrorUnimplemented, "snapshot.materialize", errors.New("private snapshot option")),
			want: codes.Unimplemented,
		},
		{
			name: "resource exhausted",
			err:  domain.NewError(domain.ErrorResourceExhausted, "snapshot.materialize", errors.New("private spill limit")),
			want: codes.ResourceExhausted,
		},
		{
			name: "snapshot unavailable after restart",
			err:  domain.NewError(domain.ErrorUnavailable, "storage_read.read_rows", errors.New("private lifecycle state")),
			want: codes.Unavailable,
		},
		{
			name: "backend internal",
			err:  errors.New("private DuckDB query"),
			want: codes.Internal,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := grpcTestContext(t)
			defer cancel()
			materializer := &wireMaterializer{err: testCase.err}
			client, _ := startReadServer(t, newWireReadService(t, materializer))
			_, err := client.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
				Parent: "projects/reader-project",
				ReadSession: &storagepb.ReadSession{
					Table:      "projects/data-project/datasets/analytics/tables/events",
					DataFormat: storagepb.DataFormat_ARROW,
				},
			})
			if status.Code(err) != testCase.want {
				t.Fatalf("generated client status = %s, want %s: %v", status.Code(err), testCase.want, err)
			}
			if !strings.Contains(err.Error(), "private") {
				t.Fatalf("generated client error omitted materializer cause: %v", err)
			}
		})
	}
}

func TestStorageReadGeneratedClientReceivesAdmissionResourceExhausted(t *testing.T) {
	for _, testCase := range []struct {
		name             string
		maxSessions      int
		maxSnapshotBytes int64
		maxTotalBytes    int64
	}{
		{name: "session slots", maxSessions: 1, maxSnapshotBytes: 64, maxTotalBytes: 128},
		{name: "global snapshot bytes", maxSessions: 2, maxSnapshotBytes: 64, maxTotalBytes: 64},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := grpcTestContext(t)
			defer cancel()
			materializer := newWireMaterializer(t, domain.FormatArrow, 1)
			client, _ := startReadServer(t, newWireReadServiceWithLimits(
				t, materializer, testCase.maxSessions, testCase.maxSnapshotBytes, testCase.maxTotalBytes,
			))
			request := &storagepb.CreateReadSessionRequest{
				Parent: "projects/reader-project",
				ReadSession: &storagepb.ReadSession{
					Table:      "projects/data-project/datasets/analytics/tables/events",
					DataFormat: storagepb.DataFormat_ARROW,
				},
			}
			if _, err := client.CreateReadSession(ctx, request); err != nil {
				t.Fatal(err)
			}
			if _, err := client.CreateReadSession(ctx, request); status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("second generated-client create status = %s, want RESOURCE_EXHAUSTED: %v", status.Code(err), err)
			}
			if materializer.calls != 1 {
				t.Fatalf("materializer calls = %d, want rejected request stopped before outbound port", materializer.calls)
			}
		})
	}
}

func TestStorageReadGeneratedClientCallerCancellationReleasesAdmission(t *testing.T) {
	ctx, cancel := grpcTestContext(t)
	defer cancel()
	base := newWireMaterializer(t, domain.FormatArrow, 1)
	materializer := &wireCallerCancelMaterializer{snapshot: base.snapshot, started: make(chan struct{})}
	var logs synchronizedLogBuffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	service := newWireReadServiceWithLimitsAndLogger(t, materializer, 1, 64, 64, logger)
	client, _ := startReadServer(t, service)
	request := wireCreateSessionRequest(storagepb.DataFormat_ARROW)
	requestContext, requestCancel := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() {
		_, err := client.CreateReadSession(requestContext, request)
		result <- err
	}()
	select {
	case <-materializer.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	requestCancel()
	select {
	case err := <-result:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("caller cancellation status = %s, want CANCELED: %v", status.Code(err), err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitForReadLog(t, ctx, &logs, `"msg":"read session admission released"`)
	if !strings.Contains(logs.String(), "private caller cancellation") {
		t.Fatalf("server diagnostics omitted caller cancellation cause: %s", logs.String())
	}
	if _, err := client.CreateReadSession(ctx, request); err != nil {
		t.Fatalf("create after canceled reservation: %v", err)
	}
	if materializer.callCount() != 2 {
		t.Fatalf("materializer calls = %d, want canceled call plus admitted retry", materializer.callCount())
	}
	if got := strings.Count(logs.String(), `"msg":"read session admission released"`); got != 1 {
		t.Fatalf("reservation release log count = %d, want 1: %s", got, logs.String())
	}
}

func TestStorageReadGeneratedClientShutdownIsFailedPrecondition(t *testing.T) {
	for _, stubborn := range []bool{false, true} {
		name := "materializer cancellation"
		if stubborn {
			name = "commit after reservation removal"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := grpcTestContext(t)
			defer cancel()
			base := newWireMaterializer(t, domain.FormatArrow, 1)
			materializer := newWireShutdownMaterializer(base.snapshot, stubborn)
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(materializer.release) })
			var logs synchronizedLogBuffer
			logger := slog.New(slog.NewJSONHandler(&logs, nil))
			service := newWireReadServiceWithLimitsAndLogger(t, materializer, 1, 64, 64, logger)
			client, _ := startReadServer(t, service)
			result := make(chan error, 1)
			go func() {
				_, err := client.CreateReadSession(ctx, wireCreateSessionRequest(storagepb.DataFormat_ARROW))
				result <- err
			}()
			select {
			case <-materializer.started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			closeResult := make(chan error, 1)
			go func() { closeResult <- service.Close(ctx) }()
			select {
			case <-materializer.canceled:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if stubborn {
				releaseOnce.Do(func() { close(materializer.release) })
			}
			select {
			case err := <-closeResult:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case err := <-result:
				if status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("shutdown create status = %s, want FAILED_PRECONDITION: %v", status.Code(err), err)
				}
				expected := []string{"private shutdown cancellation", "context canceled"}
				if stubborn {
					expected = []string{"Storage Read service closed before session commit"}
				}
				for _, private := range expected {
					if !strings.Contains(err.Error(), private) {
						t.Fatalf("generated-client shutdown status omitted %q: %v", private, err)
					}
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if got := strings.Count(logs.String(), `"msg":"read session admission released"`); got != 1 {
				t.Fatalf("reservation release log count = %d, want 1: %s", got, logs.String())
			}
			if stubborn && base.snapshot.closes != 1 {
				t.Fatalf("post-cancel snapshot close calls = %d, want 1", base.snapshot.closes)
			}
		})
	}
}

func assertReferenceSchemaWire(t *testing.T, format domain.Format, session *storagepb.ReadSession) {
	t.Helper()
	switch format {
	case domain.FormatArrow:
		assertSingleArrowMessage(t, session.GetArrowSchema().GetSerializedSchema(), ipc.MessageSchema)
		if session.GetAvroSchema() != nil {
			t.Fatal("ARROW session unexpectedly contains an Avro schema")
		}
	case domain.FormatAvro:
		if !strings.Contains(session.GetAvroSchema().GetSchema(), `"type":"record"`) {
			t.Fatalf("unexpected Avro schema: %q", session.GetAvroSchema().GetSchema())
		}
		if session.GetArrowSchema() != nil {
			t.Fatal("AVRO session unexpectedly contains an Arrow schema")
		}
	}
}

func assertRowsWire(t *testing.T, format domain.Format, response *storagepb.ReadRowsResponse) {
	t.Helper()
	switch format {
	case domain.FormatArrow:
		assertSingleArrowMessage(t, response.GetArrowRecordBatch().GetSerializedRecordBatch(), ipc.MessageRecordBatch)
		assertSingleArrowMessage(t, response.GetArrowSchema().GetSerializedSchema(), ipc.MessageSchema)
		if response.GetAvroRows() != nil {
			t.Fatal("ARROW response unexpectedly contains Avro rows")
		}
	case domain.FormatAvro:
		payload := response.GetAvroRows().GetSerializedBinaryRows()
		if bytes.HasPrefix(payload, []byte{'O', 'b', 'j', 1}) {
			t.Fatalf("Avro response is an object-container file: %x", payload)
		}
		value, read := binary.Uvarint(payload)
		if read <= 0 || value != 6 { // Avro zig-zag encoding of row id 3.
			t.Fatalf("raw Avro datum = %x, want encoded id 3", payload)
		}
		if response.GetAvroSchema() == nil || response.GetArrowRecordBatch() != nil {
			t.Fatalf("unexpected Avro response shape: %+v", response)
		}
	}
}

func assertSingleArrowMessage(t *testing.T, payload []byte, expected ipc.MessageType) {
	t.Helper()
	reader := ipc.NewMessageReader(bytes.NewReader(payload))
	defer reader.Release()
	message, err := reader.Message()
	if err != nil {
		t.Fatalf("decode Arrow IPC message: %v", err)
	}
	defer message.Release()
	if message.Type() != expected {
		t.Fatalf("Arrow IPC type = %s, want %s", message.Type(), expected)
	}
	if _, err := reader.Message(); !errors.Is(err, io.EOF) {
		t.Fatalf("payload contains more than one Arrow IPC message: %v", err)
	}
}

func startReadServer(t *testing.T, service *readapp.Service) (storagepb.BigQueryReadClient, grpc_health_v1.HealthClient) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := NewWithServices(Services{Read: service})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return storagepb.NewBigQueryReadClient(connection), grpc_health_v1.NewHealthClient(connection)
}

func newWireReadService(t *testing.T, materializer ports.SnapshotMaterializer) *readapp.Service {
	t.Helper()
	return newWireReadServiceWithLimits(t, materializer, 16, 1<<20, 16<<20)
}

func newWireReadServiceWithLimits(t *testing.T, materializer ports.SnapshotMaterializer, maxSessions int, maxSnapshotBytes, maxTotalSnapshotBytes int64) *readapp.Service {
	t.Helper()
	return newWireReadServiceWithLimitsAndLogger(t, materializer, maxSessions, maxSnapshotBytes, maxTotalSnapshotBytes, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func newWireReadServiceWithLimitsAndLogger(t *testing.T, materializer ports.SnapshotMaterializer, maxSessions int, maxSnapshotBytes, maxTotalSnapshotBytes int64, logger *slog.Logger) *readapp.Service {
	t.Helper()
	service, err := readapp.New(readapp.Config{
		Location:              "test-location",
		ProtocolModelVersion:  "google.cloud.bigquery.storage.v1@cloud-bigquery-go-v1.79.0",
		MaxStreams:            16,
		DefaultStreamCount:    4,
		SessionTTL:            30 * time.Minute,
		CleanupInterval:       time.Minute,
		MaxRowsPerResponse:    2,
		MaxSessions:           maxSessions,
		MaxSnapshotBytes:      maxSnapshotBytes,
		MaxTotalSnapshotBytes: maxTotalSnapshotBytes,
	}, materializer, wireRowRestrictionParser{}, wireClock{}, &wireIDs{}, logger)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func grpcTestContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	timeout := 5 * time.Second
	if configured := os.Getenv("BQEMU_STORAGE_READ_TEST_TIMEOUT"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		if err != nil {
			t.Fatalf("BQEMU_STORAGE_READ_TEST_TIMEOUT: %v", err)
		}
		timeout = parsed
	}
	return context.WithTimeout(context.Background(), timeout)
}

type wireClock struct{}

func (wireClock) Now() time.Time { return time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC) }

type wireRowRestrictionParser struct{}

func (wireRowRestrictionParser) ParseExpression(context.Context, string) (queryast.Expression, error) {
	span, _ := queryast.NewSpan(0, 1)
	key, _ := queryast.NewNodeKey(strings.Repeat("0", 64), span, "wire-row-restriction", 0)
	return queryast.NewBooleanLiteral(key, true)
}

type wireIDs struct {
	mu   sync.Mutex
	next int
}

func (g *wireIDs) NewID() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.next++
	return fmt.Sprintf("wire-session-%d", g.next)
}

type wireMaterializer struct {
	calls    int
	snapshot *wireSnapshot
	err      error
}

func newWireMaterializer(t *testing.T, format domain.Format, rows int64) *wireMaterializer {
	t.Helper()
	return &wireMaterializer{snapshot: &wireSnapshot{
		format: format,
		metadata: domain.SnapshotMetadata{
			Schema:         domain.ReferenceSchema{Format: format, Serialized: wireReferenceSchema(t, format)},
			RowCount:       rows,
			EstimatedBytes: rows * 8,
			RetainedBytes:  rows * 8,
		},
	}}
}

func wireCreateSessionRequest(format storagepb.DataFormat) *storagepb.CreateReadSessionRequest {
	return &storagepb.CreateReadSessionRequest{
		Parent: "projects/reader-project",
		ReadSession: &storagepb.ReadSession{
			Table:      "projects/data-project/datasets/analytics/tables/events",
			DataFormat: format,
		},
	}
}

func (m *wireMaterializer) Materialize(context.Context, ports.MaterializeRequest) (ports.ReadSnapshot, error) {
	m.calls++
	return m.snapshot, m.err
}

type wireRange struct {
	start   int64
	end     int64
	maxRows int64
}

type wireSnapshot struct {
	format    domain.Format
	metadata  domain.SnapshotMetadata
	lastRange wireRange
	closes    int
}

func (s *wireSnapshot) Metadata() domain.SnapshotMetadata { return s.metadata }

func (s *wireSnapshot) OpenRange(_ context.Context, start, end, maxRows int64) (ports.BatchIterator, error) {
	s.lastRange = wireRange{start: start, end: end, maxRows: maxRows}
	return &wireIterator{format: s.format, next: start, end: end, maxRows: maxRows}, nil
}

func (s *wireSnapshot) Close(context.Context) error {
	s.closes++
	return nil
}

type wireCallerCancelMaterializer struct {
	mu       sync.Mutex
	calls    int
	snapshot *wireSnapshot
	started  chan struct{}
}

func (m *wireCallerCancelMaterializer) Materialize(ctx context.Context, _ ports.MaterializeRequest) (ports.ReadSnapshot, error) {
	m.mu.Lock()
	m.calls++
	call := m.calls
	m.mu.Unlock()
	if call == 1 {
		close(m.started)
		<-ctx.Done()
		return nil, fmt.Errorf("private caller cancellation: %w", ctx.Err())
	}
	return m.snapshot, nil
}

func (m *wireCallerCancelMaterializer) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

type wireShutdownMaterializer struct {
	snapshot *wireSnapshot
	stubborn bool
	started  chan struct{}
	canceled chan struct{}
	release  chan struct{}
}

func newWireShutdownMaterializer(snapshot *wireSnapshot, stubborn bool) *wireShutdownMaterializer {
	return &wireShutdownMaterializer{
		snapshot: snapshot,
		stubborn: stubborn,
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
		release:  make(chan struct{}),
	}
}

func (m *wireShutdownMaterializer) Materialize(ctx context.Context, _ ports.MaterializeRequest) (ports.ReadSnapshot, error) {
	close(m.started)
	<-ctx.Done()
	close(m.canceled)
	if m.stubborn {
		<-m.release
		return m.snapshot, nil
	}
	return nil, fmt.Errorf("private shutdown cancellation: %w", ctx.Err())
}

type synchronizedLogBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *synchronizedLogBuffer) Write(payload []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(payload)
}

func (b *synchronizedLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func waitForReadLog(t *testing.T, ctx context.Context, logs *synchronizedLogBuffer, marker string) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if strings.Contains(logs.String(), marker) {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("log marker %q not observed: %s", marker, logs.String())
		}
	}
}

type wireIterator struct {
	format  domain.Format
	next    int64
	end     int64
	maxRows int64
}

func (i *wireIterator) Next(context.Context) (domain.EncodedBatch, error) {
	if i.next >= i.end {
		return domain.EncodedBatch{}, io.EOF
	}
	end := min(i.end, i.next+i.maxRows)
	payload, err := wireRows(i.format, i.next, end)
	if err != nil {
		return domain.EncodedBatch{}, err
	}
	batch := domain.EncodedBatch{Offset: i.next, RowCount: end - i.next, SerializedRows: payload}
	i.next = end
	return batch, nil
}

func (*wireIterator) Close() error { return nil }

func wireReferenceSchema(t *testing.T, format domain.Format) []byte {
	t.Helper()
	if format == domain.FormatAvro {
		return []byte(`{"type":"record","name":"row","fields":[{"name":"id","type":"long"}]}`)
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	payload := ipc.GetSchemaPayload(schema, memory.DefaultAllocator)
	defer payload.Release()
	var output bytes.Buffer
	if _, err := payload.WritePayload(&output); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func wireRows(format domain.Format, start, end int64) ([]byte, error) {
	if format == domain.FormatAvro {
		var output []byte
		var buffer [binary.MaxVarintLen64]byte
		for value := start; value < end; value++ {
			encoded := uint64(value << 1)
			written := binary.PutUvarint(buffer[:], encoded)
			output = append(output, buffer[:written]...)
		}
		return output, nil
	}
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	for value := start; value < end; value++ {
		builder.Append(value)
	}
	values := builder.NewArray()
	defer values.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{values}, end-start)
	defer record.Release()
	payload, err := ipc.GetRecordBatchPayload(record, ipc.WithAllocator(memory.DefaultAllocator))
	if err != nil {
		return nil, err
	}
	defer payload.Release()
	var output bytes.Buffer
	_, err = payload.WritePayload(&output)
	return output.Bytes(), err
}

var _ ports.SnapshotMaterializer = (*wireMaterializer)(nil)
var _ ports.ReadSnapshot = (*wireSnapshot)(nil)
var _ ports.BatchIterator = (*wireIterator)(nil)
