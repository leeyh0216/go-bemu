package observability

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestTimelineEvictsOldestEventAndCopiesInputs(t *testing.T) {
	timeline := NewTimeline(TimelineConfig{MaxEvents: 2, MaxBytes: 4 << 10, MaxPayloadBytes: 32})
	headers := map[string][]string{"X-Test": {"before"}}
	payload := []byte("first")
	timeline.Record(DiagnosticEvent{Protocol: "http", Headers: headers}, payload)
	headers["X-Test"][0] = "after"
	payload[0] = 'X'
	timeline.Record(DiagnosticEvent{Protocol: "http", OperationID: "second"}, []byte("second"))
	timeline.Record(DiagnosticEvent{Protocol: "grpc", OperationID: "third"}, []byte("third"))

	snapshot := timeline.Snapshot(0, 0)
	if got, want := len(snapshot.Events), 2; got != want {
		t.Fatalf("events = %d, want %d", got, want)
	}
	if got, want := snapshot.Events[0].OperationID, "second"; got != want {
		t.Fatalf("oldest retained operation = %q, want %q", got, want)
	}
	if got, want := snapshot.Stats.DroppedEvents, uint64(1); got != want {
		t.Fatalf("dropped events = %d, want %d", got, want)
	}
	var encodedBytes int64
	for _, event := range snapshot.Events {
		encoded, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		encodedBytes += int64(len(encoded))
	}
	if got, want := snapshot.Stats.Bytes, encodedBytes; got != want {
		t.Fatalf("retained accounting = %d, want %d", got, want)
	}

	timeline = NewTimeline(TimelineConfig{MaxEvents: 2, MaxBytes: 4 << 10, MaxPayloadBytes: 32})
	timeline.Record(DiagnosticEvent{Headers: headers}, []byte("first"))
	snapshot = timeline.Snapshot(0, 0)
	if got, want := snapshot.Events[0].Headers["X-Test"][0], "after"; got != want {
		t.Fatalf("headers = %q, want %q", got, want)
	}
	if got, want := snapshot.Events[0].PayloadBase64, base64.StdEncoding.EncodeToString([]byte("first")); got != want {
		t.Fatalf("payload = %q, want %q", got, want)
	}
}

func TestTimelineUnaryInterceptorCapturesProtobufJSON(t *testing.T) {
	timeline := ConfigureTimeline(TimelineConfig{MaxEvents: 10, MaxBytes: 4 << 10, MaxPayloadBytes: 64})
	_, err := UnaryServerInterceptor(context.Background(), &emptypb.Empty{}, &grpc.UnaryServerInfo{FullMethod: "/example.Service/Call"}, func(context.Context, any) (any, error) { return &emptypb.Empty{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	snapshot := timeline.Snapshot(0, 0)
	if got, want := len(snapshot.Events), 2; got != want {
		t.Fatalf("events = %d, want %d", got, want)
	}
	if got, want := snapshot.Events[0].RPCMethod, "/example.Service/Call"; got != want {
		t.Fatalf("rpc = %q, want %q", got, want)
	}
	if got := snapshot.Events[0].PayloadJSON; got != "{}" {
		t.Fatalf("protobuf JSON = %q", got)
	}
}

func TestTimelineStreamInterceptorMarksClientHalfClose(t *testing.T) {
	timeline := ConfigureTimeline(TimelineConfig{MaxEvents: 10, MaxBytes: 4 << 10, MaxPayloadBytes: 64})
	stream := &loggingServerStream{ServerStream: timelineTestStream{ctx: context.Background(), recvErr: io.EOF}, ctx: WithRequestMetadata(context.Background(), "request", "trace"), rpc: "/example.Service/Stream"}
	if err := stream.RecvMsg(&emptypb.Empty{}); err != io.EOF {
		t.Fatalf("recv error = %v", err)
	}
	snapshot := timeline.Snapshot(0, 0)
	if got, want := snapshot.Events[0].Phase, "half_close"; got != want {
		t.Fatalf("phase = %q, want %q", got, want)
	}
}

type timelineTestStream struct {
	grpc.ServerStream
	ctx     context.Context
	recvErr error
}

func (s timelineTestStream) Context() context.Context { return s.ctx }
func (s timelineTestStream) RecvMsg(any) error        { return s.recvErr }

func TestTimelineHTTPMiddlewareCapturesBoundedRequestAndResponse(t *testing.T) {
	timeline := ConfigureTimeline(TimelineConfig{MaxEvents: 10, MaxBytes: 4 << 10, MaxPayloadBytes: 4})
	handler := HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		SetHTTPOperation(w, "bqemu.test")
		if _, err := io.ReadAll(r.Body); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("X-Response", "yes")
		_, _ = w.Write([]byte("response"))
	}))
	request := httptest.NewRequest(http.MethodPost, "http://example.test/path", strings.NewReader("request"))
	request.Header.Set("X-Test", "yes")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	snapshot := timeline.Snapshot(0, 0)
	if got, want := len(snapshot.Events), 2; got != want {
		t.Fatalf("events = %d, want %d", got, want)
	}
	for index, event := range snapshot.Events {
		if got, want := event.OperationID, "bqemu.test"; got != want {
			t.Fatalf("operation = %q, want %q", got, want)
		}
		wantBytes := int64(7)
		if index == 1 {
			wantBytes = 8
		}
		if !event.Truncated || event.OriginalBytes != wantBytes {
			t.Fatalf("event not bounded: %#v", event)
		}
	}
	if got, want := snapshot.Events[0].PayloadBase64, base64.StdEncoding.EncodeToString([]byte("requ")); got != want {
		t.Fatalf("request payload = %q, want %q", got, want)
	}
	if got, want := snapshot.Events[1].PayloadBase64, base64.StdEncoding.EncodeToString([]byte("resp")); got != want {
		t.Fatalf("response payload = %q, want %q", got, want)
	}
}

func TestTimelineTruncatesAndRemainsRaceSafe(t *testing.T) {
	timeline := NewTimeline(TimelineConfig{MaxEvents: 100, MaxBytes: 16 << 10, MaxPayloadBytes: 3})
	timeline.Record(DiagnosticEvent{Protocol: "http"}, []byte("abcdef"))
	snapshot := timeline.Snapshot(0, 1)
	if !snapshot.Events[0].Truncated || snapshot.Events[0].OriginalBytes != 6 {
		t.Fatalf("truncation = %#v", snapshot.Events[0])
	}
	if got, want := snapshot.Events[0].PayloadBase64, base64.StdEncoding.EncodeToString([]byte("abc")); got != want {
		t.Fatalf("payload = %q, want %q", got, want)
	}

	var group sync.WaitGroup
	for index := 0; index < 16; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for attempt := 0; attempt < 100; attempt++ {
				timeline.Record(DiagnosticEvent{Protocol: "grpc"}, []byte("payload"))
				_ = timeline.Snapshot(0, 10)
			}
		}()
	}
	group.Wait()
	if snapshot := timeline.Snapshot(0, 0); snapshot.Stats.Bytes > 16<<10 {
		t.Fatalf("retained bytes = %d", snapshot.Stats.Bytes)
	}
}
