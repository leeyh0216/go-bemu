package observability

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestPayloadAndErrorAttributesRetainRawDiagnostics(t *testing.T) {
	payload := `SELECT 'diagnostic-value'`
	errorMessage := `request failed: {"row":"invalid-value"}`
	attrs := fmt.Sprint(
		PayloadAttrs("query", []byte(payload)),
		ErrorAttrs(fmt.Errorf("%s", errorMessage)),
		RedactText(payload),
	)
	for _, expected := range []string{payload, errorMessage, "query_bytes", "error_type"} {
		if !strings.Contains(attrs, expected) {
			t.Fatalf("diagnostic attributes omitted %q: %s", expected, attrs)
		}
	}
}

func TestProtoAttributesRetainMessageAndResolvedMetrics(t *testing.T) {
	request := &storagepb.CreateReadSessionRequest{
		Parent: "projects/reader-project",
		ReadSession: &storagepb.ReadSession{
			Table: "projects/data-project/datasets/analytics/tables/events",
			ReadOptions: &storagepb.ReadSession_TableReadOptions{
				SelectedFields: []string{"selected_field"},
				RowRestriction: "payload = 'diagnostic-value'",
			},
		},
	}
	attrs := fmt.Sprint(ProtoAttrs(request))
	for _, expected := range []string{
		"CreateReadSessionRequest", "wire_bytes", "selected_field", "diagnostic-value",
		"selected_fields_count 1", "row_restriction_bytes",
	} {
		if !strings.Contains(attrs, expected) {
			t.Fatalf("protobuf attributes omitted %q: %s", expected, attrs)
		}
	}
}

func TestMetadataEntriesRetainValues(t *testing.T) {
	entries := fmt.Sprint(MetadataEntries(map[string][]string{
		"authorization": {"Bearer diagnostic-token"},
		"content-type":  {"application/grpc"},
	}))
	for _, expected := range []string{"authorization=Bearer diagnostic-token", "content-type=application/grpc"} {
		if !strings.Contains(entries, expected) {
			t.Fatalf("metadata entries omitted %q: %s", expected, entries)
		}
	}
}

func TestHTTPBoundaryLogsRawQueryAndHeaderContext(t *testing.T) {
	logs, restore := captureLogs(t)
	defer restore()
	handler := HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("response-body"))
	}))
	request := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p?access_token=query-value&maxResults=10", nil)
	request.Header.Set("Authorization", "Bearer header-value")
	request.Header.Set("X-Request-Id", "request-one")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	output := logs.String()
	for _, expected := range []string{
		"boundary.enter", "boundary.exit", "request-one",
		"access_token=query-value", "authorization=Bearer header-value", `"response_bytes":13`,
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("HTTP log omitted %q: %s", expected, output)
		}
	}
}

func TestGRPCBoundaryLogsRawProtoErrorAndMetadataContext(t *testing.T) {
	logs, restore := captureLogs(t)
	defer restore()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer grpc-token",
		"x-request-id", "grpc-request",
	))
	request := &storagepb.CreateReadSessionRequest{
		Parent: "projects/reader-project",
		ReadSession: &storagepb.ReadSession{ReadOptions: &storagepb.ReadSession_TableReadOptions{
			SelectedFields: []string{"diagnostic_field"},
			RowRestriction: "payload = 'diagnostic-row'",
		}},
	}
	_, err := UnaryServerInterceptor(ctx, request, &grpc.UnaryServerInfo{
		FullMethod: "/google.cloud.bigquery.storage.v1.BigQueryRead/CreateReadSession",
	}, func(context.Context, any) (any, error) {
		return nil, fmt.Errorf("backend diagnostic error")
	})
	if err == nil {
		t.Fatal("expected handler error")
	}
	output := logs.String()
	for _, expected := range []string{
		"diagnostic_field", "diagnostic-row", "backend diagnostic error",
		"authorization=Bearer grpc-token", "grpc-request",
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("gRPC log omitted %q: %s", expected, output)
		}
	}
}

func captureLogs(t *testing.T) (*bytes.Buffer, func()) {
	t.Helper()
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	return &output, func() { slog.SetDefault(previous) }
}
