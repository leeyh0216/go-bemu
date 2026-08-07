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

func TestPayloadAndErrorSummariesNeverIncludeRawText(t *testing.T) {
	const payload = `SELECT 'sql-secret' AS payload /* Bearer token-secret */`
	const errorMessage = `request failed: {"rows":["row-secret"],"password":"credential-secret"}`
	for _, legacyUnsafeSetting := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy-unsafe-%t", legacyUnsafeSetting), func(t *testing.T) {
			Configure(legacyUnsafeSetting)
			attrs := fmt.Sprint(
				PayloadAttrs("query", []byte(payload)),
				ErrorAttrs(fmt.Errorf("%s", errorMessage)),
				RedactText(payload),
			)
			for _, raw := range []string{"sql-secret", "token-secret", "row-secret", "credential-secret", "SELECT"} {
				if strings.Contains(attrs, raw) {
					t.Fatalf("legacy unsafe setting %t exposed %q: %s", legacyUnsafeSetting, raw, attrs)
				}
			}
			for _, expected := range []string{
				"query_shape opaque_bytes", "query_bytes", "query_digest sha256:",
				"error_type", "error_bytes", "error_digest sha256:", "OMITTED bytes=",
			} {
				if !strings.Contains(attrs, expected) {
					t.Fatalf("legacy unsafe setting %t missing %q: %s", legacyUnsafeSetting, expected, attrs)
				}
			}
		})
	}
}

func TestDefaultPayloadLoggingUsesDigestOnly(t *testing.T) {
	Configure(false)
	attrs := fmt.Sprint(PayloadAttrs("rows", []byte("private-row")))
	if strings.Contains(attrs, "private-row") || !strings.Contains(attrs, "sha256:") {
		t.Fatalf("unexpected safe payload attrs: %s", attrs)
	}
}

func TestProtoSummaryNeverIncludesRawFieldsInAnyConfiguration(t *testing.T) {
	request := &storagepb.CreateReadSessionRequest{
		Parent: "projects/reader-project",
		ReadSession: &storagepb.ReadSession{
			Table: "projects/data-project/datasets/analytics/tables/events",
			ReadOptions: &storagepb.ReadSession_TableReadOptions{
				SelectedFields: []string{"selected-field-secret"},
				RowRestriction: "payload = 'restriction-secret'",
			},
		},
	}
	for _, legacyUnsafeSetting := range []bool{false, true} {
		Configure(legacyUnsafeSetting)
		attrs := fmt.Sprint(ProtoAttrs(request))
		for _, raw := range []string{"selected-field-secret", "restriction-secret", "payload =", "data-project"} {
			if strings.Contains(attrs, raw) {
				t.Fatalf("legacy unsafe setting %t exposed protobuf field %q: %s", legacyUnsafeSetting, raw, attrs)
			}
		}
		for _, expected := range []string{
			"CreateReadSessionRequest", "wire_bytes", "payload_digest",
			"selected_fields_count 1", "row_restriction_bytes", "row_restriction_digest",
		} {
			if !strings.Contains(attrs, expected) {
				t.Fatalf("legacy unsafe setting %t missing %q: %s", legacyUnsafeSetting, expected, attrs)
			}
		}
	}
}

func TestMetadataNeverContainsAuthorizationValue(t *testing.T) {
	keys := fmt.Sprint(MetadataKeys(map[string][]string{
		"authorization": {"Bearer secret"},
		"content-type":  {"application/grpc"},
	}))
	if strings.Contains(keys, "secret") || !strings.Contains(keys, "authorization=[REDACTED]") {
		t.Fatalf("unexpected metadata summary: %s", keys)
	}
}

func TestProtoMetricsIncludeOffsetStreamBytesAndDigest(t *testing.T) {
	attrs := fmt.Sprint(ProtoAttrs(&storagepb.ReadRowsRequest{ReadStream: "streams/one", Offset: 42}))
	for _, expected := range []string{"read_stream", "streams/one", "offset", "42", "wire_bytes", "payload_digest"} {
		if !strings.Contains(attrs, expected) {
			t.Fatalf("missing %q in %s", expected, attrs)
		}
	}
}

func TestHTTPBoundaryLogNeverIncludesHeaderQueryOrBodyValues(t *testing.T) {
	logs, restore := captureLogs(t)
	defer restore()
	Configure(true)
	handler := HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("raw-response-secret"))
	}))
	request := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/p?access_token=query-secret&maxResults=10", strings.NewReader("raw-request-secret"))
	request.Header.Set("Authorization", "Bearer header-secret")
	request.Header.Set("X-Request-Id", "request-one")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	output := logs.String()
	for _, secret := range []string{"query-secret", "header-secret", "raw-request-secret", "raw-response-secret"} {
		if strings.Contains(output, secret) {
			t.Fatalf("secret %q leaked in HTTP log: %s", secret, output)
		}
	}
	for _, expected := range []string{
		"boundary.enter", "boundary.exit", "request-one", "authorization=[REDACTED]", "access_token=[REDACTED]",
		`"response_bytes":19`, `"response_digest":"sha256:3d9fd8a2374dc5bb0d47be58937fe93c743ee67c756fdf590ad21f6f6856ef10"`,
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("missing %q in HTTP log: %s", expected, output)
		}
	}
}

func TestGRPCBoundaryNeverLogsRawProtoOrErrorWithDeprecatedFlag(t *testing.T) {
	logs, restore := captureLogs(t)
	defer restore()
	Configure(true)
	request := &storagepb.CreateReadSessionRequest{
		Parent: "projects/reader-project",
		ReadSession: &storagepb.ReadSession{ReadOptions: &storagepb.ReadSession_TableReadOptions{
			SelectedFields: []string{"grpc-field-secret"},
			RowRestriction: "payload = 'grpc-row-secret'",
		}},
	}
	_, err := UnaryServerInterceptor(context.Background(), request, &grpc.UnaryServerInfo{
		FullMethod: "/google.cloud.bigquery.storage.v1.BigQueryRead/CreateReadSession",
	}, func(context.Context, any) (any, error) {
		return nil, fmt.Errorf("backend error contains grpc-error-secret")
	})
	if err == nil {
		t.Fatal("expected handler error")
	}
	output := logs.String()
	for _, secret := range []string{"grpc-field-secret", "grpc-row-secret", "grpc-error-secret", "payload ="} {
		if strings.Contains(output, secret) {
			t.Fatalf("raw value %q leaked in gRPC log: %s", secret, output)
		}
	}
	for _, expected := range []string{"selected_fields_count", "row_restriction_digest", "error_bytes", "error_digest"} {
		if !strings.Contains(output, expected) {
			t.Fatalf("missing %q in gRPC log: %s", expected, output)
		}
	}
}

func TestGRPCBoundaryLogNeverIncludesAuthorizationMetadata(t *testing.T) {
	logs, restore := captureLogs(t)
	defer restore()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer grpc-secret",
		"x-request-id", "grpc-request",
	))
	request := &storagepb.ReadRowsRequest{ReadStream: "streams/one", Offset: 7}
	_, err := UnaryServerInterceptor(ctx, request, &grpc.UnaryServerInfo{FullMethod: "/google.cloud.bigquery.storage.v1.BigQueryRead/ReadRows"}, func(ctx context.Context, request any) (any, error) {
		return &storagepb.ReadRowsResponse{RowCount: 2}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	output := logs.String()
	if strings.Contains(output, "grpc-secret") {
		t.Fatalf("gRPC credential leaked: %s", output)
	}
	for _, expected := range []string{"boundary.enter", "boundary.exit", "grpc-request", "authorization=[REDACTED]", "read_stream", "streams/one", "offset"} {
		if !strings.Contains(output, expected) {
			t.Fatalf("missing %q in gRPC log: %s", expected, output)
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
