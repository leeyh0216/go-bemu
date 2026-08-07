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

func TestCredentialsAreRedactedEvenWithUnsafePayloadLogging(t *testing.T) {
	Configure(true)
	t.Cleanup(func() { Configure(false) })
	payload := []byte("row=visible authorization=Bearer secret-token password=hunter2")
	attrs := fmt.Sprint(PayloadAttrs("query", payload))
	if !strings.Contains(attrs, "row=visible") {
		t.Fatalf("unsafe mode should include non-secret payload: %s", attrs)
	}
	for _, secret := range []string{"secret-token", "hunter2"} {
		if strings.Contains(attrs, secret) {
			t.Fatalf("secret %q leaked: %s", secret, attrs)
		}
	}
	if !strings.Contains(attrs, "[REDACTED]") {
		t.Fatalf("expected redaction marker: %s", attrs)
	}
}

func TestStructuredJSONSecretsAndPrivateKeysAreAlwaysRedacted(t *testing.T) {
	Configure(true)
	t.Cleanup(func() { Configure(false) })
	payload := []byte(`{"rows":[{"value":"visible"}],"credentials":{"access_token":"token-value","private_key":"-----BEGIN PRIVATE KEY-----\nkey-material\n-----END PRIVATE KEY-----"},"client_secret":"client-value"}`)
	attrs := fmt.Sprint(PayloadAttrs("request", payload))
	for _, secret := range []string{"token-value", "key-material", "client-value"} {
		if strings.Contains(attrs, secret) {
			t.Fatalf("secret %q leaked: %s", secret, attrs)
		}
	}
	if !strings.Contains(attrs, "visible") || !strings.Contains(attrs, "[REDACTED]") {
		t.Fatalf("unexpected unsafe payload output: %s", attrs)
	}
}

func TestDefaultPayloadLoggingUsesDigestOnly(t *testing.T) {
	Configure(false)
	attrs := fmt.Sprint(PayloadAttrs("rows", []byte("private-row")))
	if strings.Contains(attrs, "private-row") || !strings.Contains(attrs, "sha256:") {
		t.Fatalf("unexpected safe payload attrs: %s", attrs)
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

func TestHTTPBoundaryLogNeverIncludesHeaderOrQueryValues(t *testing.T) {
	logs, restore := captureLogs(t)
	defer restore()
	handler := HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/p?access_token=query-secret&maxResults=10", nil)
	request.Header.Set("Authorization", "Bearer header-secret")
	request.Header.Set("X-Request-Id", "request-one")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	output := logs.String()
	for _, secret := range []string{"query-secret", "header-secret"} {
		if strings.Contains(output, secret) {
			t.Fatalf("secret %q leaked in HTTP log: %s", secret, output)
		}
	}
	for _, expected := range []string{"boundary.enter", "boundary.exit", "request-one", "authorization=[REDACTED]", "access_token=[REDACTED]"} {
		if !strings.Contains(output, expected) {
			t.Fatalf("missing %q in HTTP log: %s", expected, output)
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
