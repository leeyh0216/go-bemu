package rest

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
)

func TestRESTGzipChunkedTablesInsertUsesDecodedJSON(t *testing.T) {
	ctx, cancel := requestBodyTestContext(t)
	defer cancel()
	warehouse := &catalogTestWarehouse{}
	catalog := application.NewCatalogService(
		memory.NewCatalogRepository(), warehouse,
		catalogTestClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)},
	)
	if _, err := catalog.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "temporary", Location: "US"}); err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(NewCatalogServer(
		catalog, warehouse, "", WithRequestBodyLimits(1<<20, 1<<20),
	).Handler())
	t.Cleanup(server.Close)

	logs, restoreLogs := captureRequestBodyLogs()
	defer restoreLogs()
	body := []byte(`{"tableReference":{"tableId":"connector_temporary"},"description":"private-body-value","schema":{"fields":[{"name":"id","type":"INT64"}]}}`)
	response := doEncodedRequest(t, ctx, server.URL+"/bigquery/v2/projects/test-project/datasets/temporary/tables", body, "gzip", true)
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("gzip chunked tables.insert status = %d, want 200: %s", response.StatusCode, payload)
	}
	if len(warehouse.tables) != 1 || warehouse.tables[0] != "test-project/temporary/connector_temporary" {
		t.Fatalf("created physical tables = %#v", warehouse.tables)
	}

	output := logs.String()
	for _, expected := range []string{`"encoding":"gzip"`, `"outcome":"accepted"`, "private-body-value", "compressed_digest", "uncompressed_digest"} {
		if !strings.Contains(output, expected) {
			t.Fatalf("request body log lacks %q: %s", expected, output)
		}
	}
}

func TestRESTGzipMethodOverrideReachesTablesPatch(t *testing.T) {
	ctx, cancel := requestBodyTestContext(t)
	defer cancel()
	warehouse := &catalogTestWarehouse{}
	clock := catalogTestClock{now: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)}
	catalog := application.NewCatalogService(memory.NewCatalogRepository(), warehouse, clock)
	if _, err := catalog.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "temporary", Location: "US"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "temporary", ID: "connector_temporary",
		Schema: []domain.Field{{Name: "id", Type: "INT64"}},
	}); err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(NewCatalogServer(
		catalog, warehouse, "", WithRequestBodyLimits(1<<20, 1<<20),
	).Handler())
	t.Cleanup(server.Close)

	body := gzipPayload(t, []byte(`{"expirationTime":"1800000000000"}`))
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		server.URL+"/bigquery/v2/projects/test-project/datasets/temporary/tables/connector_temporary",
		io.NopCloser(bytes.NewReader(body)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Content-Encoding", "gzip")
	request.Header.Set("X-HTTP-Method-Override", http.MethodPatch)
	request.ContentLength = -1
	request.TransferEncoding = []string{"chunked"}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("method-override tables.patch status = %d, want 200: %s", response.StatusCode, payload)
	}
	var patched tableResource
	if err := json.NewDecoder(response.Body).Decode(&patched); err != nil {
		t.Fatal(err)
	}
	if patched.ExpirationTime != "1800000000000" {
		t.Fatalf("patched expirationTime = %q, want 1800000000000", patched.ExpirationTime)
	}
}

func TestRESTRequestBodyLimitsRejectWireAndDecodedOverflow(t *testing.T) {
	for _, testCase := range []struct {
		name            string
		compressedLimit int64
		decodedLimit    int64
		body            []byte
	}{
		{
			name: "decoded gzip amplification", compressedLimit: 1 << 10, decodedLimit: 128,
			body: []byte(`{"value":"` + strings.Repeat("a", 2<<10) + `"}`),
		},
		{
			name: "compressed wire overflow", compressedLimit: 24, decodedLimit: 8 << 10,
			body: []byte(`{"value":"` + strings.Repeat("0123456789", 200) + `"}`),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := requestBodyTestContext(t)
			defer cancel()
			server := newJSONEchoServer(t, testCase.compressedLimit, testCase.decodedLimit, nil)
			response := doEncodedRequest(t, ctx, server.URL+"/echo", testCase.body, "gzip", true)
			defer response.Body.Close()
			if response.StatusCode != http.StatusRequestEntityTooLarge {
				payload, _ := io.ReadAll(response.Body)
				t.Fatalf("overflow status = %d, want 413: %s", response.StatusCode, payload)
			}
			assertErrorReason(t, response.Body, "requestTooLarge")
		})
	}
}

func TestRESTRequestContentEncodingFailuresStopBeforeHandler(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		encoding   string
		body       []byte
		wantStatus int
	}{
		{name: "unsupported", encoding: "br", body: []byte(`{"value":1}`), wantStatus: http.StatusUnsupportedMediaType},
		{name: "multiple", encoding: "gzip, identity", body: gzipPayload(t, []byte(`{"value":1}`)), wantStatus: http.StatusBadRequest},
		{name: "invalid gzip", encoding: "gzip", body: []byte("not-a-gzip-stream"), wantStatus: http.StatusBadRequest},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := requestBodyTestContext(t)
			defer cancel()
			var calls atomic.Int64
			server := newJSONEchoServer(t, 1<<20, 1<<20, &calls)
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/echo", bytes.NewReader(testCase.body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Content-Encoding", testCase.encoding)
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != testCase.wantStatus {
				payload, _ := io.ReadAll(response.Body)
				t.Fatalf("status = %d, want %d: %s", response.StatusCode, testCase.wantStatus, payload)
			}
			if calls.Load() != 0 {
				t.Fatalf("handler calls = %d, want 0", calls.Load())
			}
		})
	}
}

func TestRESTCorruptGzipStreamIsRejectedAfterHandlerRead(t *testing.T) {
	ctx, cancel := requestBodyTestContext(t)
	defer cancel()
	logs, restoreLogs := captureRequestBodyLogs()
	defer restoreLogs()
	var calls atomic.Int64
	server := newJSONEchoServer(t, 1<<20, 1<<20, &calls)
	compressed := gzipPayload(t, []byte(`{"value":"private-corrupt-body"}`))
	compressed = compressed[:len(compressed)-4]
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/echo", bytes.NewReader(compressed))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Content-Encoding", "gzip")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("corrupt gzip status = %d, want 400: %s", response.StatusCode, payload)
	}
	if calls.Load() != 1 {
		t.Fatalf("handler calls = %d, want one streaming decode attempt", calls.Load())
	}
	output := logs.String()
	for _, expected := range []string{`"encoding":"gzip"`, `"outcome":"rejected"`, `"http_status":400`} {
		if !strings.Contains(output, expected) {
			t.Fatalf("corrupt gzip log lacks %q: %s", expected, output)
		}
	}
	if !strings.Contains(output, "private-corrupt-body") {
		t.Fatalf("corrupt gzip body omitted from logs: %s", output)
	}
}

func TestParseRequestContentEncodingRejectsMultipleHeaderLines(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		values     []string
		want       string
		wantStatus int
	}{
		{name: "absent", values: nil, want: "identity"},
		{name: "case insensitive", values: []string{"GZip"}, want: "gzip"},
		{name: "two lines", values: []string{"gzip", "identity"}, want: "multiple", wantStatus: http.StatusBadRequest},
		{name: "empty", values: []string{""}, want: "malformed", wantStatus: http.StatusBadRequest},
		{name: "unsupported", values: []string{"br"}, want: "unsupported", wantStatus: http.StatusUnsupportedMediaType},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := parseRequestContentEncoding(testCase.values)
			if got != testCase.want {
				t.Fatalf("encoding = %q, want %q", got, testCase.want)
			}
			if testCase.wantStatus == 0 {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var protocolError *httpProtocolError
			if !errors.As(err, &protocolError) || protocolError.status != testCase.wantStatus {
				t.Fatalf("error = %#v, want HTTP %d", err, testCase.wantStatus)
			}
		})
	}
}

func TestRESTRejectedBodyLogsMatchHTTPStatuses(t *testing.T) {
	ctx, cancel := requestBodyTestContext(t)
	defer cancel()
	logs, restoreLogs := captureRequestBodyLogs()
	defer restoreLogs()
	server := newJSONEchoServer(t, 1<<10, 64, nil)
	response := doEncodedRequest(t, ctx, server.URL+"/echo", []byte(`{"value":"`+strings.Repeat("sensitive", 100)+`"}`), "gzip", true)
	defer response.Body.Close()
	if response.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", response.StatusCode)
	}
	invalidResponse := doEncodedRequest(t, ctx, server.URL+"/echo", []byte(`{"value":"private-invalid"`), "identity", true)
	defer invalidResponse.Body.Close()
	if invalidResponse.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid JSON status = %d, want 400", invalidResponse.StatusCode)
	}
	output := logs.String()
	for _, expected := range []string{
		`"outcome":"rejected"`, `"http_status":413`, `"reason":"requestTooLarge"`,
		`"http_status":400`, `"reason":"invalid"`,
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("rejection log lacks %q: %s", expected, output)
		}
	}
	if !strings.Contains(output, "sensitivesensitive") || !strings.Contains(output, "private-invalid") {
		t.Fatalf("rejected request body omitted from logs: %s", output)
	}
}

func newJSONEchoServer(t *testing.T, compressedLimit, decodedLimit int64, calls *atomic.Int64) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /echo", func(w http.ResponseWriter, r *http.Request) {
		if calls != nil {
			calls.Add(1)
		}
		var value map[string]any
		if err := decodeJSON(r, &value); err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, value)
	})
	handler := requestBodyMiddleware(normalizedRequestBodyLimits(compressedLimit, decodedLimit), mux)
	handler = methodOverrideMiddleware(handler)
	handler = recoverMiddleware(handler)
	server := httptest.NewServer(observability.HTTPMiddleware(handler))
	t.Cleanup(server.Close)
	return server
}

func doEncodedRequest(t *testing.T, ctx context.Context, endpoint string, payload []byte, encoding string, chunked bool) *http.Response {
	t.Helper()
	body := payload
	if encoding == "gzip" {
		body = gzipPayload(t, payload)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, io.NopCloser(bytes.NewReader(body)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Content-Encoding", encoding)
	if chunked {
		request.ContentLength = -1
		request.TransferEncoding = []string{"chunked"}
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func gzipPayload(t *testing.T, payload []byte) []byte {
	t.Helper()
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return compressed.Bytes()
}

func assertErrorReason(t *testing.T, body io.Reader, want string) {
	t.Helper()
	var response struct {
		Error struct {
			Errors []errorProto `json:"errors"`
		} `json:"error"`
	}
	if err := json.NewDecoder(body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Error.Errors) != 1 || response.Error.Errors[0].Reason != want {
		t.Fatalf("error response = %#v, want reason %q", response, want)
	}
}

func captureRequestBodyLogs() (*bytes.Buffer, func()) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	return &output, func() { slog.SetDefault(previous) }
}

func requestBodyTestContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	timeout := 5 * time.Second
	if configured := os.Getenv("BQEMU_REST_TEST_TIMEOUT"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		if err != nil || parsed <= 0 {
			t.Fatalf("BQEMU_REST_TEST_TIMEOUT must be a positive Go duration: %q", configured)
		}
		timeout = parsed
	}
	return context.WithTimeout(context.Background(), timeout)
}
