package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/adapters/objectstore"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/contracttest"
	"github.com/leeyh0216/go-bemu/internal/domain"
	loadApplication "github.com/leeyh0216/go-bemu/internal/loadjob/application"
	loadDomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
)

func TestMediaUploadMultipartStoresGCSObjectBeforeSubmittingLoad(t *testing.T) {
	contracttest.Operation(t, "bigquery.jobs.insert.upload")
	loads := &mediaUploadLoadJobs{}
	store := &mediaUploadStore{}
	server := newMediaUploadTestServer(t, loads, store)
	payload, contentType := multipartLoadBody(t, `{
  "jobReference":{"location":"US"},
  "configuration":{"load":{
    "sourceFormat":"PARQUET",
    "destinationTable":{"datasetId":"analytics","tableId":"events"},
    "writeDisposition":"WRITE_APPEND"
  }}
}`, []byte("parquet-bytes"))
	request, err := http.NewRequest(http.MethodPost, server.URL+"/upload/bigquery/v2/projects/test-project/jobs?uploadType=multipart", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", contentType)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("multipart status=%d body=%s", response.StatusCode, body)
	}
	if string(store.payload) != "parquet-bytes" || store.bucket != "bqemu-media" || !strings.HasPrefix(store.name, ".bqemu-media/test-project/") {
		t.Fatalf("stored media = bucket=%q name=%q payload=%q", store.bucket, store.name, store.payload)
	}
	if len(loads.submissions) != 1 {
		t.Fatalf("load submissions = %d", len(loads.submissions))
	}
	configuration := loads.submissions[0].configuration
	if len(configuration.SourceURIs) != 1 || configuration.SourceURIs[0] != "gs://bqemu-media/"+store.name {
		t.Fatalf("load source URIs = %#v", configuration.SourceURIs)
	}
	if configuration.SourceFormat != loadDomain.FormatParquet {
		t.Fatalf("source format = %q", configuration.SourceFormat)
	}
}

func TestMediaUploadResumableTracksRangesAndAliases(t *testing.T) {
	contracttest.Operation(t, "bigquery.jobs.insert.upload")
	contracttest.Operation(t, "bigquery.jobs.insert.upload-resume")
	contracttest.Operation(t, "bigquery.jobs.insert.resumable")
	contracttest.Operation(t, "bigquery.jobs.insert.resumable-resume")
	loads := &mediaUploadLoadJobs{}
	store := &mediaUploadStore{}
	server := newMediaUploadTestServer(t, loads, store)

	location := beginMediaSession(t, server.URL, "/upload/bigquery/v2/projects/test-project/jobs", 6)
	putMediaChunk(t, location, "bytes 0-2/6", "abc", http.StatusPermanentRedirect, "bytes=0-2")
	putMediaChunk(t, location, "bytes */6", "", http.StatusPermanentRedirect, "bytes=0-2")
	putMediaChunk(t, location, "bytes 3-5/6", "def", http.StatusOK, "")
	putMediaChunk(t, location, "bytes 3-5/6", "def", http.StatusOK, "")
	if string(store.payload) != "abcdef" || len(loads.submissions) != 1 {
		t.Fatalf("completed upload payload=%q submissions=%d", store.payload, len(loads.submissions))
	}

	alias := beginMediaSession(t, server.URL, "/resumable/upload/bigquery/v2/projects/test-project/jobs", 3)
	putMediaChunk(t, alias, "bytes 0-2/3", "xyz", http.StatusOK, "")

	unknown := beginUnknownMediaSession(t, server.URL, "/upload/bigquery/v2/projects/test-project/jobs")
	putMediaChunk(t, unknown, "bytes 0-2/*", "ghi", http.StatusPermanentRedirect, "bytes=0-2")
	putMediaChunk(t, unknown, "bytes */*", "", http.StatusPermanentRedirect, "bytes=0-2")
	putMediaChunk(t, unknown, "bytes 3-5/6", "jkl", http.StatusOK, "")

	exactBoundary := beginUnknownMediaSession(t, server.URL, "/upload/bigquery/v2/projects/test-project/jobs")
	putMediaChunk(t, exactBoundary, "bytes 0-2/*", "mno", http.StatusPermanentRedirect, "bytes=0-2")
	putMediaChunk(t, exactBoundary, "bytes */3", "", http.StatusOK, "")
	if len(loads.submissions) != 4 {
		t.Fatalf("resumable sessions submitted %d loads, want 4", len(loads.submissions))
	}
}

func TestMediaUploadRejectsSourceURIsAndOutOfOrderChunks(t *testing.T) {
	loads := &mediaUploadLoadJobs{}
	store := &mediaUploadStore{}
	server := newMediaUploadTestServer(t, loads, store)
	metadata := `{"configuration":{"load":{"sourceUris":["gs://elsewhere/source.parquet"],"sourceFormat":"PARQUET","destinationTable":{"datasetId":"analytics","tableId":"events"}}}}`
	request, err := http.NewRequest(http.MethodPost, server.URL+"/upload/bigquery/v2/projects/test-project/jobs?uploadType=resumable", strings.NewReader(metadata))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Upload-Content-Type", "application/octet-stream")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("sourceUris status=%d", response.StatusCode)
	}

	location := beginMediaSession(t, server.URL, "/upload/bigquery/v2/projects/test-project/jobs", 6)
	putMediaChunk(t, location, "bytes 1-3/6", "bcd", http.StatusBadRequest, "")
	if len(loads.submissions) != 0 || len(store.payload) != 0 {
		t.Fatalf("invalid upload mutated state: submissions=%d payload=%q", len(loads.submissions), store.payload)
	}
}

func TestMediaUploadCompletedSessionsDoNotConsumeActiveQuota(t *testing.T) {
	loads := &mediaUploadLoadJobs{}
	store := &mediaUploadStore{}
	server := newMediaUploadTestServerWithConfig(t, loads, store, MediaUploadConfig{
		Bucket: "bqemu-media", MaxSessions: 1, MaxBytes: 1024, MaxChunkBytes: 1024, SessionTTL: time.Hour,
	})
	first := beginMediaSession(t, server.URL, "/upload/bigquery/v2/projects/test-project/jobs", 3)
	putMediaChunk(t, first, "bytes 0-2/3", "one", http.StatusOK, "")
	second := beginMediaSession(t, server.URL, "/upload/bigquery/v2/projects/test-project/jobs", 3)
	putMediaChunk(t, second, "bytes 0-2/3", "two", http.StatusOK, "")
	if len(loads.submissions) != 2 {
		t.Fatalf("successful sessions submitted %d loads, want 2", len(loads.submissions))
	}
}

func TestMediaUploadDoesNotExpireFinalizingSession(t *testing.T) {
	loads := &mediaUploadLoadJobs{}
	store := &mediaUploadStore{}
	support, err := NewMediaUploadSupport(store, MediaUploadConfig{
		Bucket: "bqemu-media", MaxSessions: 1, MaxBytes: 1024, MaxChunkBytes: 1024, SessionTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC)
	support.now = func() time.Time { return now }
	handlers := newMediaUploadHandlers(&combinedJobHandlers{loads: loads}, support, "")
	handlers.sessions["finalizing"] = &mediaUploadSession{
		id: "finalizing", payload: []byte("parquet"), finalizing: true,
		updatedAt: now.Add(-2 * time.Minute),
	}
	handlers.used = int64(len("parquet"))
	handlers.pruneLocked(now)
	if _, ok := handlers.sessions["finalizing"]; !ok || handlers.used != int64(len("parquet")) {
		t.Fatalf("TTL cleanup removed finalizing session: sessions=%d used=%d", len(handlers.sessions), handlers.used)
	}
}

func TestMediaUploadExpiredSessionDoesNotSubmitLoad(t *testing.T) {
	loads := &mediaUploadLoadJobs{}
	store := &mediaUploadStore{}
	support, err := NewMediaUploadSupport(store, MediaUploadConfig{
		Bucket: "bqemu-media", MaxSessions: 1, MaxBytes: 1024, MaxChunkBytes: 1024, SessionTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC)
	support.now = func() time.Time { return now }
	handlers := newMediaUploadHandlers(&combinedJobHandlers{loads: loads}, support, "")
	handlers.sessions["expired"] = &mediaUploadSession{
		id: "expired", payload: []byte("old"), expected: 6, updatedAt: now.Add(-2 * time.Minute),
	}
	handlers.used = int64(len("old"))
	_, _, err = handlers.append(context.Background(), "expired", uploadContentRange{start: 3, end: 5, total: 6, totalKnown: true}, []byte("new"), 0)
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expired session error = %v, want not found", err)
	}
	if len(loads.submissions) != 0 || len(store.payload) != 0 || len(handlers.sessions) != 0 {
		t.Fatalf("expired upload mutated state: submissions=%d payload=%q sessions=%d", len(loads.submissions), store.payload, len(handlers.sessions))
	}
}

func TestMediaUploadMetadataIsBoundedSeparatelyFromMedia(t *testing.T) {
	if _, err := readMediaMetadata(strings.NewReader(strings.Repeat("m", mediaUploadMetadataLimit+1))); err == nil {
		t.Fatal("oversized media metadata was accepted")
	}
}

func TestMediaUploadReservationsBoundMultipartAndResumablePayloads(t *testing.T) {
	support, err := NewMediaUploadSupport(&mediaUploadStore{}, MediaUploadConfig{
		Bucket: "bqemu-media", MaxSessions: 2, MaxBytes: 6, MaxChunkBytes: 6, SessionTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	handlers := newMediaUploadHandlers(&combinedJobHandlers{}, support, "")
	handlers.sessions["active"] = &mediaUploadSession{id: "active", payload: []byte("abc"), updatedAt: support.now()}
	handlers.used = 3
	reservation, err := handlers.reserveMultipart()
	if err != nil || reservation != 3 {
		t.Fatalf("multipart reservation = %d, %v; want 3, nil", reservation, err)
	}
	if err := handlers.reserveBytes(1); !errors.Is(err, domain.ErrPrecondition) {
		t.Fatalf("concurrent chunk reservation error = %v, want precondition", err)
	}
	handlers.releaseReservation(reservation)
	if err := handlers.reserveBytes(3); err != nil {
		t.Fatalf("released budget did not admit chunk: %v", err)
	}
	handlers.releaseReservation(3)
}

func TestMediaUploadBodyLimitIncludesMultipartFraming(t *testing.T) {
	support, err := NewMediaUploadSupport(&mediaUploadStore{}, MediaUploadConfig{
		Bucket: "bqemu-media", MaxSessions: 1, MaxBytes: 17, MaxChunkBytes: 17, SessionTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := NewCatalogServer(nil, nil, "", WithMediaUpload(support))
	limits := server.mediaUploadBodyLimits()
	if limits == nil || limits.maxCompressedBytes <= support.config.MaxBytes || limits.maxUncompressedBytes <= support.config.MaxBytes {
		t.Fatalf("media request limits = %#v; must include multipart metadata and framing", limits)
	}
}

func TestMediaUploadExecutesParquetLoadThroughGCS(t *testing.T) {
	contracttest.Operation(t, "bigquery.jobs.insert.upload")
	ctx := context.Background()
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	clock := testClock{value: time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC)}
	ids := &testIDs{}
	catalog := application.NewCatalogService(memory.NewCatalogRepository(), warehouse, clock)
	if _, err := catalog.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateDataset(ctx, domain.Dataset{ProjectID: "test-project", ID: "analytics", Location: "US"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateTable(ctx, domain.Table{
		ProjectID: "test-project", DatasetID: "analytics", ID: "events",
		Schema: []domain.Field{{Name: "id", Type: "INT64", Mode: "REQUIRED"}, {Name: "name", Type: "STRING"}},
	}); err != nil {
		t.Fatal(err)
	}
	parquetPath := createRESTParquet(t, "SELECT 1::BIGINT AS id, 'first'::VARCHAR AS name UNION ALL SELECT 2, 'second'")
	parquet, err := os.ReadFile(parquetPath)
	if err != nil {
		t.Fatal(err)
	}
	objects := make(map[string][]byte)
	fakeGCS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/storage/v1/b/bqemu-media":
			http.NotFound(w, r)
		case r.Method == http.MethodPost && r.URL.Path == "/storage/v1/b":
			_, _ = io.WriteString(w, `{"name":"bqemu-media"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/upload/storage/v1/b/bqemu-media/o":
			name := r.URL.Query().Get("name")
			if r.URL.Query().Get("uploadType") != "media" || name == "" {
				http.Error(w, "invalid upload", http.StatusBadRequest)
				return
			}
			payload, readErr := io.ReadAll(r.Body)
			if readErr != nil {
				http.Error(w, readErr.Error(), http.StatusInternalServerError)
				return
			}
			objects[name] = payload
			_ = json.NewEncoder(w).Encode(map[string]string{
				"name": name, "size": strconv.Itoa(len(payload)), "generation": "1",
			})
		case strings.HasPrefix(r.URL.Path, "/storage/v1/b/bqemu-media/o/"):
			name := strings.TrimPrefix(r.URL.Path, "/storage/v1/b/bqemu-media/o/")
			payload, ok := objects[name]
			if !ok {
				http.NotFound(w, r)
				return
			}
			if r.URL.Query().Get("alt") == "media" {
				w.Header().Set("x-goog-generation", "1")
				_, _ = w.Write(payload)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{
				"name": name, "size": strconv.Itoa(len(payload)), "generation": "1",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(fakeGCS.Close)
	gcs, err := objectstore.NewGCSJSON(objectstore.GCSJSONConfig{Endpoint: fakeGCS.URL, Client: fakeGCS.Client()})
	if err != nil {
		t.Fatal(err)
	}
	loadConfig := loadApplication.DefaultConfig()
	loadConfig.TempDirectory = t.TempDir()
	loadConfig.OperationTimeout = 5 * time.Second
	loads, err := loadApplication.NewService(
		loadApplication.NewMemoryJobRepository(), gcs, NewLoadTableCatalog(catalog), warehouse,
		clock, ids, loadConfig,
	)
	if err != nil {
		t.Fatal(err)
	}
	queries := newRESTTestQueryService(
		memory.NewJobRepository(), warehouse, clock, ids,
		application.WithGoogleSQLGateway(newRESTGoogleSQLGateway(catalog)),
		application.WithQueryDestinationCatalog(catalog),
		application.WithStatementMaterializer(warehouse),
	)
	support, err := NewMediaUploadSupport(gcs, MediaUploadConfig{
		Bucket: "bqemu-media", MaxSessions: 2, MaxBytes: 1 << 20, MaxChunkBytes: 1 << 20, SessionTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	support.newID = func() string { return "media-load" }
	server := httptest.NewServer(NewServerWithLoadJobs(catalog, queries, loads, warehouse, "", WithMediaUpload(support)).Handler())
	t.Cleanup(server.Close)

	body, contentType := multipartLoadBody(t, `{
  "jobReference":{"jobId":"media-load","location":"US"},
  "configuration":{"load":{
    "sourceFormat":"PARQUET",
    "destinationTable":{"datasetId":"analytics","tableId":"events"},
    "writeDisposition":"WRITE_APPEND"
  }}
}`, parquet)
	request, err := http.NewRequest(http.MethodPost, server.URL+"/upload/bigquery/v2/projects/test-project/jobs?uploadType=multipart", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", contentType)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("media upload status=%d body=%s", response.StatusCode, payload)
	}
	job := waitForRESTLoad(t, server.URL, "media-load")
	if result := job["status"].(map[string]any)["errorResult"]; result != nil {
		t.Fatalf("media load job failed: %#v", job)
	}
	query := restLoadRequest(t, server.URL, http.MethodPost, "/bigquery/v2/projects/test-project/queries",
		`{"query":"SELECT count(*) AS row_count FROM `+"`test-project.analytics.events`"+`","useLegacySql":false}`, http.StatusOK)
	if rows := query["rows"].([]any); len(rows) != 1 || rows[0].(map[string]any)["f"].([]any)[0].(map[string]any)["v"] != "2" {
		t.Fatalf("unexpected query result after media load: %#v", query)
	}
	if len(objects) != 1 {
		t.Fatalf("GCS objects = %d, want one immutable media object", len(objects))
	}

	invalidBody, invalidContentType := multipartLoadBody(t, `{
  "jobReference":{"jobId":"media-invalid","location":"US"},
  "configuration":{"load":{
    "sourceFormat":"PARQUET",
    "destinationTable":{"datasetId":"analytics","tableId":"events"},
    "writeDisposition":"WRITE_APPEND"
  }}
}`, []byte("not a parquet file"))
	invalidRequest, err := http.NewRequest(http.MethodPost, server.URL+"/upload/bigquery/v2/projects/test-project/jobs?uploadType=multipart", bytes.NewReader(invalidBody))
	if err != nil {
		t.Fatal(err)
	}
	invalidRequest.Header.Set("Content-Type", invalidContentType)
	invalidResponse, err := http.DefaultClient.Do(invalidRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer invalidResponse.Body.Close()
	if invalidResponse.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(invalidResponse.Body)
		t.Fatalf("invalid media upload status=%d body=%s", invalidResponse.StatusCode, payload)
	}
	invalidJob := waitForRESTLoad(t, server.URL, "media-invalid")
	if invalidJob["status"].(map[string]any)["errorResult"] == nil {
		t.Fatalf("invalid Parquet unexpectedly succeeded: %#v", invalidJob)
	}
	afterFailure := restLoadRequest(t, server.URL, http.MethodPost, "/bigquery/v2/projects/test-project/queries",
		`{"query":"SELECT count(*) AS row_count FROM `+"`test-project.analytics.events`"+`","useLegacySql":false}`, http.StatusOK)
	if afterFailure["rows"].([]any)[0].(map[string]any)["f"].([]any)[0].(map[string]any)["v"] != "2" {
		t.Fatalf("failed media load changed destination: %#v", afterFailure)
	}
}

func newMediaUploadTestServer(t *testing.T, loads *mediaUploadLoadJobs, store *mediaUploadStore) *httptest.Server {
	t.Helper()
	return newMediaUploadTestServerWithConfig(t, loads, store, MediaUploadConfig{
		Bucket: "bqemu-media", MaxSessions: 4, MaxBytes: 1024, MaxChunkBytes: 1024, SessionTTL: time.Minute,
	})
}

func newMediaUploadTestServerWithConfig(t *testing.T, loads *mediaUploadLoadJobs, store *mediaUploadStore, config MediaUploadConfig) *httptest.Server {
	t.Helper()
	support, err := NewMediaUploadSupport(store, config)
	if err != nil {
		t.Fatal(err)
	}
	sequence := 0
	support.newID = func() string {
		sequence++
		return "upload-id-" + strconv.Itoa(sequence)
	}
	server := httptest.NewServer(NewServerWithLoadJobs(nil, nil, loads, nil, "", WithMediaUpload(support)).Handler())
	t.Cleanup(server.Close)
	return server
}

func multipartLoadBody(t *testing.T, metadata string, media []byte) ([]byte, string) {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	metadataHeader := textproto.MIMEHeader{}
	metadataHeader.Set("Content-Type", "application/json; charset=UTF-8")
	part, err := writer.CreatePart(metadataHeader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(part, metadata); err != nil {
		t.Fatal(err)
	}
	mediaHeader := textproto.MIMEHeader{}
	mediaHeader.Set("Content-Type", "*/*")
	part, err = writer.CreatePart(mediaHeader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write(media); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return body.Bytes(), "multipart/related; boundary=" + writer.Boundary()
}

func beginMediaSession(t *testing.T, baseURL, path string, length int64) string {
	t.Helper()
	return beginMediaSessionWithLength(t, baseURL, path, &length)
}

func beginUnknownMediaSession(t *testing.T, baseURL, path string) string {
	t.Helper()
	return beginMediaSessionWithLength(t, baseURL, path, nil)
}

func beginMediaSessionWithLength(t *testing.T, baseURL, path string, length *int64) string {
	t.Helper()
	metadata := `{"configuration":{"load":{"sourceFormat":"PARQUET","destinationTable":{"datasetId":"analytics","tableId":"events"}}}}`
	request, err := http.NewRequest(http.MethodPost, baseURL+path+"?uploadType=resumable", strings.NewReader(metadata))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json; charset=UTF-8")
	request.Header.Set("X-Upload-Content-Type", "*/*")
	if length != nil {
		request.Header.Set("X-Upload-Content-Length", strconv.FormatInt(*length, 10))
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("initiate status=%d body=%s", response.StatusCode, body)
	}
	location := response.Header.Get("Location")
	if location == "" {
		t.Fatal("missing resumable session Location")
	}
	return location
}

func putMediaChunk(t *testing.T, location, contentRange, body string, expectedStatus int, expectedRange string) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPut, location, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Range", contentRange)
	request.Header.Set("Content-Type", "*/*")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != expectedStatus {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("chunk %s status=%d body=%s", contentRange, response.StatusCode, payload)
	}
	if got := response.Header.Get("Range"); got != expectedRange {
		t.Fatalf("chunk %s Range=%q want %q", contentRange, got, expectedRange)
	}
}

type mediaUploadStore struct {
	bucket  string
	name    string
	payload []byte
}

func (s *mediaUploadStore) Upload(_ context.Context, bucket, name, _ string, payload io.Reader, _ int64) (loadports.ObjectInfo, error) {
	contents, err := io.ReadAll(payload)
	if err != nil {
		return loadports.ObjectInfo{}, err
	}
	s.bucket, s.name, s.payload = bucket, name, append([]byte(nil), contents...)
	return loadports.ObjectInfo{URI: "gs://" + bucket + "/" + name, Size: int64(len(contents)), Generation: "1"}, nil
}

func (s *mediaUploadStore) Delete(_ context.Context, _ loadports.ObjectInfo) error {
	s.payload = nil
	return nil
}

type mediaUploadLoadJobs struct {
	submissions []loadSubmission
}

func (s *mediaUploadLoadJobs) Submit(_ context.Context, reference loadDomain.JobReference, configuration loadDomain.LoadConfiguration) (*loadDomain.Job, error) {
	if reference.JobID == "" {
		reference.JobID = "generated-job"
	}
	if reference.Location == "" {
		reference.Location = "US"
	}
	job, err := loadDomain.NewJob(reference, configuration, time.Now())
	if err != nil {
		return nil, err
	}
	s.submissions = append(s.submissions, loadSubmission{reference: reference, configuration: configuration})
	return job, nil
}

func (s *mediaUploadLoadJobs) Get(_ context.Context, _ loadDomain.JobReference) (*loadDomain.Job, error) {
	return nil, loadDomain.ErrNotFound
}

func (s *mediaUploadLoadJobs) List(_ context.Context, _, _ string) ([]*loadDomain.Job, error) {
	return nil, nil
}
