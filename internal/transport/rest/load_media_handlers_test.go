package rest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	"github.com/leeyh0216/go-bemu/internal/adapters/memory"
	"github.com/leeyh0216/go-bemu/internal/adapters/objectstore"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/domain"
	loadApplication "github.com/leeyh0216/go-bemu/internal/loadjob/application"
	loadDomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
)

type mediaTestLoads struct {
	mu             sync.Mutex
	media          loadports.MediaUploadStore
	configurations []loadDomain.LoadConfiguration
}

// mediaLoadRuntime keeps media source ownership at the adapter boundary while
// exposing the ordinary load service to the REST dispatcher.
type mediaLoadRuntime struct {
	*loadApplication.Service
	media loadports.MediaUploadStore
}

func (r *mediaLoadRuntime) MediaUploads() loadports.MediaUploadStore { return r.media }

type mediaTestQueries struct{}

func (mediaTestQueries) RunSync(context.Context, application.QueryInput) (*domain.Job, error) {
	return nil, domain.ErrNotFound
}
func (mediaTestQueries) Submit(context.Context, application.QueryInput) (*domain.Job, error) {
	return nil, domain.ErrNotFound
}
func (mediaTestQueries) Get(context.Context, domain.JobReference) (*domain.Job, error) {
	return nil, domain.ErrNotFound
}
func (mediaTestQueries) List(context.Context, string, string) ([]*domain.Job, error) { return nil, nil }

func (m *mediaTestLoads) MediaUploads() loadports.MediaUploadStore { return m.media }
func (m *mediaTestLoads) Submit(_ context.Context, reference loadDomain.JobReference, configuration loadDomain.LoadConfiguration) (*loadDomain.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.configurations = append(m.configurations, configuration)
	if reference.JobID == "" {
		reference.JobID = "media"
	}
	if reference.Location == "" {
		reference.Location = "US"
	}
	return loadDomain.NewJob(reference, configuration, time.Now())
}
func (m *mediaTestLoads) Get(context.Context, loadDomain.JobReference) (*loadDomain.Job, error) {
	return nil, loadDomain.ErrNotFound
}
func (m *mediaTestLoads) List(context.Context, string, string) ([]*loadDomain.Job, error) {
	return nil, nil
}

func TestMediaUploadMultipartAndResumableSubmitOpaqueObjects(t *testing.T) {
	store, err := objectstore.NewMediaStore(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	loads := &mediaTestLoads{media: store}
	h := &combinedJobHandlers{query: &queryHandlers{queries: mediaTestQueries{}}, loads: loads, media: store, sessions: &mediaUploadSessions{items: make(map[string]*mediaUploadSession)}}
	metadata := []byte(`{"jobReference":{"jobId":"media-one","location":"US"},"configuration":{"load":{"destinationTable":{"projectId":"p","datasetId":"d","tableId":"t"},"sourceFormat":"PARQUET"}}}`)
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreatePart(textHeader("application/json"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = part.Write(metadata)
	part, err = writer.CreatePart(textHeader("application/octet-stream"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = part.Write([]byte("parquet-media"))
	_ = writer.Close()
	req := httptest.NewRequest(http.MethodPost, "/upload/bigquery/v2/projects/p/jobs?uploadType=multipart", &body)
	req.SetPathValue("projectId", "p")
	req.Header.Set("Content-Type", "multipart/related; boundary="+writer.Boundary())
	response := httptest.NewRecorder()
	h.uploadLoadJob(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("multipart status=%d body=%s", response.Code, response.Body.String())
	}

	init := httptest.NewRequest(http.MethodPost, "/resumable/upload/bigquery/v2/projects/p/jobs?uploadType=resumable", bytes.NewReader(metadata))
	init.SetPathValue("projectId", "p")
	init.Header.Set("X-Upload-Content-Length", "6")
	start := httptest.NewRecorder()
	h.uploadLoadJob(start, init)
	if start.Code != http.StatusOK {
		t.Fatalf("start status=%d body=%s", start.Code, start.Body.String())
	}
	location := start.Header().Get("Location")
	if location == "" {
		t.Fatal("missing resumable Location")
	}
	u, err := url.Parse(location)
	if err != nil {
		t.Fatal(err)
	}
	chunk := func(content, contentRange string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPut, u.String(), strings.NewReader(content))
		request.SetPathValue("projectId", "p")
		request.Header.Set("Content-Range", contentRange)
		recorder := httptest.NewRecorder()
		h.uploadLoadJob(recorder, request)
		return recorder
	}
	partial := chunk("abc", "bytes 0-2/6")
	if partial.Code != http.StatusPermanentRedirect || partial.Header().Get("Range") != "bytes=0-2" {
		t.Fatalf("partial=%d range=%q body=%s", partial.Code, partial.Header().Get("Range"), partial.Body.String())
	}
	status := chunk("", "bytes */6")
	if status.Code != http.StatusPermanentRedirect || status.Header().Get("Range") != "bytes=0-2" {
		t.Fatalf("status=%d range=%q", status.Code, status.Header().Get("Range"))
	}
	complete := chunk("def", "bytes 3-5/6")
	if complete.Code != http.StatusOK {
		t.Fatalf("complete=%d body=%s", complete.Code, complete.Body.String())
	}

	loads.mu.Lock()
	defer loads.mu.Unlock()
	if len(loads.configurations) != 2 {
		t.Fatalf("submissions=%d", len(loads.configurations))
	}
	for _, configuration := range loads.configurations {
		if len(configuration.SourceURIs) != 1 || !strings.HasPrefix(configuration.SourceURIs[0], "bqemu-upload://media/") {
			t.Fatalf("sourceUris=%v", configuration.SourceURIs)
		}
		object, err := store.Get(context.Background(), configuration.SourceURIs[0])
		if err != nil {
			t.Fatal(err)
		}
		reader, err := store.Open(context.Background(), object)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestExpiredMediaUploadIsDiscardedBeforeLoadSubmission(t *testing.T) {
	store, err := objectstore.NewMediaStore(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	id, err := store.Create(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), id, 0, strings.NewReader("partial")); err != nil {
		t.Fatal(err)
	}
	loads := &mediaTestLoads{media: store}
	h := &combinedJobHandlers{
		query:    &queryHandlers{queries: mediaTestQueries{}},
		loads:    loads,
		media:    store,
		sessions: &mediaUploadSessions{items: map[string]*mediaUploadSession{id: &mediaUploadSession{storeID: id, created: time.Now().Add(-mediaSessionTTL - time.Second)}}},
	}
	h.sessions.mu.Lock()
	h.sessions.expireLocked(store, time.Now())
	h.sessions.mu.Unlock()
	if _, err := store.Size(context.Background(), id); !errors.Is(err, loadDomain.ErrNotFound) {
		t.Fatalf("expired media staging object error=%v, want not found", err)
	}
	request := httptest.NewRequest(http.MethodPut, "/resumable/upload/bigquery/v2/projects/p/jobs?uploadType=resumable&upload_id="+id, strings.NewReader("partial"))
	request.SetPathValue("projectId", "p")
	request.Header.Set("Content-Range", "bytes 0-6/7")
	response := httptest.NewRecorder()
	h.uploadLoadJob(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("expired upload status=%d body=%s", response.Code, response.Body.String())
	}
	loads.mu.Lock()
	defer loads.mu.Unlock()
	if len(loads.configurations) != 0 {
		t.Fatalf("expired upload submitted load configurations=%#v", loads.configurations)
	}
}

func TestMediaUploadMultipartAndResumableLoadSameParquetWithoutRetryRows(t *testing.T) {
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
	for _, tableID := range []string{"multipart_events", "resumable_events", "resumable_single_events"} {
		if _, err := catalog.CreateTable(ctx, domain.Table{ProjectID: "test-project", DatasetID: "analytics", ID: tableID, Schema: []domain.Field{{Name: "id", Type: "INT64"}, {Name: "name", Type: "STRING"}}}); err != nil {
			t.Fatal(err)
		}
	}
	parquet, err := os.ReadFile(createRESTParquet(t, "SELECT 1::BIGINT AS id, 'one'::VARCHAR AS name UNION ALL SELECT 2, 'two'"))
	if err != nil {
		t.Fatal(err)
	}
	store, err := objectstore.NewMediaStore(t.TempDir(), int64(len(parquet)+1024))
	if err != nil {
		t.Fatal(err)
	}
	objects, err := objectstore.NewRouterWithMedia(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	loadConfig := loadApplication.DefaultConfig()
	loadConfig.TempDirectory, loadConfig.OperationTimeout = t.TempDir(), 5*time.Second
	service, err := loadApplication.NewService(loadApplication.NewMemoryJobRepository(), objects, NewLoadTableCatalog(catalog), warehouse, clock, ids, loadConfig)
	if err != nil {
		t.Fatal(err)
	}
	loads := &mediaLoadRuntime{Service: service, media: store}
	queries := application.NewQueryService(memory.NewJobRepository(), warehouse, clock, ids)
	server := httptest.NewServer(NewServerWithLoadJobs(catalog, queries, loads, warehouse, "").Handler())
	t.Cleanup(server.Close)

	metadata := func(jobID, tableID string) []byte {
		return []byte(`{"jobReference":{"jobId":"` + jobID + `","location":"US"},"configuration":{"load":{"destinationTable":{"projectId":"test-project","datasetId":"analytics","tableId":"` + tableID + `"},"sourceFormat":"PARQUET","writeDisposition":"WRITE_APPEND","parquetOptions":{"enableListInference":true}}}}`)
	}
	uploadMultipartParquet(t, server.URL, metadata("multipart-job", "multipart_events"), parquet)
	multipartJob := waitForRESTLoad(t, server.URL, "multipart-job")
	if errorResult := multipartJob["status"].(map[string]any)["errorResult"]; errorResult != nil {
		t.Fatalf("multipart job failed: %#v", multipartJob)
	}
	resumableURL := startResumableParquet(t, server.URL, metadata("resumable-job", "resumable_events"), len(parquet))
	middle := len(parquet) / 2
	uploadResumableParquetChunk(t, server.URL, resumableURL, parquet[:middle], 0, middle-1, len(parquet), http.StatusPermanentRedirect)
	uploadResumableParquetChunk(t, server.URL, resumableURL, parquet[middle:], middle, len(parquet)-1, len(parquet), http.StatusOK)

	resumableJob := waitForRESTLoad(t, server.URL, "resumable-job")
	if errorResult := resumableJob["status"].(map[string]any)["errorResult"]; errorResult != nil {
		t.Fatalf("resumable job failed: %#v", resumableJob)
	}
	singleURL := startResumableParquet(t, server.URL, metadata("resumable-single-job", "resumable_single_events"), len(parquet))
	uploadResumableParquetChunk(t, server.URL, singleURL, parquet, 0, len(parquet)-1, len(parquet), http.StatusOK)
	singleJob := waitForRESTLoad(t, server.URL, "resumable-single-job")
	if errorResult := singleJob["status"].(map[string]any)["errorResult"]; errorResult != nil {
		t.Fatalf("single-chunk resumable job failed: %#v", singleJob)
	}
	// Replaying the final resumable PUT must return the stored job, not append.
	uploadResumableParquetChunk(t, server.URL, resumableURL, parquet[middle:], middle, len(parquet)-1, len(parquet), http.StatusOK)
	for _, tableID := range []string{"multipart_events", "resumable_events", "resumable_single_events"} {
		result := restLoadRequest(t, server.URL, http.MethodPost, "/bigquery/v2/projects/test-project/queries", `{"query":"SELECT count(*) AS rows FROM `+"`test-project.analytics."+tableID+"`"+`","useLegacySql":false}`, http.StatusOK)
		if result["rows"].([]any)[0].(map[string]any)["f"].([]any)[0].(map[string]any)["v"] != "2" {
			t.Fatalf("table %s rows after media upload=%#v", tableID, result)
		}
	}
}

func uploadMultipartParquet(t *testing.T, baseURL string, metadata, parquet []byte) {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreatePart(textHeader("application/json"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = part.Write(metadata)
	part, err = writer.CreatePart(textHeader("application/octet-stream"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = part.Write(parquet)
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, baseURL+"/upload/bigquery/v2/projects/test-project/jobs?uploadType=multipart", &body)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "multipart/related; boundary="+writer.Boundary())
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("multipart status=%d payload=%s", response.StatusCode, payload)
	}
}

func startResumableParquet(t *testing.T, baseURL string, metadata []byte, total int) string {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, baseURL+"/resumable/upload/bigquery/v2/projects/test-project/jobs?uploadType=resumable", bytes.NewReader(metadata))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Upload-Content-Length", strconv.Itoa(total))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("start resumable status=%d payload=%s", response.StatusCode, payload)
	}
	location := response.Header.Get("Location")
	if location == "" {
		t.Fatal("missing resumable Location")
	}
	return baseURL + location
}

func uploadResumableParquetChunk(t *testing.T, baseURL, location string, chunk []byte, start, end, total, expectedStatus int) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPut, location, bytes.NewReader(chunk))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != expectedStatus {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("resumable chunk status=%d want=%d payload=%s", response.StatusCode, expectedStatus, payload)
	}
}

func textHeader(contentType string) textproto.MIMEHeader {
	return textproto.MIMEHeader{"Content-Type": {contentType}}
}
