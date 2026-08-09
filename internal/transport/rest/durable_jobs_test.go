package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	stateadapter "github.com/leeyh0216/go-bemu/internal/adapters/sqlite"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/domain"
	loadapplication "github.com/leeyh0216/go-bemu/internal/loadjob/application"
)

func TestPublicJobsInsertReturnsDuplicateForConcurrentCrossKindIdentity(t *testing.T) {
	ctx := context.Background()
	state, err := stateadapter.Open(ctx, stateadapter.DefaultConfig(":memory:"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	clock := testClock{value: time.Date(2026, 8, 8, 7, 0, 0, 0, time.UTC)}
	ids := &testIDs{}
	catalog := application.NewCatalogService(state, warehouse, clock)
	if _, err := catalog.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	queries := application.NewQueryService(stateadapter.NewQueryJobRepository(state), warehouse, clock, ids)
	loadConfig := loadapplication.DefaultConfig()
	loadConfig.TempDirectory = t.TempDir()
	loads, err := loadapplication.NewService(
		stateadapter.NewLoadJobRepository(state), fixtureObjectStore{}, NewLoadTableCatalog(catalog),
		warehouse, clock, ids, loadConfig,
	)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewServerWithLoadJobs(catalog, queries, loads, warehouse, "").Handler())
	t.Cleanup(server.Close)

	requests := []string{
		`{"jobReference":{"projectId":"test-project","jobId":"shared-public-id","location":"US"},"configuration":{"query":{"query":"SELECT 1","useLegacySql":false}}}`,
		`{"jobReference":{"projectId":"test-project","jobId":"shared-public-id","location":"us"},"configuration":{"load":{"sourceUris":["gs://bucket/missing.avro"],"destinationTable":{"projectId":"test-project","datasetId":"analytics","tableId":"events"},"sourceFormat":"AVRO"}}}`,
	}
	type response struct {
		status int
		body   map[string]any
		err    error
	}
	start := make(chan struct{})
	responses := make(chan response, len(requests))
	var group sync.WaitGroup
	for _, payload := range requests {
		payload := payload
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			request, err := http.NewRequest(http.MethodPost, server.URL+"/bigquery/v2/projects/test-project/jobs", bytes.NewBufferString(payload))
			if err != nil {
				responses <- response{err: err}
				return
			}
			request.Header.Set("Content-Type", "application/json")
			result, err := server.Client().Do(request)
			if err != nil {
				responses <- response{err: err}
				return
			}
			defer result.Body.Close()
			var body map[string]any
			err = json.NewDecoder(result.Body).Decode(&body)
			responses <- response{status: result.StatusCode, body: body, err: err}
		}()
	}
	close(start)
	group.Wait()
	close(responses)

	statuses := make(map[int]int)
	for result := range responses {
		if result.err != nil {
			t.Fatal(result.err)
		}
		statuses[result.status]++
		if result.status == http.StatusConflict {
			errorResource := result.body["error"].(map[string]any)
			reasons := errorResource["errors"].([]any)
			if reasons[0].(map[string]any)["reason"] != "duplicate" {
				t.Fatalf("duplicate response = %#v", result.body)
			}
		}
	}
	if statuses[http.StatusOK] != 1 || statuses[http.StatusConflict] != 1 {
		t.Fatalf("cross-kind statuses = %#v", statuses)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		job := restLoadRequest(t, server.URL, http.MethodGet,
			"/bigquery/v2/projects/test-project/jobs/shared-public-id?location=US", "", http.StatusOK)
		if job["status"].(map[string]any)["state"] == "DONE" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("winning job did not reach DONE: %#v", job)
		}
		time.Sleep(time.Millisecond)
	}
	listed := restLoadRequest(t, server.URL, http.MethodGet,
		"/bigquery/v2/projects/test-project/jobs?location=US", "", http.StatusOK)
	if jobs := listed["jobs"].([]any); len(jobs) != 1 {
		t.Fatalf("shared identity list = %#v", listed)
	}
}

func TestGetQueryResultsReturnsStableUnavailableErrorAfterStateRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	state, err := stateadapter.Open(ctx, stateadapter.DefaultConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 8, 8, 0, 0, 0, time.UTC)
	if err := state.CreateProject(ctx, domain.Project{ID: "test-project", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	repository := stateadapter.NewQueryJobRepository(state)
	job, err := domain.NewQueryJob(domain.JobReference{
		ProjectID: "test-project", Location: "US", JobID: "result-restart",
	}, "SELECT 1", now)
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := repository.CreateOrGet(ctx, job); err != nil || !created {
		t.Fatalf("create query job: created=%v err=%v", created, err)
	}
	if err := job.Start(now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repository.Update(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := job.Complete(domain.QueryResult{
		Columns: []domain.Column{{Name: "value", Type: "INT64"}}, Rows: [][]any{{int64(1)}},
	}, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repository.Update(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := stateadapter.Open(ctx, stateadapter.DefaultConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	warehouse, err := duckdb.New("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = warehouse.Close() })
	clock := testClock{value: now.Add(time.Hour)}
	catalog := application.NewCatalogService(reopened, warehouse, clock)
	queries := application.NewQueryService(stateadapter.NewQueryJobRepository(reopened), warehouse, clock, &testIDs{})
	server := httptest.NewServer(NewServer(catalog, queries, warehouse, "").Handler())
	t.Cleanup(server.Close)

	failed := restLoadRequest(t, server.URL, http.MethodGet,
		"/bigquery/v2/projects/test-project/queries/result-restart?location=US", "", http.StatusServiceUnavailable)
	errorResource := failed["error"].(map[string]any)
	reasons := errorResource["errors"].([]any)
	if reasons[0].(map[string]any)["reason"] != "backendError" ||
		errorResource["message"] != "query result rows are unavailable after emulator restart" {
		t.Fatalf("unavailable query result error = %#v", failed)
	}
	resource := restLoadRequest(t, server.URL, http.MethodGet,
		"/bigquery/v2/projects/test-project/jobs/result-restart?location=US", "", http.StatusOK)
	if resource["status"].(map[string]any)["state"] != "DONE" {
		t.Fatalf("persisted job resource = %#v", resource)
	}
}
