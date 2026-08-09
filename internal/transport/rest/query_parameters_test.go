package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/leeyh0216/go-bemu/internal/adapters/duckdb"
	googlesqladapter "github.com/leeyh0216/go-bemu/internal/adapters/googlesql"
	stateadapter "github.com/leeyh0216/go-bemu/internal/adapters/sqlite"
	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestQueryParametersRoundTripAcrossRESTAndSQLite(t *testing.T) {
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
	parser, err := googlesqladapter.NewParser()
	if err != nil {
		t.Fatal(err)
	}
	clock := testClock{value: time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC)}
	catalog := application.NewCatalogService(state, warehouse, clock)
	if _, err := catalog.CreateProject(ctx, domain.Project{ID: "test-project"}); err != nil {
		t.Fatal(err)
	}
	queries := application.NewQueryService(stateadapter.NewQueryJobRepository(state), warehouse, clock, &testIDs{}, application.WithQueryParameterValidator(parser))
	server := httptest.NewServer(NewServer(catalog, queries, warehouse, "").Handler())
	t.Cleanup(server.Close)

	body := `{"jobReference":{"jobId":"parameter-round-trip","location":"US"},"configuration":{"labels":{"consumer":"python"},"query":{"query":"SELECT @id AS id, @text AS text","priority":"BATCH","parameterMode":"NAMED","queryParameters":[{"name":"id","parameterType":{"type":"INT64"},"parameterValue":{"value":"42"}},{"name":"text","parameterType":{"type":"STRING"},"parameterValue":{"value":"bound value"}}]}}}`
	response, err := http.Post(server.URL+"/bigquery/v2/projects/test-project/jobs", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("jobs.insert status=%d", response.StatusCode)
	}

	var job *domain.Job
	for deadline := time.Now().Add(3 * time.Second); ; {
		job, err = queries.Get(ctx, domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "parameter-round-trip"})
		if err == nil && job.State == domain.JobDone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("query job did not complete: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	encoded, err := json.Marshal(jobFromDomain(job))
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	query := wire["configuration"].(map[string]any)["query"].(map[string]any)
	if query["parameterMode"] != "NAMED" || query["priority"] != "BATCH" {
		t.Fatalf("query configuration = %#v", query)
	}
	if len(query["queryParameters"].([]any)) != 2 || wire["configuration"].(map[string]any)["labels"].(map[string]any)["consumer"] != "python" {
		t.Fatalf("parameter/label round trip = %#v", wire)
	}
	if job.Result == nil || len(job.Result.Rows) != 1 || job.Result.Rows[0][0] != int64(42) || job.Result.Rows[0][1] != "bound value" {
		t.Fatalf("bound result = %#v", job.Result)
	}

	positional := `{"jobReference":{"jobId":"positional-round-trip","location":"US"},"configuration":{"query":{"query":"SELECT ? + ? AS sum","parameterMode":"POSITIONAL","queryParameters":[{"parameterType":{"type":"INT64"},"parameterValue":{"value":"2"}},{"parameterType":{"type":"INT64"},"parameterValue":{"value":"3"}}]}}}`
	response, err = http.Post(server.URL+"/bigquery/v2/projects/test-project/jobs", "application/json", strings.NewReader(positional))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("positional jobs.insert status=%d", response.StatusCode)
	}
	for deadline := time.Now().Add(3 * time.Second); ; {
		job, err = queries.Get(ctx, domain.JobReference{ProjectID: "test-project", Location: "US", JobID: "positional-round-trip"})
		if err == nil && job.State == domain.JobDone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("positional query job did not complete: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if job.Error != nil || job.Result == nil || job.Result.Rows[0][0] != int64(5) {
		t.Fatalf("positional bound result = %#v, error=%#v", job.Result, job.Error)
	}
}
