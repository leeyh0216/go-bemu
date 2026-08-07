package rest

// Official jobs/query methods: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type QueryUseCases interface {
	RunSync(context.Context, application.QueryInput) (*domain.Job, error)
	Submit(context.Context, application.QueryInput) (*domain.Job, error)
	Get(context.Context, string, string) (*domain.Job, error)
	List(context.Context, string) ([]*domain.Job, error)
}

var _ QueryUseCases = (*application.QueryService)(nil)

type queryHandlers struct {
	queries QueryUseCases
}

func NewServer(catalog CatalogUseCases, queries QueryUseCases, readiness ports.HealthChecker, baseURL string, options ...Option) *Server {
	defaults := []Option{withQueryAPI(queries), withCapabilitiesAPI(), withConsoleAPI()}
	return NewCatalogServer(catalog, readiness, baseURL, append(defaults, options...)...)
}

func withQueryAPI(queries QueryUseCases) Option {
	return func(server *Server) {
		handlers := &queryHandlers{queries: queries}
		server.routeExtensions = append(server.routeExtensions, func(mux *http.ServeMux) {
			mux.HandleFunc("POST /bigquery/v2/projects/{projectId}/queries", handlers.query)
			mux.HandleFunc("GET /bigquery/v2/projects/{projectId}/queries/{jobId}", handlers.getQueryResults)
			mux.HandleFunc("POST /bigquery/v2/projects/{projectId}/jobs", handlers.insertJob)
			mux.HandleFunc("GET /bigquery/v2/projects/{projectId}/jobs", handlers.listJobs)
			mux.HandleFunc("GET /bigquery/v2/projects/{projectId}/jobs/{jobId}", handlers.getJob)
		})
		server.discoveryExtensions = append(server.discoveryExtensions, extendQueryDiscovery)
	}
}

func (h *queryHandlers) query(w http.ResponseWriter, r *http.Request) {
	var request queryRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	if request.UseLegacySQL {
		writeError(w, fmt.Errorf("%w: legacy SQL is not supported", domain.ErrInvalid))
		return
	}
	input := application.QueryInput{ProjectID: r.PathValue("projectId"), SQL: request.Query, Location: request.Location}
	if request.DefaultDataset != nil {
		input.DefaultDataset = request.DefaultDataset.DatasetID
	}
	job, err := h.queries.RunSync(r.Context(), input)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, queryResponseFromDomain(job, request.MaxResults, 0))
}

func (h *queryHandlers) insertJob(w http.ResponseWriter, r *http.Request) {
	var request jobResource
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	if request.Configuration.Query == nil {
		writeError(w, fmt.Errorf("%w: only query jobs are implemented", domain.ErrInvalid))
		return
	}
	query := request.Configuration.Query
	if query.UseLegacySQL {
		writeError(w, fmt.Errorf("%w: legacy SQL is not supported", domain.ErrInvalid))
		return
	}
	input := application.QueryInput{
		ProjectID: r.PathValue("projectId"), JobID: request.JobReference.JobID,
		Location: request.JobReference.Location, SQL: query.Query,
	}
	if query.DefaultDataset != nil {
		input.DefaultDataset = query.DefaultDataset.DatasetID
	}
	job, err := h.queries.Submit(r.Context(), input)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, jobFromDomain(job))
}

func (h *queryHandlers) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := h.queries.Get(r.Context(), r.PathValue("projectId"), r.PathValue("jobId"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, jobFromDomain(job))
}

func (h *queryHandlers) listJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := h.queries.List(r.Context(), r.PathValue("projectId"))
	if err != nil {
		writeError(w, err)
		return
	}
	maxResults, _ := strconv.Atoi(r.URL.Query().Get("maxResults"))
	if maxResults > 0 && maxResults < len(jobs) {
		jobs = jobs[:maxResults]
	}
	resources := make([]jobResource, len(jobs))
	for i, job := range jobs {
		resources[i] = jobFromDomain(job)
	}
	writeJSON(w, http.StatusOK, map[string]any{"kind": "bigquery#jobList", "jobs": resources})
}

func (h *queryHandlers) getQueryResults(w http.ResponseWriter, r *http.Request) {
	job, err := h.queries.Get(r.Context(), r.PathValue("projectId"), r.PathValue("jobId"))
	if err != nil {
		writeError(w, err)
		return
	}
	maxResults, _ := strconv.Atoi(r.URL.Query().Get("maxResults"))
	startIndex, _ := strconv.Atoi(r.URL.Query().Get("startIndex"))
	writeJSON(w, http.StatusOK, queryResponseFromDomain(job, maxResults, startIndex))
}
