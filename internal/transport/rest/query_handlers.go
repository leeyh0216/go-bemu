package rest

// Official jobs/query methods: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type QueryUseCases interface {
	RunSync(context.Context, application.QueryInput) (*domain.Job, error)
	Submit(context.Context, application.QueryInput) (*domain.Job, error)
	Get(context.Context, domain.JobReference) (*domain.Job, error)
	List(context.Context, string, string) ([]*domain.Job, error)
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
	if err := validateSynchronousQueryOptions(request); err != nil {
		writeError(w, err)
		return
	}
	input := queryInputFromWire(r.PathValue("projectId"), "", request.Location, request.Query,
		request.DefaultDataset, request.DestinationTable, request.WriteDisposition, request.CreateDisposition, "", nil)
	job, err := h.queries.RunSync(r.Context(), input)
	if err != nil {
		writeError(w, err)
		return
	}
	pageRequest := r.Clone(r.Context())
	query := pageRequest.URL.Query()
	if request.MaxResults != 0 {
		query.Set("maxResults", strconv.Itoa(request.MaxResults))
	}
	pageRequest.URL.RawQuery = query.Encode()
	start, end, next, err := queryResultPageBounds(pageRequest, job)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, queryResponseFromDomain(job, start, end, next))
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
	projectID := r.PathValue("projectId")
	if request.JobReference.ProjectID != "" && request.JobReference.ProjectID != projectID {
		writeError(w, fmt.Errorf("%w: route and jobReference projectId differ", domain.ErrInvalid))
		return
	}
	if query.UseLegacySQL {
		writeError(w, fmt.Errorf("%w: legacy SQL is not supported", domain.ErrInvalid))
		return
	}
	if err := validateQueryJobOptions(request.Configuration); err != nil {
		writeError(w, err)
		return
	}
	input := queryInputFromWire(projectID, request.JobReference.JobID, request.JobReference.Location, query.Query,
		query.DefaultDataset, query.DestinationTable, query.WriteDisposition, query.CreateDisposition,
		query.Priority, request.Configuration.Labels)
	job, err := h.queries.Submit(r.Context(), input)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, jobFromDomain(job))
}

// Unsupported request options are represented as RawMessage so a syntactically
// valid option can never disappear through Go's zero values. Only field names
// are reported; parameter values, labels, and other payload content stay out of
// errors and logs.
//   - QueryRequest: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#QueryRequest
//   - JobConfigurationQuery: https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery
func validateSynchronousQueryOptions(request queryRequest) error {
	// requestId may be ignored for read-only queries because they are
	// nullipotent. BQEMU currently executes the synchronous query to completion,
	// so timeoutMs is validated and accepted while the bounded-wait/unfinished
	// response behavior remains an explicit partial capability.
	// https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#body.request_body.FIELDS.request_id
	// https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#body.request_body.FIELDS.timeout_ms
	if err := validateSynchronousRequestControls(request); err != nil {
		return err
	}
	return rejectPresentQueryOptions(map[string]json.RawMessage{
		"jobTimeoutMs": request.JobTimeoutMs,
		"dryRun":       request.DryRun, "priority": request.Priority, "parameterMode": request.ParameterMode,
		"queryParameters": request.QueryParameters, "labels": request.Labels, "useQueryCache": request.UseQueryCache,
		"maximumBytesBilled": request.MaximumBytesBilled,
	})
}

func validateSynchronousRequestControls(request queryRequest) error {
	if len(request.RequestID) > 36 {
		return fmt.Errorf("%w: requestId must contain at most 36 ASCII characters", domain.ErrInvalid)
	}
	for _, value := range []byte(request.RequestID) {
		if value > 0x7f {
			return fmt.Errorf("%w: requestId must contain at most 36 ASCII characters", domain.ErrInvalid)
		}
	}
	if request.TimeoutMs != nil && *request.TimeoutMs < 0 {
		return fmt.Errorf("%w: timeoutMs must not be negative", domain.ErrInvalid)
	}
	return nil
}

func validateQueryJobOptions(configuration jobConfiguration) error {
	options := map[string]json.RawMessage{
		"configuration.dryRun":       configuration.DryRun,
		"configuration.jobTimeoutMs": configuration.JobTimeoutMs,
	}
	if configuration.Query != nil {
		options["configuration.query.parameterMode"] = configuration.Query.ParameterMode
		options["configuration.query.queryParameters"] = configuration.Query.QueryParameters
		options["configuration.query.useQueryCache"] = configuration.Query.UseQueryCache
		options["configuration.query.maximumBytesBilled"] = configuration.Query.MaximumBytesBilled
	}
	return rejectPresentQueryOptions(options)
}

func rejectPresentQueryOptions(options map[string]json.RawMessage) error {
	unsupported := make([]string, 0, len(options))
	for name, value := range options {
		if rawPresent(value) {
			unsupported = append(unsupported, name)
		}
	}
	if len(unsupported) == 0 {
		return nil
	}
	sort.Strings(unsupported)
	return fmt.Errorf("%w: unsupported query options=%s capability=%s", domain.ErrInvalid,
		strings.Join(unsupported, ","), domain.GapQueryUnsupportedOptionsV1)
}

func queryInputFromWire(projectID, jobID, location, sql string, defaultDataset *datasetReference, destination *tableReference, writeDisposition, createDisposition, priority string, labels *map[string]string) application.QueryInput {
	input := application.QueryInput{
		ProjectID: projectID, JobID: jobID, Location: location, SQL: sql,
		WriteDisposition: domain.WriteDisposition(writeDisposition), CreateDisposition: domain.CreateDisposition(createDisposition),
		Priority: domain.QueryPriority(priority),
	}
	if labels != nil {
		input.Labels = make(map[string]string, len(*labels))
		for key, value := range *labels {
			input.Labels[key] = value
		}
	}
	if defaultDataset != nil {
		input.DefaultProjectID = defaultDataset.ProjectID
		input.DefaultDataset = defaultDataset.DatasetID
	}
	if destination != nil {
		input.Destination = &domain.TableReference{
			ProjectID: destination.ProjectID, DatasetID: destination.DatasetID, TableID: destination.TableID,
		}
	}
	return input
}

func (h *queryHandlers) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := h.queries.Get(r.Context(), domain.JobReference{
		ProjectID: r.PathValue("projectId"), Location: r.URL.Query().Get("location"), JobID: r.PathValue("jobId"),
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, jobFromDomain(job))
}

func (h *queryHandlers) listJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := h.queries.List(r.Context(), r.PathValue("projectId"), r.URL.Query().Get("location"))
	if err != nil {
		writeError(w, err)
		return
	}
	jobs, nextPageToken, err := paginateQueryJobs(r, jobs, r.PathValue("projectId"), r.URL.Query().Get("location"))
	if err != nil {
		writeError(w, err)
		return
	}
	resources := make([]jobResource, len(jobs))
	for i, job := range jobs {
		resources[i] = jobFromDomain(job)
	}
	response := map[string]any{"kind": "bigquery#jobList", "jobs": resources}
	if nextPageToken != "" {
		response["nextPageToken"] = nextPageToken
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *queryHandlers) getQueryResults(w http.ResponseWriter, r *http.Request) {
	job, err := h.queries.Get(r.Context(), domain.JobReference{
		ProjectID: r.PathValue("projectId"), Location: r.URL.Query().Get("location"), JobID: r.PathValue("jobId"),
	})
	if err != nil {
		writeError(w, err)
		return
	}
	start, end, next, err := queryResultPageBounds(r, job)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, queryResponseFromDomain(job, start, end, next))
}
