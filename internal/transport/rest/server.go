package rest

// Official REST resource source: https://cloud.google.com/bigquery/docs/reference/rest/v2
//
// Server is the public HTTP edge. The catalog routes form the independently
// usable baseline; later protocol slices contribute routes and discovery
// resources through Option without adding query/job concerns to the catalog
// application boundary.

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type CatalogUseCases interface {
	CreateProject(context.Context, domain.Project) (domain.Project, error)
	GetProject(context.Context, string) (domain.Project, error)
	ListProjects(context.Context) ([]domain.Project, error)
	DeleteProject(context.Context, string) error
	CreateDataset(context.Context, domain.Dataset) (domain.Dataset, error)
	UpdateDataset(context.Context, string, string, application.DatasetPatch) (domain.Dataset, error)
	GetDataset(context.Context, string, string) (domain.Dataset, error)
	ListDatasets(context.Context, string) ([]domain.Dataset, error)
	DeleteDataset(context.Context, string, string, bool) error
	CreateTable(context.Context, domain.Table) (domain.Table, error)
	UpdateTable(context.Context, string, string, string, application.TablePatch) (domain.Table, error)
	GetTable(context.Context, string, string, string) (domain.Table, error)
	ListTables(context.Context, string, string) ([]domain.Table, error)
	DeleteTable(context.Context, string, string, string) error
}

var _ CatalogUseCases = (*application.CatalogService)(nil)

type routeRegistration func(*http.ServeMux)
type discoveryExtension func(map[string]any)

type Server struct {
	catalog             CatalogUseCases
	readiness           ports.HealthChecker
	baseURL             string
	requestBodyLimits   requestBodyLimits
	routeExtensions     []routeRegistration
	discoveryExtensions []discoveryExtension
}

type Option func(*Server)

func withRoutes(registration routeRegistration) Option {
	return func(server *Server) {
		server.routeExtensions = append(server.routeExtensions, registration)
	}
}

func withDiscovery(extension discoveryExtension) Option {
	return func(server *Server) {
		server.discoveryExtensions = append(server.discoveryExtensions, extension)
	}
}

func NewCatalogServer(catalog CatalogUseCases, readiness ports.HealthChecker, baseURL string, options ...Option) *Server {
	server := &Server{
		catalog: catalog, readiness: readiness, baseURL: strings.TrimRight(baseURL, "/"),
		requestBodyLimits: normalizedRequestBodyLimits(0, 0),
	}
	for _, option := range options {
		option(server)
	}
	return server
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /$discovery/rest", s.discovery)
	mux.HandleFunc("GET /discovery/v1/apis/bigquery/v2/rest", s.discovery)
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /readyz", s.ready)
	s.registerCatalogRoutes(mux)
	for _, register := range s.routeExtensions {
		register(mux)
	}
	return observability.HTTPMiddleware(methodOverrideMiddleware(recoverMiddleware(requestBodyMiddleware(s.requestBodyLimits, mux))))
}

// google-api-java-client may tunnel PATCH through POST with
// X-HTTP-Method-Override. Accept only the one method used by BigQuery Tables.Patch;
// all other override values remain ordinary POST requests and fail routing.
// https://cloud.google.com/apis/docs/system-parameters#http_method_override
func methodOverrideMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.EqualFold(strings.TrimSpace(r.Header.Get("X-HTTP-Method-Override")), http.MethodPatch) {
			slog.InfoContext(r.Context(), "HTTP method override",
				"event", "transport.http.method_override", "original_method", http.MethodPost,
				"effective_method", http.MethodPatch, "path_bytes", len(r.URL.Path),
				"path_digest", observability.Digest([]byte(r.URL.Path)))
			r.Method = http.MethodPatch
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) registerCatalogRoutes(mux *http.ServeMux) {
	// Projects are normally provisioned outside BigQuery. The emulator-only
	// create/get/delete edge provides that missing local lifecycle while project
	// listing remains on the official BigQuery v2 path used by bq and SDKs.
	mux.HandleFunc("POST /bqemu/v1/projects", s.createProject)
	mux.HandleFunc("GET /bqemu/v1/projects", s.listProjects)
	mux.HandleFunc("GET /bqemu/v1/projects/{projectId}", s.getProject)
	mux.HandleFunc("DELETE /bqemu/v1/projects/{projectId}", s.deleteProject)
	mux.HandleFunc("GET /bigquery/v2/projects", s.listProjects)
	mux.HandleFunc("POST /bigquery/v2/projects/{projectId}/datasets", s.createDataset)
	mux.HandleFunc("GET /bigquery/v2/projects/{projectId}/datasets", s.listDatasets)
	mux.HandleFunc("GET /bigquery/v2/projects/{projectId}/datasets/{datasetId}", s.getDataset)
	mux.HandleFunc("PATCH /bigquery/v2/projects/{projectId}/datasets/{datasetId}", s.patchDataset)
	mux.HandleFunc("PUT /bigquery/v2/projects/{projectId}/datasets/{datasetId}", s.updateDataset)
	mux.HandleFunc("DELETE /bigquery/v2/projects/{projectId}/datasets/{datasetId}", s.deleteDataset)
	mux.HandleFunc("POST /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables", s.createTable)
	mux.HandleFunc("GET /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables", s.listTables)
	mux.HandleFunc("GET /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}", s.getTable)
	mux.HandleFunc("PATCH /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}", s.patchTable)
	mux.HandleFunc("PUT /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}", s.updateTable)
	mux.HandleFunc("DELETE /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}", s.deleteTable)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if err := s.readiness.Ping(r.Context()); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
