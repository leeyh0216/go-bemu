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
	"github.com/leeyh0216/go-bemu/internal/contractspec"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

const mediaUploadMultipartFramingLimit int64 = 128 << 10

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

// ViewMetadataUseCases is deliberately separate from CatalogUseCases so the
// established table-only catalog surface remains source-compatible.
type ViewMetadataUseCases interface {
	GetView(context.Context, string, string, string) (domain.View, error)
}

var _ CatalogUseCases = (*application.CatalogService)(nil)

type operationRouteRegistration func() []routeBinding
type discoveryExtension func(map[string]any)

type routeBinding struct {
	operationID string
	handler     http.Handler
}
type Server struct {
	catalog             CatalogUseCases
	queries             QueryUseCases
	readiness           ports.HealthChecker
	baseURL             string
	requestBodyLimits   requestBodyLimits
	mediaUpload         *MediaUploadSupport
	operationRoutes     []operationRouteRegistration
	discoveryExtensions []discoveryExtension
}

type Option func(*Server)

func withOperationRoutes(registration operationRouteRegistration) Option {
	return func(server *Server) {
		server.operationRoutes = append(server.operationRoutes, registration)
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
	registerOperationRoutes(mux, s.coreRouteBindings())
	for _, routes := range s.operationRoutes {
		registerOperationRoutes(mux, routes())
	}
	handler := requestBodyMiddleware(s.requestBodyLimits, s.mediaUploadBodyLimits(), mux)
	handler = methodOverrideMiddleware(handler)
	handler = recoverMiddleware(handler)
	return observability.HTTPMiddleware(handler)
}

func (s *Server) mediaUploadBodyLimits() *requestBodyLimits {
	if s.mediaUpload == nil {
		return nil
	}
	// MaxBytes is the media payload budget. Multipart bodies also carry a
	// bounded JSON metadata part and MIME framing, so applying MaxBytes to the
	// complete HTTP body would reject a legal payload exactly at that limit.
	overhead := int64(mediaUploadMetadataLimit) + mediaUploadMultipartFramingLimit
	maximum := s.mediaUpload.config.MaxBytes
	if maximum <= int64(^uint64(0)>>1)-overhead {
		maximum += overhead
	} else {
		maximum = int64(^uint64(0) >> 1)
	}
	limits := normalizedRequestBodyLimits(maximum, maximum)
	return &limits
}

func registerOperationRoutes(mux *http.ServeMux, bindings []routeBinding) {
	for _, binding := range bindings {
		spec, ok := contractspec.RESTRoute(binding.operationID)
		if !ok {
			panic("REST route has no generated operation specification: " + binding.operationID)
		}
		operationID := binding.operationID
		handler := binding.handler
		mux.Handle(spec.Pattern(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			observability.SetHTTPOperation(w, operationID)
			handler.ServeHTTP(w, r)
		}))
	}
}

func handlerBinding(operationID string, handler http.HandlerFunc) routeBinding {
	return routeBinding{operationID: operationID, handler: handler}
}

func (s *Server) operationIDs() []string {
	bindings := s.coreRouteBindings()
	for _, routes := range s.operationRoutes {
		bindings = append(bindings, routes()...)
	}
	ids := make([]string, len(bindings))
	for index, binding := range bindings {
		ids[index] = binding.operationID
	}
	return ids
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

func (s *Server) coreRouteBindings() []routeBinding {
	// Projects are normally provisioned outside BigQuery. The emulator-only
	// create/get/delete edge provides that missing local lifecycle while project
	// listing remains on the official BigQuery v2 path.
	return []routeBinding{
		handlerBinding("bqemu.discovery.get", s.discovery),
		handlerBinding("bqemu.discovery.googleapis.get", s.discovery),
		handlerBinding("bqemu.health.live", s.health),
		handlerBinding("bqemu.health.ready", s.ready),
		handlerBinding("bqemu.projects.create", s.createProject),
		handlerBinding("bqemu.projects.list", s.listProjects),
		handlerBinding("bqemu.projects.get", s.getProject),
		handlerBinding("bqemu.projects.delete", s.deleteProject),
		handlerBinding("bigquery.projects.list", s.listProjects),
		handlerBinding("bigquery.datasets.insert", s.createDataset),
		handlerBinding("bigquery.datasets.list", s.listDatasets),
		handlerBinding("bigquery.datasets.get", s.getDataset),
		handlerBinding("bigquery.datasets.patch", s.patchDataset),
		handlerBinding("bigquery.datasets.update", s.updateDataset),
		handlerBinding("bigquery.datasets.delete", s.deleteDataset),
		handlerBinding("bigquery.tables.insert", s.createTable),
		handlerBinding("bigquery.tables.list", s.listTables),
		handlerBinding("bigquery.tables.get", s.getTable),
		handlerBinding("bigquery.tables.patch", s.patchTable),
		handlerBinding("bigquery.tables.update", s.updateTable),
		handlerBinding("bigquery.tables.delete", s.deleteTable),
	}
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
