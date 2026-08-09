package rest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
	loadApplication "github.com/leeyh0216/go-bemu/internal/loadjob/application"
	loadDomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type LoadJobUseCases interface {
	Submit(context.Context, loadDomain.JobReference, loadDomain.LoadConfiguration) (*loadDomain.Job, error)
	Get(context.Context, loadDomain.JobReference) (*loadDomain.Job, error)
	List(context.Context, string, string) ([]*loadDomain.Job, error)
}

// MediaUploadProvider is intentionally optional so embedded users retaining
// the source-URI-only load service keep the existing surface.
type MediaUploadProvider interface {
	MediaUploads() loadports.MediaUploadStore
}

var _ LoadJobUseCases = (*loadApplication.Service)(nil)

type combinedJobHandlers struct {
	query    *queryHandlers
	loads    LoadJobUseCases
	media    loadports.MediaUploadStore
	sessions *mediaUploadSessions
}

// NewServerWithLoadJobs installs one jobs.insert dispatcher for query and load
// configurations. NewServer remains the query-only compatibility constructor.
func NewServerWithLoadJobs(catalog CatalogUseCases, queries QueryUseCases, loads LoadJobUseCases, readiness ports.HealthChecker, baseURL string, options ...Option) *Server {
	var media loadports.MediaUploadStore
	if provider, ok := loads.(MediaUploadProvider); ok {
		media = provider.MediaUploads()
	}
	defaults := []Option{withCombinedJobAPI(queries, loads, media), withCapabilitiesAPI(), withConsoleAPI()}
	return NewCatalogServer(catalog, readiness, baseURL, append(defaults, options...)...)
}

func withCombinedJobAPI(queries QueryUseCases, loads LoadJobUseCases, media loadports.MediaUploadStore) Option {
	return func(server *Server) {
		handlers := &combinedJobHandlers{query: &queryHandlers{queries: queries}, loads: loads, media: media, sessions: &mediaUploadSessions{items: make(map[string]*mediaUploadSession)}}
		server.routeExtensions = append(server.routeExtensions, func(mux *http.ServeMux) {
			mux.HandleFunc("POST /bigquery/v2/projects/{projectId}/queries", handlers.query.query)
			mux.HandleFunc("GET /bigquery/v2/projects/{projectId}/queries/{jobId}", handlers.query.getQueryResults)
			mux.HandleFunc("POST /bigquery/v2/projects/{projectId}/jobs", handlers.insertJob)
			mux.HandleFunc("POST /upload/bigquery/v2/projects/{projectId}/jobs", handlers.uploadLoadJob)
			mux.HandleFunc("POST /resumable/upload/bigquery/v2/projects/{projectId}/jobs", handlers.uploadLoadJob)
			mux.HandleFunc("PUT /resumable/upload/bigquery/v2/projects/{projectId}/jobs", handlers.uploadLoadJob)
			mux.HandleFunc("GET /bigquery/v2/projects/{projectId}/jobs", handlers.listJobs)
			mux.HandleFunc("GET /bigquery/v2/projects/{projectId}/jobs/{jobId}", handlers.getJob)
		})
		server.discoveryExtensions = append(server.discoveryExtensions, extendQueryDiscovery)
	}
}

func (h *combinedJobHandlers) insertJob(w http.ResponseWriter, r *http.Request) {
	payload, err := readJobPayload(r)
	if err != nil {
		writeError(w, err)
		return
	}
	var probe combinedJobProbe
	if err := json.Unmarshal(payload, &probe); err != nil {
		writeError(w, fmt.Errorf("%w: invalid JSON body: %v", domain.ErrInvalid, err))
		return
	}
	hasQuery, hasLoad := rawPresent(probe.Configuration.Query), rawPresent(probe.Configuration.Load)
	if hasQuery == hasLoad {
		writeError(w, fmt.Errorf("%w: configuration must contain exactly one of query or load", domain.ErrInvalid))
		return
	}
	if hasQuery {
		h.insertQueryJob(w, r, payload)
		return
	}
	h.insertLoadJob(w, r, payload, probe.Configuration.Load)
}

func (h *combinedJobHandlers) insertQueryJob(w http.ResponseWriter, r *http.Request, payload []byte) {
	var request jobResource
	if err := json.Unmarshal(payload, &request); err != nil {
		writeError(w, fmt.Errorf("%w: invalid query job: %v", domain.ErrInvalid, err))
		return
	}
	if request.JobReference.JobID != "" {
		_, err := h.loads.Get(r.Context(), loadDomain.JobReference{
			ProjectID: r.PathValue("projectId"), Location: request.JobReference.Location, JobID: request.JobReference.JobID,
		})
		if err == nil {
			writeLoadError(w, fmt.Errorf("%w: job ID already identifies a load job", loadDomain.ErrConflict))
			return
		}
		if !errors.Is(err, loadDomain.ErrNotFound) {
			writeLoadError(w, err)
			return
		}
	}
	r.Body = io.NopCloser(bytes.NewReader(payload))
	r.ContentLength = int64(len(payload))
	h.query.insertJob(w, r)
}

func (h *combinedJobHandlers) insertLoadJob(w http.ResponseWriter, r *http.Request, payload, loadPayload []byte) {
	h.insertLoadJobWithSource(w, r, payload, loadPayload, "")
}

func (h *combinedJobHandlers) insertLoadJobWithSource(w http.ResponseWriter, r *http.Request, payload, loadPayload []byte, sourceURI string) {
	var request loadJobRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		writeLoadError(w, fmt.Errorf("%w: invalid load job JSON", loadDomain.ErrInvalid))
		return
	}
	projectID := r.PathValue("projectId")
	if request.JobReference.ProjectID != "" && request.JobReference.ProjectID != projectID {
		writeLoadError(w, fmt.Errorf("%w: route and jobReference projectId differ", loadDomain.ErrInvalid))
		return
	}
	if request.JobReference.JobID != "" {
		if _, err := h.query.queries.Get(r.Context(), domain.JobReference{
			ProjectID: projectID, Location: request.JobReference.Location, JobID: request.JobReference.JobID,
		}); err == nil {
			writeLoadError(w, fmt.Errorf("%w: job ID already identifies a query job", loadDomain.ErrConflict))
			return
		} else if !errors.Is(err, domain.ErrNotFound) {
			writeError(w, err)
			return
		}
	}
	var wire loadConfigurationResource
	if err := json.Unmarshal(loadPayload, &wire); err != nil {
		writeLoadError(w, fmt.Errorf("%w: invalid load configuration JSON", loadDomain.ErrInvalid))
		return
	}
	if sourceURI == "" {
		if err := validatePublicLoadSourceURIs(wire.SourceURIs); err != nil {
			writeLoadError(w, err)
			return
		}
	}
	unsupported, err := unsupportedLoadOptions(loadPayload, wire)
	if err != nil {
		writeLoadError(w, err)
		return
	}
	configuration := loadDomain.LoadConfiguration{
		SourceURIs: append([]string(nil), wire.SourceURIs...),
		Destination: loadDomain.TableReference{
			ProjectID: wire.DestinationTable.ProjectID, DatasetID: wire.DestinationTable.DatasetID, TableID: wire.DestinationTable.TableID,
		},
		SourceFormat: loadDomain.SourceFormat(wire.SourceFormat), WriteDisposition: loadDomain.WriteDisposition(wire.WriteDisposition),
		CreateDisposition: loadDomain.CreateDisposition(wire.CreateDisposition), Autodetect: wire.Autodetect,
		SchemaUpdateOptions: append([]string(nil), wire.SchemaUpdateOptions...), IgnoreUnknownValues: wire.IgnoreUnknownValues,
		MaxBadRecords: wire.MaxBadRecords, UnsupportedOptions: unsupported,
	}
	if request.Configuration.Labels != nil {
		configuration.Labels = make(map[string]string, len(*request.Configuration.Labels))
		for key, value := range *request.Configuration.Labels {
			configuration.Labels[key] = value
		}
	}
	if sourceURI != "" {
		configuration.SourceURIs = []string{sourceURI}
	}
	if wire.Schema != nil {
		configuration.Schema = loadFieldsFromWire(wire.Schema.Fields)
	}
	job, err := h.loads.Submit(r.Context(), loadDomain.JobReference{
		ProjectID: projectID, Location: request.JobReference.Location, JobID: request.JobReference.JobID,
	}, configuration)
	if err != nil {
		writeLoadError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, loadJobFromDomain(job))
}

// validatePublicLoadSourceURIs runs before a job is persisted. Media uploads
// receive their private source URI only after the server has committed bytes,
// so this public REST boundary deliberately accepts gs:// and nothing else.
func validatePublicLoadSourceURIs(uris []string) error {
	for _, rawURI := range uris {
		parsed, err := url.Parse(rawURI)
		if err != nil || parsed.Scheme == "" {
			return fmt.Errorf("%w: load source URI must use gs://", loadDomain.ErrInvalid)
		}
		if !strings.EqualFold(parsed.Scheme, "gs") {
			return fmt.Errorf("%w: public load source URI scheme %q", loadDomain.ErrUnsupported, parsed.Scheme)
		}
		if parsed.Host == "" || parsed.Path == "" || parsed.Path == "/" {
			return fmt.Errorf("%w: gs:// load source requires bucket and object", loadDomain.ErrInvalid)
		}
	}
	return nil
}

func (h *combinedJobHandlers) getJob(w http.ResponseWriter, r *http.Request) {
	reference := loadDomain.JobReference{
		ProjectID: r.PathValue("projectId"), Location: r.URL.Query().Get("location"), JobID: r.PathValue("jobId"),
	}
	job, err := h.loads.Get(r.Context(), reference)
	if err == nil {
		writeJSON(w, http.StatusOK, loadJobFromDomain(job))
		return
	}
	if !errors.Is(err, loadDomain.ErrNotFound) {
		writeLoadError(w, err)
		return
	}
	h.query.getJob(w, r)
}

func (h *combinedJobHandlers) listJobs(w http.ResponseWriter, r *http.Request) {
	projectID, location := r.PathValue("projectId"), r.URL.Query().Get("location")
	queryJobs, err := h.query.queries.List(r.Context(), projectID, location)
	if err != nil {
		writeError(w, err)
		return
	}
	loadJobs, err := h.loads.List(r.Context(), projectID, location)
	if err != nil {
		writeLoadError(w, err)
		return
	}
	resources := make([]any, 0, len(queryJobs)+len(loadJobs))
	for _, job := range queryJobs {
		if location == "" || strings.EqualFold(job.Reference.Location, location) {
			resources = append(resources, jobFromDomain(job))
		}
	}
	for _, job := range loadJobs {
		resources = append(resources, loadJobFromDomain(job))
	}
	// jobs.list returns the most recently created jobs first. The full
	// location/type-qualified identity is the deterministic tie-break because
	// query and load repositories are currently independent.
	// https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/list
	sort.SliceStable(resources, func(i, j int) bool { return combinedJobLess(resources[i], resources[j]) })
	resources, nextPageToken, err := paginateCombinedJobs(r, resources, projectID, location)
	if err != nil {
		writeError(w, err)
		return
	}
	response := map[string]any{"kind": "bigquery#jobList", "jobs": resources}
	if nextPageToken != "" {
		response["nextPageToken"] = nextPageToken
	}
	writeJSON(w, http.StatusOK, response)
}

func readJobPayload(r *http.Request) ([]byte, error) {
	return readJSONBody(r)
}

func rawPresent(value json.RawMessage) bool {
	trimmed := bytes.TrimSpace(value)
	return len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null"))
}

// The pinned Spark connector selects FormatOptions.parquet(), and the pinned
// Java client consequently serializes parquetOptions even when every option is
// at its default. The bq CLI likewise serializes several neutral load fields.
// Keep those shapes explicit here: accepting arbitrary fields would hide a
// future client contract change and could silently change loaded data.
//   - https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/719817782a214b8ca72be520870013a3e0253d92/spark-bigquery-connector-common/src/main/java/com/google/cloud/spark/bigquery/SparkBigQueryConfig.java#L1312-L1318
//   - https://github.com/googleapis/java-bigquery/blob/v2.60.0/google-cloud-bigquery/src/main/java/com/google/cloud/bigquery/LoadJobConfiguration.java#L922-L925
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad
//   - https://cloud.google.com/bigquery/docs/reference/bq-cli-reference#bq_load
func unsupportedLoadOptions(payload []byte, wire loadConfigurationResource) ([]string, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return nil, fmt.Errorf("%w: load configuration must be an object", loadDomain.ErrInvalid)
	}
	allowed := map[string]struct{}{
		"sourceUris": {}, "destinationTable": {}, "schema": {}, "sourceFormat": {},
		"writeDisposition": {}, "createDisposition": {}, "autodetect": {},
		"schemaUpdateOptions": {}, "ignoreUnknownValues": {}, "maxBadRecords": {},
		"parquetOptions": {}, "decimalTargetTypes": {}, "nullMarkers": {},
		"projectionFields": {}, "timestampTargetPrecision": {},
	}
	unsupported := make([]string, 0)
	for field, value := range fields {
		if _, ok := allowed[field]; !ok {
			unsupported = append(unsupported, unsupportedLoadOption(field, value))
		}
	}
	if value, ok := fields["parquetOptions"]; ok && rawJSONNull(value) {
		unsupported = append(unsupported, unsupportedLoadOption("parquetOptions", value))
	} else if ok {
		var parquetFields map[string]json.RawMessage
		if err := json.Unmarshal(value, &parquetFields); err != nil {
			return nil, fmt.Errorf("%w: parquetOptions must be an object", loadDomain.ErrInvalid)
		}
		for field, fieldValue := range parquetFields {
			switch field {
			case "enableListInference":
				if wire.ParquetOptions == nil || wire.ParquetOptions.EnableListInference == nil {
					unsupported = append(unsupported, unsupportedLoadOption("parquetOptions."+field, fieldValue))
				}
			case "enumAsString":
				if wire.ParquetOptions == nil || wire.ParquetOptions.EnumAsString == nil || *wire.ParquetOptions.EnumAsString {
					unsupported = append(unsupported, unsupportedLoadOption("parquetOptions."+field, fieldValue))
				}
			case "mapTargetType":
				unsupported = append(unsupported, unsupportedLoadOption("parquetOptions."+field, fieldValue))
			default:
				unsupported = append(unsupported, unsupportedLoadOption("parquetOptions."+field, fieldValue))
			}
		}
	}
	// decimalTargetTypes has a data-dependent default, so only the CLI's
	// explicit JSON null is equivalent to omission for the supported slice.
	if value, ok := fields["decimalTargetTypes"]; ok && !rawJSONNull(value) {
		unsupported = append(unsupported, unsupportedLoadOption("decimalTargetTypes", value))
	}
	// These CLI defaults are accepted only as typed empty arrays. JSON null and
	// non-empty arrays are intentionally visible unsupported capabilities.
	for _, option := range []struct {
		name   string
		length int
	}{
		{name: "nullMarkers", length: len(wire.NullMarkers)},
		{name: "projectionFields", length: len(wire.ProjectionFields)},
		{name: "timestampTargetPrecision", length: len(wire.TimestampTargetPrecision)},
	} {
		if value, ok := fields[option.name]; ok && (rawJSONNull(value) || option.length != 0) {
			unsupported = append(unsupported, unsupportedLoadOption(option.name, value))
		}
	}
	sort.Strings(unsupported)
	return unsupported, nil
}

func rawJSONNull(value json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(value), []byte("null"))
}

func unsupportedLoadOption(name string, value json.RawMessage) string {
	digest := sha256.Sum256(value)
	return fmt.Sprintf("%s:%x", name, digest)
}

func writeLoadError(w http.ResponseWriter, err error) {
	status, reason := http.StatusInternalServerError, "backendError"
	switch {
	case errors.Is(err, loadDomain.ErrInvalid):
		status, reason = http.StatusBadRequest, "invalid"
	case errors.Is(err, loadDomain.ErrNotFound):
		status, reason = http.StatusNotFound, "notFound"
	case errors.Is(err, loadDomain.ErrConflict):
		status, reason = http.StatusConflict, "duplicate"
	case errors.Is(err, loadDomain.ErrPrecondition):
		status, reason = http.StatusPreconditionFailed, "conditionNotMet"
	case errors.Is(err, loadDomain.ErrUnsupported):
		status, reason = http.StatusNotImplemented, "notImplemented"
	}
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"code": status, "message": err.Error(),
			"errors": []errorProto{{Reason: reason, Message: err.Error()}},
		},
	})
}

func combinedJobIdentity(resource any) string {
	switch typed := resource.(type) {
	case jobResource:
		return typed.JobReference.JobID + "\x00" + strings.ToUpper(typed.JobReference.Location) + "\x00QUERY"
	case loadJobResource:
		return typed.JobReference.JobID + "\x00" + strings.ToUpper(typed.JobReference.Location) + "\x00LOAD"
	default:
		return ""
	}
}

func combinedJobCreationMillis(resource any) int64 {
	var raw string
	switch typed := resource.(type) {
	case jobResource:
		raw = typed.Statistics.CreationTime
	case loadJobResource:
		raw = typed.Statistics.CreationTime
	}
	value, _ := strconv.ParseInt(raw, 10, 64)
	return value
}

func combinedJobLess(left, right any) bool {
	leftCreated, rightCreated := combinedJobCreationMillis(left), combinedJobCreationMillis(right)
	if leftCreated != rightCreated {
		return leftCreated > rightCreated
	}
	return combinedJobIdentity(left) < combinedJobIdentity(right)
}

func combinedJobCursor(resource any) string {
	return strconv.FormatInt(combinedJobCreationMillis(resource), 10) + "\x00" + combinedJobIdentity(resource)
}
