package rest

// Official dataset and table methods: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets
// and https://cloud.google.com/bigquery/docs/reference/rest/v2/tables
//
// Path resource identifiers are authoritative. A conflicting identifier in a
// request body is rejected so metadata cannot be persisted under a different
// parent from the resource named on the wire.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/application"
	"github.com/leeyh0216/go-bemu/internal/domain"
)

func (s *Server) createProject(w http.ResponseWriter, r *http.Request) {
	var request projectCreateRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	project, err := s.catalog.CreateProject(r.Context(), domain.Project{
		ID: request.ProjectID, FriendlyName: request.FriendlyName, Description: request.Description,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, projectFromDomain(project))
}

func (s *Server) getProject(w http.ResponseWriter, r *http.Request) {
	project, err := s.catalog.GetProject(r.Context(), r.PathValue("projectId"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, projectFromDomain(project))
}

func (s *Server) listProjects(w http.ResponseWriter, r *http.Request) {
	projects, err := s.catalog.ListProjects(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	start, end, nextPageToken, err := pageBounds(r, len(projects))
	if err != nil {
		writeError(w, err)
		return
	}
	resources := make([]projectResource, end-start)
	for i, project := range projects[start:end] {
		resources[i] = projectFromDomain(project)
	}
	response := map[string]any{
		"kind": "bigquery#projectList", "projects": resources, "totalItems": len(projects),
	}
	if nextPageToken != "" {
		response["nextPageToken"] = nextPageToken
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) deleteProject(w http.ResponseWriter, r *http.Request) {
	if err := s.catalog.DeleteProject(r.Context(), r.PathValue("projectId")); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) createDataset(w http.ResponseWriter, r *http.Request) {
	var request datasetResource
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	projectID := r.PathValue("projectId")
	if request.DatasetReference.ProjectID != "" && request.DatasetReference.ProjectID != projectID {
		writeError(w, fmt.Errorf("%w: datasetReference.projectId must match path projectId", domain.ErrInvalid))
		return
	}
	defaultTableExpiration, err := parseOptionalInt64Pointer(request.DefaultTableExpirationMs, "defaultTableExpirationMs")
	if err != nil {
		writeError(w, err)
		return
	}
	defaultPartitionExpiration, err := parseOptionalInt64Pointer(request.DefaultPartitionExpirationMs, "defaultPartitionExpirationMs")
	if err != nil {
		writeError(w, err)
		return
	}
	dataset, err := s.catalog.CreateDataset(r.Context(), domain.Dataset{
		ProjectID: projectID, ID: request.DatasetReference.DatasetID,
		FriendlyName: request.FriendlyName, Description: request.Description,
		Location: request.Location, Labels: request.Labels,
		DefaultTableExpirationMs: defaultTableExpiration, DefaultPartitionExpirationMs: defaultPartitionExpiration,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, datasetFromDomain(dataset, s.baseURLFor(r)))
}

func (s *Server) patchDataset(w http.ResponseWriter, r *http.Request) {
	s.mutateDataset(w, r, false)
}

func (s *Server) updateDataset(w http.ResponseWriter, r *http.Request) {
	s.mutateDataset(w, r, true)
}

func (s *Server) mutateDataset(w http.ResponseWriter, r *http.Request, replace bool) {
	var request datasetResource
	fields, err := decodeJSONWithFields(r, &request)
	if err != nil {
		writeError(w, err)
		return
	}
	projectID, datasetID := r.PathValue("projectId"), r.PathValue("datasetId")
	current, err := s.catalog.GetDataset(r.Context(), projectID, datasetID)
	if err != nil {
		writeError(w, err)
		return
	}
	if err := checkIfMatch(r, metadataETag(current)); err != nil {
		writeError(w, err)
		return
	}
	if _, present := fields["datasetReference"]; present &&
		((request.DatasetReference.ProjectID != "" && request.DatasetReference.ProjectID != projectID) ||
			(request.DatasetReference.DatasetID != "" && request.DatasetReference.DatasetID != datasetID)) {
		writeError(w, fmt.Errorf("%w: datasetReference must match the request path", domain.ErrInvalid))
		return
	}
	if _, present := fields["location"]; present && request.Location != "" && request.Location != current.Location {
		writeError(w, fmt.Errorf("%w: dataset location is immutable", domain.ErrInvalid))
		return
	}
	patch := application.DatasetPatch{}
	patch.FriendlyName = application.PatchValue[string]{Set: replace || hasField(fields, "friendlyName"), Value: request.FriendlyName}
	patch.Description = application.PatchValue[string]{Set: replace || hasField(fields, "description"), Value: request.Description}
	patch.Labels = application.PatchValue[map[string]string]{Set: replace || hasField(fields, "labels"), Value: request.Labels}
	if replace || hasField(fields, "defaultTableExpirationMs") {
		value, parseErr := parseOptionalInt64Pointer(request.DefaultTableExpirationMs, "defaultTableExpirationMs")
		if parseErr != nil {
			writeError(w, parseErr)
			return
		}
		patch.DefaultTableExpirationMs = application.PatchValue[*int64]{Set: true, Value: value}
	}
	if replace || hasField(fields, "defaultPartitionExpirationMs") {
		value, parseErr := parseOptionalInt64Pointer(request.DefaultPartitionExpirationMs, "defaultPartitionExpirationMs")
		if parseErr != nil {
			writeError(w, parseErr)
			return
		}
		patch.DefaultPartitionExpirationMs = application.PatchValue[*int64]{Set: true, Value: value}
	}
	updated, err := s.catalog.UpdateDataset(r.Context(), projectID, datasetID, patch)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, datasetFromDomain(updated, s.baseURLFor(r)))
}

func rejectTableDefaultRoundingMode(present bool) error {
	if !present {
		return nil
	}
	return fmt.Errorf("%w: capability=%s table defaultRoundingMode inheritance is not implemented", domain.ErrUnsupported, domain.GapTableDefaultRoundingV1)
}

func (s *Server) getDataset(w http.ResponseWriter, r *http.Request) {
	dataset, err := s.catalog.GetDataset(r.Context(), r.PathValue("projectId"), r.PathValue("datasetId"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, datasetFromDomain(dataset, s.baseURLFor(r)))
}

func (s *Server) listDatasets(w http.ResponseWriter, r *http.Request) {
	datasets, err := s.catalog.ListDatasets(r.Context(), r.PathValue("projectId"))
	if err != nil {
		writeError(w, err)
		return
	}
	all, err := optionalBoolQuery(r, "all")
	if err != nil {
		writeError(w, err)
		return
	}
	if !all {
		visible := datasets[:0]
		for _, dataset := range datasets {
			if !dataset.Hidden {
				visible = append(visible, dataset)
			}
		}
		datasets = visible
	}
	start, end, nextPageToken, err := pageBounds(r, len(datasets))
	if err != nil {
		writeError(w, err)
		return
	}
	resources := make([]datasetResource, end-start)
	for i, dataset := range datasets[start:end] {
		resources[i] = datasetFromDomain(dataset, s.baseURLFor(r))
	}
	response := map[string]any{"kind": "bigquery#datasetList", "datasets": resources}
	if nextPageToken != "" {
		response["nextPageToken"] = nextPageToken
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) deleteDataset(w http.ResponseWriter, r *http.Request) {
	deleteContents, err := optionalBoolQuery(r, "deleteContents")
	if err != nil {
		writeError(w, err)
		return
	}
	if err := s.catalog.DeleteDataset(r.Context(), r.PathValue("projectId"), r.PathValue("datasetId"), deleteContents); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) createTable(w http.ResponseWriter, r *http.Request) {
	var request tableResource
	fields, err := decodeJSONWithFields(r, &request)
	if err != nil {
		writeError(w, err)
		return
	}
	if err := rejectTableDefaultRoundingMode(hasField(fields, "defaultRoundingMode")); err != nil {
		writeError(w, err)
		return
	}
	projectID, datasetID := r.PathValue("projectId"), r.PathValue("datasetId")
	if (request.TableReference.ProjectID != "" && request.TableReference.ProjectID != projectID) ||
		(request.TableReference.DatasetID != "" && request.TableReference.DatasetID != datasetID) {
		writeError(w, fmt.Errorf("%w: tableReference parent must match the request path", domain.ErrInvalid))
		return
	}
	if request.View != nil {
		s.createLogicalView(w, r, request, fields, projectID, datasetID)
		return
	}
	table := domain.Table{
		ProjectID: projectID, DatasetID: datasetID,
		ID: request.TableReference.TableID, FriendlyName: request.FriendlyName,
		Description: request.Description, Labels: request.Labels, Type: request.Type, Schema: fieldsToDomain(request.Schema.Fields),
	}
	if request.ExpirationTime != "" {
		expiration, err := parseMillis(request.ExpirationTime, "expirationTime")
		if err != nil {
			writeError(w, err)
			return
		}
		table.ExpirationTime = &expiration
	}
	if request.TimePartitioning != nil {
		expiration, err := parseOptionalInt64(request.TimePartitioning.ExpirationMs, "timePartitioning.expirationMs")
		if err != nil {
			writeError(w, err)
			return
		}
		table.TimePartitioning = &domain.TimePartitioning{
			Type: request.TimePartitioning.Type, Field: request.TimePartitioning.Field, ExpirationMs: expiration,
		}
	}
	if request.RangePartitioning != nil {
		start, err := parseRequiredInt64(request.RangePartitioning.Range.Start, "rangePartitioning.range.start")
		if err != nil {
			writeError(w, err)
			return
		}
		end, err := parseRequiredInt64(request.RangePartitioning.Range.End, "rangePartitioning.range.end")
		if err != nil {
			writeError(w, err)
			return
		}
		interval, err := parseRequiredInt64(request.RangePartitioning.Range.Interval, "rangePartitioning.range.interval")
		if err != nil {
			writeError(w, err)
			return
		}
		table.RangePartitioning = &domain.RangePartitioning{
			Field: request.RangePartitioning.Field, Range: domain.Range{Start: start, End: end, Interval: interval},
		}
	}
	if request.Clustering != nil {
		table.ClusteringFields = append([]string(nil), request.Clustering.Fields...)
	}
	created, err := s.catalog.CreateTable(r.Context(), table)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, tableFromDomain(created, s.baseURLFor(r)))
}

func (s *Server) createLogicalView(w http.ResponseWriter, r *http.Request, request tableResource, fields map[string]json.RawMessage, projectID, datasetID string) {
	if s.queries == nil {
		writeError(w, fmt.Errorf("%w: logical view DDL requires the query service", domain.ErrUnsupported))
		return
	}
	if request.View.UseLegacySQL {
		writeError(w, fmt.Errorf("%w: legacy SQL views are not supported", domain.ErrInvalid))
		return
	}
	if request.Type != "" && request.Type != "VIEW" {
		writeError(w, fmt.Errorf("%w: table type conflicts with view resource", domain.ErrInvalid))
		return
	}
	for _, field := range []string{"schema", "expirationTime", "timePartitioning", "rangePartitioning", "clustering"} {
		if hasField(fields, field) {
			writeError(w, fmt.Errorf("%w: %s is derived or unsupported for logical views", domain.ErrInvalid, field))
			return
		}
	}
	reference := domain.TableReference{ProjectID: projectID, DatasetID: datasetID, TableID: request.TableReference.TableID}
	if reference.ProjectID == "" || reference.DatasetID == "" || reference.TableID == "" || strings.ContainsAny(reference.ProjectID+reference.DatasetID+reference.TableID, "`\x00") {
		writeError(w, fmt.Errorf("%w: view tableReference is invalid", domain.ErrInvalid))
		return
	}
	if strings.TrimSpace(request.View.Query) == "" {
		writeError(w, fmt.Errorf("%w: view.query is required", domain.ErrInvalid))
		return
	}
	// This only serializes a validated resource identity. The view query itself
	// crosses the normal official GoogleSQL parser/analyzer admission path.
	sql := fmt.Sprintf("CREATE VIEW `%s.%s.%s` AS %s", projectID, datasetID, reference.TableID, request.View.Query)
	if _, err := s.queries.RunSync(r.Context(), queryInputFromWire(projectID, "", "", sql, nil, nil, "", "", "", nil)); err != nil {
		writeError(w, err)
		return
	}
	table, err := s.catalog.GetTable(r.Context(), projectID, datasetID, reference.TableID)
	if err != nil {
		writeError(w, err)
		return
	}
	resource, err := s.tableResource(r.Context(), table, s.baseURLFor(r))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resource)
}

func (s *Server) patchTable(w http.ResponseWriter, r *http.Request) {
	s.mutateTable(w, r, false)
}

func (s *Server) updateTable(w http.ResponseWriter, r *http.Request) {
	s.mutateTable(w, r, true)
}

func (s *Server) mutateTable(w http.ResponseWriter, r *http.Request, replace bool) {
	var request tableResource
	fields, err := decodeJSONWithFields(r, &request)
	if err != nil {
		writeError(w, err)
		return
	}
	if err := rejectTableDefaultRoundingMode(hasField(fields, "defaultRoundingMode")); err != nil {
		writeError(w, err)
		return
	}
	projectID, datasetID, tableID := r.PathValue("projectId"), r.PathValue("datasetId"), r.PathValue("tableId")
	current, err := s.catalog.GetTable(r.Context(), projectID, datasetID, tableID)
	if err != nil {
		writeError(w, err)
		return
	}
	if err := checkIfMatch(r, metadataETag(current)); err != nil {
		writeError(w, err)
		return
	}
	if current.Type == "VIEW" {
		writeError(w, fmt.Errorf("%w: logical-view updates are not supported; use CREATE OR REPLACE VIEW", domain.ErrUnsupported))
		return
	}
	if _, present := fields["tableReference"]; present &&
		((request.TableReference.ProjectID != "" && request.TableReference.ProjectID != projectID) ||
			(request.TableReference.DatasetID != "" && request.TableReference.DatasetID != datasetID) ||
			(request.TableReference.TableID != "" && request.TableReference.TableID != tableID)) {
		writeError(w, fmt.Errorf("%w: tableReference must match the request path", domain.ErrInvalid))
		return
	}
	if _, present := fields["type"]; present && request.Type != "" && request.Type != current.Type {
		writeError(w, fmt.Errorf("%w: table type is immutable", domain.ErrInvalid))
		return
	}
	if _, present := fields["location"]; present && request.Location != "" && request.Location != current.Location {
		writeError(w, fmt.Errorf("%w: table location is immutable", domain.ErrInvalid))
		return
	}
	patch := application.TablePatch{
		FriendlyName: application.PatchValue[string]{Set: replace || hasField(fields, "friendlyName"), Value: request.FriendlyName},
		Description:  application.PatchValue[string]{Set: replace || hasField(fields, "description"), Value: request.Description},
		Labels:       application.PatchValue[map[string]string]{Set: replace || hasField(fields, "labels"), Value: request.Labels},
	}
	if replace || hasField(fields, "expirationTime") {
		var expiration *time.Time
		if request.ExpirationTime != "" {
			parsed, parseErr := parseMillis(request.ExpirationTime, "expirationTime")
			if parseErr != nil {
				writeError(w, parseErr)
				return
			}
			expiration = &parsed
		}
		patch.ExpirationTime = application.PatchValue[*time.Time]{Set: true, Value: expiration}
	}
	if replace || hasField(fields, "schema") {
		patch.Schema = application.PatchValue[[]domain.Field]{Set: true, Value: fieldsToDomain(request.Schema.Fields)}
	}
	updated, err := s.catalog.UpdateTable(r.Context(), projectID, datasetID, tableID, patch)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, tableFromDomain(updated, s.baseURLFor(r)))
}

func (s *Server) getTable(w http.ResponseWriter, r *http.Request) {
	table, err := s.catalog.GetTable(r.Context(), r.PathValue("projectId"), r.PathValue("datasetId"), r.PathValue("tableId"))
	if err != nil {
		writeError(w, err)
		return
	}
	resource, err := s.tableResource(r.Context(), table, s.baseURLFor(r))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resource)
}

func (s *Server) listTables(w http.ResponseWriter, r *http.Request) {
	tables, err := s.catalog.ListTables(r.Context(), r.PathValue("projectId"), r.PathValue("datasetId"))
	if err != nil {
		writeError(w, err)
		return
	}
	start, end, nextPageToken, err := pageBounds(r, len(tables))
	if err != nil {
		writeError(w, err)
		return
	}
	resources := make([]tableResource, end-start)
	for i, table := range tables[start:end] {
		resource, resourceErr := s.tableResource(r.Context(), table, s.baseURLFor(r))
		if resourceErr != nil {
			writeError(w, resourceErr)
			return
		}
		resources[i] = resource
	}
	response := map[string]any{
		"kind": "bigquery#tableList", "tables": resources, "totalItems": len(tables),
	}
	if nextPageToken != "" {
		response["nextPageToken"] = nextPageToken
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) deleteTable(w http.ResponseWriter, r *http.Request) {
	table, err := s.catalog.GetTable(r.Context(), r.PathValue("projectId"), r.PathValue("datasetId"), r.PathValue("tableId"))
	if err != nil {
		writeError(w, err)
		return
	}
	if table.Type == "VIEW" {
		if s.queries == nil {
			writeError(w, fmt.Errorf("%w: logical view DDL requires the query service", domain.ErrUnsupported))
			return
		}
		sql := fmt.Sprintf("DROP VIEW `%s.%s.%s`", table.ProjectID, table.DatasetID, table.ID)
		if _, err := s.queries.RunSync(r.Context(), queryInputFromWire(table.ProjectID, "", "", sql, nil, nil, "", "", "", nil)); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err := s.catalog.DeleteTable(r.Context(), r.PathValue("projectId"), r.PathValue("datasetId"), r.PathValue("tableId")); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) tableResource(ctx context.Context, table domain.Table, baseURL string) (tableResource, error) {
	if table.Type != "VIEW" {
		return tableFromDomain(table, baseURL), nil
	}
	views, ok := s.catalog.(ViewMetadataUseCases)
	if !ok {
		return tableResource{}, fmt.Errorf("%w: logical view metadata is not configured", domain.ErrUnsupported)
	}
	view, err := views.GetView(ctx, table.ProjectID, table.DatasetID, table.ID)
	if err != nil {
		return tableResource{}, err
	}
	return tableFromLogicalView(view, baseURL), nil
}

func parseOptionalInt64(value, field string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	return parseRequiredInt64(value, field)
}

func parseOptionalInt64Pointer(value, field string) (*int64, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := parseRequiredInt64(value, field)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func parseRequiredInt64(value, field string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %s must be an int64 decimal string", domain.ErrInvalid, field)
	}
	return parsed, nil
}

func parseMillis(value, field string) (time.Time, error) {
	milliseconds, err := parseRequiredInt64(value, field)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(milliseconds).UTC(), nil
}

func hasField(fields map[string]json.RawMessage, name string) bool {
	if _, present := fields[name]; present {
		return true
	}
	for field := range fields {
		if strings.EqualFold(field, name) {
			return true
		}
	}
	return false
}

func checkIfMatch(r *http.Request, current string) error {
	condition := strings.TrimSpace(r.Header.Get("If-Match"))
	if condition == "" || condition == "*" {
		return nil
	}
	condition = strings.Trim(condition, `"`)
	if condition != current {
		return fmt.Errorf("%w: metadata ETag does not match; fix_hint=GET the resource and retry with its latest etag", domain.ErrPrecondition)
	}
	return nil
}
