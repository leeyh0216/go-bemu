package rest

// Official dataset and table methods: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets
// and https://cloud.google.com/bigquery/docs/reference/rest/v2/tables
//
// Path resource identifiers are authoritative. A conflicting identifier in a
// request body is rejected so metadata cannot be persisted under a different
// parent from the resource named on the wire.

import (
	"fmt"
	"net/http"
	"strconv"

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
	dataset, err := s.catalog.CreateDataset(r.Context(), domain.Dataset{
		ProjectID: projectID, ID: request.DatasetReference.DatasetID,
		FriendlyName: request.FriendlyName, Description: request.Description,
		Location: request.Location, Labels: request.Labels,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, datasetFromDomain(dataset, s.baseURLFor(r)))
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
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	projectID, datasetID := r.PathValue("projectId"), r.PathValue("datasetId")
	if (request.TableReference.ProjectID != "" && request.TableReference.ProjectID != projectID) ||
		(request.TableReference.DatasetID != "" && request.TableReference.DatasetID != datasetID) {
		writeError(w, fmt.Errorf("%w: tableReference parent must match the request path", domain.ErrInvalid))
		return
	}
	table := domain.Table{
		ProjectID: projectID, DatasetID: datasetID,
		ID: request.TableReference.TableID, FriendlyName: request.FriendlyName,
		Description: request.Description, Type: request.Type, Schema: fieldsToDomain(request.Schema.Fields),
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

func (s *Server) getTable(w http.ResponseWriter, r *http.Request) {
	table, err := s.catalog.GetTable(r.Context(), r.PathValue("projectId"), r.PathValue("datasetId"), r.PathValue("tableId"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, tableFromDomain(table, s.baseURLFor(r)))
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
		resources[i] = tableFromDomain(table, s.baseURLFor(r))
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
	if err := s.catalog.DeleteTable(r.Context(), r.PathValue("projectId"), r.PathValue("datasetId"), r.PathValue("tableId")); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func parseOptionalInt64(value, field string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	return parseRequiredInt64(value, field)
}

func parseRequiredInt64(value, field string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %s must be an int64 decimal string", domain.ErrInvalid, field)
	}
	return parsed, nil
}
