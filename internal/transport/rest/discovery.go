package rest

// Official discovery format and BigQuery v2 surface:
//   - https://developers.google.com/discovery/v1/reference/apis/getRest
//   - https://bigquery.googleapis.com/$discovery/rest?version=v2
//
// The catalog server advertises only methods it installs. Optional protocol
// slices add their schemas/resources to a fresh document per request, avoiding
// shared mutable discovery state.

import (
	"net/http"
	"strings"
)

func (s *Server) discovery(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, discoveryDocument(s.baseURLFor(r), s.discoveryExtensions...))
}

func (s *Server) baseURLFor(r *http.Request) string {
	if s.baseURL != "" {
		return s.baseURL
	}
	scheme := r.Header.Get("X-Forwarded-Proto")
	if scheme == "" {
		scheme = "http"
		if r.TLS != nil {
			scheme = "https"
		}
	}
	return strings.TrimRight(scheme+"://"+r.Host, "/")
}

func discoveryDocument(baseURL string, extensions ...discoveryExtension) map[string]any {
	document := map[string]any{
		"kind": "discovery#restDescription", "discoveryVersion": "v1",
		"id": "bigquery:v2", "name": "bigquery", "version": "v2",
		"revision": "20260808", "title": "BigQuery API (BQEMU compatible subset)",
		"description": "BigQuery-compatible local emulator subset.",
		"protocol":    "rest", "baseUrl": baseURL + "/bigquery/v2/",
		"basePath": "/bigquery/v2/", "rootUrl": baseURL + "/", "servicePath": "bigquery/v2/",
		"batchPath": "batch/bigquery/v2",
		"parameters": map[string]any{
			"alt": map[string]any{
				"type": "string", "location": "query", "default": "json", "enum": []string{"json"},
			},
			"fields": discoveryQueryParameter("string"), "prettyPrint": discoveryQueryParameter("boolean"),
			"quotaUser": discoveryQueryParameter("string"),
		},
		"schemas": map[string]any{
			"Dataset": discoveryObjectSchema("Dataset"), "DatasetList": discoveryObjectSchema("DatasetList"),
			"Table": discoveryObjectSchema("Table"), "TableList": discoveryObjectSchema("TableList"),
			"ProjectList": discoveryObjectSchema("ProjectList"), "Empty": discoveryObjectSchema("Empty"),
		},
		"resources": catalogDiscoveryResources(),
	}
	for _, extend := range extensions {
		extend(document)
	}
	return document
}

func catalogDiscoveryResources() map[string]any {
	projectDataset := map[string]any{
		"projectId": discoveryPathParameter("projectId"), "datasetId": discoveryPathParameter("datasetId"),
	}
	projectDatasetTable := map[string]any{
		"projectId": discoveryPathParameter("projectId"), "datasetId": discoveryPathParameter("datasetId"),
		"tableId": discoveryPathParameter("tableId"),
	}
	tableGet := map[string]any{
		"projectId": discoveryPathParameter("projectId"), "datasetId": discoveryPathParameter("datasetId"),
		"tableId": discoveryPathParameter("tableId"), "selectedFields": discoveryQueryParameter("string"),
		"view": discoveryQueryParameter("string"),
	}
	datasetMutation := map[string]any{
		"projectId": discoveryPathParameter("projectId"), "datasetId": discoveryPathParameter("datasetId"),
		"accessPolicyVersion": discoveryQueryParameter("integer"), "updateMode": discoveryQueryParameter("string"),
	}
	tableMutation := map[string]any{
		"projectId": discoveryPathParameter("projectId"), "datasetId": discoveryPathParameter("datasetId"),
		"tableId": discoveryPathParameter("tableId"), "autodetect_schema": discoveryQueryParameter("boolean"),
	}
	return map[string]any{
		"projects": map[string]any{"methods": map[string]any{
			"list": discoveryMethod("bigquery.projects.list", "GET", "projects", "", "ProjectList", map[string]any{
				"maxResults": discoveryQueryParameter("integer"), "pageToken": discoveryQueryParameter("string"),
			}),
		}},
		"datasets": map[string]any{"methods": map[string]any{
			"insert": discoveryMethod("bigquery.datasets.insert", "POST", "projects/{projectId}/datasets", "Dataset", "Dataset", map[string]any{
				"projectId": discoveryPathParameter("projectId"), "accessPolicyVersion": discoveryQueryParameter("integer"),
			}, "projectId"),
			"get": discoveryMethod("bigquery.datasets.get", "GET", "projects/{projectId}/datasets/{datasetId}", "", "Dataset", map[string]any{
				"projectId": discoveryPathParameter("projectId"), "datasetId": discoveryPathParameter("datasetId"),
				"accessPolicyVersion": discoveryQueryParameter("integer"), "datasetView": discoveryQueryParameter("string"),
			}, "projectId", "datasetId"),
			"patch":  discoveryMethod("bigquery.datasets.patch", "PATCH", "projects/{projectId}/datasets/{datasetId}", "Dataset", "Dataset", datasetMutation, "projectId", "datasetId"),
			"update": discoveryMethod("bigquery.datasets.update", "PUT", "projects/{projectId}/datasets/{datasetId}", "Dataset", "Dataset", datasetMutation, "projectId", "datasetId"),
			"list": discoveryMethod("bigquery.datasets.list", "GET", "projects/{projectId}/datasets", "", "DatasetList", map[string]any{
				"projectId": discoveryPathParameter("projectId"), "maxResults": discoveryQueryParameter("integer"),
				"pageToken": discoveryQueryParameter("string"), "all": discoveryQueryParameter("boolean"),
				"filter": discoveryQueryParameter("string"),
			}, "projectId"),
			"delete": discoveryMethod("bigquery.datasets.delete", "DELETE", "projects/{projectId}/datasets/{datasetId}", "", "Empty", map[string]any{
				"projectId": discoveryPathParameter("projectId"), "datasetId": discoveryPathParameter("datasetId"),
				"deleteContents": discoveryQueryParameter("boolean"),
			}, "projectId", "datasetId"),
		}},
		"tables": map[string]any{"methods": map[string]any{
			"insert": discoveryMethod("bigquery.tables.insert", "POST", "projects/{projectId}/datasets/{datasetId}/tables", "Table", "Table", projectDataset, "projectId", "datasetId"),
			"get":    discoveryMethod("bigquery.tables.get", "GET", "projects/{projectId}/datasets/{datasetId}/tables/{tableId}", "", "Table", tableGet, "projectId", "datasetId", "tableId"),
			"patch":  discoveryMethod("bigquery.tables.patch", "PATCH", "projects/{projectId}/datasets/{datasetId}/tables/{tableId}", "Table", "Table", tableMutation, "projectId", "datasetId", "tableId"),
			"update": discoveryMethod("bigquery.tables.update", "PUT", "projects/{projectId}/datasets/{datasetId}/tables/{tableId}", "Table", "Table", tableMutation, "projectId", "datasetId", "tableId"),
			"list": discoveryMethod("bigquery.tables.list", "GET", "projects/{projectId}/datasets/{datasetId}/tables", "", "TableList", map[string]any{
				"projectId": discoveryPathParameter("projectId"), "datasetId": discoveryPathParameter("datasetId"),
				"maxResults": discoveryQueryParameter("integer"), "pageToken": discoveryQueryParameter("string"),
			}, "projectId", "datasetId"),
			"delete": discoveryMethod("bigquery.tables.delete", "DELETE", "projects/{projectId}/datasets/{datasetId}/tables/{tableId}", "", "Empty", projectDatasetTable, "projectId", "datasetId", "tableId"),
		}},
	}
}

func discoveryPathParameter(name string) map[string]any {
	return map[string]any{
		"type": "string", "required": true, "location": "path", "pattern": "^[^/]+$", "description": name,
	}
}

func discoveryQueryParameter(valueType string) map[string]any {
	return map[string]any{"type": valueType, "location": "query"}
}

func discoveryMethod(id, httpMethod, path, requestRef, responseRef string, parameters map[string]any, order ...string) map[string]any {
	// Discovery consumers iterate parameterOrder without a
	// nil check. The official discovery schema defines this as an array, so a
	// method with no required path parameters must encode [] rather than null.
	// https://developers.google.com/discovery/v1/reference/apis/getRest
	if order == nil {
		order = []string{}
	}
	value := map[string]any{
		"id": id, "httpMethod": httpMethod, "path": path,
		"parameters": parameters, "parameterOrder": order,
	}
	if requestRef != "" {
		value["request"] = map[string]any{"$ref": requestRef}
	}
	if responseRef != "" {
		value["response"] = map[string]any{"$ref": responseRef}
	}
	return value
}

func discoveryObjectSchema(id string) map[string]any {
	return map[string]any{"id": id, "type": "object", "properties": map[string]any{}}
}
