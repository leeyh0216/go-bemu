package rest

// Official discovery method shape:
// https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list

func extendTableDataDiscovery(document map[string]any) {
	schemas := document["schemas"].(map[string]any)
	schemas["TableDataList"] = discoveryObjectSchema("TableDataList")
	parameters := map[string]any{
		"projectId":                           discoveryPathParameter("projectId"),
		"datasetId":                           discoveryPathParameter("datasetId"),
		"tableId":                             discoveryPathParameter("tableId"),
		"startIndex":                          discoveryQueryParameter("string"),
		"maxResults":                          discoveryQueryParameter("integer"),
		"pageToken":                           discoveryQueryParameter("string"),
		"selectedFields":                      discoveryQueryParameter("string"),
		"formatOptions.useInt64Timestamp":     discoveryQueryParameter("boolean"),
		"formatOptions.timestampOutputFormat": discoveryQueryParameter("string"),
	}
	document["resources"].(map[string]any)["tabledata"] = map[string]any{"methods": map[string]any{
		"list": discoveryMethod(
			"bigquery.tabledata.list", "GET", "projects/{projectId}/datasets/{datasetId}/tables/{tableId}/data",
			"", "TableDataList", parameters, "projectId", "datasetId", "tableId",
		),
	}}
}
