package rest

// Official jobs discovery source: https://bigquery.googleapis.com/$discovery/rest?version=v2

func extendQueryDiscovery(document map[string]any) {
	schemas := document["schemas"].(map[string]any)
	for _, name := range []string{"Job", "JobList", "QueryRequest", "QueryResponse", "GetQueryResultsResponse"} {
		schemas[name] = discoveryObjectSchema(name)
	}
	project := map[string]any{"projectId": discoveryPathParameter("projectId")}
	projectJob := map[string]any{
		"projectId": discoveryPathParameter("projectId"), "jobId": discoveryPathParameter("jobId"),
		"location": discoveryQueryParameter("string"),
	}
	jobInsert := discoveryMethod("bigquery.jobs.insert", "POST", "projects/{projectId}/jobs", "Job", "Job", project, "projectId")
	document["resources"].(map[string]any)["jobs"] = map[string]any{"methods": map[string]any{
		"query":  discoveryMethod("bigquery.jobs.query", "POST", "projects/{projectId}/queries", "QueryRequest", "QueryResponse", project, "projectId"),
		"insert": jobInsert,
		"get":    discoveryMethod("bigquery.jobs.get", "GET", "projects/{projectId}/jobs/{jobId}", "", "Job", projectJob, "projectId", "jobId"),
		"list": discoveryMethod("bigquery.jobs.list", "GET", "projects/{projectId}/jobs", "", "JobList", map[string]any{
			"projectId": discoveryPathParameter("projectId"), "maxResults": discoveryQueryParameter("integer"),
			"pageToken":       discoveryQueryParameter("string"),
			"projection":      discoveryQueryParameter("string"),
			"minCreationTime": discoveryQueryParameter("string"),
			"maxCreationTime": discoveryQueryParameter("string"),
			"parentJobId":     discoveryQueryParameter("string"),
		}, "projectId"),
		"getQueryResults": discoveryMethod("bigquery.jobs.getQueryResults", "GET", "projects/{projectId}/queries/{jobId}", "", "GetQueryResultsResponse", map[string]any{
			"projectId": discoveryPathParameter("projectId"), "jobId": discoveryPathParameter("jobId"),
			"location": discoveryQueryParameter("string"), "maxResults": discoveryQueryParameter("integer"),
			"pageToken": discoveryQueryParameter("string"), "timeoutMs": discoveryQueryParameter("integer"),
			"startIndex":                          discoveryQueryParameter("string"),
			"formatOptions.useInt64Timestamp":     discoveryQueryParameter("boolean"),
			"formatOptions.timestampOutputFormat": discoveryQueryParameter("string"),
		}, "projectId", "jobId"),
	}}
}
