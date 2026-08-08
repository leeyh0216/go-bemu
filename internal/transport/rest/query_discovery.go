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
	// bq 2.1.31 always supplies the generated client's media_body argument,
	// including nil for ordinary query jobs. googleapiclient exposes that
	// argument only when Discovery contains mediaUpload metadata. The upload
	// endpoints remain deliberately unregistered and are documented as a gap.
	jobInsert["supportsMediaUpload"] = true
	jobInsert["mediaUpload"] = map[string]any{
		"accept": []string{"*/*"},
		"protocols": map[string]any{
			"simple": map[string]any{
				"multipart": true, "path": "/upload/bigquery/v2/projects/{projectId}/jobs",
			},
			"resumable": map[string]any{
				"multipart": true, "path": "/resumable/upload/bigquery/v2/projects/{projectId}/jobs",
			},
		},
	}
	document["resources"].(map[string]any)["jobs"] = map[string]any{"methods": map[string]any{
		"query":  discoveryMethod("bigquery.jobs.query", "POST", "projects/{projectId}/queries", "QueryRequest", "QueryResponse", project, "projectId"),
		"insert": jobInsert,
		"get":    discoveryMethod("bigquery.jobs.get", "GET", "projects/{projectId}/jobs/{jobId}", "", "Job", projectJob, "projectId", "jobId"),
		"list": discoveryMethod("bigquery.jobs.list", "GET", "projects/{projectId}/jobs", "", "JobList", map[string]any{
			"projectId": discoveryPathParameter("projectId"), "maxResults": discoveryQueryParameter("integer"),
			"pageToken": discoveryQueryParameter("string"), "allUsers": discoveryQueryParameter("boolean"),
			"stateFilter":     map[string]any{"type": "string", "location": "query", "repeated": true},
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
