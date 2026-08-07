package rest

// Pagination sources:
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/list
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults

import (
	"net/http/httptest"
	"net/url"
	"sort"
	"testing"
)

func TestCombinedJobPaginationDistinguishesLocationAndJobType(t *testing.T) {
	ctx, cancel := staticOverwriteRESTTestContext(t)
	defer cancel()
	resources := []any{
		jobResource{JobReference: jobReferenceResource{ProjectID: "test-project", JobID: "same", Location: "US"}, Statistics: jobStatistics{CreationTime: "1000"}},
		jobResource{JobReference: jobReferenceResource{ProjectID: "test-project", JobID: "same", Location: "EU"}, Statistics: jobStatistics{CreationTime: "2000"}},
		loadJobResource{JobReference: jobReferenceResource{ProjectID: "test-project", JobID: "same", Location: "US"}, Statistics: loadJobStatisticsResource{CreationTime: "1000"}},
		loadJobResource{JobReference: jobReferenceResource{ProjectID: "test-project", JobID: "same", Location: "EU"}, Statistics: loadJobStatisticsResource{CreationTime: "2000"}},
	}
	sort.SliceStable(resources, func(i, j int) bool { return combinedJobLess(resources[i], resources[j]) })
	if got := combinedJobCreationMillis(resources[0]); got != 2000 {
		t.Fatalf("first combined job creationTime = %d, want newest 2000", got)
	}

	seen := make(map[string]struct{}, len(resources))
	token := ""
	for {
		request := httptest.NewRequest("GET", "/bigquery/v2/projects/test-project/jobs?maxResults=1&pageToken="+url.QueryEscape(token), nil).WithContext(ctx)
		page, next, err := paginateCombinedJobs(request, resources, "test-project", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(page) != 1 {
			t.Fatalf("combined page length = %d, want 1", len(page))
		}
		key := combinedJobCursor(page[0])
		if _, duplicate := seen[key]; duplicate {
			t.Fatalf("combined page repeated key %q", key)
		}
		seen[key] = struct{}{}
		if next == "" {
			break
		}
		token = next
	}
	if len(seen) != len(resources) {
		t.Fatalf("combined pagination saw %d/%d identities: %#v", len(seen), len(resources), seen)
	}
}
