package contract

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCheckOperationSourcesReportsReachableAndMissingSources(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/missing" {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	results, err := CheckOperationSources(t.Context(), server.Client(), OperationManifest{Sources: []OperationSource{
		{ID: "missing", URL: server.URL + "/missing"},
		{ID: "reachable", URL: server.URL + "/reachable"},
	}})
	if err == nil {
		t.Fatal("source check error = nil")
	}
	if len(results) != 2 || results[0].ID != "missing" || results[0].Status != http.StatusNotFound || results[0].Error == "" {
		t.Fatalf("missing result = %#v", results)
	}
	if results[1].ID != "reachable" || results[1].Status != http.StatusNoContent || results[1].Error != "" {
		t.Fatalf("reachable result = %#v", results)
	}
}
