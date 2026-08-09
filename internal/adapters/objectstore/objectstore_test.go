package objectstore

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/loadjob/domain"
)

func TestGCSJSONSupportsFakeServerListGetAndMedia(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.RequestURI())
		switch {
		case r.URL.Path == "/storage/v1/b/bucket/o" && r.URL.Query().Get("pageToken") == "":
			if r.URL.Query().Get("prefix") != "data/" {
				t.Errorf("prefix = %q", r.URL.Query().Get("prefix"))
			}
			_, _ = io.WriteString(w, `{"items":[{"name":"data/b.parquet","size":"2","generation":"8"},{"name":"other.parquet","size":"9","generation":"9"}],"nextPageToken":"next"}`)
		case r.URL.Path == "/storage/v1/b/bucket/o" && r.URL.Query().Get("pageToken") == "next":
			_, _ = io.WriteString(w, `{"items":[{"name":"data/a.parquet","size":"1","generation":"7","etag":"tag"}]}`)
		case strings.HasSuffix(r.URL.Path, "/o/data/a.parquet") && r.URL.Query().Get("alt") == "media":
			if r.URL.Query().Get("generation") != "7" {
				t.Errorf("generation = %q", r.URL.Query().Get("generation"))
			}
			w.Header().Set("x-goog-generation", "7")
			_, _ = io.WriteString(w, "a")
		case strings.HasSuffix(r.URL.Path, "/o/data/a.parquet"):
			_, _ = io.WriteString(w, `{"name":"data/a.parquet","size":"1","generation":"7","etag":"tag"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	store, err := NewGCSJSON(GCSJSONConfig{Endpoint: server.URL, Client: server.Client(), MaxMetadataBytes: 1 << 20, MaxListedObjects: 10})
	if err != nil {
		t.Fatal(err)
	}
	objects, err := store.List(context.Background(), "gs://bucket/data/*.parquet")
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 2 || objects[0].URI != "gs://bucket/data/a.parquet" || objects[1].URI != "gs://bucket/data/b.parquet" {
		t.Fatalf("objects = %+v", objects)
	}
	object, err := store.Get(context.Background(), "gs://bucket/data/a.parquet")
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Open(context.Background(), object)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || string(payload) != "a" {
		t.Fatalf("payload=%q err=%v", payload, err)
	}
	if len(requests) != 4 {
		t.Fatalf("requests = %v", requests)
	}
}

func TestGCSJSONRejectsMediaGenerationDrift(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("alt") == "media" {
			w.Header().Set("x-goog-generation", "8")
			_, _ = io.WriteString(w, "changed")
			return
		}
		_, _ = io.WriteString(w, `{"name":"data.parquet","size":"7","generation":"7"}`)
	}))
	t.Cleanup(server.Close)
	store, err := NewGCSJSON(GCSJSONConfig{Endpoint: server.URL, Client: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	object, err := store.Get(context.Background(), "gs://bucket/data.parquet")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Open(context.Background(), object); !errors.Is(err, domain.ErrPrecondition) {
		t.Fatalf("Open() error = %v", err)
	}
}

func TestGCSJSONRequiresImmutableObjectMetadata(t *testing.T) {
	for name, payload := range map[string]string{
		"missing generation": `{"name":"data.parquet","size":"1"}`,
		"different name":     `{"name":"other.parquet","size":"1","generation":"7"}`,
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, payload)
			}))
			t.Cleanup(server.Close)
			store, err := NewGCSJSON(GCSJSONConfig{Endpoint: server.URL, Client: server.Client()})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Get(context.Background(), "gs://bucket/data.parquet"); !errors.Is(err, domain.ErrPrecondition) {
				t.Fatalf("Get() error = %v", err)
			}
		})
	}
}

func TestGCSJSONRetainsHTTPErrorBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "secret-payload")
	}))
	defer server.Close()
	store, err := NewGCSJSON(GCSJSONConfig{Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Get(context.Background(), "gs://bucket/object")
	if err == nil || !strings.Contains(err.Error(), "secret-payload") || !strings.Contains(err.Error(), "500") {
		t.Fatalf("diagnostic error = %v", err)
	}
}

func TestGCSJSONRejectsNonGCSURIWithoutHTTPRequest(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests++
	}))
	t.Cleanup(server.Close)
	gcs, err := NewGCSJSON(GCSJSONConfig{Endpoint: server.URL, Client: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = gcs.Get(context.Background(), "file:///etc/passwd")
	if !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("file source error = %v", err)
	}
	if requests != 0 {
		t.Fatalf("invalid source made %d HTTP requests", requests)
	}
}
