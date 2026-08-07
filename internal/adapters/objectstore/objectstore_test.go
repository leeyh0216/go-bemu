package objectstore

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/loadjob/domain"
)

func TestFileSystemListGetAndOpen(t *testing.T) {
	directory := t.TempDir()
	for name, contents := range map[string]string{"a.parquet": "a", "b.parquet": "bb", "ignored.txt": "x"} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store := FileSystem{}
	objects, err := store.List(context.Background(), canonicalFileURI(filepath.Join(directory, "*.parquet")))
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 2 || objects[0].Size != 1 || objects[1].Size != 2 {
		t.Fatalf("unexpected objects: %+v", objects)
	}
	object, err := store.Get(context.Background(), canonicalFileURI(filepath.Join(directory, "b.parquet")))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Open(context.Background(), object)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	payload, err := io.ReadAll(reader)
	if err != nil || string(payload) != "bb" {
		t.Fatalf("payload=%q err=%v", payload, err)
	}
	if _, err := store.Get(context.Background(), filepath.Join(directory, "b.parquet")); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("bare path error = %v", err)
	}
}

func TestGCSJSONSupportsFakeServerListGetAndMedia(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.RequestURI())
		switch {
		case r.URL.Path == "/storage/v1/b/bucket/o" && r.URL.Query().Get("pageToken") == "":
			if r.URL.Query().Get("prefix") != "data/" {
				t.Errorf("prefix = %q", r.URL.Query().Get("prefix"))
			}
			_, _ = io.WriteString(w, `{"items":[{"name":"data/b.parquet","size":"2"},{"name":"other.parquet","size":"9"}],"nextPageToken":"next"}`)
		case r.URL.Path == "/storage/v1/b/bucket/o" && r.URL.Query().Get("pageToken") == "next":
			_, _ = io.WriteString(w, `{"items":[{"name":"data/a.parquet","size":"1","generation":"7","etag":"tag"}]}`)
		case strings.HasSuffix(r.URL.Path, "/o/data/a.parquet") && r.URL.Query().Get("alt") == "media":
			if r.URL.Query().Get("generation") != "7" {
				t.Errorf("generation = %q", r.URL.Query().Get("generation"))
			}
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

func TestGCSJSONDoesNotExposeHTTPErrorBody(t *testing.T) {
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
	if err == nil || strings.Contains(err.Error(), "secret-payload") || !strings.Contains(err.Error(), "500") {
		t.Fatalf("unsafe error = %v", err)
	}
}

func TestGCSOnlyRouterRejectsFileSourcesBeforeAccess(t *testing.T) {
	gcs, err := NewGCSJSON(GCSJSONConfig{Endpoint: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	router, err := NewGCSOnlyRouter(gcs)
	if err != nil {
		t.Fatal(err)
	}
	_, err = router.Get(context.Background(), "file:///etc/passwd")
	if !errors.Is(err, domain.ErrUnsupported) {
		t.Fatalf("file source error = %v", err)
	}
}
