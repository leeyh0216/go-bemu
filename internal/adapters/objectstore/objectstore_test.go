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

func TestMediaStoreStartupCleansOnlyPrivateIncompleteUploads(t *testing.T) {
	directory := t.TempDir()
	staging := filepath.Join(directory, ".upload-interrupted.part")
	immutable := filepath.Join(directory, strings.Repeat("a", 64)+".object")
	unrelated := filepath.Join(directory, "keep.txt")
	for path := range map[string]string{staging: "partial", immutable: "complete", unrelated: "keep"} {
		if err := os.WriteFile(path, []byte("payload"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := NewMediaStore(directory, 1024); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(staging); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale upload error=%v, want not exist", err)
	}
	for _, path := range []string{immutable, unrelated} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("startup cleanup removed %q: %v", path, err)
		}
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
