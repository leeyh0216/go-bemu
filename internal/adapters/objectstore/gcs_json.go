package objectstore

// Google Cloud Storage JSON API provenance:
//   - objects.get: https://cloud.google.com/storage/docs/json_api/v1/objects/get
//   - objects.list: https://cloud.google.com/storage/docs/json_api/v1/objects/list
//
// The adapter intentionally uses only those HTTP/JSON surfaces so it works
// with fake-gcs-server without cloning or patching that server.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
)

const (
	defaultMetadataLimit = int64(8 << 20)
	defaultListLimit     = 10_000
)

type GCSJSONConfig struct {
	Endpoint         string
	Client           *http.Client
	MaxMetadataBytes int64
	MaxListedObjects int
}

type GCSJSON struct {
	endpoint         string
	client           *http.Client
	maxMetadataBytes int64
	maxListedObjects int
}

func NewGCSJSON(config GCSJSONConfig) (*GCSJSON, error) {
	endpoint, err := url.Parse(strings.TrimRight(config.Endpoint, "/"))
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" {
		return nil, fmt.Errorf("%w: object-store endpoint must be an absolute HTTP URL", domain.ErrInvalid)
	}
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return nil, fmt.Errorf("%w: object-store endpoint must use HTTP or HTTPS", domain.ErrInvalid)
	}
	if config.Client == nil {
		config.Client = http.DefaultClient
	}
	if config.MaxMetadataBytes == 0 {
		config.MaxMetadataBytes = defaultMetadataLimit
	}
	if config.MaxListedObjects == 0 {
		config.MaxListedObjects = defaultListLimit
	}
	if config.MaxMetadataBytes < 1 || config.MaxListedObjects < 1 {
		return nil, fmt.Errorf("%w: object-store response limits must be positive", domain.ErrInvalid)
	}
	return &GCSJSON{
		endpoint: strings.TrimRight(config.Endpoint, "/"), client: config.Client,
		maxMetadataBytes: config.MaxMetadataBytes, maxListedObjects: config.MaxListedObjects,
	}, nil
}

type gcsObjectResource struct {
	Name       string `json:"name"`
	Size       string `json:"size"`
	Generation string `json:"generation"`
	ETag       string `json:"etag"`
}

type gcsObjectList struct {
	Items         []gcsObjectResource `json:"items"`
	NextPageToken string              `json:"nextPageToken"`
}

func (g *GCSJSON) Get(ctx context.Context, rawURI string) (loadports.ObjectInfo, error) {
	bucket, object, err := parseGCSURI(rawURI)
	if err != nil {
		return loadports.ObjectInfo{}, err
	}
	if strings.ContainsAny(object, "*?[") {
		return loadports.ObjectInfo{}, fmt.Errorf("%w: exact object URI contains a glob", domain.ErrInvalid)
	}
	var resource gcsObjectResource
	if err := g.getJSON(ctx, g.objectURL(bucket, object, nil), &resource); err != nil {
		return loadports.ObjectInfo{}, err
	}
	if resource.Name == "" {
		resource.Name = object
	}
	return objectInfo(bucket, resource)
}

func (g *GCSJSON) List(ctx context.Context, rawPattern string) ([]loadports.ObjectInfo, error) {
	bucket, pattern, err := parseGCSURI(rawPattern)
	if err != nil {
		return nil, err
	}
	if strings.Contains(pattern, "**") || strings.ContainsAny(pattern, "{}") {
		return nil, fmt.Errorf("%w: recursive and brace GCS globs are not implemented", domain.ErrUnsupported)
	}
	if _, err := path.Match(pattern, pattern); err != nil {
		return nil, fmt.Errorf("%w: invalid GCS URI glob", domain.ErrInvalid)
	}
	prefix := literalPrefix(pattern)
	pageToken := ""
	objects := make([]loadports.ObjectInfo, 0)
	listed := 0
	pages := 0
	for {
		pages++
		if pages > g.maxListedObjects {
			return nil, fmt.Errorf("%w: object listing exceeds configured page limit", domain.ErrPrecondition)
		}
		values := url.Values{}
		if prefix != "" {
			values.Set("prefix", prefix)
		}
		if pageToken != "" {
			values.Set("pageToken", pageToken)
		}
		var page gcsObjectList
		if err := g.getJSON(ctx, g.listURL(bucket, values), &page); err != nil {
			return nil, err
		}
		for _, resource := range page.Items {
			listed++
			if listed > g.maxListedObjects {
				return nil, fmt.Errorf("%w: object listing exceeds configured limit", domain.ErrPrecondition)
			}
			matched, matchErr := path.Match(pattern, resource.Name)
			if matchErr != nil {
				return nil, fmt.Errorf("%w: invalid GCS URI glob", domain.ErrInvalid)
			}
			if !matched {
				continue
			}
			info, err := objectInfo(bucket, resource)
			if err != nil {
				return nil, err
			}
			objects = append(objects, info)
		}
		if page.NextPageToken == "" {
			break
		}
		if page.NextPageToken == pageToken {
			return nil, fmt.Errorf("object-store list returned a repeated page token")
		}
		pageToken = page.NextPageToken
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].URI < objects[j].URI })
	return objects, nil
}

func (g *GCSJSON) Open(ctx context.Context, object loadports.ObjectInfo) (io.ReadCloser, error) {
	bucket, name, err := parseGCSURI(object.URI)
	if err != nil {
		return nil, err
	}
	values := url.Values{"alt": []string{"media"}}
	if object.Generation != "" {
		values.Set("generation", object.Generation)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, g.objectURL(bucket, name, values), nil)
	if err != nil {
		return nil, fmt.Errorf("construct object media request: %w", err)
	}
	response, err := g.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("request object media: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_ = response.Body.Close()
		return nil, g.httpStatusError(response.StatusCode)
	}
	return response.Body, nil
}

func (g *GCSJSON) getJSON(ctx context.Context, requestURL string, target any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return fmt.Errorf("construct object metadata request: %w", err)
	}
	response, err := g.client.Do(request)
	if err != nil {
		return fmt.Errorf("request object metadata: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return g.httpStatusError(response.StatusCode)
	}
	limited := io.LimitReader(response.Body, g.maxMetadataBytes+1)
	payload, err := io.ReadAll(limited)
	if err != nil {
		return fmt.Errorf("read object metadata response: %w", err)
	}
	if int64(len(payload)) > g.maxMetadataBytes {
		return fmt.Errorf("%w: object metadata response exceeds configured limit", domain.ErrPrecondition)
	}
	if err := json.Unmarshal(payload, target); err != nil {
		return fmt.Errorf("decode object metadata response: %w", err)
	}
	return nil
}

func (g *GCSJSON) httpStatusError(status int) error {
	if status == http.StatusNotFound {
		return fmt.Errorf("%w: object-store HTTP status %d", domain.ErrNotFound, status)
	}
	if status == http.StatusPreconditionFailed || status == http.StatusForbidden || status == http.StatusUnauthorized {
		return fmt.Errorf("%w: object-store HTTP status %d", domain.ErrPrecondition, status)
	}
	return fmt.Errorf("object-store HTTP status %d", status)
}

func (g *GCSJSON) objectURL(bucket, object string, values url.Values) string {
	result := g.endpoint + "/storage/v1/b/" + url.PathEscape(bucket) + "/o/" + url.PathEscape(object)
	if len(values) > 0 {
		result += "?" + values.Encode()
	}
	return result
}

func (g *GCSJSON) listURL(bucket string, values url.Values) string {
	result := g.endpoint + "/storage/v1/b/" + url.PathEscape(bucket) + "/o"
	if len(values) > 0 {
		result += "?" + values.Encode()
	}
	return result
}

func parseGCSURI(rawURI string) (string, string, error) {
	u, err := url.Parse(rawURI)
	if err != nil || u.Scheme != "gs" || u.Host == "" || u.User != nil || u.Port() != "" || u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" {
		return "", "", fmt.Errorf("%w: object URI must be a gs:// URI", domain.ErrInvalid)
	}
	object := strings.TrimPrefix(u.Path, "/")
	if object == "" {
		return "", "", fmt.Errorf("%w: GCS object name is required", domain.ErrInvalid)
	}
	return u.Host, object, nil
}

func objectInfo(bucket string, resource gcsObjectResource) (loadports.ObjectInfo, error) {
	size, err := strconv.ParseInt(resource.Size, 10, 64)
	if err != nil || size < 0 {
		return loadports.ObjectInfo{}, fmt.Errorf("%w: object metadata size is invalid", domain.ErrInvalid)
	}
	return loadports.ObjectInfo{
		URI:  (&url.URL{Scheme: "gs", Host: bucket, Path: "/" + resource.Name}).String(),
		Size: size, Generation: resource.Generation, ETag: resource.ETag,
	}, nil
}

func literalPrefix(pattern string) string {
	index := strings.IndexAny(pattern, "*?[")
	if index < 0 {
		return pattern
	}
	return pattern[:index]
}

var _ loadports.ObjectStore = (*GCSJSON)(nil)
