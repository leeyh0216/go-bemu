package contract

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"time"
)

// SourceCheckResult is one immutable operation-manifest source observation.
// It intentionally records only the configured URL, final URL, and response
// status, so scheduled evidence can identify a moved or deleted upstream
// source without retaining a remote document body.
type SourceCheckResult struct {
	ID       string `json:"id"`
	URL      string `json:"url"`
	FinalURL string `json:"finalUrl,omitempty"`
	Status   int    `json:"status,omitempty"`
	Error    string `json:"error,omitempty"`
}

// CheckOperationSources resolves every canonical operation-manifest source.
// A non-2xx response is an error: a source reference that has moved or been
// deleted must be repaired deliberately rather than silently retained.
func CheckOperationSources(ctx context.Context, client *http.Client, manifest OperationManifest) ([]SourceCheckResult, error) {
	if client == nil {
		return nil, fmt.Errorf("source check client is required")
	}
	sources := append([]OperationSource(nil), manifest.Sources...)
	sort.Slice(sources, func(i, j int) bool { return sources[i].ID < sources[j].ID })
	results := make([]SourceCheckResult, 0, len(sources))
	var failures []error
	for _, source := range sources {
		result := SourceCheckResult{ID: source.ID, URL: source.URL}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, source.URL, nil)
		if err != nil {
			result.Error = err.Error()
			failures = append(failures, fmt.Errorf("%s: %w", source.ID, err))
			results = append(results, result)
			continue
		}
		request.Header.Set("User-Agent", "go-bemu-contract-source-check/1")
		response, err := client.Do(request)
		if err != nil {
			result.Error = err.Error()
			failures = append(failures, fmt.Errorf("%s: %w", source.ID, err))
			results = append(results, result)
			continue
		}
		result.Status = response.StatusCode
		result.FinalURL = response.Request.URL.String()
		_ = response.Body.Close()
		if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
			result.Error = fmt.Sprintf("unexpected HTTP status %d", response.StatusCode)
			failures = append(failures, fmt.Errorf("%s: %s", source.ID, result.Error))
		}
		results = append(results, result)
	}
	if err := errors.Join(failures...); err != nil {
		return results, fmt.Errorf("operation source availability: %w", err)
	}
	return results, nil
}

// DefaultSourceCheckClient bounds each remote observation so a source outage
// reports deterministically instead of consuming an unbounded scheduled job.
func DefaultSourceCheckClient() *http.Client {
	return &http.Client{Timeout: 15 * time.Second}
}
