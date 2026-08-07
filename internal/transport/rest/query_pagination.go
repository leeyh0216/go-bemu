package rest

// BigQuery page tokens are opaque continuations scoped to the originating
// method. getQueryResults also accepts a zero-based startIndex.
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/list

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
)

const queryPageTokenVersion = 1

type queryPageToken struct {
	Version  int    `json:"v"`
	Kind     string `json:"k"`
	Scope    string `json:"s"`
	Cursor   string `json:"c"`
	Checksum string `json:"h"`
}

func encodeQueryPageToken(kind, scope, cursor string) string {
	token := queryPageToken{Version: queryPageTokenVersion, Kind: kind, Scope: scope, Cursor: cursor}
	token.Checksum = queryPageTokenChecksum(token)
	payload, _ := json.Marshal(token)
	return base64.RawURLEncoding.EncodeToString(payload)
}

func decodeQueryPageToken(raw, kind, scope string) (string, error) {
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return "", fmt.Errorf("%w: invalid pageToken", domain.ErrInvalid)
	}
	var token queryPageToken
	if err := json.Unmarshal(payload, &token); err != nil || token.Version != queryPageTokenVersion ||
		token.Kind != kind || token.Scope != scope || token.Checksum != queryPageTokenChecksum(token) {
		return "", fmt.Errorf("%w: pageToken does not match this request", domain.ErrInvalid)
	}
	return token.Cursor, nil
}

func queryPageTokenChecksum(token queryPageToken) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%d\x00%s\x00%s\x00%s", token.Version, token.Kind, token.Scope, token.Cursor)))
	return hex.EncodeToString(digest[:])
}

func queryResultPageBounds(r *http.Request, job *domain.Job) (start, end int, next string, err error) {
	total := 0
	if job.Result != nil {
		total = len(job.Result.Rows)
	}
	scope := queryResultPageScope(job)
	rawToken := r.URL.Query().Get("pageToken")
	rawStart := r.URL.Query().Get("startIndex")
	if rawToken != "" && rawStart != "" {
		return 0, 0, "", fmt.Errorf("%w: pageToken and startIndex cannot be combined", domain.ErrInvalid)
	}
	if rawToken != "" {
		cursor, decodeErr := decodeQueryPageToken(rawToken, "query-results", scope)
		if decodeErr != nil {
			return 0, 0, "", decodeErr
		}
		start, err = parseQueryRowOffset(cursor, total, "pageToken")
		if err != nil {
			return 0, 0, "", err
		}
	} else if rawStart != "" {
		start, err = parseQueryRowOffset(rawStart, total, "startIndex")
		if err != nil {
			return 0, 0, "", err
		}
	}
	maximum, err := parseMaximumResults(r.URL.Query().Get("maxResults"), total-start)
	if err != nil {
		return 0, 0, "", err
	}
	end = start + maximum
	if end > total {
		end = total
	}
	if end < total && end > start {
		next = encodeQueryPageToken("query-results", scope, strconv.Itoa(end))
	}
	return start, end, next, nil
}

func queryResultPageScope(job *domain.Job) string {
	ended := ""
	if job.EndedAt != nil {
		ended = job.EndedAt.UTC().Format(time.RFC3339Nano)
	}
	rowCount := 0
	columns := []domain.Column(nil)
	if job.Result != nil {
		rowCount = len(job.Result.Rows)
		columns = job.Result.Columns
	}
	shape := fmt.Sprintf("%v", columns)
	return observability.Digest([]byte(job.Reference.ProjectID + "\x00" + job.Reference.Location + "\x00" +
		job.Reference.JobID + "\x00" + job.ConfigurationDigest + "\x00" + ended + "\x00" +
		strconv.Itoa(rowCount) + "\x00" + observability.Digest([]byte(shape))))
}

func parseQueryRowOffset(raw string, total int, field string) (int, error) {
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || value > uint64(math.MaxInt) || value > uint64(total) {
		return 0, fmt.Errorf("%w: %s must identify a row in this result", domain.ErrInvalid, field)
	}
	return int(value), nil
}

func parseMaximumResults(raw string, fallback int) (int, error) {
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseUint(raw, 10, 31)
	if err != nil {
		return 0, fmt.Errorf("%w: maxResults must be a non-negative integer", domain.ErrInvalid)
	}
	return int(value), nil
}

func paginateQueryJobs(r *http.Request, jobs []*domain.Job, projectID, location string) ([]*domain.Job, string, error) {
	scope := observability.Digest([]byte(projectID + "\x00" + strings.ToUpper(location)))
	start := 0
	if raw := r.URL.Query().Get("pageToken"); raw != "" {
		cursor, err := decodeQueryPageToken(raw, "query-jobs", scope)
		if err != nil {
			return nil, "", err
		}
		for start < len(jobs) && queryJobCursor(jobs[start]) != cursor {
			start++
		}
		if start == len(jobs) {
			return nil, "", fmt.Errorf("%w: pageToken cursor is no longer available", domain.ErrInvalid)
		}
		start++
	}
	maximum, err := parseMaximumResults(r.URL.Query().Get("maxResults"), len(jobs)-start)
	if err != nil {
		return nil, "", err
	}
	end := start + maximum
	if end > len(jobs) {
		end = len(jobs)
	}
	next := ""
	if end < len(jobs) && end > start {
		next = encodeQueryPageToken("query-jobs", scope, queryJobCursor(jobs[end-1]))
	}
	return jobs[start:end], next, nil
}

func queryJobCursor(job *domain.Job) string {
	return job.CreatedAt.UTC().Format(time.RFC3339Nano) + "\x00" + job.Reference.JobID + "\x00" + strings.ToUpper(job.Reference.Location)
}

func paginateCombinedJobs(r *http.Request, resources []any, projectID, location string) ([]any, string, error) {
	scope := observability.Digest([]byte(projectID + "\x00" + strings.ToUpper(location)))
	start := 0
	if raw := r.URL.Query().Get("pageToken"); raw != "" {
		cursor, err := decodeQueryPageToken(raw, "combined-jobs", scope)
		if err != nil {
			return nil, "", err
		}
		for start < len(resources) && combinedJobCursor(resources[start]) != cursor {
			start++
		}
		if start == len(resources) {
			return nil, "", fmt.Errorf("%w: pageToken cursor is no longer available", domain.ErrInvalid)
		}
		start++
	}
	maximum, err := parseMaximumResults(r.URL.Query().Get("maxResults"), len(resources)-start)
	if err != nil {
		return nil, "", err
	}
	end := start + maximum
	if end > len(resources) {
		end = len(resources)
	}
	next := ""
	if end < len(resources) && end > start {
		next = encodeQueryPageToken("combined-jobs", scope, combinedJobCursor(resources[end-1]))
	}
	return resources[start:end], next, nil
}
