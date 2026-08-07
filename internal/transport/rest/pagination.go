package rest

// Official list pagination convention: https://cloud.google.com/bigquery/docs/paging-results
//
// Page tokens are opaque to clients. BQEMU encodes the stable sorted-list index
// and rejects malformed/out-of-range tokens instead of silently restarting.

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

func pageBounds(r *http.Request, total int) (start, end int, nextPageToken string, err error) {
	start = 0
	if token := r.URL.Query().Get("pageToken"); token != "" {
		decoded, decodeErr := base64.RawURLEncoding.DecodeString(token)
		if decodeErr != nil {
			return 0, 0, "", fmt.Errorf("%w: invalid pageToken", domain.ErrInvalid)
		}
		start, decodeErr = strconv.Atoi(string(decoded))
		if decodeErr != nil || start < 0 || start > total {
			return 0, 0, "", fmt.Errorf("%w: invalid pageToken", domain.ErrInvalid)
		}
	}
	maximum := total - start
	if raw := r.URL.Query().Get("maxResults"); raw != "" {
		maximum, err = strconv.Atoi(raw)
		if err != nil || maximum < 0 {
			return 0, 0, "", fmt.Errorf("%w: maxResults must be a non-negative integer", domain.ErrInvalid)
		}
	}
	end = start + maximum
	if end > total {
		end = total
	}
	if end < total {
		nextPageToken = base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	}
	return start, end, nextPageToken, nil
}

func optionalBoolQuery(r *http.Request, name string) (bool, error) {
	value := r.URL.Query().Get(name)
	if value == "" {
		return false, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%w: %s must be true or false", domain.ErrInvalid, name)
	}
	return parsed, nil
}
