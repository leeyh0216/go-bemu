package rest

// Official error envelope source: https://cloud.google.com/bigquery/docs/error-messages

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

const maximumJSONBodyBytes = 2 << 20

type httpProtocolError struct {
	status  int
	reason  string
	message string
	err     error
}

func (e *httpProtocolError) Error() string { return e.message }
func (e *httpProtocolError) Unwrap() error { return e.err }

func requestBodyTooLarge(err error) error {
	return &httpProtocolError{
		status: http.StatusRequestEntityTooLarge, reason: "requestTooLarge",
		message: "request body exceeds the configured byte limit", err: err,
	}
}

func readJSONBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	limit := int64(maximumJSONBodyBytes)
	if configured, ok := requestBodyLimitsFromContext(r.Context()); ok {
		limit = configured.maxUncompressedBytes
	}
	payload, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			classified := requestBodyTooLarge(err)
			recordRequestBodyFailure(r.Context(), classified)
			return nil, classified
		}
		classified := fmt.Errorf("%w: read JSON body: %v", domain.ErrInvalid, err)
		recordRequestBodyFailure(r.Context(), classified)
		return nil, classified
	}
	if int64(len(payload)) > limit {
		classified := requestBodyTooLarge(fmt.Errorf("body exceeds %d bytes", limit))
		recordRequestBodyFailure(r.Context(), classified)
		return nil, classified
	}
	return payload, nil
}

type errorProto struct {
	Reason   string `json:"reason"`
	Message  string `json:"message"`
	Location string `json:"location,omitempty"`
}

func decodeJSON(r *http.Request, target any) error {
	payload, err := readJSONBody(r)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	if err := decoder.Decode(target); err != nil {
		classified := fmt.Errorf("%w: invalid JSON body: %v", domain.ErrInvalid, err)
		recordRequestBodyFailure(r.Context(), classified)
		return classified
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		classified := fmt.Errorf("%w: JSON body must contain one value", domain.ErrInvalid)
		recordRequestBodyFailure(r.Context(), classified)
		return classified
	}
	return nil
}

func decodeJSONWithFields(r *http.Request, target any) (map[string]json.RawMessage, error) {
	payload, err := readJSONBody(r)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	if err := decoder.Decode(target); err != nil {
		classified := fmt.Errorf("%w: invalid JSON body: %v", domain.ErrInvalid, err)
		recordRequestBodyFailure(r.Context(), classified)
		return nil, classified
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		classified := fmt.Errorf("%w: JSON body must contain one value", domain.ErrInvalid)
		recordRequestBodyFailure(r.Context(), classified)
		return nil, classified
	}
	fields := make(map[string]json.RawMessage)
	if err := json.Unmarshal(payload, &fields); err != nil {
		classified := fmt.Errorf("%w: JSON resource must be an object", domain.ErrInvalid)
		recordRequestBodyFailure(r.Context(), classified)
		return nil, classified
	}
	return fields, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	reason := "backendError"
	var protocolError *httpProtocolError
	switch {
	case errors.As(err, &protocolError):
		status, reason = protocolError.status, protocolError.reason
	case errors.Is(err, domain.ErrInvalid):
		status, reason = http.StatusBadRequest, "invalid"
	case errors.Is(err, domain.ErrNotFound):
		status, reason = http.StatusNotFound, "notFound"
	case errors.Is(err, domain.ErrConflict):
		status, reason = http.StatusConflict, "duplicate"
	case errors.Is(err, domain.ErrPrecondition):
		status, reason = http.StatusPreconditionFailed, "conditionNotMet"
	case errors.Is(err, domain.ErrUnsupported):
		status, reason = http.StatusNotImplemented, "notImplemented"
	}
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"code": status, "message": err.Error(),
			"errors": []errorProto{{Reason: reason, Message: err.Error()}},
		},
	})
}

func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				writeError(w, fmt.Errorf("panic handling request: %v", recovered))
			}
		}()
		next.ServeHTTP(w, r)
	})
}
