package rest

// Official error envelope source: https://cloud.google.com/bigquery/docs/error-messages

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

type errorProto struct {
	Reason   string `json:"reason"`
	Message  string `json:"message"`
	Location string `json:"location,omitempty"`
}

func decodeJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, 2<<20))
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("%w: invalid JSON body: %v", domain.ErrInvalid, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: JSON body must contain one value", domain.ErrInvalid)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	reason := "backendError"
	switch {
	case errors.Is(err, domain.ErrInvalid):
		status, reason = http.StatusBadRequest, "invalid"
	case errors.Is(err, domain.ErrNotFound):
		status, reason = http.StatusNotFound, "notFound"
	case errors.Is(err, domain.ErrConflict):
		status, reason = http.StatusConflict, "duplicate"
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
