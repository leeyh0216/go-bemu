package application

// BigQuery ErrorProto reason values:
// https://cloud.google.com/bigquery/docs/error-messages

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

func TestQueryTerminalReasonPreservesProtocolClassification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "deadline", err: context.DeadlineExceeded, want: "timeout"},
		{name: "canceled", err: context.Canceled, want: "stopped"},
		{name: "not found", err: domain.ErrNotFound, want: "notFound"},
		{name: "duplicate", err: domain.ErrConflict, want: "duplicate"},
		{name: "precondition", err: domain.ErrPrecondition, want: "conditionNotMet"},
		{name: "invalid query", err: domain.ErrInvalidQuery, want: "invalidQuery"},
		{name: "invalid request", err: domain.ErrInvalid, want: "invalidQuery"},
		{name: "backend", err: domain.ErrBackend, want: "jobBackendError"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wrapped := fmt.Errorf("stage: %w", test.err)
			if got := queryTerminalReason(wrapped); got != test.want {
				t.Fatalf("reason = %q, want %q", got, test.want)
			}
		})
	}
	if got := queryTerminalReason(errors.New("unclassified query parser error")); got != "invalidQuery" {
		t.Fatalf("unclassified query reason = %q, want invalidQuery", got)
	}
}
