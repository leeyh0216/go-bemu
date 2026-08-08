package rest

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPublicRESTIgnoresAuthorizationValues(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	handler := NewCatalogServer(nil, nil, "http://bqemu.test").Handler()
	cases := []struct {
		name   string
		values []string
	}{
		{name: "missing"},
		{name: "arbitrary", values: []string{"Basic private-rest-arbitrary"}},
		{name: "malformed", values: []string{"not-a-bearer private-rest-malformed"}},
		{name: "fixture-issued", values: []string{"Bearer private-rest-local-fixture"}},
		{name: "expired-looking", values: []string{"Bearer private-rest-expired.fixture.value"}},
		{name: "duplicates", values: []string{"Bearer private-rest-duplicate-a", "Bearer private-rest-duplicate-b"}},
	}

	var baselineBody string
	for index, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "http://bqemu.test/$discovery/rest?version=v2", nil)
			for _, value := range test.values {
				request.Header.Add("Authorization", value)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			if value := response.Header().Get("WWW-Authenticate"); value != "" {
				t.Fatalf("WWW-Authenticate = %q", value)
			}
			if index == 0 {
				baselineBody = response.Body.String()
			} else if response.Body.String() != baselineBody {
				t.Fatal("Authorization changed the discovery response")
			}
			for _, secret := range test.values {
				if strings.Contains(response.Body.String(), secret) {
					t.Fatalf("response exposed Authorization value %q", secret)
				}
			}
		})
	}

	output := logs.String()
	for _, test := range cases {
		for _, secret := range test.values {
			if strings.Contains(output, secret) {
				t.Fatalf("logs exposed Authorization value %q: %s", secret, output)
			}
		}
	}
	if !strings.Contains(output, "authorization=[REDACTED]") {
		t.Fatalf("logs did not retain the redacted metadata key: %s", output)
	}
}
