package rest

import "net/http"

const authenticationFailureMessage = "request is not authenticated"

// authenticationMiddleware enforces the same RFC 6750 bearer contract used by
// the gRPC edge. It executes after observability has attached request metadata
// and before method rewriting, body decoding, routing, or application calls.
// The readiness probes are public process-liveness contracts; discovery and
// all data-plane routes remain protected when authentication is enabled.
//
// Official sources:
//   - https://www.rfc-editor.org/rfc/rfc6750#section-2.1
//   - https://www.rfc-editor.org/rfc/rfc6750#section-3
//   - https://cloud.google.com/bigquery/docs/reference/rest
func authenticationMiddleware(authentication AuthenticationUseCases, next http.Handler) http.Handler {
	if authentication == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if publicAuthenticationPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		authenticatedContext, _, err := authentication.Authenticate(
			r.Context(), r.Header.Values("Authorization"),
		)
		if err != nil {
			if r.Body != nil {
				_ = r.Body.Close()
			}
			w.Header().Set("WWW-Authenticate", `Bearer realm="bqemu"`)
			writeJSON(w, http.StatusUnauthorized, map[string]any{
				"error": map[string]any{
					"code":    http.StatusUnauthorized,
					"message": authenticationFailureMessage,
					"errors": []errorProto{{
						Reason: "authError", Message: authenticationFailureMessage,
					}},
				},
			})
			return
		}
		next.ServeHTTP(w, r.WithContext(authenticatedContext))
	})
}

func publicAuthenticationPath(path string) bool {
	return path == "/healthz" || path == "/readyz"
}
