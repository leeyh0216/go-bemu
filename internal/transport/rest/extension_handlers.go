package rest

import (
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/observability"
)

type ConsoleAPI struct {
	Kind         string            `json:"kind"`
	Version      string            `json:"version"`
	Capabilities []string          `json:"capabilities"`
	Links        map[string]string `json:"links"`
}

func withCapabilitiesAPI() Option {
	return func(server *Server) {
		server.operationRoutes = append(server.operationRoutes, func() []routeBinding {
			return []routeBinding{handlerBinding("bqemu.capabilities.get", func(w http.ResponseWriter, _ *http.Request) {
				operations := server.operationIDs()
				sort.Strings(operations)
				writeJSON(w, http.StatusOK, map[string]any{
					"kind": "bqemu#capabilityRegistry", "operations": operations,
				})
			})}
		})
	}
}

func withConsoleAPI() Option {
	return func(server *Server) {
		server.operationRoutes = append(server.operationRoutes, func() []routeBinding {
			return []routeBinding{handlerBinding("bqemu.console.get", func(w http.ResponseWriter, r *http.Request) {
				baseURL := server.baseURLFor(r)
				writeJSON(w, http.StatusOK, ConsoleAPI{
					Kind: "bqemu#consoleAPI", Version: "v1",
					Capabilities: []string{"projects", "datasets", "tables", "queries", "jobs"},
					Links: map[string]string{
						"projects": baseURL + "/bigquery/v2/projects", "projectAdmin": baseURL + "/bqemu/v1/projects",
						"capabilities": baseURL + "/bqemu/v1/capabilities",
					},
				})
			})}
		})
	}
}

func withDiagnosticsAPI() Option {
	return func(server *Server) {
		server.operationRoutes = append(server.operationRoutes, func() []routeBinding {
			return []routeBinding{
				handlerBinding("bqemu.diagnostics.timeline.get", func(w http.ResponseWriter, r *http.Request) {
					cursor, err := timelineCursor(r)
					if err != nil {
						writeError(w, err)
						return
					}
					limit, err := timelineLimit(r)
					if err != nil {
						writeError(w, err)
						return
					}
					snapshot := observability.ProcessTimeline().Snapshot(cursor, limit)
					snapshot.Events = filterTimelineEvents(snapshot.Events, r)
					writeJSON(w, http.StatusOK, snapshot)
				}),
				handlerBinding("bqemu.diagnostics.timeline.clear", func(w http.ResponseWriter, _ *http.Request) {
					observability.ProcessTimeline().Clear()
					w.WriteHeader(http.StatusNoContent)
				}),
			}
		})
	}
}

func timelineCursor(r *http.Request) (uint64, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("cursor"))
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid diagnostic cursor: %w", err)
	}
	return value, nil
}

func timelineLimit(r *http.Request) (int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("limit"))
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("invalid diagnostic limit")
	}
	return value, nil
}

func filterTimelineEvents(events []observability.DiagnosticEvent, r *http.Request) []observability.DiagnosticEvent {
	query := r.URL.Query()
	protocol, status := query.Get("protocol"), query.Get("status")
	operation, requestID := query.Get("operationId"), query.Get("requestId")
	filtered := events[:0]
	for _, event := range events {
		if (protocol != "" && event.Protocol != protocol) || (status != "" && event.Status != status) ||
			(operation != "" && event.OperationID != operation) || (requestID != "" && event.RequestID != requestID) {
			continue
		}
		filtered = append(filtered, event)
	}
	return filtered
}

func WithConsoleDirectory(directory string) Option {
	return withOperationRoutes(func() []routeBinding {
		console := newSPAHandler(directory)
		return []routeBinding{
			{operationID: "bqemu.console.assets", handler: http.StripPrefix("/console/", console)},
			handlerBinding("bqemu.console.redirect", func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/" {
					http.NotFound(w, r)
					return
				}
				http.Redirect(w, r, "/console/", http.StatusTemporaryRedirect)
			}),
		}
	})
}

func newSPAHandler(directory string) http.Handler {
	root := os.DirFS(directory)
	files := http.FileServerFS(root)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		candidate := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
		fallback := false
		if candidate == "." || candidate == "" {
			candidate = "index.html"
			fallback = true
		}
		info, err := fs.Stat(root, candidate)
		if err != nil || info.IsDir() {
			candidate = "index.html"
			fallback = true
		}
		clone := r.Clone(r.Context())
		clone.URL.Path = "/" + candidate
		if fallback {
			clone.URL.Path = "/"
		}
		files.ServeHTTP(w, clone)
	})
}
