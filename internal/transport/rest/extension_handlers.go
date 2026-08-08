package rest

import (
	"encoding/json"
	"io/fs"
	"net/http"
	"os"
	"path"
	"strings"
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
				profiles := server.capabilityProfiles
				if len(profiles) == 0 {
					profiles = json.RawMessage("[]")
				}
				writeJSON(w, http.StatusOK, map[string]any{
					"kind": "bqemu#capabilityRegistry", "profiles": profiles,
				})
			})}
		})
	}
}

// WithCapabilityProfiles injects the immutable profile snapshot exposed by the
// emulator-only capability endpoint. The contract compiler owns the snapshot;
// the transport only owns its HTTP representation.
func WithCapabilityProfiles(profiles json.RawMessage) Option {
	return func(server *Server) {
		server.capabilityProfiles = append(json.RawMessage(nil), profiles...)
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
