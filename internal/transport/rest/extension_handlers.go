package rest

import (
	"io/fs"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/leeyh0216/go-bemu/contract"
)

type ConsoleAPI struct {
	Kind         string            `json:"kind"`
	Version      string            `json:"version"`
	Capabilities []string          `json:"capabilities"`
	Links        map[string]string `json:"links"`
}

func withCapabilitiesAPI() Option {
	return withRoutes(func(mux *http.ServeMux) {
		mux.HandleFunc("GET /bqemu/v1/capabilities", func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusOK, map[string]any{
				"kind": "bqemu#capabilityRegistry", "profiles": contract.DefaultRegistry().Profiles(),
			})
		})
	})
}

func withConsoleAPI() Option {
	return func(server *Server) {
		server.routeExtensions = append(server.routeExtensions, func(mux *http.ServeMux) {
			mux.HandleFunc("GET /bqemu/v1/console", func(w http.ResponseWriter, r *http.Request) {
				baseURL := server.baseURLFor(r)
				writeJSON(w, http.StatusOK, ConsoleAPI{
					Kind: "bqemu#consoleAPI", Version: "v1",
					Capabilities: []string{"projects", "datasets", "tables", "queries", "jobs"},
					Links: map[string]string{
						"projects": baseURL + "/bigquery/v2/projects", "projectAdmin": baseURL + "/bqemu/v1/projects",
						"capabilities": baseURL + "/bqemu/v1/capabilities",
					},
				})
			})
		})
	}
}

func WithConsoleDirectory(directory string) Option {
	return withRoutes(func(mux *http.ServeMux) {
		console := newSPAHandler(directory)
		mux.Handle("GET /console/", http.StripPrefix("/console/", console))
		mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/" {
				http.NotFound(w, r)
				return
			}
			http.Redirect(w, r, "/console/", http.StatusTemporaryRedirect)
		})
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
