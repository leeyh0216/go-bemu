package admin

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/contractspec"
)

const (
	APIVersion       = "admin.bqemu.dev/v1alpha1"
	maxTokenFileSize = 64 << 10
)

type Clock interface {
	Now() time.Time
}

type Runtime interface {
	Snapshot(startedAt, now time.Time) RuntimeSnapshot
	GoroutineStack(maxBytes int) StackCapture
}

type RuntimeSnapshot struct {
	APIVersion       string    `json:"apiVersion"`
	CapturedAt       time.Time `json:"capturedAt"`
	StartedAt        time.Time `json:"startedAt"`
	UptimeMillis     int64     `json:"uptimeMillis"`
	GoVersion        string    `json:"goVersion"`
	GOOS             string    `json:"goos"`
	GOARCH           string    `json:"goarch"`
	PID              int       `json:"pid"`
	Goroutines       int       `json:"goroutines"`
	HeapAllocBytes   uint64    `json:"heapAllocBytes"`
	HeapObjects      uint64    `json:"heapObjects"`
	GCCount          uint32    `json:"gcCount"`
	BuildPath        string    `json:"buildPath,omitempty"`
	BuildMainVersion string    `json:"buildMainVersion,omitempty"`
}

type StackCapture struct {
	Payload   []byte
	Truncated bool
}

type Options struct {
	TokenFile     string
	MaxStackBytes int
	Clock         Clock
	Runtime       Runtime
	Logger        *slog.Logger
}

type Server struct {
	handler       http.Handler
	clock         Clock
	runtime       Runtime
	logger        *slog.Logger
	startedAt     time.Time
	maxStackBytes int
	token         []byte
}

func New(options Options) (*Server, error) {
	if options.MaxStackBytes < 64<<10 {
		return nil, errors.New("max stack bytes must be at least 64 KiB")
	}
	if options.Clock == nil {
		options.Clock = systemClock{}
	}
	if options.Runtime == nil {
		options.Runtime = goRuntime{}
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	server := &Server{
		clock: options.Clock, runtime: options.Runtime, logger: options.Logger,
		startedAt: options.Clock.Now(), maxStackBytes: options.MaxStackBytes,
	}
	if options.TokenFile != "" {
		token, err := readToken(options.TokenFile)
		if err != nil {
			return nil, err
		}
		server.token = token
	}
	mux := http.NewServeMux()
	for _, binding := range server.routeBindings() {
		spec, ok := contractspec.RESTRoute(binding.operationID)
		if !ok {
			return nil, fmt.Errorf("admin route has no generated operation specification: %s", binding.operationID)
		}
		mux.HandleFunc(spec.Pattern(), binding.handler)
	}
	server.handler = server.authenticate(mux)
	return server, nil
}

type routeBinding struct {
	operationID string
	handler     http.HandlerFunc
}

func (s *Server) routeBindings() []routeBinding {
	return []routeBinding{
		{operationID: "bqemu.admin.health", handler: s.health},
		{operationID: "bqemu.admin.runtime.get", handler: s.runtimeSnapshot},
		{operationID: "bqemu.admin.goroutines.get", handler: s.goroutines},
	}
}

func (s *Server) Handler() http.Handler { return s.handler }

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"scope": "admin", "status": "live", "apiVersion": APIVersion})
}

func (s *Server) runtimeSnapshot(w http.ResponseWriter, r *http.Request) {
	started := s.clock.Now()
	snapshot := s.runtime.Snapshot(s.startedAt, started)
	s.logger.InfoContext(r.Context(), "admin runtime diagnostics returned",
		"boundary", "admin.response.sent", "operation", "runtime-snapshot",
		"model_version", APIVersion, "shape", "RuntimeSnapshot",
		"goroutines", snapshot.Goroutines, "duration_ms", s.clock.Now().Sub(started).Milliseconds(),
	)
	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) goroutines(w http.ResponseWriter, r *http.Request) {
	started := s.clock.Now()
	capture := s.runtime.GoroutineStack(s.maxStackBytes)
	digest := payloadDigest(capture.Payload)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-BQEMU-Model-Version", APIVersion)
	w.Header().Set("X-BQEMU-Payload-Digest", digest)
	w.Header().Set("X-BQEMU-Truncated", fmt.Sprint(capture.Truncated))
	w.Header().Set("Content-Disposition", "attachment; filename=bqemu-goroutines.txt")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(capture.Payload)
	s.logger.InfoContext(r.Context(), "admin goroutine diagnostics returned",
		"boundary", "admin.response.sent", "operation", "goroutine-stack",
		"model_version", APIVersion, "shape", "GoRuntimeStack", "bytes", len(capture.Payload),
		"payload_digest", digest, "truncated", capture.Truncated,
		"duration_ms", s.clock.Now().Sub(started).Milliseconds(),
	)
}

func (s *Server) authenticate(next http.Handler) http.Handler {
	if len(s.token) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		supplied, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(supplied), s.token) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="bqemu-admin"`)
			writeJSON(w, http.StatusUnauthorized, map[string]any{
				"error": map[string]string{"code": "UNAUTHENTICATED", "message": "valid admin bearer token required"},
			})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func readToken(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open admin token file: %w", err)
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, maxTokenFileSize+1))
	if err != nil {
		return nil, fmt.Errorf("read admin token file: %w", err)
	}
	if len(payload) > maxTokenFileSize {
		return nil, fmt.Errorf("admin token file exceeds %d bytes", maxTokenFileSize)
	}
	token := []byte(strings.TrimSpace(string(payload)))
	if len(token) < 16 {
		return nil, errors.New("admin token must contain at least 16 non-whitespace bytes")
	}
	return token, nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func payloadDigest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

type goRuntime struct{}

func (goRuntime) Snapshot(startedAt, now time.Time) RuntimeSnapshot {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	snapshot := RuntimeSnapshot{
		APIVersion: APIVersion, CapturedAt: now, StartedAt: startedAt,
		UptimeMillis: now.Sub(startedAt).Milliseconds(), GoVersion: runtime.Version(),
		GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, PID: os.Getpid(),
		Goroutines: runtime.NumGoroutine(), HeapAllocBytes: memory.HeapAlloc,
		HeapObjects: memory.HeapObjects, GCCount: memory.NumGC,
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		snapshot.BuildPath = info.Path
		snapshot.BuildMainVersion = info.Main.Version
	}
	return snapshot
}

func (goRuntime) GoroutineStack(maxBytes int) StackCapture {
	size := 64 << 10
	if size > maxBytes {
		size = maxBytes
	}
	for {
		buffer := make([]byte, size)
		written := runtime.Stack(buffer, true)
		if written < len(buffer) {
			return StackCapture{Payload: buffer[:written]}
		}
		if size >= maxBytes {
			return StackCapture{Payload: buffer[:written], Truncated: true}
		}
		size *= 2
		if size > maxBytes {
			size = maxBytes
		}
	}
}
