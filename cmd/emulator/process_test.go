package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// TestProcessSIGTERMDrainsListeners starts the public command, rather than the
// bootstrap package directly, so the signal-to-exit boundary remains covered.
func TestProcessSIGTERMDrainsListeners(t *testing.T) {
	directory := t.TempDir()
	httpAddress := processUnusedLoopbackAddress(t)
	grpcAddress := processUnusedLoopbackAddress(t)
	configPath := filepath.Join(directory, "bqemu.yaml")
	config := fmt.Sprintf(`
apiVersion: config.bqemu.dev/v1alpha1
kind: BQEMUConfig
defaults:
  projectId: process-test
  location: US
server:
  http:
    address: %q
    publicUrl: %q
  grpc:
    address: %q
database:
  adapter: duckdb
  dsn: %q
  tempDirectory: %q
state:
  dsn: %q
runtime:
  shutdownTimeout: "5s"
  serverDrainTimeout: "1s"
  storageCloseTimeout: "2s"
storage:
  read:
    enabled: false
  write:
    enabled: false
logging:
  level: error
  format: text
`, httpAddress, "http://"+httpAddress, grpcAddress,
		filepath.Join(directory, "engine.duckdb"), filepath.Join(directory, "tmp"), filepath.Join(directory, "state.sqlite"))
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}

	binary := buildProcessBinary(t, directory)
	command := exec.Command(binary, "--config", configPath)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil || !command.ProcessState.Exited() {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	processWaitReady(t, "http://"+httpAddress+"/readyz", command, &output)
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("SIGTERM exit: %v\n%s", err, output.String())
	}
	for _, address := range []string{httpAddress, grpcAddress} {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatalf("listener %s leaked after SIGTERM: %v\n%s", address, err, output.String())
		}
		_ = listener.Close()
	}

	// Force the second listener to fail after the public HTTP listener was
	// acquired. The command must return an error and release its first
	// listener before process exit.
	failingHTTP := processUnusedLoopbackAddress(t)
	failingGRPC := processUnusedLoopbackAddress(t)
	reservedGRPC, err := net.Listen("tcp", failingGRPC)
	if err != nil {
		t.Fatal(err)
	}
	defer reservedGRPC.Close()
	failingConfigPath := filepath.Join(directory, "listener-failure.yaml")
	failingConfig := fmt.Sprintf(`
apiVersion: config.bqemu.dev/v1alpha1
kind: BQEMUConfig
defaults:
  projectId: process-test
  location: US
server:
  http:
    address: %q
    publicUrl: %q
  grpc:
    address: %q
database:
  adapter: duckdb
  dsn: %q
  tempDirectory: %q
state:
  dsn: %q
storage:
  read:
    enabled: false
  write:
    enabled: false
logging:
  level: error
  format: text
`, failingHTTP, "http://"+failingHTTP, failingGRPC,
		filepath.Join(directory, "failing-engine.duckdb"), filepath.Join(directory, "failing-tmp"), filepath.Join(directory, "failing-state.sqlite"))
	if err := os.WriteFile(failingConfigPath, []byte(failingConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	failure := exec.Command(binary, "--config", failingConfigPath)
	failureOutput, failureErr := failure.CombinedOutput()
	if failureErr == nil {
		t.Fatalf("listener failure process unexpectedly succeeded: %s", failureOutput)
	}
	listener, err := net.Listen("tcp", failingHTTP)
	if err != nil {
		t.Fatalf("public listener leaked after gRPC startup failure: %v\n%s", err, failureOutput)
	}
	_ = listener.Close()
}

// TestProcessIdleShutdownHonorsConfiguredPhaseBudgets keeps the three
// documented shutdown budgets connected to the executable boundary.  The
// individual closer tests establish repeatability; this test verifies that an
// idle public process can consume each valid budget combination, exit cleanly,
// and release both public transports without relying on an in-process caller.
func TestProcessIdleShutdownHonorsConfiguredPhaseBudgets(t *testing.T) {
	directory := t.TempDir()
	binary := buildProcessBinary(t, directory)
	for _, budget := range []struct {
		name, shutdown, drain, storage string
	}{
		{name: "short", shutdown: "750ms", drain: "150ms", storage: "150ms"},
		{name: "balanced", shutdown: "2s", drain: "500ms", storage: "750ms"},
		{name: "startup-fallback-dominates", shutdown: "3s", drain: "1s", storage: "1s"},
	} {
		t.Run(budget.name, func(t *testing.T) {
			httpAddress := processUnusedLoopbackAddress(t)
			grpcAddress := processUnusedLoopbackAddress(t)
			configPath := filepath.Join(directory, budget.name+".yaml")
			config := fmt.Sprintf(`
apiVersion: config.bqemu.dev/v1alpha1
kind: BQEMUConfig
defaults:
  projectId: process-test
  location: US
server:
  http:
    address: %q
    publicUrl: %q
  grpc:
    address: %q
database:
  adapter: duckdb
  dsn: %q
  tempDirectory: %q
state:
  dsn: %q
runtime:
  shutdownTimeout: %q
  serverDrainTimeout: %q
  storageCloseTimeout: %q
storage:
  read:
    enabled: false
  write:
    enabled: false
logging:
  level: error
  format: text
`, httpAddress, "http://"+httpAddress, grpcAddress,
				filepath.Join(directory, budget.name+".duckdb"), filepath.Join(directory, budget.name+"-tmp"), filepath.Join(directory, budget.name+".sqlite"),
				budget.shutdown, budget.drain, budget.storage)
			if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
				t.Fatal(err)
			}

			command := exec.Command(binary, "--config", configPath)
			var output bytes.Buffer
			command.Stdout = &output
			command.Stderr = &output
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if command.ProcessState == nil || !command.ProcessState.Exited() {
					_ = command.Process.Kill()
					_ = command.Wait()
				}
			})
			processWaitReady(t, "http://"+httpAddress+"/readyz", command, &output)

			started := time.Now()
			if err := command.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			if err := command.Wait(); err != nil {
				t.Fatalf("idle shutdown: %v\n%s", err, output.String())
			}
			if elapsed := time.Since(started); elapsed > 2*time.Second {
				t.Fatalf("idle shutdown took %s with budgets shutdown=%s drain=%s storage=%s\n%s", elapsed, budget.shutdown, budget.drain, budget.storage, output.String())
			}
			for _, address := range []string{httpAddress, grpcAddress} {
				listener, err := net.Listen("tcp", address)
				if err != nil {
					t.Fatalf("listener %s leaked after idle shutdown: %v\n%s", address, err, output.String())
				}
				_ = listener.Close()
			}
		})
	}
}

// TestProcessRejectsRemoteAdminBeforeListeners verifies the public command
// cannot briefly expose its normal listeners when an unsafe diagnostics
// listener configuration is rejected during startup validation.
func TestProcessRejectsRemoteAdminBeforeListeners(t *testing.T) {
	directory := t.TempDir()
	httpAddress := processUnusedLoopbackAddress(t)
	grpcAddress := processUnusedLoopbackAddress(t)
	binary := buildProcessBinary(t, directory)
	command := exec.Command(binary,
		"--set", "server.http.address="+httpAddress,
		"--set", "server.grpc.address="+grpcAddress,
		"--set", "admin.enabled=true",
		"--set", "admin.address=0.0.0.0:19051",
	)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("unsafe remote admin process unexpectedly succeeded: %s", output)
	}
	for _, address := range []string{httpAddress, grpcAddress} {
		listener, listenErr := net.Listen("tcp", address)
		if listenErr != nil {
			t.Fatalf("listener %s opened despite rejected remote admin: %v\n%s", address, listenErr, output)
		}
		_ = listener.Close()
	}
}

// TestProcessDrainDeadlineClosesListenersWithAStalledPublicRequest exercises
// the executable's forced-drain path.  A request that has been admitted but
// has not finished sending its declared body must not keep the process or its
// public listener alive past serverDrainTimeout.
func TestProcessDrainDeadlineClosesListenersWithAStalledPublicRequest(t *testing.T) {
	directory := t.TempDir()
	httpAddress := processUnusedLoopbackAddress(t)
	grpcAddress := processUnusedLoopbackAddress(t)
	configPath := filepath.Join(directory, "bqemu.yaml")
	config := fmt.Sprintf(`
apiVersion: config.bqemu.dev/v1alpha1
kind: BQEMUConfig
defaults:
  projectId: process-test
  location: US
server:
  http:
    address: %q
    publicUrl: %q
  grpc:
    address: %q
database:
  adapter: duckdb
  dsn: %q
  tempDirectory: %q
state:
  dsn: %q
runtime:
  shutdownTimeout: "2s"
  serverDrainTimeout: "150ms"
  storageCloseTimeout: "500ms"
storage:
  read:
    enabled: false
  write:
    enabled: false
logging:
  level: error
  format: text
`, httpAddress, "http://"+httpAddress, grpcAddress,
		filepath.Join(directory, "engine.duckdb"), filepath.Join(directory, "tmp"), filepath.Join(directory, "state.sqlite"))
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}

	binary := buildProcessBinary(t, directory)
	command := exec.Command(binary, "--config", configPath)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil || !command.ProcessState.Exited() {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	processWaitReady(t, "http://"+httpAddress+"/readyz", command, &output)

	connection, err := net.DialTimeout("tcp", httpAddress, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := fmt.Fprintf(connection, "POST /bigquery/v2/projects/process-test/jobs HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: 1048576\r\n\r\n{", httpAddress); err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	// A second signal during a graceful drain must not bypass the documented
	// bounded drain path. signal.NotifyContext continues to own the signal until
	// Run returns, so this validates the executable contract rather than merely
	// a cancellable in-process context.
	time.Sleep(25 * time.Millisecond)
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	waitContext, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	waitResult := make(chan error, 1)
	go func() { waitResult <- command.Wait() }()
	select {
	case err := <-waitResult:
		if err == nil {
			t.Fatalf("drain deadline process unexpectedly succeeded: %s", output.String())
		}
		if elapsed := time.Since(started); elapsed < 100*time.Millisecond || elapsed > 2*time.Second {
			t.Fatalf("drain deadline exit took %s, want the configured bounded drain\n%s", elapsed, output.String())
		}
	case <-waitContext.Done():
		t.Fatalf("process did not exit after drain deadline: %s", output.String())
	}
	for _, address := range []string{httpAddress, grpcAddress} {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatalf("listener %s leaked after drain deadline: %v\n%s", address, err, output.String())
		}
		_ = listener.Close()
	}
}

// TestProcessDrainDeadlineClosesListenersWithAnActiveGRPCWatch covers the
// other public transport: a health Watch is deliberately long-lived, so a
// graceful gRPC stop must obey serverDrainTimeout and force-release the
// listener once the deadline expires.
func TestProcessDrainDeadlineClosesListenersWithAnActiveGRPCWatch(t *testing.T) {
	directory := t.TempDir()
	httpAddress := processUnusedLoopbackAddress(t)
	grpcAddress := processUnusedLoopbackAddress(t)
	configPath := filepath.Join(directory, "bqemu.yaml")
	config := fmt.Sprintf(`
apiVersion: config.bqemu.dev/v1alpha1
kind: BQEMUConfig
defaults:
  projectId: process-test
  location: US
server:
  http:
    address: %q
    publicUrl: %q
  grpc:
    address: %q
database:
  adapter: duckdb
  dsn: %q
  tempDirectory: %q
state:
  dsn: %q
runtime:
  shutdownTimeout: "2s"
  serverDrainTimeout: "150ms"
  storageCloseTimeout: "500ms"
storage:
  read:
    enabled: false
  write:
    enabled: false
logging:
  level: error
  format: text
`, httpAddress, "http://"+httpAddress, grpcAddress,
		filepath.Join(directory, "engine.duckdb"), filepath.Join(directory, "tmp"), filepath.Join(directory, "state.sqlite"))
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}

	binary := buildProcessBinary(t, directory)
	command := exec.Command(binary, "--config", configPath)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil || !command.ProcessState.Exited() {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	processWaitReady(t, "http://"+httpAddress+"/readyz", command, &output)

	connection, err := grpc.NewClient(grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	watch, err := grpc_health_v1.NewHealthClient(connection).Watch(t.Context(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := watch.Recv(); err != nil {
		t.Fatalf("receive initial health watch status: %v", err)
	}

	started := time.Now()
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	waitContext, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	waitResult := make(chan error, 1)
	go func() { waitResult <- command.Wait() }()
	select {
	case err := <-waitResult:
		if err == nil {
			t.Fatalf("gRPC drain deadline process unexpectedly succeeded: %s", output.String())
		}
		if elapsed := time.Since(started); elapsed > 2*time.Second {
			t.Fatalf("gRPC drain deadline exit took %s, want bounded exit\n%s", elapsed, output.String())
		}
	case <-waitContext.Done():
		t.Fatalf("process did not exit after gRPC drain deadline: %s", output.String())
	}
	for _, address := range []string{httpAddress, grpcAddress} {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatalf("listener %s leaked after gRPC drain deadline: %v\n%s", address, err, output.String())
		}
		_ = listener.Close()
	}
}

// TestProcessCleanShutdownRetainsMountedStateOnRestart verifies the
// executable, rather than an in-process bootstrap caller, flushes persistent
// state before a clean SIGTERM exit. The test directory models the mounted
// DuckDB and SQLite volume paths used by Compose deployments.
func TestProcessCleanShutdownRetainsMountedStateOnRestart(t *testing.T) {
	directory := t.TempDir()
	httpAddress := processUnusedLoopbackAddress(t)
	grpcAddress := processUnusedLoopbackAddress(t)
	baseURL := "http://" + httpAddress
	configPath := filepath.Join(directory, "bqemu.yaml")
	config := fmt.Sprintf(`
apiVersion: config.bqemu.dev/v1alpha1
kind: BQEMUConfig
defaults:
  projectId: process-test
  location: US
server:
  http:
    address: %q
    publicUrl: %q
  grpc:
    address: %q
database:
  adapter: duckdb
  dsn: %q
  tempDirectory: %q
state:
  dsn: %q
runtime:
  shutdownTimeout: "5s"
  serverDrainTimeout: "1s"
  storageCloseTimeout: "2s"
storage:
  read:
    enabled: false
  write:
    enabled: false
logging:
  level: error
  format: text
`, httpAddress, baseURL, grpcAddress,
		filepath.Join(directory, "mounted-engine.duckdb"), filepath.Join(directory, "tmp"), filepath.Join(directory, "mounted-state.sqlite"))
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	binary := buildProcessBinary(t, directory)

	start := func() (*exec.Cmd, *bytes.Buffer) {
		t.Helper()
		command := exec.Command(binary, "--config", configPath)
		output := &bytes.Buffer{}
		command.Stdout = output
		command.Stderr = output
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		processWaitReady(t, baseURL+"/readyz", command, output)
		return command, output
	}
	stop := func(command *exec.Cmd, output *bytes.Buffer) {
		t.Helper()
		if err := command.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		if err := command.Wait(); err != nil {
			t.Fatalf("clean process shutdown: %v\n%s", err, output.String())
		}
	}

	first, firstOutput := start()
	t.Cleanup(func() {
		if first.ProcessState == nil || !first.ProcessState.Exited() {
			_ = first.Process.Kill()
			_ = first.Wait()
		}
	})
	processJSONRequest(t, http.MethodPost, baseURL+"/bigquery/v2/projects/process-test/jobs", `{
  "jobReference":{"projectId":"process-test","jobId":"persisted-query","location":"US"},
  "configuration":{"query":{"query":"SELECT 42 AS answer","useLegacySql":false}}
}`, http.StatusOK)
	var completed map[string]any
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		completed = processJSONRequest(t, http.MethodGet, baseURL+"/bigquery/v2/projects/process-test/jobs/persisted-query?location=US", "", http.StatusOK)
		if completed["status"].(map[string]any)["state"] == "DONE" {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if completed["status"].(map[string]any)["state"] != "DONE" {
		t.Fatalf("persisted job did not finish before restart: %#v", completed)
	}
	stop(first, firstOutput)

	second, secondOutput := start()
	t.Cleanup(func() {
		if second.ProcessState == nil || !second.ProcessState.Exited() {
			_ = second.Process.Kill()
			_ = second.Wait()
		}
	})
	restarted := processJSONRequest(t, http.MethodGet, baseURL+"/bigquery/v2/projects/process-test/jobs/persisted-query?location=US", "", http.StatusOK)
	if restarted["status"].(map[string]any)["state"] != "DONE" ||
		restarted["configuration"].(map[string]any)["query"].(map[string]any)["query"] != "SELECT 42 AS answer" {
		t.Fatalf("persisted job after process restart = %#v", restarted)
	}
	stop(second, secondOutput)
}

// TestProcessStateStartupFailureReleasesResources verifies that a failure
// after DuckDB composition still releases the engine before the executable
// exits, and that no public address was bound on the failed startup path.
func TestProcessStateStartupFailureReleasesResources(t *testing.T) {
	directory := t.TempDir()
	httpAddress := processUnusedLoopbackAddress(t)
	grpcAddress := processUnusedLoopbackAddress(t)
	stateParent := filepath.Join(directory, "state-parent-file")
	if err := os.WriteFile(stateParent, []byte("not-a-directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(directory, "bqemu.yaml")
	config := fmt.Sprintf(`
apiVersion: config.bqemu.dev/v1alpha1
kind: BQEMUConfig
defaults:
  projectId: process-test
  location: US
server:
  http:
    address: %q
    publicUrl: %q
  grpc:
    address: %q
database:
  adapter: duckdb
  dsn: %q
  tempDirectory: %q
state:
  dsn: %q
storage:
  read:
    enabled: false
  write:
    enabled: false
logging:
  level: error
  format: text
`, httpAddress, "http://"+httpAddress, grpcAddress,
		filepath.Join(directory, "engine.duckdb"), filepath.Join(directory, "tmp"), filepath.Join(stateParent, "state.sqlite"))
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	binary := buildProcessBinary(t, directory)
	failure := exec.Command(binary, "--config", configPath)
	output, err := failure.CombinedOutput()
	if err == nil {
		t.Fatalf("state startup failure process unexpectedly succeeded: %s", output)
	}
	for _, address := range []string{httpAddress, grpcAddress} {
		listener, listenErr := net.Listen("tcp", address)
		if listenErr != nil {
			t.Fatalf("listener %s leaked after state startup failure: %v\n%s", address, listenErr, output)
		}
		_ = listener.Close()
	}
}

// TestProcessTLSStartupFailurePrecedesPublicListeners keeps certificate
// identity loading in the startup transaction: an unreadable TLS identity must
// fail only after composition and still before either public address is bound.
func TestProcessTLSStartupFailurePrecedesPublicListeners(t *testing.T) {
	directory := t.TempDir()
	httpAddress := processUnusedLoopbackAddress(t)
	grpcAddress := processUnusedLoopbackAddress(t)
	configPath := filepath.Join(directory, "bqemu.yaml")
	config := fmt.Sprintf(`
apiVersion: config.bqemu.dev/v1alpha1
kind: BQEMUConfig
defaults:
  projectId: process-test
  location: US
server:
  http:
    address: %q
    publicUrl: %q
  grpc:
    address: %q
  tls:
    certFile: %q
    keyFile: %q
database:
  adapter: duckdb
  dsn: %q
  tempDirectory: %q
state:
  dsn: %q
storage:
  read:
    enabled: false
  write:
    enabled: false
logging:
  level: error
  format: text
`, httpAddress, "http://"+httpAddress, grpcAddress,
		filepath.Join(directory, "missing-server.pem"), filepath.Join(directory, "missing-server-key.pem"),
		filepath.Join(directory, "engine.duckdb"), filepath.Join(directory, "tmp"), filepath.Join(directory, "state.sqlite"))
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	binary := buildProcessBinary(t, directory)
	failure := exec.Command(binary, "--config", configPath)
	output, err := failure.CombinedOutput()
	if err == nil {
		t.Fatalf("TLS startup failure process unexpectedly succeeded: %s", output)
	}
	for _, address := range []string{httpAddress, grpcAddress} {
		listener, listenErr := net.Listen("tcp", address)
		if listenErr != nil {
			t.Fatalf("listener %s leaked after TLS startup failure: %v\n%s", address, listenErr, output)
		}
		_ = listener.Close()
	}
}

func buildProcessBinary(t *testing.T, directory string) string {
	t.Helper()
	binary := filepath.Join(directory, "bqemu")
	build := exec.Command("go", "build", "-o", binary, ".")
	build.Dir = "."
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build command: %v\n%s", err, output)
	}
	return binary
}

func processWaitReady(t *testing.T, url string, command *exec.Cmd, output *bytes.Buffer) {
	t.Helper()
	client := &http.Client{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get(url)
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		if command.ProcessState != nil && command.ProcessState.Exited() {
			t.Fatalf("process stopped before readiness: %s", output.String())
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("process did not become ready: %s", output.String())
}

func processJSONRequest(t *testing.T, method, url, body string, wantStatus int) map[string]any {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), method, url, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != wantStatus {
		t.Fatalf("%s %s: status=%d want=%d body=%s", method, url, response.StatusCode, wantStatus, payload)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode %s %s response: %v; body=%s", method, url, err, payload)
	}
	return decoded
}

func processUnusedLoopbackAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}
