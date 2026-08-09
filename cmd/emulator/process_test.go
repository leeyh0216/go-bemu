package main

import (
	"bytes"
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

	binary := filepath.Join(directory, "bqemu")
	build := exec.Command("go", "build", "-o", binary, ".")
	build.Dir = "."
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build command: %v\n%s", err, output)
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
