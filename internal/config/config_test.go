package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadPrecedenceAndEffectiveFingerprint(t *testing.T) {
	path := writeConfig(t, `
apiVersion: config.bqemu.dev/v1alpha1
kind: BQEMUConfig
defaults:
  projectId: from-file
server:
  http:
    address: ":19050"
logging:
  level: warn
`)
	environment := map[string]string{
		"BQEMU_CONFIG":    path,
		"BQEMU_PROJECT":   "from-environment",
		"BQEMU_LOG_LEVEL": "error",
	}
	result, err := load([]string{"--set", "defaults.projectId=from-cli", "--set", "runtime.shutdownTimeout=17s"}, lookup(environment))
	if err != nil {
		t.Fatal(err)
	}
	if result.Config.Defaults.ProjectID != "from-cli" {
		t.Fatalf("project = %q", result.Config.Defaults.ProjectID)
	}
	if result.Config.Server.HTTP.Address != ":19050" {
		t.Fatalf("HTTP address = %q", result.Config.Server.HTTP.Address)
	}
	if result.Config.Logging.Level != "error" {
		t.Fatalf("level = %q", result.Config.Logging.Level)
	}
	if result.Config.Runtime.ShutdownTimeout.Value() != 17*time.Second {
		t.Fatalf("shutdown timeout = %s", result.Config.Runtime.ShutdownTimeout.Value())
	}
	if !strings.HasPrefix(result.SourceFingerprint, "sha256:") || !strings.HasPrefix(result.EffectiveFingerprint, "sha256:") {
		t.Fatalf("missing fingerprints: %#v", result)
	}
	if !strings.Contains(string(result.EffectiveYAML), "projectId: from-cli") {
		t.Fatalf("effective YAML = %s", result.EffectiveYAML)
	}
}

func TestLoadRejectsUnknownFieldsWithStructuredHint(t *testing.T) {
	path := writeConfig(t, `
apiVersion: config.bqemu.dev/v1alpha1
kind: BQEMUConfig
server:
  http:
    adress: ":9050"
`)
	_, err := load([]string{"--config", path}, lookup(nil))
	if err == nil {
		t.Fatal("expected unknown-field error")
	}
	var configErr *Error
	if !errors.As(err, &configErr) {
		t.Fatalf("error type = %T", err)
	}
	for _, marker := range []string{"operation=decode-yaml", "model_version=" + APIVersion, "shape=BQEMUConfig", "fix_hint=fix-config-shape", "fingerprint=sha256:"} {
		if !strings.Contains(err.Error(), marker) {
			t.Fatalf("error %q lacks %q", err, marker)
		}
	}
}

func TestLoadRejectsMultipleDocumentsAndAmbiguousDuration(t *testing.T) {
	for name, body := range map[string]string{
		"documents": "apiVersion: config.bqemu.dev/v1alpha1\nkind: BQEMUConfig\n---\nkind: Other\n",
		"duration":  "apiVersion: config.bqemu.dev/v1alpha1\nkind: BQEMUConfig\nruntime:\n  shutdownTimeout: 5\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := load([]string{"--config", writeConfig(t, body)}, lookup(nil))
			if err == nil {
				t.Fatal("expected decode error")
			}
		})
	}
}

func TestValidateProtectsRemoteAdminAndStaticAuth(t *testing.T) {
	for name, override := range map[string][]string{
		"remote-admin": {"--set", "admin.enabled=true", "--set", "admin.address=0.0.0.0:9051"},
		"remote-admin-with-token-but-no-tls": {
			"--set", "admin.enabled=true", "--set", "admin.address=0.0.0.0:9051",
			"--set", "admin.tokenFile=token.txt",
		},
		"static-auth":     {"--set", "auth.mode=static"},
		"oversize-append": {"--set", "storage.write.maxAppendRequestBytes=20971521"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := load(override, lookup(nil))
			if err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestEveryLeafOverrideIsTyped(t *testing.T) {
	overrides := []string{
		"defaults.projectId=p", "defaults.location=EU",
		"server.http.address=127.0.0.1:19050", "server.http.publicUrl=https://localhost:19050",
		"server.http.readHeaderTimeout=1s", "server.http.readTimeout=2s", "server.http.writeTimeout=3s", "server.http.idleTimeout=4s",
		"server.grpc.address=127.0.0.1:19060", "server.grpc.maxReceiveMessageBytes=1048576", "server.grpc.maxSendMessageBytes=1048576",
		"server.tls.certFile=cert.pem", "server.tls.keyFile=key.pem",
		"database.adapter=duckdb", "database.dsn=:memory:", "database.tempDirectory=/tmp",
		"runtime.shutdownTimeout=5s", "runtime.jobPollInterval=6ms", "runtime.readSessionTtl=7m", "runtime.cleanupInterval=8s",
		"storage.read.maxStreams=16", "storage.read.rowsPerResponse=100", "storage.read.maxResponseBytes=1048576",
		"storage.write.maxStreams=16", "storage.write.maxAppendRequestBytes=1048576",
		"load.enabled=true", "load.gcsEndpoint=http://fake-gcs:4443", "load.allowFileSources=true",
		"load.operationTimeout=30s", "load.maxObjects=20", "load.maxObjectBytes=1048576",
		"load.maxTotalBytes=2097152", "load.maxMetadataBytes=65536", "load.maxListedObjects=30",
		"auth.mode=static", "auth.staticTokensFile=tokens.txt", "logging.level=debug", "logging.format=text", "logging.unsafePayloads=true",
		"admin.enabled=true", "admin.address=0.0.0.0:19051", "admin.tokenFile=admin-token", "admin.readHeaderTimeout=2s", "admin.maxStackBytes=65536",
		"ui.enabled=true", "ui.directory=web/dist",
		"contracts.profileDirectory=contract/profiles",
	}
	args := make([]string, 0, len(overrides)*2)
	for _, item := range overrides {
		args = append(args, "--set", item)
	}
	if _, err := load(args, lookup(nil)); err != nil {
		t.Fatal(err)
	}
}

func TestLoadConfigurationRequiresExplicitEndpointAndPositiveBounds(t *testing.T) {
	for name, overrides := range map[string][]string{
		"missing-endpoint": {"--set", "load.enabled=true"},
		"relative-endpoint": {
			"--set", "load.enabled=true", "--set", "load.gcsEndpoint=fake-gcs:4443",
		},
		"object-over-total": {
			"--set", "load.maxObjectBytes=2048", "--set", "load.maxTotalBytes=1024",
		},
		"zero-list-limit": {"--set", "load.maxListedObjects=0"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := load(overrides, lookup(nil)); err == nil {
				t.Fatal("expected load configuration validation error")
			}
		})
	}
}

func TestConfigFileSizeIsBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large.yaml")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", maxConfigFileSize+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := load([]string{"--config", path}, lookup(nil))
	if err == nil || !strings.Contains(err.Error(), "reduce-config-below-1MiB") {
		t.Fatalf("error = %v", err)
	}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bqemu.yaml")
	if err := os.WriteFile(path, []byte(strings.TrimSpace(body)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func lookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) { value, ok := values[key]; return value, ok }
}
