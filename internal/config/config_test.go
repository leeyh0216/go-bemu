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
tableData:
  maxResponseBytes: 7654321
  maxRowBytes: 87654321
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
	if result.Config.TableData.MaxResponseBytes != 7_654_321 || result.Config.TableData.MaxRowBytes != 87_654_321 {
		t.Fatalf("file table data byte policy = %#v", result.Config.TableData)
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

func TestPublicAuthenticationConfigurationIsNotPartOfRuntimeContract(t *testing.T) {
	if _, err := load([]string{"--set", "auth.mode=disabled"}, lookup(nil)); err == nil ||
		!strings.Contains(err.Error(), `unknown configuration path "auth.mode"`) {
		t.Fatalf("removed CLI authentication setting error = %v", err)
	}

	path := writeConfig(t, `
apiVersion: config.bqemu.dev/v1alpha1
kind: BQEMUConfig
auth:
  mode: disabled
`)
	if _, err := load([]string{"--config", path}, lookup(nil)); err == nil ||
		!strings.Contains(err.Error(), "decode-yaml") {
		t.Fatalf("removed YAML authentication setting error = %v", err)
	}

	result, err := load(nil, lookup(map[string]string{"BQEMU_AUTH_MODE": "static"}))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(result.EffectiveYAML), "\nauth:") {
		t.Fatalf("effective configuration retained public authentication: %s", result.EffectiveYAML)
	}
}

func TestValidateProtectsRemoteAdminAndAppendBounds(t *testing.T) {
	for name, override := range map[string][]string{
		"remote-admin": {"--set", "admin.enabled=true", "--set", "admin.address=0.0.0.0:9051"},
		"remote-admin-with-token-but-no-tls": {
			"--set", "admin.enabled=true", "--set", "admin.address=0.0.0.0:9051",
			"--set", "admin.tokenFile=token.txt",
		},
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
		"server.http.maxCompressedRequestBytes=1048576", "server.http.maxUncompressedRequestBytes=2097152",
		"server.grpc.address=127.0.0.1:19060", "server.grpc.maxReceiveMessageBytes=2097152", "server.grpc.maxSendMessageBytes=3145728",
		"server.tls.certFile=cert.pem", "server.tls.keyFile=key.pem",
		"database.adapter=duckdb", "database.dsn=:memory:", "database.tempDirectory=/tmp",
		"state.dsn=/tmp/bqemu-state.sqlite",
		"runtime.shutdownTimeout=9s", "runtime.serverDrainTimeout=4s", "runtime.storageCloseTimeout=4s",
		"runtime.jobPollInterval=6ms", "runtime.readSessionTtl=7m", "runtime.cleanupInterval=8s",
		"query.operationTimeout=45s", "query.compensationTimeout=10s", "query.anonymousResultTtl=12h",
		"tableData.operationTimeout=11s", "tableData.maxPageRows=1234",
		"tableData.maxResponseBytes=1048576", "tableData.maxRowBytes=2097152",
		"storage.read.enabled=true", "storage.read.maxStreams=16", "storage.read.defaultStreamCount=4",
		"storage.read.rowsPerResponse=100", "storage.read.maxResponseBytes=1048576", "storage.read.maxSchemaBytes=1048576",
		"storage.read.maxSessions=8",
		"storage.read.spillThresholdBytes=524288", "storage.read.maxRowBytes=524288",
		"storage.read.maxSnapshotBytes=1048576", "storage.read.maxTotalSnapshotBytes=8388608",
		"storage.read.maxSnapshotRows=1000",
		"storage.read.tempFilePattern=read-*", "storage.read.protocolModelVersion=test-read-v1",
		"storage.write.enabled=true", "storage.write.maxStreams=16", "storage.write.maxAppendRequestBytes=1048576",
		"storage.write.maxAppendEnvelopeBytes=65536",
		"storage.write.maxConcurrentAppendRequests=4",
		"storage.write.queueCapacity=8", "storage.write.queueWaitTimeout=2s", "storage.write.operationTimeout=20s",
		"storage.write.maxInFlightBytes=4194304", "storage.write.maxInFlightBytesPerStream=2097152",
		"storage.write.maxStagedBytes=8388608", "storage.write.maxStagedBytesPerStream=4194304",
		"storage.write.orphanTtl=2h", "storage.write.cleanupInterval=30s",
		"storage.write.protocolModelVersion=test-storage-v1",
		"load.enabled=true", "load.gcsEndpoint=http://fake-gcs:4443", "load.allowFileSources=true",
		"load.operationTimeout=30s", "load.maxObjects=20", "load.maxObjectBytes=1048576",
		"load.maxTotalBytes=2097152", "load.maxMetadataBytes=65536", "load.maxListedObjects=30",
		"logging.level=debug", "logging.format=text", "logging.unsafePayloads=true",
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

func TestStateDSNLoadsIndependentlyFromEngineDatabase(t *testing.T) {
	result, err := load(nil, lookup(map[string]string{
		"BQEMU_DATABASE_DSN": ":memory:",
		"BQEMU_STATE_DSN":    "/state/bqemu.sqlite",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Config.Database.DSN != ":memory:" || result.Config.State.DSN != "/state/bqemu.sqlite" {
		t.Fatalf("database/state DSNs = %#v / %#v", result.Config.Database, result.Config.State)
	}
	if _, err := load([]string{"--set", "state.dsn="}, lookup(nil)); err == nil ||
		!strings.Contains(err.Error(), "state.dsn is required") {
		t.Fatalf("empty state DSN error = %v", err)
	}
}

func TestQueryPolicyLoadsFromEnvironmentAndRejectsNonPositiveValues(t *testing.T) {
	result, err := load(nil, lookup(map[string]string{
		"BQEMU_QUERY_OPERATION_TIMEOUT":    "45s",
		"BQEMU_QUERY_COMPENSATION_TIMEOUT": "10s",
		"BQEMU_QUERY_ANONYMOUS_RESULT_TTL": "12h",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Config.Query.OperationTimeout.Value() != 45*time.Second ||
		result.Config.Query.CompensationTimeout.Value() != 10*time.Second ||
		result.Config.Query.AnonymousResultTTL.Value() != 12*time.Hour {
		t.Fatalf("query policy = %#v", result.Config.Query)
	}
	for _, path := range []string{"query.operationTimeout", "query.compensationTimeout", "query.anonymousResultTtl"} {
		if _, err := load([]string{"--set", path + "=0s"}, lookup(nil)); err == nil {
			t.Fatalf("expected positive-duration validation for %s", path)
		}
	}
}

func TestTableDataPolicyLoadsFromEnvironmentAndRejectsInvalidValues(t *testing.T) {
	result, err := load(nil, lookup(map[string]string{
		"BQEMU_TABLE_DATA_OPERATION_TIMEOUT":  "11s",
		"BQEMU_TABLE_DATA_MAX_PAGE_ROWS":      "1234",
		"BQEMU_TABLE_DATA_MAX_RESPONSE_BYTES": "1048576",
		"BQEMU_TABLE_DATA_MAX_ROW_BYTES":      "2097152",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Config.TableData.OperationTimeout.Value() != 11*time.Second || result.Config.TableData.MaxPageRows != 1234 ||
		result.Config.TableData.MaxResponseBytes != 1_048_576 || result.Config.TableData.MaxRowBytes != 2_097_152 {
		t.Fatalf("table data policy = %#v", result.Config.TableData)
	}
	for _, override := range []string{
		"tableData.operationTimeout=0s",
		"tableData.maxPageRows=0",
		"tableData.maxPageRows=100001",
		"tableData.maxResponseBytes=0",
		"tableData.maxResponseBytes=1023",
		"tableData.maxResponseBytes=10000001",
		"tableData.maxRowBytes=9999999",
		"tableData.maxRowBytes=100000001",
	} {
		if _, err := load([]string{"--set", override}, lookup(nil)); err == nil {
			t.Fatalf("expected table data validation failure for %s", override)
		}
	}
}

func TestShutdownPhasesMustFitConfiguredTotal(t *testing.T) {
	_, err := load([]string{
		"--set", "runtime.shutdownTimeout=5s",
		"--set", "runtime.serverDrainTimeout=3s",
		"--set", "runtime.storageCloseTimeout=3s",
	}, lookup(nil))
	if err == nil || !strings.Contains(err.Error(), "must not exceed") {
		t.Fatalf("shutdown phase validation error = %v", err)
	}
}

func TestHTTPRequestByteLimitsLoadFromEnvironment(t *testing.T) {
	result, err := load(nil, lookup(map[string]string{
		"BQEMU_HTTP_MAX_COMPRESSED_REQUEST_BYTES":   "1048576",
		"BQEMU_HTTP_MAX_UNCOMPRESSED_REQUEST_BYTES": "4194304",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Config.Server.HTTP.MaxCompressedRequestBytes != 1<<20 ||
		result.Config.Server.HTTP.MaxUncompressedRequestBytes != 4<<20 {
		t.Fatalf("unexpected HTTP request byte limits: %#v", result.Config.Server.HTTP)
	}
}

func TestHTTPRequestByteLimitsLoadFromYAMLThenCLI(t *testing.T) {
	path := writeConfig(t, `
apiVersion: config.bqemu.dev/v1alpha1
kind: BQEMUConfig
server:
  http:
    maxCompressedRequestBytes: 1048576
    maxUncompressedRequestBytes: 2097152
`)
	result, err := load([]string{
		"--config", path,
		"--set", "server.http.maxUncompressedRequestBytes=4194304",
	}, lookup(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Config.Server.HTTP.MaxCompressedRequestBytes != 1<<20 ||
		result.Config.Server.HTTP.MaxUncompressedRequestBytes != 4<<20 {
		t.Fatalf("unexpected file/CLI HTTP request byte limits: %#v", result.Config.Server.HTTP)
	}
}

func TestHTTPRequestByteLimitsMustBePositive(t *testing.T) {
	for _, path := range []string{"server.http.maxCompressedRequestBytes", "server.http.maxUncompressedRequestBytes"} {
		t.Run(path, func(t *testing.T) {
			if _, err := load([]string{"--set", path + "=0"}, lookup(nil)); err == nil {
				t.Fatal("expected HTTP request byte limit validation error")
			}
		})
	}
}

func TestStorageReadSnapshotByteLimitsLoadFromEnvironment(t *testing.T) {
	result, err := load(nil, lookup(map[string]string{
		"BQEMU_STORAGE_READ_MAX_SNAPSHOT_BYTES":       "268435456",
		"BQEMU_STORAGE_READ_MAX_TOTAL_SNAPSHOT_BYTES": "1073741824",
	}))
	if err != nil {
		t.Fatal(err)
	}
	read := result.Config.Storage.Read
	if read.MaxSnapshotBytes != 256<<20 || read.MaxTotalSnapshotBytes != 1<<30 {
		t.Fatalf("unexpected Storage Read snapshot byte limits: %#v", read)
	}
}

func TestStorageWriteByteLimitsLoadFromEnvironment(t *testing.T) {
	result, err := load(nil, lookup(map[string]string{
		"BQEMU_STORAGE_WRITE_QUEUE_WAIT_TIMEOUT":             "3s",
		"BQEMU_STORAGE_WRITE_OPERATION_TIMEOUT":              "45s",
		"BQEMU_STORAGE_WRITE_MAX_APPEND_ENVELOPE_BYTES":      "131072",
		"BQEMU_STORAGE_WRITE_MAX_CONCURRENT_APPEND_REQUESTS": "8",
		"BQEMU_STORAGE_WRITE_MAX_IN_FLIGHT_BYTES":            "67108864",
		"BQEMU_STORAGE_WRITE_MAX_IN_FLIGHT_BYTES_PER_STREAM": "33554432",
		"BQEMU_STORAGE_WRITE_MAX_STAGED_BYTES":               "1073741824",
		"BQEMU_STORAGE_WRITE_MAX_STAGED_BYTES_PER_STREAM":    "536870912",
	}))
	if err != nil {
		t.Fatal(err)
	}
	write := result.Config.Storage.Write
	if write.QueueWaitTimeout.Value() != 3*time.Second || write.OperationTimeout.Value() != 45*time.Second ||
		write.MaxAppendEnvelopeBytes != 128<<10 || write.MaxConcurrentAppendRequests != 8 ||
		write.MaxInFlightBytes != 64<<20 || write.MaxInFlightBytesPerStream != 32<<20 ||
		write.MaxStagedBytes != 1<<30 || write.MaxStagedBytesPerStream != 512<<20 {
		t.Fatalf("unexpected Storage Write byte limits: %#v", write)
	}
}

func TestStorageWriteByteLimitRelationshipsAreValidated(t *testing.T) {
	for name, override := range map[string][]string{
		"grpc-receive-below-append-envelope": {"--set", "server.grpc.maxReceiveMessageBytes=20971520"},
		"zero-append-envelope":               {"--set", "storage.write.maxAppendEnvelopeBytes=0"},
		"zero-concurrent-appends":            {"--set", "storage.write.maxConcurrentAppendRequests=0"},
		"zero-queue-wait":                    {"--set", "storage.write.queueWaitTimeout=0s"},
		"zero-operation-timeout":             {"--set", "storage.write.operationTimeout=0s"},
		"in-flight-per-stream-below-append":  {"--set", "storage.write.maxInFlightBytesPerStream=1048576"},
		"in-flight-per-stream-below-wire":    {"--set", "storage.write.maxInFlightBytesPerStream=20971520"},
		"in-flight-global-below-per-stream":  {"--set", "storage.write.maxInFlightBytes=16777216"},
		"staged-per-stream-below-append":     {"--set", "storage.write.maxStagedBytesPerStream=1048576"},
		"staged-global-below-per-stream":     {"--set", "storage.write.maxStagedBytes=268435456"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := load(override, lookup(nil)); err == nil {
				t.Fatal("expected Storage Write byte limit validation error")
			}
		})
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

func TestStorageReadConfigurationRejectsUnsafeOrInconsistentLimits(t *testing.T) {
	for name, overrides := range map[string][]string{
		"default-over-max": {
			"--set", "storage.read.maxStreams=2", "--set", "storage.read.defaultStreamCount=4",
		},
		"over-system-stream-max": {"--set", "storage.read.maxStreams=1001"},
		"row-over-response": {
			"--set", "storage.read.maxResponseBytes=1048576", "--set", "storage.read.maxRowBytes=1048577",
		},
		"negative-spill":      {"--set", "storage.read.spillThresholdBytes=-1"},
		"zero-snapshot-bytes": {"--set", "storage.read.maxSnapshotBytes=0"},
		"snapshot-over-total": {
			"--set", "storage.read.maxSnapshotBytes=2097152", "--set", "storage.read.maxTotalSnapshotBytes=1048576",
		},
		"zero-snapshot": {"--set", "storage.read.maxSnapshotRows=0"},
		"path-pattern":  {"--set", "storage.read.tempFilePattern=outside/read-*"},
		"missing-model": {"--set", "storage.read.protocolModelVersion="},
		"send-envelope": {
			"--set", "server.grpc.maxSendMessageBytes=1048576",
			"--set", "storage.read.maxResponseBytes=1048576",
			"--set", "storage.read.maxSchemaBytes=1",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := load(overrides, lookup(nil)); err == nil {
				t.Fatal("expected Storage Read configuration validation error")
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
