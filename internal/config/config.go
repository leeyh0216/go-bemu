package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	APIVersion                     = "config.bqemu.dev/v1alpha1"
	Kind                           = "BQEMUConfig"
	maxConfigFileSize              = 1 << 20
	storageReadSystemMaxStreams    = 1_000
	storageReadGRPCEnvelopeReserve = 64 << 10
)

// The generated CreateReadSession contract documents a default system maximum
// of 1,000 streams. The envelope reserve covers protobuf framing around one
// bounded ReadRows payload and reference schema; both data bounds remain
// independently configurable and are checked against the gRPC send ceiling.
// Source: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#createreadsessionrequest
//
// The official API limits the complete AppendRowsRequest. The Java Storage
// client 3.22.1 pinned by connector 0.44.2 separately measures ProtoData when
// batching, while grpc-go receives the enclosing request. BQEMU therefore
// models the pinned client's payload bound and a configurable envelope bound;
// this split is a compatibility profile, not a redefinition of the API limit.
// Sources:
//   - https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#appendrowsrequest
//   - https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryDirectDataWriterHelper.java
//   - https://repo.maven.apache.org/maven2/com/google/cloud/google-cloud-bigquerystorage/3.22.1/google-cloud-bigquerystorage-3.22.1-sources.jar

// Duration uses Go duration strings on the wire, for example "5s" or "6h".
// Numeric values are rejected because their unit would be ambiguous.
type Duration time.Duration

func (d Duration) Value() time.Duration { return time.Duration(d) }

func (d Duration) MarshalYAML() (any, error) {
	return time.Duration(d).String(), nil
}

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return errors.New("duration must be a quoted unit-bearing string such as 5s")
	}
	parsed, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", node.Value, err)
	}
	*d = Duration(parsed)
	return nil
}

type Config struct {
	APIVersion string          `yaml:"apiVersion" json:"apiVersion"`
	Kind       string          `yaml:"kind" json:"kind"`
	Defaults   DefaultsConfig  `yaml:"defaults" json:"defaults"`
	Server     ServerConfig    `yaml:"server" json:"server"`
	Database   DatabaseConfig  `yaml:"database" json:"database"`
	Runtime    RuntimeConfig   `yaml:"runtime" json:"runtime"`
	Query      QueryConfig     `yaml:"query" json:"query"`
	TableData  TableDataConfig `yaml:"tableData" json:"tableData"`
	Storage    StorageConfig   `yaml:"storage" json:"storage"`
	Load       LoadConfig      `yaml:"load" json:"load"`
	Logging    LoggingConfig   `yaml:"logging" json:"logging"`
	Admin      AdminConfig     `yaml:"admin" json:"admin"`
	UI         UIConfig        `yaml:"ui" json:"ui"`
	Contracts  ContractsConfig `yaml:"contracts" json:"contracts"`
}

type DefaultsConfig struct {
	ProjectID string `yaml:"projectId" json:"projectId"`
	Location  string `yaml:"location" json:"location"`
}

type ServerConfig struct {
	HTTP HTTPConfig `yaml:"http" json:"http"`
	GRPC GRPCConfig `yaml:"grpc" json:"grpc"`
	TLS  TLSConfig  `yaml:"tls" json:"tls"`
}

type HTTPConfig struct {
	Address                     string   `yaml:"address" json:"address"`
	PublicURL                   string   `yaml:"publicUrl" json:"publicUrl"`
	ReadHeaderTimeout           Duration `yaml:"readHeaderTimeout" json:"readHeaderTimeout"`
	ReadTimeout                 Duration `yaml:"readTimeout" json:"readTimeout"`
	WriteTimeout                Duration `yaml:"writeTimeout" json:"writeTimeout"`
	IdleTimeout                 Duration `yaml:"idleTimeout" json:"idleTimeout"`
	MaxCompressedRequestBytes   int64    `yaml:"maxCompressedRequestBytes" json:"maxCompressedRequestBytes"`
	MaxUncompressedRequestBytes int64    `yaml:"maxUncompressedRequestBytes" json:"maxUncompressedRequestBytes"`
}

// Request byte budgets are independent because Content-Encoding is decoded at
// the public edge. In particular, chunked requests have no trustworthy
// ContentLength and must be bounded while they are read.
//
// Sources:
//   - Request body and ContentLength: https://pkg.go.dev/net/http#Request
//   - bounded server request bodies: https://pkg.go.dev/net/http#MaxBytesReader

type GRPCConfig struct {
	Address                string `yaml:"address" json:"address"`
	MaxReceiveMessageBytes int    `yaml:"maxReceiveMessageBytes" json:"maxReceiveMessageBytes"`
	MaxSendMessageBytes    int    `yaml:"maxSendMessageBytes" json:"maxSendMessageBytes"`
}

type TLSConfig struct {
	CertFile string `yaml:"certFile" json:"certFile"`
	KeyFile  string `yaml:"keyFile" json:"keyFile"`
}

type DatabaseConfig struct {
	Adapter       string `yaml:"adapter" json:"adapter"`
	DSN           string `yaml:"dsn" json:"dsn"`
	TempDirectory string `yaml:"tempDirectory" json:"tempDirectory"`
}

// RuntimeConfig divides the bounded process shutdown into a network drain
// phase followed by storage cleanup. The phase sum may not exceed the total,
// so an open streaming RPC cannot consume the cleanup budget implicitly.
//
// Sources:
//   - https://grpc.io/docs/guides/server-graceful-stop/
//   - https://pkg.go.dev/net/http#Server.Shutdown
type RuntimeConfig struct {
	ShutdownTimeout     Duration `yaml:"shutdownTimeout" json:"shutdownTimeout"`
	ServerDrainTimeout  Duration `yaml:"serverDrainTimeout" json:"serverDrainTimeout"`
	StorageCloseTimeout Duration `yaml:"storageCloseTimeout" json:"storageCloseTimeout"`
	JobPollInterval     Duration `yaml:"jobPollInterval" json:"jobPollInterval"`
	ReadSessionTTL      Duration `yaml:"readSessionTtl" json:"readSessionTtl"`
	CleanupInterval     Duration `yaml:"cleanupInterval" json:"cleanupInterval"`
}

// QueryConfig bounds server-owned query execution independently from an HTTP
// client's lifetime and controls the documented lifetime of anonymous result
// tables. QueryRequest.timeoutMs/jobTimeoutMs remain wire-level compatibility
// contracts layered on top of this local hard ceiling.
//
// Sources:
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#body.request_body.FIELDS.timeout_ms
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfiguration.FIELDS.job_timeout_ms
//   - https://cloud.google.com/bigquery/docs/cached-results#how_cached_results_are_stored
type QueryConfig struct {
	OperationTimeout    Duration `yaml:"operationTimeout" json:"operationTimeout"`
	CompensationTimeout Duration `yaml:"compensationTimeout" json:"compensationTimeout"`
	AnonymousResultTTL  Duration `yaml:"anonymousResultTtl" json:"anonymousResultTtl"`
}

// TableDataConfig bounds the REST tabledata.list adapter independently from
// the HTTP connection. BigQuery may return fewer rows than maxResults and caps
// one response at 100,000 rows. It normally paginates around 10 MB but can
// return one row up to 100 MB, so BQEMU exposes exact local JSON limits.
//
// Sources:
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
//   - https://cloud.google.com/bigquery/docs/paging-results#api-limits
//   - https://cloud.google.com/bigquery/quotas#api-limits
type TableDataConfig struct {
	OperationTimeout Duration `yaml:"operationTimeout" json:"operationTimeout"`
	MaxPageRows      int      `yaml:"maxPageRows" json:"maxPageRows"`
	MaxResponseBytes int64    `yaml:"maxResponseBytes" json:"maxResponseBytes"`
	MaxRowBytes      int64    `yaml:"maxRowBytes" json:"maxRowBytes"`
}

type StorageConfig struct {
	Read  StorageReadConfig  `yaml:"read" json:"read"`
	Write StorageWriteConfig `yaml:"write" json:"write"`
}

// StorageReadConfig bounds session negotiation, materialization, spill files,
// and each encoded response. The server may return fewer streams than the
// request's maximum; defaultStreamCount is used when the client supplies no
// useful preference, while maxStreams is the local admission ceiling.
//
// Official contracts:
//   - stream negotiation: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#createreadsessionrequest
//   - session lifetime: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readsession
//   - response size behavior: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readrows
type StorageReadConfig struct {
	Enabled               bool   `yaml:"enabled" json:"enabled"`
	MaxStreams            int    `yaml:"maxStreams" json:"maxStreams"`
	DefaultStreamCount    int    `yaml:"defaultStreamCount" json:"defaultStreamCount"`
	RowsPerResponse       int    `yaml:"rowsPerResponse" json:"rowsPerResponse"`
	MaxResponseBytes      int    `yaml:"maxResponseBytes" json:"maxResponseBytes"`
	MaxSchemaBytes        int    `yaml:"maxSchemaBytes" json:"maxSchemaBytes"`
	MaxSessions           int    `yaml:"maxSessions" json:"maxSessions"`
	SpillThresholdBytes   int64  `yaml:"spillThresholdBytes" json:"spillThresholdBytes"`
	MaxRowBytes           int64  `yaml:"maxRowBytes" json:"maxRowBytes"`
	MaxSnapshotBytes      int64  `yaml:"maxSnapshotBytes" json:"maxSnapshotBytes"`
	MaxTotalSnapshotBytes int64  `yaml:"maxTotalSnapshotBytes" json:"maxTotalSnapshotBytes"`
	MaxSnapshotRows       int64  `yaml:"maxSnapshotRows" json:"maxSnapshotRows"`
	TempFilePattern       string `yaml:"tempFilePattern" json:"tempFilePattern"`
	ProtocolModelVersion  string `yaml:"protocolModelVersion" json:"protocolModelVersion"`
}

// StorageWriteConfig separates the count-bounded coordinator queue from byte
// admission. PENDING streams have an official aggregate buffer quota and remain
// invisible until atomic BatchCommitWriteStreams, so both transient requests
// and durable local staging need explicit independent byte ceilings.
// Sources:
//   - https://cloud.google.com/bigquery/docs/write-api-batch
//   - https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.AppendRows
type StorageWriteConfig struct {
	Enabled                     bool     `yaml:"enabled" json:"enabled"`
	MaxStreams                  int      `yaml:"maxStreams" json:"maxStreams"`
	MaxAppendRequestBytes       int      `yaml:"maxAppendRequestBytes" json:"maxAppendRequestBytes"`
	MaxAppendEnvelopeBytes      int      `yaml:"maxAppendEnvelopeBytes" json:"maxAppendEnvelopeBytes"`
	MaxConcurrentAppendRequests int      `yaml:"maxConcurrentAppendRequests" json:"maxConcurrentAppendRequests"`
	QueueCapacity               int      `yaml:"queueCapacity" json:"queueCapacity"`
	QueueWaitTimeout            Duration `yaml:"queueWaitTimeout" json:"queueWaitTimeout"`
	OperationTimeout            Duration `yaml:"operationTimeout" json:"operationTimeout"`
	MaxInFlightBytes            int64    `yaml:"maxInFlightBytes" json:"maxInFlightBytes"`
	MaxInFlightBytesPerStream   int64    `yaml:"maxInFlightBytesPerStream" json:"maxInFlightBytesPerStream"`
	MaxStagedBytes              int64    `yaml:"maxStagedBytes" json:"maxStagedBytes"`
	MaxStagedBytesPerStream     int64    `yaml:"maxStagedBytesPerStream" json:"maxStagedBytesPerStream"`
	OrphanTTL                   Duration `yaml:"orphanTtl" json:"orphanTtl"`
	CleanupInterval             Duration `yaml:"cleanupInterval" json:"cleanupInterval"`
	ProtocolModelVersion        string   `yaml:"protocolModelVersion" json:"protocolModelVersion"`
}

// LoadConfig bounds every network and filesystem side effect of a load job.
// GCS sources use the JSON objects.get/list protocol; file sources are an
// explicit local-only opt-in because they can read host-mounted paths.
//
// Official contracts:
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad
//   - https://cloud.google.com/storage/docs/json_api/v1/objects/get
//   - https://cloud.google.com/storage/docs/json_api/v1/objects/list
type LoadConfig struct {
	Enabled          bool     `yaml:"enabled" json:"enabled"`
	GCSEndpoint      string   `yaml:"gcsEndpoint" json:"gcsEndpoint"`
	AllowFileSources bool     `yaml:"allowFileSources" json:"allowFileSources"`
	OperationTimeout Duration `yaml:"operationTimeout" json:"operationTimeout"`
	MaxObjects       int      `yaml:"maxObjects" json:"maxObjects"`
	MaxObjectBytes   int64    `yaml:"maxObjectBytes" json:"maxObjectBytes"`
	MaxTotalBytes    int64    `yaml:"maxTotalBytes" json:"maxTotalBytes"`
	MaxMetadataBytes int64    `yaml:"maxMetadataBytes" json:"maxMetadataBytes"`
	MaxListedObjects int      `yaml:"maxListedObjects" json:"maxListedObjects"`
}

type LoggingConfig struct {
	Level  string `yaml:"level" json:"level"`
	Format string `yaml:"format" json:"format"`
	// UnsafePayloads is a deprecated, parse-compatible no-op. It remains in the
	// v1alpha1 file/environment/CLI model so existing deployments keep loading,
	// but observability never emits raw SQL, rows, protobuf, HTTP bodies, error
	// text, or credentials. See the Cloud Logging security guidance:
	// https://cloud.google.com/logging/docs/audit/best-practices
	UnsafePayloads bool `yaml:"unsafePayloads" json:"unsafePayloads"`
}

type AdminConfig struct {
	Enabled           bool     `yaml:"enabled" json:"enabled"`
	Address           string   `yaml:"address" json:"address"`
	TokenFile         string   `yaml:"tokenFile" json:"tokenFile"`
	ReadHeaderTimeout Duration `yaml:"readHeaderTimeout" json:"readHeaderTimeout"`
	MaxStackBytes     int      `yaml:"maxStackBytes" json:"maxStackBytes"`
}

type UIConfig struct {
	Enabled   bool   `yaml:"enabled" json:"enabled"`
	Directory string `yaml:"directory" json:"directory"`
}

type ContractsConfig struct {
	ProfileDirectory string `yaml:"profileDirectory" json:"profileDirectory"`
}

type Result struct {
	Config               Config
	ConfigPath           string
	SourceFingerprint    string
	EffectiveYAML        []byte
	EffectiveFingerprint string
	PrintEffective       bool
}

// Error carries stable fields that can be emitted directly by CI or structured
// startup logging without exposing the configuration payload.
type Error struct {
	Stage       string
	Operation   string
	Field       string
	Shape       string
	Fingerprint string
	FixHint     string
	Err         error
}

func (e *Error) Error() string {
	return fmt.Sprintf("stage=%s operation=%s model_version=%s field=%s shape=%s fingerprint=%s fix_hint=%s: %v",
		e.Stage, e.Operation, APIVersion, e.Field, e.Shape, emptyAs(e.Fingerprint, "none"), e.FixHint, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

func Defaults() Config {
	return Config{
		APIVersion: APIVersion,
		Kind:       Kind,
		Defaults:   DefaultsConfig{ProjectID: "local-project", Location: "US"},
		Server: ServerConfig{
			HTTP: HTTPConfig{
				Address: ":9050", PublicURL: "http://localhost:9050",
				ReadHeaderTimeout: Duration(5 * time.Second), ReadTimeout: Duration(30 * time.Second),
				WriteTimeout: Duration(30 * time.Second), IdleTimeout: Duration(2 * time.Minute),
				MaxCompressedRequestBytes: 2 << 20, MaxUncompressedRequestBytes: 2 << 20,
			},
			GRPC: GRPCConfig{Address: ":9060", MaxReceiveMessageBytes: 32 << 20, MaxSendMessageBytes: 32 << 20},
		},
		Database: DatabaseConfig{Adapter: "duckdb", DSN: ":memory:", TempDirectory: os.TempDir()},
		Runtime: RuntimeConfig{
			ShutdownTimeout: Duration(10 * time.Second), ServerDrainTimeout: Duration(5 * time.Second),
			StorageCloseTimeout: Duration(4 * time.Second), JobPollInterval: Duration(100 * time.Millisecond),
			ReadSessionTTL: Duration(6 * time.Hour), CleanupInterval: Duration(time.Minute),
		},
		Query: QueryConfig{
			OperationTimeout: Duration(2 * time.Minute), CompensationTimeout: Duration(30 * time.Second),
			AnonymousResultTTL: Duration(24 * time.Hour),
		},
		TableData: TableDataConfig{
			OperationTimeout: Duration(30 * time.Second), MaxPageRows: 10_000,
			MaxResponseBytes: 10_000_000, MaxRowBytes: 100_000_000,
		},
		Storage: StorageConfig{
			Read: StorageReadConfig{
				Enabled: true, MaxStreams: 64, DefaultStreamCount: 4,
				RowsPerResponse: 10_000, MaxResponseBytes: 16 << 20, MaxSchemaBytes: 1 << 20, MaxSessions: 128,
				SpillThresholdBytes: 64 << 20, MaxRowBytes: 8 << 20,
				MaxSnapshotBytes: 512 << 20, MaxTotalSnapshotBytes: 4 << 30, MaxSnapshotRows: 10_000_000,
				TempFilePattern:      "bqemu-storage-read-*",
				ProtocolModelVersion: "google.cloud.bigquery.storage.v1+spark-bigquery-connector-0.44.2",
			},
			Write: StorageWriteConfig{
				Enabled: true, MaxStreams: 1_024, MaxAppendRequestBytes: 20 << 20, MaxAppendEnvelopeBytes: 64 << 10,
				MaxConcurrentAppendRequests: 16, QueueCapacity: 256,
				QueueWaitTimeout: Duration(5 * time.Second), OperationTimeout: Duration(30 * time.Second),
				MaxInFlightBytes: 256 << 20, MaxInFlightBytesPerStream: 32 << 20,
				MaxStagedBytes: 4 << 30, MaxStagedBytesPerStream: 512 << 20,
				OrphanTTL: Duration(6 * time.Hour), CleanupInterval: Duration(time.Minute),
				ProtocolModelVersion: "google.cloud.bigquery.storage.v1+spark-bigquery-connector-0.44.2",
			},
		},
		Load: LoadConfig{
			OperationTimeout: Duration(2 * time.Minute), MaxObjects: 1_000,
			MaxObjectBytes: 1 << 30, MaxTotalBytes: 4 << 30,
			MaxMetadataBytes: 8 << 20, MaxListedObjects: 10_000,
		},
		Logging: LoggingConfig{Level: "info", Format: "json"},
		Admin: AdminConfig{
			Address: "127.0.0.1:9051", ReadHeaderTimeout: Duration(5 * time.Second), MaxStackBytes: 4 << 20,
		},
		Contracts: ContractsConfig{ProfileDirectory: "contract/profiles"},
	}
}

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }
func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

// Load parses command-line arguments and process environment. The only
// required CLI surface is --config; repeated --set path=value makes every leaf
// setting overridable without growing a flag for each field.
func Load(args []string) (Result, error) {
	return load(args, os.LookupEnv)
}

func load(args []string, lookupEnv func(string) (string, bool)) (Result, error) {
	configPath, _ := lookupEnv("BQEMU_CONFIG")
	var overrides stringList
	var printEffective bool
	fs := flag.NewFlagSet("go-bemu", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&configPath, "config", configPath, "path to a BQEMU YAML configuration file")
	fs.Var(&overrides, "set", "override a configuration leaf as path=value; repeatable")
	fs.BoolVar(&printEffective, "print-effective-config", false, "validate and print the merged non-secret configuration")
	if err := fs.Parse(args); err != nil {
		return Result{}, configError("arguments", "parse-flags", "arguments", "flag-set", "check-command-line", "", err)
	}
	if fs.NArg() != 0 {
		return Result{}, configError("arguments", "parse-flags", "arguments", "flag-set", "remove-positional-arguments", "", fmt.Errorf("unexpected arguments: %v", fs.Args()))
	}

	cfg := Defaults()
	result := Result{ConfigPath: configPath, PrintEffective: printEffective}
	if configPath != "" {
		payload, err := readConfigFile(configPath)
		if err != nil {
			return Result{}, err
		}
		result.SourceFingerprint = digest(payload)
		if err := decodeStrict(payload, &cfg); err != nil {
			return Result{}, configError("configuration", "decode-yaml", configPath, "BQEMUConfig", "fix-config-shape", result.SourceFingerprint, err)
		}
	}

	for _, item := range environmentOverrides {
		if value, ok := lookupEnv(item.environment); ok && strings.TrimSpace(value) != "" {
			if err := applyOverride(&cfg, item.path, value); err != nil {
				return Result{}, configError("configuration", "apply-environment", item.path, "scalar", "fix-environment-value", result.SourceFingerprint, err)
			}
		}
	}
	for _, raw := range overrides {
		path, value, found := strings.Cut(raw, "=")
		if !found || strings.TrimSpace(path) == "" {
			return Result{}, configError("configuration", "apply-override", "--set", "path=value", "use-path-equals-value", result.SourceFingerprint, fmt.Errorf("invalid override %q", raw))
		}
		if err := applyOverride(&cfg, strings.TrimSpace(path), value); err != nil {
			return Result{}, configError("configuration", "apply-override", path, "scalar", "fix-override-value", result.SourceFingerprint, err)
		}
	}
	if err := cfg.Validate(); err != nil {
		return Result{}, configError("configuration", "validate", "effective-config", "BQEMUConfig", "fix-invalid-setting", result.SourceFingerprint, err)
	}
	effective, err := yaml.Marshal(cfg)
	if err != nil {
		return Result{}, configError("configuration", "encode-effective", "effective-config", "BQEMUConfig", "report-encoder-bug", result.SourceFingerprint, err)
	}
	result.Config = cfg
	result.EffectiveYAML = effective
	result.EffectiveFingerprint = digest(effective)
	return result, nil
}

func readConfigFile(path string) ([]byte, error) {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, configError("configuration", "open-file", path, "yaml-file", "mount-or-create-config-file", "", err)
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, maxConfigFileSize+1))
	if err != nil {
		return nil, configError("configuration", "read-file", path, "yaml-file", "check-config-file", "", err)
	}
	if len(payload) > maxConfigFileSize {
		return nil, configError("configuration", "read-file", path, "yaml-file", "reduce-config-below-1MiB", digest(payload), fmt.Errorf("configuration exceeds %d bytes", maxConfigFileSize))
	}
	return payload, nil
}

func decodeStrict(payload []byte, cfg *Config) error {
	decoder := yaml.NewDecoder(bytes.NewReader(payload))
	decoder.KnownFields(true)
	if err := decoder.Decode(cfg); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("multiple YAML documents are not supported")
		}
		return err
	}
	return nil
}

type environmentOverride struct {
	environment string
	path        string
}

var environmentOverrides = []environmentOverride{
	{"BQEMU_PROJECT", "defaults.projectId"}, {"BQEMU_LOCATION", "defaults.location"},
	{"BQEMU_HTTP_ADDRESS", "server.http.address"}, {"BQEMU_PUBLIC_URL", "server.http.publicUrl"},
	{"BQEMU_HTTP_MAX_COMPRESSED_REQUEST_BYTES", "server.http.maxCompressedRequestBytes"},
	{"BQEMU_HTTP_MAX_UNCOMPRESSED_REQUEST_BYTES", "server.http.maxUncompressedRequestBytes"},
	{"BQEMU_GRPC_ADDRESS", "server.grpc.address"}, {"BQEMU_TLS_CERT_FILE", "server.tls.certFile"},
	{"BQEMU_TLS_KEY_FILE", "server.tls.keyFile"}, {"BQEMU_DATABASE_ADAPTER", "database.adapter"},
	{"BQEMU_DATABASE_DSN", "database.dsn"}, {"BQEMU_TEMP_DIRECTORY", "database.tempDirectory"},
	{"BQEMU_QUERY_OPERATION_TIMEOUT", "query.operationTimeout"},
	{"BQEMU_QUERY_COMPENSATION_TIMEOUT", "query.compensationTimeout"},
	{"BQEMU_QUERY_ANONYMOUS_RESULT_TTL", "query.anonymousResultTtl"},
	{"BQEMU_TABLE_DATA_OPERATION_TIMEOUT", "tableData.operationTimeout"},
	{"BQEMU_TABLE_DATA_MAX_PAGE_ROWS", "tableData.maxPageRows"},
	{"BQEMU_TABLE_DATA_MAX_RESPONSE_BYTES", "tableData.maxResponseBytes"},
	{"BQEMU_TABLE_DATA_MAX_ROW_BYTES", "tableData.maxRowBytes"},
	{"BQEMU_LOAD_ENABLED", "load.enabled"}, {"BQEMU_LOAD_GCS_ENDPOINT", "load.gcsEndpoint"},
	{"BQEMU_LOAD_ALLOW_FILE_SOURCES", "load.allowFileSources"},
	{"BQEMU_LOAD_OPERATION_TIMEOUT", "load.operationTimeout"}, {"BQEMU_LOAD_MAX_OBJECTS", "load.maxObjects"},
	{"BQEMU_LOAD_MAX_OBJECT_BYTES", "load.maxObjectBytes"}, {"BQEMU_LOAD_MAX_TOTAL_BYTES", "load.maxTotalBytes"},
	{"BQEMU_LOAD_MAX_METADATA_BYTES", "load.maxMetadataBytes"}, {"BQEMU_LOAD_MAX_LISTED_OBJECTS", "load.maxListedObjects"},
	{"BQEMU_STORAGE_READ_ENABLED", "storage.read.enabled"},
	{"BQEMU_STORAGE_READ_MAX_STREAMS", "storage.read.maxStreams"},
	{"BQEMU_STORAGE_READ_DEFAULT_STREAM_COUNT", "storage.read.defaultStreamCount"},
	{"BQEMU_STORAGE_READ_ROWS_PER_RESPONSE", "storage.read.rowsPerResponse"},
	{"BQEMU_STORAGE_READ_MAX_RESPONSE_BYTES", "storage.read.maxResponseBytes"},
	{"BQEMU_STORAGE_READ_MAX_SCHEMA_BYTES", "storage.read.maxSchemaBytes"},
	{"BQEMU_STORAGE_READ_MAX_SESSIONS", "storage.read.maxSessions"},
	{"BQEMU_STORAGE_READ_SPILL_THRESHOLD_BYTES", "storage.read.spillThresholdBytes"},
	{"BQEMU_STORAGE_READ_MAX_ROW_BYTES", "storage.read.maxRowBytes"},
	{"BQEMU_STORAGE_READ_MAX_SNAPSHOT_BYTES", "storage.read.maxSnapshotBytes"},
	{"BQEMU_STORAGE_READ_MAX_TOTAL_SNAPSHOT_BYTES", "storage.read.maxTotalSnapshotBytes"},
	{"BQEMU_STORAGE_READ_MAX_SNAPSHOT_ROWS", "storage.read.maxSnapshotRows"},
	{"BQEMU_STORAGE_READ_TEMP_FILE_PATTERN", "storage.read.tempFilePattern"},
	{"BQEMU_STORAGE_READ_PROTOCOL_MODEL_VERSION", "storage.read.protocolModelVersion"},
	{"BQEMU_STORAGE_WRITE_MAX_STREAMS", "storage.write.maxStreams"},
	{"BQEMU_STORAGE_WRITE_ENABLED", "storage.write.enabled"},
	{"BQEMU_STORAGE_WRITE_MAX_APPEND_REQUEST_BYTES", "storage.write.maxAppendRequestBytes"},
	{"BQEMU_STORAGE_WRITE_MAX_APPEND_ENVELOPE_BYTES", "storage.write.maxAppendEnvelopeBytes"},
	{"BQEMU_STORAGE_WRITE_MAX_CONCURRENT_APPEND_REQUESTS", "storage.write.maxConcurrentAppendRequests"},
	{"BQEMU_STORAGE_WRITE_QUEUE_CAPACITY", "storage.write.queueCapacity"},
	{"BQEMU_STORAGE_WRITE_QUEUE_WAIT_TIMEOUT", "storage.write.queueWaitTimeout"},
	{"BQEMU_STORAGE_WRITE_OPERATION_TIMEOUT", "storage.write.operationTimeout"},
	{"BQEMU_STORAGE_WRITE_MAX_IN_FLIGHT_BYTES", "storage.write.maxInFlightBytes"},
	{"BQEMU_STORAGE_WRITE_MAX_IN_FLIGHT_BYTES_PER_STREAM", "storage.write.maxInFlightBytesPerStream"},
	{"BQEMU_STORAGE_WRITE_MAX_STAGED_BYTES", "storage.write.maxStagedBytes"},
	{"BQEMU_STORAGE_WRITE_MAX_STAGED_BYTES_PER_STREAM", "storage.write.maxStagedBytesPerStream"},
	{"BQEMU_STORAGE_WRITE_ORPHAN_TTL", "storage.write.orphanTtl"},
	{"BQEMU_STORAGE_WRITE_CLEANUP_INTERVAL", "storage.write.cleanupInterval"},
	{"BQEMU_STORAGE_WRITE_PROTOCOL_MODEL_VERSION", "storage.write.protocolModelVersion"},
	{"BQEMU_LOG_LEVEL", "logging.level"}, {"BQEMU_LOG_FORMAT", "logging.format"},
	{"BQEMU_LOG_UNSAFE_PAYLOADS", "logging.unsafePayloads"}, {"BQEMU_ADMIN_ENABLED", "admin.enabled"},
	{"BQEMU_ADMIN_ADDRESS", "admin.address"}, {"BQEMU_ADMIN_TOKEN_FILE", "admin.tokenFile"},
	{"BQEMU_UI_ENABLED", "ui.enabled"}, {"BQEMU_UI_DIRECTORY", "ui.directory"},
}

func applyOverride(cfg *Config, path, value string) error {
	setString := func(target *string) error { *target = value; return nil }
	setBool := func(target *bool) error {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return err
		}
		*target = parsed
		return nil
	}
	setInt := func(target *int) error {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		*target = parsed
		return nil
	}
	setInt64 := func(target *int64) error {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		*target = parsed
		return nil
	}
	setDuration := func(target *Duration) error {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return err
		}
		*target = Duration(parsed)
		return nil
	}

	switch path {
	case "defaults.projectId":
		return setString(&cfg.Defaults.ProjectID)
	case "defaults.location":
		return setString(&cfg.Defaults.Location)
	case "server.http.address":
		return setString(&cfg.Server.HTTP.Address)
	case "server.http.publicUrl":
		return setString(&cfg.Server.HTTP.PublicURL)
	case "server.http.readHeaderTimeout":
		return setDuration(&cfg.Server.HTTP.ReadHeaderTimeout)
	case "server.http.readTimeout":
		return setDuration(&cfg.Server.HTTP.ReadTimeout)
	case "server.http.writeTimeout":
		return setDuration(&cfg.Server.HTTP.WriteTimeout)
	case "server.http.idleTimeout":
		return setDuration(&cfg.Server.HTTP.IdleTimeout)
	case "server.http.maxCompressedRequestBytes":
		return setInt64(&cfg.Server.HTTP.MaxCompressedRequestBytes)
	case "server.http.maxUncompressedRequestBytes":
		return setInt64(&cfg.Server.HTTP.MaxUncompressedRequestBytes)
	case "server.grpc.address":
		return setString(&cfg.Server.GRPC.Address)
	case "server.grpc.maxReceiveMessageBytes":
		return setInt(&cfg.Server.GRPC.MaxReceiveMessageBytes)
	case "server.grpc.maxSendMessageBytes":
		return setInt(&cfg.Server.GRPC.MaxSendMessageBytes)
	case "server.tls.certFile":
		return setString(&cfg.Server.TLS.CertFile)
	case "server.tls.keyFile":
		return setString(&cfg.Server.TLS.KeyFile)
	case "database.adapter":
		return setString(&cfg.Database.Adapter)
	case "database.dsn":
		return setString(&cfg.Database.DSN)
	case "database.tempDirectory":
		return setString(&cfg.Database.TempDirectory)
	case "runtime.shutdownTimeout":
		return setDuration(&cfg.Runtime.ShutdownTimeout)
	case "runtime.serverDrainTimeout":
		return setDuration(&cfg.Runtime.ServerDrainTimeout)
	case "runtime.storageCloseTimeout":
		return setDuration(&cfg.Runtime.StorageCloseTimeout)
	case "runtime.jobPollInterval":
		return setDuration(&cfg.Runtime.JobPollInterval)
	case "runtime.readSessionTtl":
		return setDuration(&cfg.Runtime.ReadSessionTTL)
	case "runtime.cleanupInterval":
		return setDuration(&cfg.Runtime.CleanupInterval)
	case "query.operationTimeout":
		return setDuration(&cfg.Query.OperationTimeout)
	case "query.compensationTimeout":
		return setDuration(&cfg.Query.CompensationTimeout)
	case "query.anonymousResultTtl":
		return setDuration(&cfg.Query.AnonymousResultTTL)
	case "tableData.operationTimeout":
		return setDuration(&cfg.TableData.OperationTimeout)
	case "tableData.maxPageRows":
		return setInt(&cfg.TableData.MaxPageRows)
	case "tableData.maxResponseBytes":
		return setInt64(&cfg.TableData.MaxResponseBytes)
	case "tableData.maxRowBytes":
		return setInt64(&cfg.TableData.MaxRowBytes)
	case "storage.read.enabled":
		return setBool(&cfg.Storage.Read.Enabled)
	case "storage.read.maxStreams":
		return setInt(&cfg.Storage.Read.MaxStreams)
	case "storage.read.defaultStreamCount":
		return setInt(&cfg.Storage.Read.DefaultStreamCount)
	case "storage.read.rowsPerResponse":
		return setInt(&cfg.Storage.Read.RowsPerResponse)
	case "storage.read.maxResponseBytes":
		return setInt(&cfg.Storage.Read.MaxResponseBytes)
	case "storage.read.maxSchemaBytes":
		return setInt(&cfg.Storage.Read.MaxSchemaBytes)
	case "storage.read.maxSessions":
		return setInt(&cfg.Storage.Read.MaxSessions)
	case "storage.read.spillThresholdBytes":
		return setInt64(&cfg.Storage.Read.SpillThresholdBytes)
	case "storage.read.maxRowBytes":
		return setInt64(&cfg.Storage.Read.MaxRowBytes)
	case "storage.read.maxSnapshotBytes":
		return setInt64(&cfg.Storage.Read.MaxSnapshotBytes)
	case "storage.read.maxTotalSnapshotBytes":
		return setInt64(&cfg.Storage.Read.MaxTotalSnapshotBytes)
	case "storage.read.maxSnapshotRows":
		return setInt64(&cfg.Storage.Read.MaxSnapshotRows)
	case "storage.read.tempFilePattern":
		return setString(&cfg.Storage.Read.TempFilePattern)
	case "storage.read.protocolModelVersion":
		return setString(&cfg.Storage.Read.ProtocolModelVersion)
	case "storage.write.maxStreams":
		return setInt(&cfg.Storage.Write.MaxStreams)
	case "storage.write.enabled":
		return setBool(&cfg.Storage.Write.Enabled)
	case "storage.write.maxAppendRequestBytes":
		return setInt(&cfg.Storage.Write.MaxAppendRequestBytes)
	case "storage.write.maxAppendEnvelopeBytes":
		return setInt(&cfg.Storage.Write.MaxAppendEnvelopeBytes)
	case "storage.write.maxConcurrentAppendRequests":
		return setInt(&cfg.Storage.Write.MaxConcurrentAppendRequests)
	case "storage.write.queueCapacity":
		return setInt(&cfg.Storage.Write.QueueCapacity)
	case "storage.write.queueWaitTimeout":
		return setDuration(&cfg.Storage.Write.QueueWaitTimeout)
	case "storage.write.operationTimeout":
		return setDuration(&cfg.Storage.Write.OperationTimeout)
	case "storage.write.maxInFlightBytes":
		return setInt64(&cfg.Storage.Write.MaxInFlightBytes)
	case "storage.write.maxInFlightBytesPerStream":
		return setInt64(&cfg.Storage.Write.MaxInFlightBytesPerStream)
	case "storage.write.maxStagedBytes":
		return setInt64(&cfg.Storage.Write.MaxStagedBytes)
	case "storage.write.maxStagedBytesPerStream":
		return setInt64(&cfg.Storage.Write.MaxStagedBytesPerStream)
	case "storage.write.orphanTtl":
		return setDuration(&cfg.Storage.Write.OrphanTTL)
	case "storage.write.cleanupInterval":
		return setDuration(&cfg.Storage.Write.CleanupInterval)
	case "storage.write.protocolModelVersion":
		return setString(&cfg.Storage.Write.ProtocolModelVersion)
	case "load.enabled":
		return setBool(&cfg.Load.Enabled)
	case "load.gcsEndpoint":
		return setString(&cfg.Load.GCSEndpoint)
	case "load.allowFileSources":
		return setBool(&cfg.Load.AllowFileSources)
	case "load.operationTimeout":
		return setDuration(&cfg.Load.OperationTimeout)
	case "load.maxObjects":
		return setInt(&cfg.Load.MaxObjects)
	case "load.maxObjectBytes":
		return setInt64(&cfg.Load.MaxObjectBytes)
	case "load.maxTotalBytes":
		return setInt64(&cfg.Load.MaxTotalBytes)
	case "load.maxMetadataBytes":
		return setInt64(&cfg.Load.MaxMetadataBytes)
	case "load.maxListedObjects":
		return setInt(&cfg.Load.MaxListedObjects)
	case "logging.level":
		return setString(&cfg.Logging.Level)
	case "logging.format":
		return setString(&cfg.Logging.Format)
	case "logging.unsafePayloads":
		return setBool(&cfg.Logging.UnsafePayloads)
	case "admin.enabled":
		return setBool(&cfg.Admin.Enabled)
	case "admin.address":
		return setString(&cfg.Admin.Address)
	case "admin.tokenFile":
		return setString(&cfg.Admin.TokenFile)
	case "admin.readHeaderTimeout":
		return setDuration(&cfg.Admin.ReadHeaderTimeout)
	case "admin.maxStackBytes":
		return setInt(&cfg.Admin.MaxStackBytes)
	case "ui.enabled":
		return setBool(&cfg.UI.Enabled)
	case "ui.directory":
		return setString(&cfg.UI.Directory)
	case "contracts.profileDirectory":
		return setString(&cfg.Contracts.ProfileDirectory)
	default:
		return fmt.Errorf("unknown configuration path %q", path)
	}
}

func (cfg Config) Validate() error {
	if cfg.APIVersion != APIVersion {
		return fmt.Errorf("apiVersion must be %q", APIVersion)
	}
	if cfg.Kind != Kind {
		return fmt.Errorf("kind must be %q", Kind)
	}
	if strings.TrimSpace(cfg.Defaults.ProjectID) == "" {
		return errors.New("defaults.projectId is required")
	}
	if strings.TrimSpace(cfg.Defaults.Location) == "" {
		return errors.New("defaults.location is required")
	}
	if err := validateAddress("server.http.address", cfg.Server.HTTP.Address); err != nil {
		return err
	}
	if err := validateAddress("server.grpc.address", cfg.Server.GRPC.Address); err != nil {
		return err
	}
	parsedURL, err := url.Parse(cfg.Server.HTTP.PublicURL)
	if err != nil || parsedURL.Host == "" || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") {
		return fmt.Errorf("server.http.publicUrl must be an absolute HTTP(S) URL")
	}
	if (cfg.Server.TLS.CertFile == "") != (cfg.Server.TLS.KeyFile == "") {
		return errors.New("server.tls.certFile and server.tls.keyFile must be configured together")
	}
	if cfg.Server.HTTP.MaxCompressedRequestBytes < 1 || cfg.Server.HTTP.MaxUncompressedRequestBytes < 1 {
		return errors.New("server.http request byte limits must be positive")
	}
	for name, value := range map[string]Duration{
		"server.http.readHeaderTimeout": cfg.Server.HTTP.ReadHeaderTimeout,
		"server.http.readTimeout":       cfg.Server.HTTP.ReadTimeout,
		"server.http.writeTimeout":      cfg.Server.HTTP.WriteTimeout,
		"server.http.idleTimeout":       cfg.Server.HTTP.IdleTimeout,
		"runtime.shutdownTimeout":       cfg.Runtime.ShutdownTimeout,
		"runtime.serverDrainTimeout":    cfg.Runtime.ServerDrainTimeout,
		"runtime.storageCloseTimeout":   cfg.Runtime.StorageCloseTimeout,
		"runtime.jobPollInterval":       cfg.Runtime.JobPollInterval,
		"runtime.readSessionTtl":        cfg.Runtime.ReadSessionTTL,
		"runtime.cleanupInterval":       cfg.Runtime.CleanupInterval,
		"query.operationTimeout":        cfg.Query.OperationTimeout,
		"query.compensationTimeout":     cfg.Query.CompensationTimeout,
		"query.anonymousResultTtl":      cfg.Query.AnonymousResultTTL,
		"tableData.operationTimeout":    cfg.TableData.OperationTimeout,
		"load.operationTimeout":         cfg.Load.OperationTimeout,
		"storage.write.orphanTtl":       cfg.Storage.Write.OrphanTTL,
		"storage.write.cleanupInterval": cfg.Storage.Write.CleanupInterval,
	} {
		if value.Value() <= 0 {
			return fmt.Errorf("%s must be positive", name)
		}
	}
	if cfg.TableData.MaxPageRows < 1 || cfg.TableData.MaxPageRows > 100_000 {
		return errors.New("tableData.maxPageRows must be between 1 and the BigQuery tabledata.list limit of 100000")
	}
	if cfg.TableData.MaxResponseBytes < 1_024 || cfg.TableData.MaxResponseBytes > 10_000_000 {
		return errors.New("tableData.maxResponseBytes must be between the local minimum envelope budget of 1024 and the BigQuery tabledata.list pagination threshold of 10000000")
	}
	if cfg.TableData.MaxRowBytes < cfg.TableData.MaxResponseBytes || cfg.TableData.MaxRowBytes > 100_000_000 {
		return errors.New("tableData.maxRowBytes must be at least maxResponseBytes and at most the BigQuery tabledata.list single-row limit of 100000000")
	}
	if cfg.Runtime.ServerDrainTimeout.Value()+cfg.Runtime.StorageCloseTimeout.Value() > cfg.Runtime.ShutdownTimeout.Value() {
		return errors.New("runtime serverDrainTimeout plus storageCloseTimeout must not exceed shutdownTimeout")
	}
	if cfg.Server.GRPC.MaxReceiveMessageBytes < 1<<20 || cfg.Server.GRPC.MaxSendMessageBytes < 1<<20 {
		return errors.New("gRPC message limits must be at least 1 MiB")
	}
	if cfg.Database.Adapter != "duckdb" {
		return fmt.Errorf("database.adapter %q is not registered", cfg.Database.Adapter)
	}
	if strings.TrimSpace(cfg.Database.DSN) == "" {
		return errors.New("database.dsn is required")
	}
	if strings.TrimSpace(cfg.Database.TempDirectory) == "" {
		return errors.New("database.tempDirectory is required")
	}
	if cfg.Storage.Read.MaxStreams < 1 || cfg.Storage.Read.MaxStreams > storageReadSystemMaxStreams ||
		cfg.Storage.Read.DefaultStreamCount < 1 || cfg.Storage.Read.DefaultStreamCount > cfg.Storage.Read.MaxStreams {
		return fmt.Errorf("storage.read stream counts must be positive, defaultStreamCount must not exceed maxStreams, and maxStreams must not exceed the protocol system max %d", storageReadSystemMaxStreams)
	}
	if cfg.Storage.Read.RowsPerResponse < 1 || cfg.Storage.Read.MaxResponseBytes < 1<<20 || cfg.Storage.Read.MaxSchemaBytes < 1 ||
		cfg.Storage.Read.MaxSessions < 1 || cfg.Storage.Read.SpillThresholdBytes < 0 ||
		cfg.Storage.Read.MaxRowBytes < 1 || cfg.Storage.Read.MaxSnapshotBytes < 1 ||
		cfg.Storage.Read.MaxTotalSnapshotBytes < 1 || cfg.Storage.Read.MaxSnapshotRows < 1 {
		return errors.New("storage.read row/session limits must be positive, spillThresholdBytes non-negative, and maxResponseBytes at least 1 MiB")
	}
	if cfg.Storage.Read.MaxRowBytes > int64(cfg.Storage.Read.MaxResponseBytes) {
		return errors.New("storage.read.maxRowBytes must not exceed maxResponseBytes")
	}
	if cfg.Storage.Read.MaxRowBytes > cfg.Storage.Read.MaxSnapshotBytes {
		return errors.New("storage.read.maxRowBytes must not exceed maxSnapshotBytes")
	}
	if cfg.Storage.Read.MaxSnapshotBytes > cfg.Storage.Read.MaxTotalSnapshotBytes {
		return errors.New("storage.read.maxSnapshotBytes must not exceed maxTotalSnapshotBytes")
	}
	minimumSendBytes := int64(cfg.Storage.Read.MaxResponseBytes) + int64(cfg.Storage.Read.MaxSchemaBytes) + storageReadGRPCEnvelopeReserve
	if int64(cfg.Server.GRPC.MaxSendMessageBytes) < minimumSendBytes {
		return fmt.Errorf("server.grpc.maxSendMessageBytes must be at least %d for the configured Storage Read payload, schema, and envelope reserve", minimumSendBytes)
	}
	if pattern := strings.TrimSpace(cfg.Storage.Read.TempFilePattern); pattern == "" || filepath.Base(pattern) != pattern || pattern == "." {
		return errors.New("storage.read.tempFilePattern must be a non-empty filename pattern without directory separators")
	}
	if strings.TrimSpace(cfg.Storage.Read.ProtocolModelVersion) == "" {
		return errors.New("storage.read.protocolModelVersion is required")
	}
	if cfg.Storage.Write.MaxStreams < 1 || cfg.Storage.Write.QueueCapacity < 1 || cfg.Storage.Write.MaxAppendEnvelopeBytes < 1 ||
		cfg.Storage.Write.MaxConcurrentAppendRequests < 1 ||
		cfg.Storage.Write.MaxAppendRequestBytes < 1<<20 || cfg.Storage.Write.MaxAppendRequestBytes > 20<<20 {
		return errors.New("storage.write stream, queue, and envelope limits must be positive and maxAppendRequestBytes between 1 MiB and 20 MiB")
	}
	if cfg.Storage.Write.QueueWaitTimeout.Value() <= 0 || cfg.Storage.Write.OperationTimeout.Value() <= 0 {
		return errors.New("storage.write queueWaitTimeout and operationTimeout must be positive")
	}
	appendBytes := int64(cfg.Storage.Write.MaxAppendRequestBytes)
	envelopeBytes := int64(cfg.Storage.Write.MaxAppendEnvelopeBytes)
	if envelopeBytes > math.MaxInt64-appendBytes {
		return errors.New("storage.write append payload and envelope byte limits overflow int64")
	}
	minimumReceiveBytes := appendBytes + envelopeBytes
	if int64(cfg.Server.GRPC.MaxReceiveMessageBytes) < minimumReceiveBytes {
		return fmt.Errorf("server.grpc.maxReceiveMessageBytes must be at least %d for the configured Storage Write ProtoData payload and envelope maximum", minimumReceiveBytes)
	}
	if cfg.Storage.Write.MaxInFlightBytesPerStream < minimumReceiveBytes ||
		cfg.Storage.Write.MaxInFlightBytes < cfg.Storage.Write.MaxInFlightBytesPerStream {
		return errors.New("storage.write byte limits must satisfy maxAppendRequestBytes + maxAppendEnvelopeBytes <= maxInFlightBytesPerStream <= maxInFlightBytes")
	}
	if cfg.Storage.Write.MaxStagedBytesPerStream < appendBytes ||
		cfg.Storage.Write.MaxStagedBytes < cfg.Storage.Write.MaxStagedBytesPerStream {
		return errors.New("storage.write byte limits must satisfy maxAppendRequestBytes <= maxStagedBytesPerStream <= maxStagedBytes")
	}
	if strings.TrimSpace(cfg.Storage.Write.ProtocolModelVersion) == "" {
		return errors.New("storage.write.protocolModelVersion is required")
	}
	if cfg.Load.MaxObjects < 1 || cfg.Load.MaxObjectBytes < 1 || cfg.Load.MaxTotalBytes < 1 ||
		cfg.Load.MaxMetadataBytes < 1 || cfg.Load.MaxListedObjects < 1 {
		return errors.New("load object and byte limits must be positive")
	}
	if cfg.Load.MaxObjectBytes > cfg.Load.MaxTotalBytes {
		return errors.New("load.maxObjectBytes must not exceed load.maxTotalBytes")
	}
	if cfg.Load.Enabled {
		endpoint, err := url.Parse(cfg.Load.GCSEndpoint)
		if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
			return errors.New("load.gcsEndpoint must be an absolute HTTP(S) URL when load is enabled")
		}
	}
	if !oneOf(cfg.Logging.Level, "debug", "info", "warn", "error") {
		return fmt.Errorf("unsupported logging.level %q", cfg.Logging.Level)
	}
	if !oneOf(cfg.Logging.Format, "json", "text") {
		return fmt.Errorf("unsupported logging.format %q", cfg.Logging.Format)
	}
	if cfg.Admin.Enabled {
		if err := validateAddress("admin.address", cfg.Admin.Address); err != nil {
			return err
		}
		host, _, _ := net.SplitHostPort(cfg.Admin.Address)
		if !isLoopbackHost(host) {
			if cfg.Admin.TokenFile == "" {
				return errors.New("admin.tokenFile is required when admin.address is not loopback-only")
			}
			if cfg.Server.TLS.CertFile == "" {
				return errors.New("server TLS is required when admin.address is not loopback-only")
			}
		}
		if cfg.Admin.ReadHeaderTimeout.Value() <= 0 {
			return errors.New("admin.readHeaderTimeout must be positive")
		}
		if cfg.Admin.MaxStackBytes < 64<<10 || cfg.Admin.MaxStackBytes > 64<<20 {
			return errors.New("admin.maxStackBytes must be between 64 KiB and 64 MiB")
		}
	}
	if cfg.UI.Enabled && strings.TrimSpace(cfg.UI.Directory) == "" {
		return errors.New("ui.directory is required when UI is enabled")
	}
	if strings.TrimSpace(cfg.Contracts.ProfileDirectory) == "" {
		return errors.New("contracts.profileDirectory is required")
	}
	return nil
}

func validateAddress(field, address string) error {
	if strings.TrimSpace(address) == "" {
		return fmt.Errorf("%s is required", field)
	}
	if _, _, err := net.SplitHostPort(address); err != nil {
		return fmt.Errorf("%s must be host:port: %w", field, err)
	}
	return nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func oneOf(value string, allowed ...string) bool {
	for _, item := range allowed {
		if value == item {
			return true
		}
	}
	return false
}

func digest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func configError(stage, operation, field, shape, fixHint, fingerprint string, err error) error {
	return &Error{Stage: stage, Operation: operation, Field: field, Shape: shape, FixHint: fixHint, Fingerprint: fingerprint, Err: err}
}

func emptyAs(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
