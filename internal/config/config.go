package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
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
	APIVersion        = "config.bqemu.dev/v1alpha1"
	Kind              = "BQEMUConfig"
	maxConfigFileSize = 1 << 20
)

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
	Storage    StorageConfig   `yaml:"storage" json:"storage"`
	Load       LoadConfig      `yaml:"load" json:"load"`
	Auth       AuthConfig      `yaml:"auth" json:"auth"`
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
	Address           string   `yaml:"address" json:"address"`
	PublicURL         string   `yaml:"publicUrl" json:"publicUrl"`
	ReadHeaderTimeout Duration `yaml:"readHeaderTimeout" json:"readHeaderTimeout"`
	ReadTimeout       Duration `yaml:"readTimeout" json:"readTimeout"`
	WriteTimeout      Duration `yaml:"writeTimeout" json:"writeTimeout"`
	IdleTimeout       Duration `yaml:"idleTimeout" json:"idleTimeout"`
}

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

type RuntimeConfig struct {
	ShutdownTimeout Duration `yaml:"shutdownTimeout" json:"shutdownTimeout"`
	JobPollInterval Duration `yaml:"jobPollInterval" json:"jobPollInterval"`
	ReadSessionTTL  Duration `yaml:"readSessionTtl" json:"readSessionTtl"`
	CleanupInterval Duration `yaml:"cleanupInterval" json:"cleanupInterval"`
}

type StorageConfig struct {
	Read  StorageReadConfig  `yaml:"read" json:"read"`
	Write StorageWriteConfig `yaml:"write" json:"write"`
}

type StorageReadConfig struct {
	MaxStreams       int `yaml:"maxStreams" json:"maxStreams"`
	RowsPerResponse  int `yaml:"rowsPerResponse" json:"rowsPerResponse"`
	MaxResponseBytes int `yaml:"maxResponseBytes" json:"maxResponseBytes"`
}

type StorageWriteConfig struct {
	MaxStreams            int      `yaml:"maxStreams" json:"maxStreams"`
	MaxAppendRequestBytes int      `yaml:"maxAppendRequestBytes" json:"maxAppendRequestBytes"`
	QueueCapacity         int      `yaml:"queueCapacity" json:"queueCapacity"`
	OrphanTTL             Duration `yaml:"orphanTtl" json:"orphanTtl"`
	CleanupInterval       Duration `yaml:"cleanupInterval" json:"cleanupInterval"`
	ProtocolModelVersion  string   `yaml:"protocolModelVersion" json:"protocolModelVersion"`
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

type AuthConfig struct {
	Mode             string `yaml:"mode" json:"mode"`
	StaticTokensFile string `yaml:"staticTokensFile" json:"staticTokensFile"`
}

type LoggingConfig struct {
	Level          string `yaml:"level" json:"level"`
	Format         string `yaml:"format" json:"format"`
	UnsafePayloads bool   `yaml:"unsafePayloads" json:"unsafePayloads"`
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
			},
			GRPC: GRPCConfig{Address: ":9060", MaxReceiveMessageBytes: 32 << 20, MaxSendMessageBytes: 32 << 20},
		},
		Database: DatabaseConfig{Adapter: "duckdb", DSN: ":memory:", TempDirectory: os.TempDir()},
		Runtime: RuntimeConfig{
			ShutdownTimeout: Duration(10 * time.Second), JobPollInterval: Duration(100 * time.Millisecond),
			ReadSessionTTL: Duration(6 * time.Hour), CleanupInterval: Duration(time.Minute),
		},
		Storage: StorageConfig{
			Read: StorageReadConfig{MaxStreams: 64, RowsPerResponse: 10_000, MaxResponseBytes: 16 << 20},
			Write: StorageWriteConfig{
				MaxStreams: 1_024, MaxAppendRequestBytes: 20 << 20, QueueCapacity: 256,
				OrphanTTL: Duration(6 * time.Hour), CleanupInterval: Duration(time.Minute),
				ProtocolModelVersion: "google.storage.v1+spark-bigquery-connector-0.44.2",
			},
		},
		Load: LoadConfig{
			OperationTimeout: Duration(2 * time.Minute), MaxObjects: 1_000,
			MaxObjectBytes: 1 << 30, MaxTotalBytes: 4 << 30,
			MaxMetadataBytes: 8 << 20, MaxListedObjects: 10_000,
		},
		Auth:    AuthConfig{Mode: "disabled"},
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
	{"BQEMU_GRPC_ADDRESS", "server.grpc.address"}, {"BQEMU_TLS_CERT_FILE", "server.tls.certFile"},
	{"BQEMU_TLS_KEY_FILE", "server.tls.keyFile"}, {"BQEMU_DATABASE_ADAPTER", "database.adapter"},
	{"BQEMU_DATABASE_DSN", "database.dsn"}, {"BQEMU_TEMP_DIRECTORY", "database.tempDirectory"},
	{"BQEMU_LOAD_ENABLED", "load.enabled"}, {"BQEMU_LOAD_GCS_ENDPOINT", "load.gcsEndpoint"},
	{"BQEMU_LOAD_ALLOW_FILE_SOURCES", "load.allowFileSources"},
	{"BQEMU_LOAD_OPERATION_TIMEOUT", "load.operationTimeout"}, {"BQEMU_LOAD_MAX_OBJECTS", "load.maxObjects"},
	{"BQEMU_LOAD_MAX_OBJECT_BYTES", "load.maxObjectBytes"}, {"BQEMU_LOAD_MAX_TOTAL_BYTES", "load.maxTotalBytes"},
	{"BQEMU_LOAD_MAX_METADATA_BYTES", "load.maxMetadataBytes"}, {"BQEMU_LOAD_MAX_LISTED_OBJECTS", "load.maxListedObjects"},
	{"BQEMU_STORAGE_WRITE_MAX_STREAMS", "storage.write.maxStreams"},
	{"BQEMU_STORAGE_WRITE_MAX_APPEND_REQUEST_BYTES", "storage.write.maxAppendRequestBytes"},
	{"BQEMU_STORAGE_WRITE_QUEUE_CAPACITY", "storage.write.queueCapacity"},
	{"BQEMU_STORAGE_WRITE_ORPHAN_TTL", "storage.write.orphanTtl"},
	{"BQEMU_STORAGE_WRITE_CLEANUP_INTERVAL", "storage.write.cleanupInterval"},
	{"BQEMU_STORAGE_WRITE_PROTOCOL_MODEL_VERSION", "storage.write.protocolModelVersion"},
	{"BQEMU_AUTH_MODE", "auth.mode"}, {"BQEMU_AUTH_STATIC_TOKENS_FILE", "auth.staticTokensFile"},
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
	case "runtime.jobPollInterval":
		return setDuration(&cfg.Runtime.JobPollInterval)
	case "runtime.readSessionTtl":
		return setDuration(&cfg.Runtime.ReadSessionTTL)
	case "runtime.cleanupInterval":
		return setDuration(&cfg.Runtime.CleanupInterval)
	case "storage.read.maxStreams":
		return setInt(&cfg.Storage.Read.MaxStreams)
	case "storage.read.rowsPerResponse":
		return setInt(&cfg.Storage.Read.RowsPerResponse)
	case "storage.read.maxResponseBytes":
		return setInt(&cfg.Storage.Read.MaxResponseBytes)
	case "storage.write.maxStreams":
		return setInt(&cfg.Storage.Write.MaxStreams)
	case "storage.write.maxAppendRequestBytes":
		return setInt(&cfg.Storage.Write.MaxAppendRequestBytes)
	case "storage.write.queueCapacity":
		return setInt(&cfg.Storage.Write.QueueCapacity)
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
	case "auth.mode":
		return setString(&cfg.Auth.Mode)
	case "auth.staticTokensFile":
		return setString(&cfg.Auth.StaticTokensFile)
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
	for name, value := range map[string]Duration{
		"server.http.readHeaderTimeout": cfg.Server.HTTP.ReadHeaderTimeout,
		"server.http.readTimeout":       cfg.Server.HTTP.ReadTimeout,
		"server.http.writeTimeout":      cfg.Server.HTTP.WriteTimeout,
		"server.http.idleTimeout":       cfg.Server.HTTP.IdleTimeout,
		"runtime.shutdownTimeout":       cfg.Runtime.ShutdownTimeout,
		"runtime.jobPollInterval":       cfg.Runtime.JobPollInterval,
		"runtime.readSessionTtl":        cfg.Runtime.ReadSessionTTL,
		"runtime.cleanupInterval":       cfg.Runtime.CleanupInterval,
		"load.operationTimeout":         cfg.Load.OperationTimeout,
		"storage.write.orphanTtl":       cfg.Storage.Write.OrphanTTL,
		"storage.write.cleanupInterval": cfg.Storage.Write.CleanupInterval,
	} {
		if value.Value() <= 0 {
			return fmt.Errorf("%s must be positive", name)
		}
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
	if cfg.Storage.Read.MaxStreams < 1 || cfg.Storage.Read.RowsPerResponse < 1 || cfg.Storage.Read.MaxResponseBytes < 1<<20 {
		return errors.New("storage.read limits must be positive and maxResponseBytes at least 1 MiB")
	}
	if cfg.Storage.Write.MaxStreams < 1 || cfg.Storage.Write.QueueCapacity < 1 ||
		cfg.Storage.Write.MaxAppendRequestBytes < 1<<20 || cfg.Storage.Write.MaxAppendRequestBytes > 20<<20 {
		return errors.New("storage.write stream/queue limits must be positive and maxAppendRequestBytes between 1 MiB and 20 MiB")
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
	if !oneOf(cfg.Auth.Mode, "disabled", "bearer-present", "static") {
		return fmt.Errorf("unsupported auth.mode %q", cfg.Auth.Mode)
	}
	if cfg.Auth.Mode == "static" && cfg.Auth.StaticTokensFile == "" {
		return errors.New("auth.staticTokensFile is required in static mode")
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
