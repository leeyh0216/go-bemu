package application

// Load jobs execute through bounded object-store and warehouse ports. The
// application never interprets Parquet payloads and never exposes local paths
// or object URIs in logs.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	"github.com/leeyh0216/go-bemu/internal/loadjob/ports"
	"github.com/leeyh0216/go-bemu/internal/observability"
)

const (
	defaultOperationTimeout = 2 * time.Minute
	defaultMaxObjects       = 1_000
	defaultMaxObjectBytes   = int64(1 << 30)
	defaultMaxTotalBytes    = int64(4 << 30)
)

type Config struct {
	DefaultLocation  string
	OperationTimeout time.Duration
	MaxObjects       int
	MaxObjectBytes   int64
	MaxTotalBytes    int64
	TempDirectory    string
}

func DefaultConfig() Config {
	return Config{
		DefaultLocation: "US", OperationTimeout: defaultOperationTimeout,
		MaxObjects: defaultMaxObjects, MaxObjectBytes: defaultMaxObjectBytes,
		MaxTotalBytes: defaultMaxTotalBytes, TempDirectory: os.TempDir(),
	}
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.DefaultLocation) == "" {
		return fmt.Errorf("%w: default location is required", domain.ErrInvalid)
	}
	if c.OperationTimeout <= 0 || c.MaxObjects <= 0 || c.MaxObjectBytes <= 0 || c.MaxTotalBytes <= 0 {
		return fmt.Errorf("%w: timeout and object limits must be positive", domain.ErrInvalid)
	}
	if strings.TrimSpace(c.TempDirectory) == "" {
		return fmt.Errorf("%w: temporary directory is required", domain.ErrInvalid)
	}
	return nil
}

type Service struct {
	jobs    ports.JobRepository
	objects ports.ObjectStore
	tables  ports.TableCatalog
	loader  ports.Loader
	clock   ports.Clock
	ids     ports.IDGenerator
	config  Config
}

func NewService(jobs ports.JobRepository, objects ports.ObjectStore, tables ports.TableCatalog, loader ports.Loader, clock ports.Clock, ids ports.IDGenerator, config Config) (*Service, error) {
	if jobs == nil || objects == nil || tables == nil || loader == nil || clock == nil || ids == nil {
		return nil, fmt.Errorf("%w: load service dependencies are required", domain.ErrInvalid)
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Service{jobs: jobs, objects: objects, tables: tables, loader: loader, clock: clock, ids: ids, config: config}, nil
}

func (s *Service) Submit(ctx context.Context, reference domain.JobReference, configuration domain.LoadConfiguration) (*domain.Job, error) {
	if reference.JobID == "" {
		reference.JobID = "job_" + s.ids.NewID()
	}
	if reference.Location == "" {
		reference.Location = s.config.DefaultLocation
	}
	if configuration.Destination.ProjectID == "" {
		configuration.Destination.ProjectID = reference.ProjectID
	}
	job, err := domain.NewJob(reference, configuration, s.clock.Now())
	if err != nil {
		return nil, err
	}
	stored, created, err := s.jobs.CreateOrGet(ctx, job)
	if err != nil {
		return nil, err
	}
	if !created {
		if stored.ConfigurationDigest != job.ConfigurationDigest {
			return nil, fmt.Errorf("%w: job ID %q already has a different load configuration", domain.ErrConflict, reference.JobID)
		}
		return stored, nil
	}
	execution := stored.Clone()
	go s.execute(context.WithoutCancel(ctx), execution)
	return stored, nil
}

func (s *Service) Get(ctx context.Context, reference domain.JobReference) (*domain.Job, error) {
	if reference.Location == "" {
		reference.Location = s.config.DefaultLocation
	}
	return s.jobs.Get(ctx, reference)
}

func (s *Service) List(ctx context.Context, projectID, location string) ([]*domain.Job, error) {
	return s.jobs.List(ctx, projectID, location)
}

func (s *Service) execute(parent context.Context, job *domain.Job) {
	ctx, cancel := context.WithTimeout(parent, s.config.OperationTimeout)
	defer cancel()
	if err := job.Start(s.clock.Now()); err != nil {
		slog.ErrorContext(ctx, "load job start failed", append([]any{
			"project_id", job.Reference.ProjectID, "location", job.Reference.Location,
			"job_id", job.Reference.JobID,
		}, observability.ErrorAttrs(err)...)...)
		return
	}
	if err := s.jobs.Update(ctx, job); err != nil {
		slog.ErrorContext(ctx, "load job state persistence failed", append([]any{
			"project_id", job.Reference.ProjectID, "location", job.Reference.Location,
			"job_id", job.Reference.JobID, "job_state", job.State,
		}, observability.ErrorAttrs(err)...)...)
		return
	}

	statistics, err := s.run(ctx, job)
	if err == nil {
		_ = job.Complete(statistics, s.clock.Now())
	} else {
		reason, message := terminalError(err)
		_ = job.Fail(reason, message, statistics, s.clock.Now())
	}
	if persistErr := s.jobs.Update(context.WithoutCancel(ctx), job); persistErr != nil {
		slog.ErrorContext(ctx, "load job terminal state persistence failed", append([]any{
			"project_id", job.Reference.ProjectID, "location", job.Reference.Location,
			"job_id", job.Reference.JobID, "job_state", job.State,
		}, observability.ErrorAttrs(persistErr)...)...)
	}
}

func (s *Service) run(ctx context.Context, job *domain.Job) (statistics domain.Statistics, err error) {
	configuration := job.Configuration
	if configuration.SourceFormat != domain.FormatParquet {
		return statistics, fmt.Errorf("%w: sourceFormat %s", domain.ErrUnsupported, configuration.SourceFormat)
	}
	if configuration.Autodetect || len(configuration.SchemaUpdateOptions) > 0 || configuration.IgnoreUnknownValues || configuration.MaxBadRecords != 0 || len(configuration.UnsupportedOptions) > 0 {
		return statistics, fmt.Errorf("%w: requested load options", domain.ErrUnsupported)
	}

	table, err := s.tables.GetTable(ctx, configuration.Destination)
	if err != nil {
		return statistics, err
	}
	if table.Location != "" && !strings.EqualFold(table.Location, job.Reference.Location) {
		return statistics, fmt.Errorf("%w: destination table and job locations differ", domain.ErrInvalid)
	}
	if len(configuration.Schema) > 0 && !schemasEqual(configuration.Schema, table.Schema) {
		return statistics, fmt.Errorf("%w: requested schema does not match the destination table", domain.ErrInvalid)
	}
	if err := validateLoadSchema(s.loader, configuration.SourceFormat, table.Schema); err != nil {
		return statistics, err
	}

	objects, err := s.resolveObjects(ctx, configuration.SourceURIs)
	if err != nil {
		return statistics, err
	}
	statistics.InputFiles = int64(len(objects))
	uriDigest := objectSetDigest(objects)
	workspace, err := os.MkdirTemp(s.config.TempDirectory, "bqemu-load-")
	if err != nil {
		return statistics, fmt.Errorf("create bounded load workspace: %w", err)
	}
	if chmodErr := os.Chmod(workspace, 0o700); chmodErr != nil {
		_ = os.RemoveAll(workspace)
		return statistics, fmt.Errorf("secure load workspace: %w", chmodErr)
	}
	defer func() {
		started := observability.LogSideEffectStart(context.WithoutCancel(ctx), "filesystem", "cleanup_load_workspace",
			"project_id", job.Reference.ProjectID, "job_id", job.Reference.JobID, "object_set_fingerprint", uriDigest)
		cleanupErr := os.RemoveAll(workspace)
		observability.LogSideEffectEnd(context.WithoutCancel(ctx), "filesystem", "cleanup_load_workspace", started, cleanupErr,
			"project_id", job.Reference.ProjectID, "job_id", job.Reference.JobID, "object_set_fingerprint", uriDigest)
		if err == nil && cleanupErr != nil {
			err = fmt.Errorf("cleanup load workspace: %w", cleanupErr)
		}
	}()

	localObjects, downloaded, err := s.download(ctx, job, objects, workspace, uriDigest)
	statistics.InputBytes = downloaded
	if err != nil {
		return statistics, err
	}
	started := observability.LogSideEffectStart(ctx, "warehouse", "commit_load_job",
		"project_id", job.Reference.ProjectID, "dataset_id", table.Reference.DatasetID,
		"table_id", table.Reference.TableID, "job_id", job.Reference.JobID,
		"file_count", len(localObjects), "input_bytes", downloaded,
		"schema_fingerprint", schemaDigest(table.Schema), "write_disposition", configuration.WriteDisposition)
	result, err := s.loader.Load(ctx, ports.LoadRequest{
		Destination: table, Schema: table.Schema, Objects: localObjects,
		SourceFormat: configuration.SourceFormat, WriteDisposition: configuration.WriteDisposition,
	})
	observability.LogSideEffectEnd(ctx, "warehouse", "commit_load_job", started, err,
		"project_id", job.Reference.ProjectID, "dataset_id", table.Reference.DatasetID,
		"table_id", table.Reference.TableID, "job_id", job.Reference.JobID,
		"file_count", len(localObjects), "input_bytes", downloaded,
		"schema_fingerprint", schemaDigest(table.Schema), "write_disposition", configuration.WriteDisposition)
	statistics.OutputRows = result.OutputRows
	if err == nil {
		// BigQuery reports the bytes produced by the load job. DuckDB does not
		// expose a comparable physical-output metric through the loader port, so
		// the emulator deliberately uses the bounded input byte total. Keeping
		// the approximation in the application layer makes the REST contract
		// non-null without coupling the domain to a particular warehouse.
		// Connector 0.44.2 reads this value as a primitive long after polling:
		// https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/719817782a214b8ca72be520870013a3e0253d92/spark-bigquery-connector-common/src/main/java/com/google/cloud/spark/bigquery/write/BigQueryWriteHelper.java#L194-L200
		// https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobStatistics3
		statistics.OutputBytes = downloaded
	}
	return statistics, err
}

func validateLoadSchema(loader ports.Loader, format domain.SourceFormat, schema []domain.Field) error {
	if err := domain.ValidateSchema(schema); err != nil {
		return err
	}
	if err := loader.EngineCapabilities().ValidateSchema(schema); err != nil {
		return translateCatalogSchemaError(err)
	}
	if err := loader.ValidateSchema(schema); err != nil {
		return translateCatalogSchemaError(err)
	}
	return loader.ValidateLoadSchema(format, schema)
}

func translateCatalogSchemaError(err error) error {
	switch {
	case errors.Is(err, catalogdomain.ErrUnsupported):
		return fmt.Errorf("%w: %v", domain.ErrUnsupported, err)
	case errors.Is(err, catalogdomain.ErrInvalid):
		return fmt.Errorf("%w: %v", domain.ErrInvalid, err)
	default:
		return err
	}
}

func (s *Service) resolveObjects(ctx context.Context, patterns []string) ([]ports.ObjectInfo, error) {
	started := observability.LogSideEffectStart(ctx, "object_store", "resolve_load_sources",
		"source_count", len(patterns), "source_set_fingerprint", observability.Digest([]byte(strings.Join(patterns, "\x00"))))
	var finalErr error
	defer func() {
		observability.LogSideEffectEnd(ctx, "object_store", "resolve_load_sources", started, finalErr,
			"source_count", len(patterns), "source_set_fingerprint", observability.Digest([]byte(strings.Join(patterns, "\x00"))))
	}()
	if len(patterns) > s.config.MaxObjects {
		finalErr = fmt.Errorf("%w: source URI list exceeds the configured object limit", domain.ErrPrecondition)
		return nil, finalErr
	}
	seen := make(map[string]ports.ObjectInfo)
	for _, pattern := range patterns {
		var matches []ports.ObjectInfo
		if strings.ContainsAny(pattern, "*?[") {
			listed, err := s.objects.List(ctx, pattern)
			if err != nil {
				finalErr = err
				return nil, finalErr
			}
			matches = listed
		} else {
			object, err := s.objects.Get(ctx, pattern)
			if err != nil {
				finalErr = err
				return nil, finalErr
			}
			matches = []ports.ObjectInfo{object}
		}
		if len(matches) == 0 {
			finalErr = fmt.Errorf("%w: a source URI pattern matched no objects", domain.ErrNotFound)
			return nil, finalErr
		}
		for _, object := range matches {
			if object.Size < 0 {
				finalErr = fmt.Errorf("%w: object metadata has a negative size", domain.ErrInvalid)
				return nil, finalErr
			}
			if object.Size > s.config.MaxObjectBytes {
				finalErr = fmt.Errorf("%w: an object exceeds the configured size limit", domain.ErrPrecondition)
				return nil, finalErr
			}
			if existing, ok := seen[object.URI]; ok && objectIdentityChanged(existing, object) {
				finalErr = fmt.Errorf("%w: overlapping source patterns resolved different object generations", domain.ErrPrecondition)
				return nil, finalErr
			}
			seen[object.URI] = object
			if len(seen) > s.config.MaxObjects {
				finalErr = fmt.Errorf("%w: source set exceeds the configured object limit", domain.ErrPrecondition)
				return nil, finalErr
			}
		}
	}
	objects := make([]ports.ObjectInfo, 0, len(seen))
	var total int64
	for _, object := range seen {
		if object.Size > s.config.MaxTotalBytes-total {
			finalErr = fmt.Errorf("%w: source set exceeds the configured total size limit", domain.ErrPrecondition)
			return nil, finalErr
		}
		total += object.Size
		objects = append(objects, object)
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].URI < objects[j].URI })
	return objects, nil
}

func (s *Service) download(ctx context.Context, job *domain.Job, objects []ports.ObjectInfo, workspace, objectSetFingerprint string) ([]ports.LocalObject, int64, error) {
	started := observability.LogSideEffectStart(ctx, "object_store", "download_load_sources",
		"project_id", job.Reference.ProjectID, "job_id", job.Reference.JobID,
		"object_count", len(objects), "object_set_fingerprint", objectSetFingerprint)
	var operationErr error
	var total int64
	defer func() {
		observability.LogSideEffectEnd(ctx, "object_store", "download_load_sources", started, operationErr,
			"project_id", job.Reference.ProjectID, "job_id", job.Reference.JobID,
			"object_count", len(objects), "downloaded_bytes", total, "object_set_fingerprint", objectSetFingerprint)
	}()
	local := make([]ports.LocalObject, 0, len(objects))
	for index, object := range objects {
		if err := ctx.Err(); err != nil {
			operationErr = err
			return nil, total, operationErr
		}
		reader, err := s.objects.Open(ctx, object)
		if err != nil {
			operationErr = err
			return nil, total, operationErr
		}
		path := filepath.Join(workspace, fmt.Sprintf("%06d.parquet", index))
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			_ = reader.Close()
			operationErr = fmt.Errorf("create local load object: %w", err)
			return nil, total, operationErr
		}
		written, copyErr := io.Copy(file, io.LimitReader(reader, s.config.MaxObjectBytes+1))
		closeFileErr := file.Close()
		closeReaderErr := reader.Close()
		if copyErr != nil {
			operationErr = fmt.Errorf("stream load object: %w", copyErr)
			return nil, total, operationErr
		}
		if closeFileErr != nil {
			operationErr = fmt.Errorf("close local load object: %w", closeFileErr)
			return nil, total, operationErr
		}
		if closeReaderErr != nil {
			operationErr = fmt.Errorf("close remote load object: %w", closeReaderErr)
			return nil, total, operationErr
		}
		if written > s.config.MaxObjectBytes || written > s.config.MaxTotalBytes-total {
			operationErr = fmt.Errorf("%w: downloaded source exceeds the configured size limit", domain.ErrPrecondition)
			return nil, total, operationErr
		}
		total += written
		local = append(local, ports.LocalObject{Path: path, Size: written})
	}
	return local, total, nil
}

func terminalError(err error) (string, string) {
	switch {
	case errors.Is(err, domain.ErrUnsupported):
		if strings.Contains(err.Error(), domain.CapabilityParquetNestedRepeatedV1) {
			return "notImplemented", "the requested load-job feature is not implemented; capability=" + domain.CapabilityParquetNestedRepeatedV1
		}
		if strings.Contains(err.Error(), domain.CapabilityDecimalRoundingV1) {
			return "notImplemented", "the requested load-job feature is not implemented; capability=" + domain.CapabilityDecimalRoundingV1
		}
		return "notImplemented", "the requested load-job feature is not implemented"
	case errors.Is(err, domain.ErrInvalid):
		return "invalid", "the load configuration or source schema is invalid"
	case errors.Is(err, domain.ErrNotFound):
		return "notFound", "a requested load source or destination was not found"
	case errors.Is(err, domain.ErrConflict):
		return "duplicate", "the load job conflicts with an existing resource"
	case errors.Is(err, domain.ErrPrecondition):
		return "conditionNotMet", "a load-job precondition was not met"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return "backendError", "the load job exceeded its configured execution deadline"
	default:
		return "backendError", "the load job failed in the local backend"
	}
}

func schemasEqual(left, right []domain.Field) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		leftMode, rightMode := strings.ToUpper(left[index].Mode), strings.ToUpper(right[index].Mode)
		if leftMode == "" {
			leftMode = "NULLABLE"
		}
		if rightMode == "" {
			rightMode = "NULLABLE"
		}
		if !strings.EqualFold(left[index].Name, right[index].Name) ||
			!strings.EqualFold(left[index].Type, right[index].Type) || leftMode != rightMode ||
			!sameOptionalInt64(left[index].Precision, right[index].Precision) ||
			!sameOptionalInt64(left[index].Scale, right[index].Scale) ||
			left[index].RoundingMode != right[index].RoundingMode ||
			!schemasEqual(left[index].Fields, right[index].Fields) {
			return false
		}
	}
	return true
}

func sameOptionalInt64(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func schemaDigest(schema []domain.Field) string {
	parts := make([]string, 0, len(schema))
	var appendFields func([]domain.Field, string)
	appendFields = func(fields []domain.Field, prefix string) {
		for _, field := range fields {
			parts = append(parts, strings.Join([]string{
				prefix + field.Name,
				strings.ToUpper(field.Type),
				strings.ToUpper(field.Mode),
				optionalInt64Digest(field.Precision),
				optionalInt64Digest(field.Scale),
				string(field.RoundingMode),
			}, ":"))
			appendFields(field.Fields, prefix+field.Name+".")
		}
	}
	appendFields(schema, "")
	return observability.Digest([]byte(strings.Join(parts, "\x00")))
}

func optionalInt64Digest(value *int64) string {
	if value == nil {
		return "omitted"
	}
	return "set=" + strconv.FormatInt(*value, 10)
}

func objectSetDigest(objects []ports.ObjectInfo) string {
	parts := make([]string, len(objects))
	for index, object := range objects {
		parts[index] = object.URI + "\x00" + object.Generation + "\x00" + object.ETag
	}
	return observability.Digest([]byte(strings.Join(parts, "\x00")))
}

func objectIdentityChanged(left, right ports.ObjectInfo) bool {
	if left.Generation != "" && right.Generation != "" && left.Generation != right.Generation {
		return true
	}
	return left.ETag != "" && right.ETag != "" && left.ETag != right.ETag
}
