package application

// Load jobs execute through bounded object-store and warehouse ports. The
// application never interprets Parquet payloads and never exposes local paths
// or object URIs in logs.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	pathpkg "path"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/engine"
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
	jobs      ports.JobRepository
	mutations ports.MutationJournal
	objects   ports.ObjectStore
	tables    ports.TableCatalog
	loader    ports.Loader
	clock     ports.Clock
	ids       ports.IDGenerator
	config    Config
}

type ServiceOption func(*Service) error

func WithMutationJournal(journal ports.MutationJournal) ServiceOption {
	return func(service *Service) error {
		if interfaceNil(journal) {
			return fmt.Errorf("%w: load mutation journal is required", domain.ErrInvalid)
		}
		service.mutations = journal
		return nil
	}
}

type destinationResolution struct {
	table        domain.Table
	partition    *domain.PartitionDecorator
	beforeSchema []domain.Field
	evolution    catalogdomain.SchemaEvolution
	create       bool
	infer        bool
}

func NewService(jobs ports.JobRepository, objects ports.ObjectStore, tables ports.TableCatalog, loader ports.Loader, clock ports.Clock, ids ports.IDGenerator, config Config, options ...ServiceOption) (*Service, error) {
	if jobs == nil || objects == nil || tables == nil || loader == nil || clock == nil || ids == nil {
		return nil, fmt.Errorf("%w: load service dependencies are required", domain.ErrInvalid)
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	service := &Service{
		jobs: jobs, mutations: NewMemoryMutationJournal(), objects: objects, tables: tables,
		loader: loader, clock: clock, ids: ids, config: config,
	}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: load service option is required", domain.ErrInvalid)
		}
		if err := option(service); err != nil {
			return nil, err
		}
	}
	return service, nil
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

// Recover completes cross-store load mutations before public listeners start.
// DuckDB receipts prove the physical commit; the journal supplies the exact
// canonical publication that must follow it.
func (s *Service) Recover(ctx context.Context) error {
	records, err := s.mutations.ListRecoverable(ctx)
	if err != nil {
		return fmt.Errorf("list recoverable load mutations: %w", err)
	}
	for _, record := range records {
		if err := s.recoverMutation(ctx, record); err != nil {
			return fmt.Errorf("recover load mutation %s: %w", record.ID, err)
		}
	}
	interrupted, err := s.jobs.ListInterrupted(ctx)
	if err != nil {
		return fmt.Errorf("list interrupted load jobs: %w", err)
	}
	for _, job := range interrupted {
		if err := failInterruptedJob(job, s.clock.Now(), "load job was interrupted before a durable physical commit"); err != nil {
			return err
		}
		if err := s.jobs.Update(ctx, job); err != nil {
			return fmt.Errorf("persist interrupted load job %s: %w", job.Reference.JobID, err)
		}
	}
	return nil
}

func (s *Service) recoverMutation(ctx context.Context, record domain.MutationRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}
	job, err := s.jobs.Get(ctx, record.Job)
	if err != nil {
		return err
	}
	if job.ConfigurationDigest != record.ConfigurationDigest {
		return fmt.Errorf("%w: load mutation job configuration changed", domain.ErrPrecondition)
	}
	if record.Phase == domain.MutationPrepared {
		receipt, found, inspectErr := s.loader.InspectLoadMutation(ctx, record.ID)
		if inspectErr != nil {
			return inspectErr
		}
		if !found {
			if err := s.mutations.MarkAborted(ctx, record.ID); err != nil {
				return err
			}
			if job.State == domain.JobDone {
				return nil
			}
			if err := failInterruptedJob(job, s.clock.Now(), "load job was interrupted before its physical commit"); err != nil {
				return err
			}
			return s.jobs.Update(ctx, job)
		}
		if receipt.PlanFingerprint != record.PlanFingerprint {
			return fmt.Errorf("%w: physical load receipt belongs to a different plan", domain.ErrPrecondition)
		}
		if err := s.mutations.MarkPhysical(ctx, record.ID, record.PlanFingerprint, receipt.Result); err != nil {
			return err
		}
		record.Phase = domain.MutationPhysical
		record.Result = &domain.MutationResult{
			OutputRows: receipt.Result.OutputRows, CreatedDestination: receipt.Result.CreatedDestination,
			UpdatedDestination: receipt.Result.UpdatedDestination,
		}
	}
	if record.Phase == domain.MutationPhysical {
		if err := s.publishMutation(ctx, record); err != nil {
			return err
		}
		if err := s.mutations.MarkApplied(ctx, record.ID); err != nil {
			return err
		}
		record.Phase = domain.MutationApplied
	}
	if record.Phase != domain.MutationApplied || record.Result == nil || job.State == domain.JobDone {
		return nil
	}
	if job.State == domain.JobPending {
		if err := job.Start(s.clock.Now()); err != nil {
			return err
		}
	}
	statistics := domain.Statistics{
		InputFiles: record.InputFiles, InputBytes: record.InputBytes,
		OutputRows: record.Result.OutputRows, OutputBytes: record.InputBytes,
	}
	if err := job.Complete(statistics, s.clock.Now()); err != nil {
		return err
	}
	return s.jobs.Update(ctx, job)
}

func (s *Service) publishMutation(ctx context.Context, record domain.MutationRecord) error {
	switch record.Publication {
	case domain.MutationPublishNone:
		return nil
	case domain.MutationPublishCreate:
		return s.tables.PublishTable(ctx, record.Destination)
	case domain.MutationPublishSchemaUpdate:
		return s.tables.PublishSchemaUpdate(
			ctx, record.Destination.Reference, record.BeforeSchema, record.Destination.Schema,
		)
	default:
		return fmt.Errorf("%w: load mutation publication is invalid", domain.ErrInvalid)
	}
}

func failInterruptedJob(job *domain.Job, now time.Time, message string) error {
	if job.State == domain.JobPending {
		if err := job.Start(now); err != nil {
			return err
		}
	}
	if job.State == domain.JobDone {
		return nil
	}
	if err := job.Fail("backendError", message, job.Statistics, now); err != nil {
		return err
	}
	return nil
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
	var pending *recoveryPendingError
	if errors.As(err, &pending) {
		slog.ErrorContext(ctx, "load job awaits durable recovery", append([]any{
			"project_id", job.Reference.ProjectID, "location", job.Reference.Location,
			"job_id", job.Reference.JobID,
		}, observability.ErrorAttrs(err)...)...)
		return
	}
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
	if configuration.Autodetect || configuration.IgnoreUnknownValues || configuration.MaxBadRecords != 0 || len(configuration.UnsupportedOptions) > 0 {
		return statistics, fmt.Errorf("%w: requested load options", domain.ErrUnsupported)
	}
	mutationID, err := domain.LoadMutationID(job.Reference, job.ConfigurationDigest)
	if err != nil {
		return statistics, err
	}

	destination, err := s.resolveDestination(ctx, job, configuration)
	if err != nil {
		return statistics, err
	}
	var schemaPlan engine.SchemaPlan
	if !destination.infer {
		schemaPlan, err = s.planDestinationSchema(ctx, destination)
		if err != nil {
			return statistics, err
		}
	}

	objects, err := s.resolveObjects(ctx, configuration.SourceURIs)
	if err != nil {
		return statistics, err
	}
	resolved := make([]ports.ResolvedObject, len(objects))
	for index, object := range objects {
		resolved[index] = ports.ResolvedObject{Fingerprint: objectFingerprint(object), Size: object.Size}
	}
	var loadPlan ports.LoadPlan
	if !destination.infer {
		loadPlan, err = s.planLoad(ctx, mutationID, configuration, destination, schemaPlan, resolved)
		if err != nil {
			return statistics, err
		}
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
	if destination.infer {
		inferred, inferErr := s.loader.InferParquetSchema(ctx, localObjects, ports.ParquetSchemaOptions{
			EnableListInference: configuration.ParquetOptions.EnableListInference,
		})
		if inferErr != nil {
			return statistics, inferErr
		}
		if err := domain.ValidateSchema(inferred); err != nil {
			return statistics, err
		}
		destination.table.Schema = catalogdomain.CloneFields(inferred)
		if !destination.create {
			destination.table.Schema = mergeInferredSchemaUpdate(
				destination.beforeSchema, inferred,
				schemaUpdateOptionEnabled(configuration.SchemaUpdateOptions, domain.AllowFieldRelaxation),
			)
			destination.evolution, err = validateRequestedSchemaUpdate(
				destination.beforeSchema, destination.table.Schema, configuration.SchemaUpdateOptions,
			)
			if err != nil {
				return statistics, err
			}
		}
		if err := domain.ValidateTable(destination.table); err != nil {
			return statistics, err
		}
		schemaPlan, err = s.planDestinationSchema(ctx, destination)
		if err != nil {
			return statistics, err
		}
		loadPlan, err = s.planLoad(ctx, mutationID, configuration, destination, schemaPlan, resolved)
		if err != nil {
			return statistics, err
		}
	}
	started := observability.LogSideEffectStart(ctx, "warehouse", "commit_load_job",
		"project_id", job.Reference.ProjectID, "dataset_id", destination.table.Reference.DatasetID,
		"table_id", destination.table.Reference.TableID, "job_id", job.Reference.JobID,
		"file_count", len(localObjects), "input_bytes", downloaded,
		"schema_fingerprint", schemaDigest(destination.table.Schema), "write_disposition", configuration.WriteDisposition)
	var result ports.LoadResult
	publication := mutationPublication(destination)
	record := domain.MutationRecord{
		ID: mutationID, Job: job.Reference, ConfigurationDigest: job.ConfigurationDigest,
		PlanFingerprint: loadPlan.Fingerprint(), Destination: domain.CloneTable(destination.table),
		BeforeSchema: catalogdomain.CloneFields(destination.beforeSchema), Publication: publication,
		Phase: domain.MutationPrepared, InputFiles: statistics.InputFiles, InputBytes: statistics.InputBytes,
	}
	if err := s.mutations.Prepare(ctx, record); err != nil {
		return statistics, err
	}
	result, err = s.executePlannedLoad(ctx, record, loadPlan, localObjects)
	if err == nil && result.CreatedDestination != destination.create {
		err = &recoveryPendingError{cause: fmt.Errorf(
			"%w: loader destination creation result differs from the plan", domain.ErrPrecondition,
		)}
	}
	updateDestination := len(destination.evolution.Additions) != 0 || len(destination.evolution.Relaxations) != 0
	if err == nil && result.UpdatedDestination != updateDestination {
		err = &recoveryPendingError{cause: fmt.Errorf(
			"%w: loader destination update result differs from the plan", domain.ErrPrecondition,
		)}
	}
	if err == nil {
		if markErr := s.mutations.MarkPhysical(ctx, mutationID, loadPlan.Fingerprint(), result); markErr != nil {
			err = &recoveryPendingError{cause: markErr}
		}
	}
	if err == nil && destination.create {
		if publishErr := s.tables.PublishTable(ctx, destination.table); publishErr != nil {
			cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			cleanupErr := s.loader.DiscardLoadedTable(cleanupCtx, mutationID, destination.table.Reference)
			cancelCleanup()
			if cleanupErr != nil {
				err = &recoveryPendingError{cause: errors.Join(publishErr, fmt.Errorf("compensate unpublished load destination: %w", cleanupErr))}
			} else {
				if abortErr := s.mutations.MarkAborted(context.WithoutCancel(ctx), mutationID); abortErr != nil {
					err = errors.Join(publishErr, abortErr)
				} else {
					err = publishErr
				}
			}
		}
	} else if err == nil && updateDestination {
		publishErr := s.tables.PublishSchemaUpdate(
			ctx, destination.table.Reference, destination.beforeSchema, destination.table.Schema,
		)
		if publishErr != nil {
			err = &recoveryPendingError{cause: publishErr}
		}
	}
	if err == nil {
		if markErr := s.mutations.MarkApplied(ctx, mutationID); markErr != nil {
			err = &recoveryPendingError{cause: markErr}
		}
	}
	observability.LogSideEffectEnd(ctx, "warehouse", "commit_load_job", started, err,
		"project_id", job.Reference.ProjectID, "dataset_id", destination.table.Reference.DatasetID,
		"table_id", destination.table.Reference.TableID, "job_id", job.Reference.JobID,
		"file_count", len(localObjects), "input_bytes", downloaded,
		"schema_fingerprint", schemaDigest(destination.table.Schema), "write_disposition", configuration.WriteDisposition)
	statistics.OutputRows = result.OutputRows
	if err == nil {
		// BigQuery reports the bytes produced by the load job. DuckDB does not
		// expose a comparable physical-output metric through the loader port, so
		// the emulator deliberately uses the bounded input byte total. Keeping
		// the approximation in the application layer makes the REST contract
		// non-null without coupling the domain to a particular warehouse.
		// https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobStatistics3
		statistics.OutputBytes = downloaded
	}
	return statistics, err
}

func (s *Service) executePlannedLoad(
	ctx context.Context,
	record domain.MutationRecord,
	plan ports.LoadPlan,
	objects []ports.LocalObject,
) (ports.LoadResult, error) {
	result, err := s.loader.ExecuteLoad(ctx, plan, objects)
	if err == nil {
		return result, nil
	}
	receipt, found, inspectErr := s.loader.InspectLoadMutation(context.WithoutCancel(ctx), record.ID)
	if inspectErr != nil {
		return ports.LoadResult{}, &recoveryPendingError{cause: errors.Join(err, inspectErr)}
	}
	if found {
		if receipt.PlanFingerprint != record.PlanFingerprint {
			return ports.LoadResult{}, &recoveryPendingError{cause: fmt.Errorf(
				"%w: physical load receipt belongs to a different plan", domain.ErrPrecondition,
			)}
		}
		return receipt.Result, nil
	}
	if abortErr := s.mutations.MarkAborted(context.WithoutCancel(ctx), record.ID); abortErr != nil {
		return ports.LoadResult{}, errors.Join(err, abortErr)
	}
	return ports.LoadResult{}, err
}

func mutationPublication(destination destinationResolution) domain.MutationPublication {
	if destination.create {
		return domain.MutationPublishCreate
	}
	if len(destination.evolution.Additions) != 0 || len(destination.evolution.Relaxations) != 0 {
		return domain.MutationPublishSchemaUpdate
	}
	return domain.MutationPublishNone
}

type recoveryPendingError struct{ cause error }

func (err *recoveryPendingError) Error() string {
	return "load mutation requires startup recovery: " + err.cause.Error()
}

func (err *recoveryPendingError) Unwrap() error { return err.cause }

func interfaceNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func (s *Service) planDestinationSchema(
	ctx context.Context,
	destination destinationResolution,
) (engine.SchemaPlan, error) {
	operation := engine.SchemaOperationValidate
	var beforeSchema []domain.Field
	if destination.create {
		operation = engine.SchemaOperationCreate
	} else if len(destination.evolution.Additions) != 0 || len(destination.evolution.Relaxations) != 0 {
		operation = engine.SchemaOperationUpdate
		beforeSchema = destination.beforeSchema
	}
	schemaIntent, err := engine.NewSchemaIntent(engine.SchemaIntentDescriptor{
		Operation: operation,
		Target: catalogdomain.TableReference{
			ProjectID: destination.table.Reference.ProjectID,
			DatasetID: destination.table.Reference.DatasetID,
			TableID:   destination.table.Reference.TableID,
		},
		BeforeSchema: beforeSchema, AfterSchema: destination.table.Schema,
		Additions: destination.evolution.Additions, Relaxations: destination.evolution.Relaxations,
	})
	if err != nil {
		return engine.SchemaPlan{}, translateCatalogSchemaError(err)
	}
	plan, err := s.loader.PlanSchema(ctx, schemaIntent)
	if err != nil {
		return engine.SchemaPlan{}, translateCatalogSchemaError(err)
	}
	return plan, nil
}

func (s *Service) planLoad(
	ctx context.Context,
	mutationID string,
	configuration domain.LoadConfiguration,
	destination destinationResolution,
	schemaPlan engine.SchemaPlan,
	objects []ports.ResolvedObject,
) (ports.LoadPlan, error) {
	updateDestination := len(destination.evolution.Additions) != 0 || len(destination.evolution.Relaxations) != 0
	return s.loader.PlanLoad(ctx, ports.LoadPlanRequest{
		MutationID:  mutationID,
		Destination: destination.table, CreateDestination: destination.create,
		UpdateDestination: updateDestination, SchemaPlan: schemaPlan,
		Partition:    destination.partition,
		SourceFormat: configuration.SourceFormat, WriteDisposition: configuration.WriteDisposition,
		Objects: objects,
	})
}

func (s *Service) resolveDestination(
	ctx context.Context,
	job *domain.Job,
	configuration domain.LoadConfiguration,
) (destinationResolution, error) {
	baseReference, partitionID, decorated, err := domain.SplitPartitionDecorator(configuration.Destination)
	if err != nil {
		return destinationResolution{}, err
	}
	table, err := s.tables.GetTable(ctx, baseReference)
	if err == nil {
		if table.Location != "" && !strings.EqualFold(table.Location, job.Reference.Location) {
			return destinationResolution{}, fmt.Errorf("%w: destination table and job locations differ", domain.ErrInvalid)
		}
		if err := validateDestinationMetadataCompatibility(table, configuration); err != nil {
			return destinationResolution{}, err
		}
		resolution := destinationResolution{
			table: domain.CloneTable(table), beforeSchema: catalogdomain.CloneFields(table.Schema),
		}
		if decorated {
			resolution.partition, err = domain.ResolvePartitionDecorator(partitionID, resolution.table)
			if err != nil {
				return destinationResolution{}, err
			}
		}
		if len(configuration.Schema) == 0 {
			resolution.infer = len(configuration.SchemaUpdateOptions) != 0
			return resolution, nil
		}
		if schemasEqual(configuration.Schema, table.Schema) {
			return resolution, nil
		}
		resolution.table.Schema = catalogdomain.CloneFields(configuration.Schema)
		resolution.evolution, err = validateRequestedSchemaUpdate(
			resolution.beforeSchema, resolution.table.Schema, configuration.SchemaUpdateOptions,
		)
		if err != nil {
			return destinationResolution{}, err
		}
		return resolution, nil
	}
	if !errors.Is(err, domain.ErrNotFound) {
		return destinationResolution{}, err
	}
	if decorated {
		return destinationResolution{}, fmt.Errorf("%w: decorated destination base table", domain.ErrNotFound)
	}
	if configuration.CreateDisposition == domain.CreateNever {
		return destinationResolution{}, fmt.Errorf("%w: destination table", domain.ErrNotFound)
	}
	dataset, err := s.tables.GetDataset(
		ctx, baseReference.ProjectID, baseReference.DatasetID,
	)
	if err != nil {
		return destinationResolution{}, err
	}
	if dataset.Location != "" && !strings.EqualFold(dataset.Location, job.Reference.Location) {
		return destinationResolution{}, fmt.Errorf("%w: destination dataset and job locations differ", domain.ErrInvalid)
	}
	return destinationResolution{
		table: domain.Table{
			Reference:         baseReference,
			Location:          dataset.Location,
			Schema:            catalogdomain.CloneFields(configuration.Schema),
			TimePartitioning:  domain.ResolveTimePartitioning(configuration.TimePartitioning, dataset.DefaultPartitionExpirationMs),
			RangePartitioning: cloneRangePartitioning(configuration.RangePartitioning),
			ClusteringFields:  cloneOptionalStrings(configuration.ClusteringFields),
		},
		create: true, infer: len(configuration.Schema) == 0,
	}, nil
}

func validateDestinationMetadataCompatibility(table domain.Table, configuration domain.LoadConfiguration) error {
	if configuration.TimePartitioning != nil && !equalTimePartitioning(table.TimePartitioning, configuration.TimePartitioning) {
		return fmt.Errorf("%w: timePartitioning cannot change an existing destination", domain.ErrPrecondition)
	}
	if configuration.RangePartitioning != nil && !equalRangePartitioning(table.RangePartitioning, configuration.RangePartitioning) {
		return fmt.Errorf("%w: rangePartitioning cannot change an existing destination", domain.ErrPrecondition)
	}
	if configuration.ClusteringFields != nil && !equalOrderedFieldNames(table.ClusteringFields, configuration.ClusteringFields) {
		return fmt.Errorf("%w: clustering cannot change an existing destination", domain.ErrPrecondition)
	}
	return nil
}

func equalTimePartitioning(left *catalogdomain.TimePartitioning, right *domain.TimePartitioning) bool {
	return left != nil && right != nil && strings.EqualFold(left.Type, right.Type) &&
		strings.EqualFold(left.Field, right.Field) && (right.ExpirationMs == nil || left.ExpirationMs == *right.ExpirationMs)
}

func equalRangePartitioning(left, right *domain.RangePartitioning) bool {
	return left != nil && right != nil && strings.EqualFold(left.Field, right.Field) && reflect.DeepEqual(left.Range, right.Range)
}

func equalOrderedFieldNames(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !strings.EqualFold(left[index], right[index]) {
			return false
		}
	}
	return true
}

func cloneRangePartitioning(value *domain.RangePartitioning) *domain.RangePartitioning {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneOptionalStrings(values []string) []string {
	if values == nil {
		return nil
	}
	return append(make([]string, 0, len(values)), values...)
}

func mergeInferredSchemaUpdate(current, inferred []domain.Field, allowRelaxation bool) []domain.Field {
	result := catalogdomain.CloneFields(inferred)
	for index := 0; index < len(current) && index < len(result); index++ {
		if !strings.EqualFold(current[index].Name, result[index].Name) ||
			canonicalLoadFieldType(current[index].Type) != canonicalLoadFieldType(result[index].Type) {
			continue
		}
		inferredMode := strings.ToUpper(result[index].Mode)
		if inferredMode == "" {
			inferredMode = "NULLABLE"
		}
		canonical := catalogdomain.CloneFields([]catalogdomain.Field{current[index]})[0]
		currentMode := strings.ToUpper(canonical.Mode)
		if currentMode == "" {
			currentMode = "NULLABLE"
		}
		if allowRelaxation && currentMode == "REQUIRED" && inferredMode == "NULLABLE" {
			canonical.Mode = "NULLABLE"
		}
		if len(current[index].Fields) != 0 && len(result[index].Fields) != 0 {
			canonical.Fields = mergeInferredSchemaUpdate(current[index].Fields, result[index].Fields, allowRelaxation)
		}
		result[index] = canonical
	}
	return result
}

func schemaUpdateOptionEnabled(options []domain.SchemaUpdateOption, target domain.SchemaUpdateOption) bool {
	for _, option := range options {
		if option == target {
			return true
		}
	}
	return false
}

func canonicalLoadFieldType(fieldType string) string {
	switch strings.ToUpper(fieldType) {
	case "BOOL":
		return "BOOLEAN"
	case "INT64":
		return "INTEGER"
	case "FLOAT64":
		return "FLOAT"
	case "STRUCT":
		return "RECORD"
	default:
		return strings.ToUpper(fieldType)
	}
}

func validateRequestedSchemaUpdate(
	current, proposed []domain.Field,
	options []domain.SchemaUpdateOption,
) (catalogdomain.SchemaEvolution, error) {
	allowed := catalogdomain.SchemaEvolutionOptions{}
	for _, option := range options {
		switch option {
		case domain.AllowFieldAddition:
			allowed.AllowFieldAddition = true
		case domain.AllowFieldRelaxation:
			allowed.AllowFieldRelaxation = true
		}
	}
	evolution, err := catalogdomain.ValidateSchemaUpdate(current, proposed, allowed)
	if err != nil {
		return catalogdomain.SchemaEvolution{}, fmt.Errorf("%w: %v", domain.ErrInvalid, err)
	}
	return evolution, nil
}

func translateCatalogSchemaError(err error) error {
	switch {
	case errors.Is(err, catalogdomain.ErrUnsupported):
		return fmt.Errorf("%w: capability=%s: %v", domain.ErrUnsupported, catalogdomain.CapabilityEngineSchemaV1, err)
	case errors.Is(err, catalogdomain.ErrInvalid):
		return fmt.Errorf("%w: %v", domain.ErrInvalid, err)
	case errors.Is(err, catalogdomain.ErrPrecondition):
		return fmt.Errorf("%w: %v", domain.ErrPrecondition, err)
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
			if err := validateResolvedObject(pattern, object); err != nil {
				finalErr = err
				return nil, finalErr
			}
			if object.Size > s.config.MaxObjectBytes {
				finalErr = fmt.Errorf("%w: an object exceeds the configured size limit", domain.ErrPrecondition)
				return nil, finalErr
			}
			if existing, ok := seen[object.URI]; ok {
				if objectIdentityChanged(existing, object) {
					finalErr = fmt.Errorf("%w: overlapping source patterns resolved different immutable object metadata", domain.ErrPrecondition)
					return nil, finalErr
				}
				if existing.ETag != "" || object.ETag == "" {
					continue
				}
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

func validateResolvedObject(pattern string, object ports.ObjectInfo) error {
	requested, err := domain.ParseGCSObjectURI(pattern)
	if err != nil {
		return err
	}
	resolved, err := domain.ParseGCSObjectURI(object.URI)
	if err != nil {
		return fmt.Errorf("%w: object store returned an invalid source URI", domain.ErrPrecondition)
	}
	if requested.Bucket() != resolved.Bucket() {
		return fmt.Errorf("%w: object store returned an object from a different bucket", domain.ErrPrecondition)
	}
	matched, err := pathpkg.Match(requested.ObjectName(), resolved.ObjectName())
	if err != nil || !matched {
		return fmt.Errorf("%w: object store returned an object outside the source pattern", domain.ErrPrecondition)
	}
	if object.Size < 0 {
		return fmt.Errorf("%w: object metadata has a negative size", domain.ErrInvalid)
	}
	if _, err := strconv.ParseUint(object.Generation, 10, 64); err != nil {
		return fmt.Errorf("%w: immutable object generation is required", domain.ErrPrecondition)
	}
	return nil
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
		if written != object.Size {
			operationErr = fmt.Errorf("%w: downloaded source size differs from resolved metadata", domain.ErrPrecondition)
			return nil, total, operationErr
		}
		local = append(local, ports.LocalObject{
			Path: path, Fingerprint: objectFingerprint(object), Size: written,
		})
	}
	return local, total, nil
}

func terminalError(err error) (string, string) {
	switch {
	case errors.Is(err, domain.ErrUnsupported):
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

func objectFingerprint(object ports.ObjectInfo) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{
		object.URI, object.Generation, object.ETag, strconv.FormatInt(object.Size, 10),
	}, "\x00")))
	return hex.EncodeToString(digest[:])
}

func objectIdentityChanged(left, right ports.ObjectInfo) bool {
	if left.Generation != right.Generation || left.Size != right.Size {
		return true
	}
	return left.ETag != "" && right.ETag != "" && left.ETag != right.ETag
}
