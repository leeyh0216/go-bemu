package application

// Query jobs follow the BigQuery REST lifecycle while execution is delegated
// through the replaceable QueryEngine port.
//
// Official sources:
//   - jobs.insert: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert
//   - jobs.get: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/get
//   - jobs.query: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
//
// The application owns PENDING -> RUNNING -> DONE transitions and persists each
// observable state through JobRepository. SQL execution remains behind the
// replaceable QueryEngine outbound port.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type QueryInput struct {
	ProjectID         string
	JobID             string
	Location          string
	DefaultDataset    string
	SQL               string
	Destination       *domain.TableReference
	WriteDisposition  domain.WriteDisposition
	CreateDisposition domain.CreateDisposition
	Priority          domain.QueryPriority
	Labels            map[string]string
}

type QueryService struct {
	jobs            ports.JobRepository
	warehouse       ports.QueryEngine
	materializer    ports.QueryMaterializer
	destinations    ports.QueryDestinationCatalog
	clock           ports.Clock
	ids             ports.IDGenerator
	defaultLocation string
}

type QueryOption func(*QueryService)

// WithQueryDefaultLocation supplies the configured location when callers omit
// jobReference.location. Values follow the BigQuery location catalog:
// https://cloud.google.com/bigquery/docs/locations.
func WithQueryDefaultLocation(location string) QueryOption {
	return func(service *QueryService) {
		if location != "" {
			service.defaultLocation = location
		}
	}
}

// WithQueryDestinationCatalog enables explicit query destinations. Keeping the
// metadata port optional preserves the query-only vertical slice while making a
// missing composition dependency fail explicitly instead of creating a physical
// table that tables.get cannot observe.
func WithQueryDestinationCatalog(catalog ports.QueryDestinationCatalog) QueryOption {
	return func(service *QueryService) { service.destinations = catalog }
}

// WithQueryMaterializer makes the physical destination capability explicit at
// composition time. Backends that only implement QueryEngine therefore cannot
// accidentally advertise destination-table support through a type assertion.
func WithQueryMaterializer(materializer ports.QueryMaterializer) QueryOption {
	return func(service *QueryService) { service.materializer = materializer }
}

func NewQueryService(jobs ports.JobRepository, warehouse ports.QueryEngine, clock ports.Clock, ids ports.IDGenerator, options ...QueryOption) *QueryService {
	service := &QueryService{jobs: jobs, warehouse: warehouse, clock: clock, ids: ids, defaultLocation: "US"}
	for _, option := range options {
		option(service)
	}
	return service
}

func (s *QueryService) RunSync(ctx context.Context, input QueryInput) (*domain.Job, error) {
	job, created, err := s.newJob(ctx, input)
	if err != nil {
		return nil, err
	}
	if !created {
		return nil, fmt.Errorf("%w: query job ID %q already exists", domain.ErrConflict, job.Reference.JobID)
	}
	s.execute(ctx, job)
	return s.jobs.Get(ctx, job.Reference)
}

func (s *QueryService) Submit(ctx context.Context, input QueryInput) (*domain.Job, error) {
	job, created, err := s.newJob(ctx, input)
	if err != nil {
		return nil, err
	}
	if !created {
		return nil, fmt.Errorf("%w: query job ID %q already exists", domain.ErrConflict, job.Reference.JobID)
	}
	executionJob := *job
	go s.execute(context.WithoutCancel(ctx), &executionJob)
	return job, nil
}

func (s *QueryService) Get(ctx context.Context, reference domain.JobReference) (*domain.Job, error) {
	if reference.Location == "" {
		reference.Location = s.defaultLocation
	}
	return s.jobs.Get(ctx, reference)
}

func (s *QueryService) List(ctx context.Context, projectID, location string) ([]*domain.Job, error) {
	return s.jobs.List(ctx, projectID, location)
}

func (s *QueryService) newJob(ctx context.Context, input QueryInput) (*domain.Job, bool, error) {
	if input.JobID == "" {
		input.JobID = "job_" + s.ids.NewID()
	}
	if input.Location == "" {
		input.Location = s.defaultLocation
	}
	if input.Destination != nil && input.Destination.ProjectID == "" {
		destination := *input.Destination
		destination.ProjectID = input.ProjectID
		input.Destination = &destination
	}
	job, err := domain.NewConfiguredQueryJob(domain.JobReference{
		ProjectID: input.ProjectID,
		JobID:     input.JobID,
		Location:  input.Location,
	}, domain.QueryConfiguration{
		SQL: input.SQL, DefaultDataset: input.DefaultDataset, Destination: input.Destination,
		WriteDisposition: input.WriteDisposition, CreateDisposition: input.CreateDisposition,
		Priority: input.Priority, Labels: input.Labels,
	}, s.clock.Now())
	if err != nil {
		return nil, false, err
	}
	started := observability.LogSideEffectStart(ctx, "job_repository", "create_job",
		"project_id", job.Reference.ProjectID, "job_id", job.Reference.JobID,
		"location", job.Reference.Location, "job_type", "QUERY",
		"priority", job.Configuration.Priority, "label_count", len(job.Configuration.Labels),
		"label_keys_fingerprint", queryLabelKeysFingerprint(job.Configuration.Labels),
		"configuration_fingerprint", job.ConfigurationDigest)
	stored, created, err := s.jobs.CreateOrGet(ctx, job)
	if err != nil {
		observability.LogSideEffectEnd(ctx, "job_repository", "create_job", started, err,
			"project_id", job.Reference.ProjectID, "job_id", job.Reference.JobID)
		return nil, false, err
	}
	if !created {
		reason := "same query configuration"
		if stored.ConfigurationDigest != job.ConfigurationDigest {
			reason = "different query configuration"
		}
		err := fmt.Errorf("%w: job ID %q already exists with %s; configuration_fingerprint=%s existing_configuration_fingerprint=%s",
			domain.ErrConflict, job.Reference.JobID, reason, job.ConfigurationDigest, stored.ConfigurationDigest)
		observability.LogSideEffectEnd(ctx, "job_repository", "create_job", started, err,
			"project_id", job.Reference.ProjectID, "job_id", job.Reference.JobID,
			"configuration_fingerprint", job.ConfigurationDigest,
			"existing_configuration_fingerprint", stored.ConfigurationDigest)
		return nil, false, err
	}
	observability.LogSideEffectEnd(ctx, "job_repository", "create_job", started, nil,
		"project_id", job.Reference.ProjectID, "job_id", job.Reference.JobID, "job_state", stored.State,
		"configuration_fingerprint", job.ConfigurationDigest, "created", created)
	return stored, created, nil
}

func queryLabelKeysFingerprint(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return observability.Digest([]byte(strings.Join(keys, "\x00")))
}

func (s *QueryService) execute(ctx context.Context, job *domain.Job) {
	boundaryAttrs := append(observability.ContextAttrs(ctx),
		"event", "boundary.enter", "boundary", "application.query",
		"project_id", job.Reference.ProjectID, "job_id", job.Reference.JobID,
		"location", job.Reference.Location, "job_state", job.State,
	)
	slog.InfoContext(ctx, "query job", boundaryAttrs...)
	if err := job.Start(s.clock.Now()); err != nil {
		attrs := []any{"project_id", job.Reference.ProjectID, "job_id", job.Reference.JobID}
		attrs = append(attrs, observability.ErrorAttrs(err)...)
		slog.ErrorContext(ctx, "query job start failed", attrs...)
		return
	}
	if err := s.jobs.Update(ctx, job); err != nil {
		attrs := []any{"project_id", job.Reference.ProjectID, "job_id", job.Reference.JobID, "job_state", job.State}
		attrs = append(attrs, observability.ErrorAttrs(err)...)
		slog.ErrorContext(ctx, "query job state persistence failed", attrs...)
		return
	}
	result, err := s.executeQuery(ctx, job)
	if err != nil {
		reason := queryTerminalReason(err)
		_ = job.Fail(reason, err.Error(), s.clock.Now())
	} else {
		_ = job.Complete(result, s.clock.Now())
	}
	persistErr := s.jobs.Update(context.WithoutCancel(ctx), job)
	columnsSummary := fmt.Sprintf("%v", result.Columns)
	exitAttrs := append(observability.ContextAttrs(ctx),
		"event", "boundary.exit", "boundary", "application.query",
		"project_id", job.Reference.ProjectID, "job_id", job.Reference.JobID,
		"location", job.Reference.Location, "job_state", job.State,
		"row_count", len(result.Rows), "affected_rows", result.AffectedRows,
		"schema_fingerprint", observability.Digest([]byte(columnsSummary)), "success", job.Error == nil && persistErr == nil,
	)
	if job.Error != nil {
		exitAttrs = append(exitAttrs, "error_reason", job.Error.Reason)
		exitAttrs = append(exitAttrs, observability.ErrorAttrs(fmt.Errorf("%s", job.Error.Message))...)
	}
	if persistErr != nil {
		exitAttrs = append(exitAttrs, "persistence_error_digest", observability.Digest([]byte(persistErr.Error())))
	}
	slog.InfoContext(ctx, "query job", exitAttrs...)
}

func (s *QueryService) executeQuery(ctx context.Context, job *domain.Job) (domain.QueryResult, error) {
	configuration := job.Configuration
	request := ports.QueryRequest{
		ProjectID: job.Reference.ProjectID, DefaultDataset: configuration.DefaultDataset, SQL: configuration.SQL,
	}
	if configuration.Destination == nil {
		return s.warehouse.Query(ctx, request)
	}
	if s.materializer == nil || s.destinations == nil {
		return domain.QueryResult{}, fmt.Errorf("%w: explicit destination requires query materializer and destination catalog ports; fix_hint=compose WithQueryDestinationCatalog", domain.ErrPrecondition)
	}

	destination := *configuration.Destination
	dataset, err := s.destinations.GetDataset(ctx, destination.ProjectID, destination.DatasetID)
	if err != nil {
		return domain.QueryResult{}, err
	}
	if dataset.Location != "" && !strings.EqualFold(dataset.Location, job.Reference.Location) {
		return domain.QueryResult{}, fmt.Errorf("%w: destination dataset and query job locations differ", domain.ErrInvalid)
	}
	existing, lookupErr := s.destinations.GetTable(ctx, destination.ProjectID, destination.DatasetID, destination.TableID)
	destinationExists := lookupErr == nil
	if lookupErr != nil && !errors.Is(lookupErr, domain.ErrNotFound) {
		return domain.QueryResult{}, lookupErr
	}
	if !destinationExists && configuration.CreateDisposition == domain.CreateNever {
		return domain.QueryResult{}, fmt.Errorf("%w: destination table does not exist and createDisposition is CREATE_NEVER", domain.ErrNotFound)
	}

	materialized, err := s.materializer.MaterializeQuery(ctx, ports.QueryMaterializationRequest{
		Query: request, Destination: destination, DestinationExists: destinationExists,
		DestinationSchema: existing.Schema, WriteDisposition: configuration.WriteDisposition,
		CreateDisposition: configuration.CreateDisposition,
	})
	if err != nil {
		return domain.QueryResult{}, err
	}
	if destinationExists {
		if materialized.DestinationCreated {
			return domain.QueryResult{}, fmt.Errorf("%w: materializer reported existing destination as newly created", domain.ErrPrecondition)
		}
		return materialized.QueryResult, nil
	}
	if !materialized.DestinationCreated {
		return domain.QueryResult{}, fmt.Errorf("%w: materializer did not create the requested destination", domain.ErrPrecondition)
	}

	fields := make([]domain.Field, len(materialized.QueryResult.Columns))
	for index, column := range materialized.QueryResult.Columns {
		fields[index] = domain.Field{Name: column.Name, Type: column.Type, Mode: "NULLABLE"}
	}
	table := domain.Table{
		ProjectID: destination.ProjectID, DatasetID: destination.DatasetID, ID: destination.TableID,
		Type: "TABLE", Schema: fields, Location: dataset.Location,
	}
	if publishErr := s.destinations.PublishMaterializedTable(ctx, table); publishErr != nil {
		cleanupErr := s.materializer.DropMaterializedDestination(context.WithoutCancel(ctx), destination)
		if cleanupErr != nil {
			return domain.QueryResult{}, errors.Join(publishErr, fmt.Errorf("compensate unpublished destination: %w", cleanupErr))
		}
		return domain.QueryResult{}, publishErr
	}
	return materialized.QueryResult, nil
}

func queryTerminalReason(err error) string {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		return "notFound"
	case errors.Is(err, domain.ErrConflict):
		return "duplicate"
	case errors.Is(err, domain.ErrPrecondition):
		return "conditionNotMet"
	default:
		return "invalidQuery"
	}
}
