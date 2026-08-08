package application

// Query jobs follow the BigQuery REST lifecycle while execution is delegated
// through the analyzed statement ports.
//
// Official sources:
//   - jobs.insert: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert
//   - jobs.get: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/get
//   - jobs.query: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
//
// The application owns PENDING -> RUNNING -> DONE transitions and persists each
// observable state through JobRepository. SQL crosses one GoogleSQL analysis
// boundary before an engine-neutral statement reaches an execution adapter.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

type QueryInput struct {
	ProjectID         string
	JobID             string
	Location          string
	DefaultProjectID  string
	DefaultDataset    string
	SQL               string
	Destination       *domain.TableReference
	WriteDisposition  domain.WriteDisposition
	CreateDisposition domain.CreateDisposition
	Priority          domain.QueryPriority
	Labels            map[string]string
}

type QueryService struct {
	jobs                  ports.JobRepository
	googleSQLGateway      ports.GoogleSQLGateway
	statementExecutor     ports.StatementExecutor
	statementMaterializer ports.StatementMaterializer
	ddlExecutor           ports.DDLExecutor
	destinations          ports.QueryDestinationCatalog
	clock                 ports.Clock
	ids                   ports.IDGenerator
	defaultLocation       string
	anonymousTTL          time.Duration
	operationTimeout      time.Duration
	compensationTimeout   time.Duration
	runtimeCtx            context.Context
	cancelRuntime         context.CancelFunc
	runtimeMu             sync.Mutex
	closing               bool
	activeWork            int
	idle                  chan struct{}
}

// ErrQueryServiceClosed rejects execution admission once shutdown begins.
// Metadata reads remain available while transports finish draining.
var ErrQueryServiceClosed = errors.New("query service is closing")

type QueryOption func(*QueryService)

type preparedQuery struct {
	statement     semantic.Statement
	analysis      ports.QueryAnalysis
	analysisError error
	statementKind string
}

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

// WithGoogleSQLGateway installs the single official parse/analyze boundary.
// Its immutable result is retained through execution without reparsing SQL.
func WithGoogleSQLGateway(gateway ports.GoogleSQLGateway) QueryOption {
	return func(service *QueryService) { service.googleSQLGateway = gateway }
}

func WithStatementExecutor(executor ports.StatementExecutor) QueryOption {
	return func(service *QueryService) { service.statementExecutor = executor }
}

func WithStatementMaterializer(materializer ports.StatementMaterializer) QueryOption {
	return func(service *QueryService) { service.statementMaterializer = materializer }
}

// WithAnonymousQueryTTL overrides the documented approximately 24-hour
// lifetime of anonymous query-result tables. Composition keeps the official
// default; tests can inject a shorter deterministic policy.
// https://cloud.google.com/bigquery/docs/cached-results#how_cached_results_are_stored
func WithAnonymousQueryTTL(ttl time.Duration) QueryOption {
	return func(service *QueryService) {
		if ttl > 0 {
			service.anonymousTTL = ttl
		}
	}
}

// WithQueryOperationTimeout sets the emulator-owned hard ceiling for one query
// execution. This is independent from request-scoped timeoutMs/jobTimeoutMs and
// ensures asynchronous jobs cannot retain the single DuckDB connection forever.
// https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfiguration.FIELDS.job_timeout_ms
func WithQueryOperationTimeout(timeout time.Duration) QueryOption {
	return func(service *QueryService) {
		if timeout > 0 {
			service.operationTimeout = timeout
		}
	}
}

// WithQueryCompensationTimeout bounds cleanup of a physical CTAS destination
// when metadata publication fails after the caller context has ended.
func WithQueryCompensationTimeout(timeout time.Duration) QueryOption {
	return func(service *QueryService) {
		if timeout > 0 {
			service.compensationTimeout = timeout
		}
	}
}

// NewQueryService owns the query job lifecycle. GoogleSQL analysis and typed
// execution are installed explicitly through options at the composition root.
func NewQueryService(
	jobs ports.JobRepository,
	clock ports.Clock,
	ids ports.IDGenerator,
	options ...QueryOption,
) (*QueryService, error) {
	for _, dependency := range []struct {
		name  string
		value any
	}{
		{name: "job repository", value: jobs},
		{name: "clock", value: clock},
		{name: "ID generator", value: ids},
	} {
		if queryServiceDependencyIsNil(dependency.value) {
			return nil, fmt.Errorf("%w: query service %s is required", domain.ErrPrecondition, dependency.name)
		}
	}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: query service option is nil", domain.ErrPrecondition)
		}
	}
	runtimeCtx, cancelRuntime := context.WithCancel(context.Background())
	idle := make(chan struct{})
	close(idle)
	service := &QueryService{
		jobs:  jobs,
		clock: clock, ids: ids,
		defaultLocation: "US", anonymousTTL: 24 * time.Hour,
		operationTimeout: 2 * time.Minute, compensationTimeout: 30 * time.Second,
		runtimeCtx: runtimeCtx, cancelRuntime: cancelRuntime, idle: idle,
	}
	for _, option := range options {
		option(service)
	}
	if queryServiceDependencyIsNil(service.googleSQLGateway) {
		cancelRuntime()
		return nil, fmt.Errorf("%w: query service requires the GoogleSQL gateway", domain.ErrPrecondition)
	}
	if queryServiceDependencyIsNil(service.statementExecutor) {
		cancelRuntime()
		return nil, fmt.Errorf("%w: analyzed statement executor is required with the GoogleSQL gateway", domain.ErrPrecondition)
	}
	return service, nil
}

func queryServiceDependencyIsNil(value any) bool {
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

func (s *QueryService) RunSync(ctx context.Context, input QueryInput) (*domain.Job, error) {
	if err := s.beginWork(); err != nil {
		return nil, err
	}
	defer s.finishWork()
	workCtx, cancelWork := s.withRuntimeCancellation(ctx)
	defer cancelWork()

	job, prepared, created, err := s.newJob(workCtx, input)
	if err != nil {
		return nil, err
	}
	if !created {
		return nil, fmt.Errorf("%w: query job ID %q already exists", domain.ErrConflict, job.Reference.JobID)
	}
	executionCtx, cancelExecution := context.WithTimeout(workCtx, s.operationTimeout)
	s.execute(executionCtx, job, prepared)
	cancelExecution()
	readBase, cancelReadBase := s.withRuntimeCancellation(context.WithoutCancel(ctx))
	defer cancelReadBase()
	readCtx, cancelRead := context.WithTimeout(readBase, s.operationTimeout)
	defer cancelRead()
	return s.jobs.Get(readCtx, job.Reference)
}

func (s *QueryService) Submit(ctx context.Context, input QueryInput) (*domain.Job, error) {
	if err := s.beginWork(); err != nil {
		return nil, err
	}
	workCtx, cancelWork := s.withRuntimeCancellation(ctx)
	job, prepared, created, err := s.newJob(workCtx, input)
	cancelWork()
	if err != nil {
		s.finishWork()
		return nil, err
	}
	if !created {
		s.finishWork()
		return nil, fmt.Errorf("%w: query job ID %q already exists", domain.ErrConflict, job.Reference.JobID)
	}
	executionJob := *job
	go func() {
		defer s.finishWork()
		executionBase, cancelExecutionBase := s.withRuntimeCancellation(context.WithoutCancel(ctx))
		defer cancelExecutionBase()
		executionCtx, cancelExecution := context.WithTimeout(executionBase, s.operationTimeout)
		defer cancelExecution()
		s.execute(executionCtx, &executionJob, prepared)
	}()
	return job, nil
}

// Close stops query admission, cancels service-owned execution, and waits for
// every admitted synchronous and asynchronous operation. The caller controls
// the bounded wait; a later Close may resume waiting after a deadline.
func (s *QueryService) Close(ctx context.Context) error {
	s.runtimeMu.Lock()
	if !s.closing {
		s.closing = true
		s.cancelRuntime()
	}
	idle := s.idle
	s.runtimeMu.Unlock()

	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("close query service: %w", ctx.Err())
	}
}

func (s *QueryService) beginWork() error {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	if s.closing {
		return ErrQueryServiceClosed
	}
	if s.activeWork == 0 {
		s.idle = make(chan struct{})
	}
	s.activeWork++
	return nil
}

func (s *QueryService) finishWork() {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	s.activeWork--
	if s.activeWork == 0 {
		close(s.idle)
	}
}

// withRuntimeCancellation preserves request values and cancellation while also
// making service shutdown authoritative. context.AfterFunc adds no waiter that
// can retain DuckDB ownership after the operation completes.
func (s *QueryService) withRuntimeCancellation(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(s.runtimeCtx, cancel)
	if s.runtimeCtx.Err() != nil {
		cancel()
	}
	return ctx, func() {
		stop()
		cancel()
	}
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

func (s *QueryService) newJob(ctx context.Context, input QueryInput) (*domain.Job, preparedQuery, bool, error) {
	if input.JobID == "" {
		input.JobID = "job_" + s.ids.NewID()
	}
	if input.Destination != nil && input.Destination.ProjectID == "" {
		destination := *input.Destination
		destination.ProjectID = input.ProjectID
		input.Destination = &destination
	}
	request := ports.QueryRequest{
		ProjectID: input.ProjectID, DefaultProjectID: input.DefaultProjectID,
		DefaultDataset: input.DefaultDataset, SQL: input.SQL,
	}
	prepared, err := s.prepareQueryAdmission(ctx, request)
	if err != nil {
		return nil, preparedQuery{}, false, err
	}
	analysis := prepared.analysis
	if analysis.RequiresCatalogMutation && input.Destination != nil {
		return nil, preparedQuery{}, false, fmt.Errorf("%w: destinationTable is not valid for catalog DDL", domain.ErrInvalid)
	}
	location, err := s.resolveQueryLocation(ctx, input, analysis)
	if err != nil {
		return nil, preparedQuery{}, false, err
	}
	input.Location = location
	anonymousDestination := false
	if analysis.ProducesRows && input.Destination == nil {
		if s.statementMaterializer == nil || s.destinations == nil {
			return nil, preparedQuery{}, false, fmt.Errorf("%w: anonymous query results require statement materializer and destination catalog ports", domain.ErrPrecondition)
		}
		destination := anonymousQueryDestination(input.ProjectID, input.Location, input.JobID)
		input.Destination = &destination
		input.WriteDisposition = domain.WriteEmpty
		input.CreateDisposition = domain.CreateIfNeeded
		anonymousDestination = true
	}
	job, err := domain.NewConfiguredQueryJob(domain.JobReference{
		ProjectID: input.ProjectID,
		JobID:     input.JobID,
		Location:  input.Location,
	}, domain.QueryConfiguration{
		SQL: input.SQL, StatementType: prepared.statementType(), DefaultProjectID: input.DefaultProjectID,
		DefaultDataset: input.DefaultDataset, Destination: input.Destination,
		WriteDisposition: input.WriteDisposition, CreateDisposition: input.CreateDisposition,
		Priority: input.Priority, Labels: input.Labels, AnonymousDestination: anonymousDestination,
	}, s.clock.Now())
	if err != nil {
		return nil, preparedQuery{}, false, err
	}
	started := observability.LogSideEffectStart(ctx, "job_repository", "create_job",
		"project_id", job.Reference.ProjectID, "job_id", job.Reference.JobID,
		"location", job.Reference.Location, "job_type", "QUERY",
		"sql", job.Configuration.SQL, "labels", job.Configuration.Labels,
		"priority", job.Configuration.Priority, "label_count", len(job.Configuration.Labels),
		"label_keys_fingerprint", queryLabelKeysFingerprint(job.Configuration.Labels),
		"configuration_fingerprint", job.ConfigurationDigest)
	stored, created, err := s.jobs.CreateOrGet(ctx, job)
	if err != nil {
		observability.LogSideEffectEnd(ctx, "job_repository", "create_job", started, err,
			"project_id", job.Reference.ProjectID, "job_id", job.Reference.JobID)
		return nil, preparedQuery{}, false, err
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
		return nil, preparedQuery{}, false, err
	}
	observability.LogSideEffectEnd(ctx, "job_repository", "create_job", started, nil,
		"project_id", job.Reference.ProjectID, "job_id", job.Reference.JobID, "job_state", stored.State,
		"configuration_fingerprint", job.ConfigurationDigest, "created", created)
	return stored, prepared, created, nil
}

func queryLabelKeysFingerprint(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return observability.Digest([]byte(strings.Join(keys, "\x00")))
}

func (s *QueryService) execute(ctx context.Context, job *domain.Job, prepared preparedQuery) {
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
	result, err := s.executeQuery(ctx, job, prepared)
	if err != nil {
		reason := queryTerminalReason(err)
		_ = job.Fail(reason, err.Error(), s.clock.Now())
	} else {
		_ = job.Complete(result, s.clock.Now())
	}
	persistBase, cancelPersistBase := s.withRuntimeCancellation(context.WithoutCancel(ctx))
	persistCtx, cancelPersist := context.WithTimeout(persistBase, s.operationTimeout)
	persistErr := s.jobs.Update(persistCtx, job)
	cancelPersist()
	cancelPersistBase()
	columnsSummary := fmt.Sprintf("%v", result.Columns)
	exitAttrs := append(observability.ContextAttrs(ctx),
		"event", "boundary.exit", "boundary", "application.query",
		"project_id", job.Reference.ProjectID, "job_id", job.Reference.JobID,
		"location", job.Reference.Location, "job_state", job.State,
		"row_count", len(result.Rows), "affected_rows", result.AffectedRows,
		"rows", result.Rows, "schema", result.Columns,
		"schema_fingerprint", observability.Digest([]byte(columnsSummary)), "success", job.Error == nil && persistErr == nil,
	)
	if job.Error != nil {
		exitAttrs = append(exitAttrs, "error_reason", job.Error.Reason)
		exitAttrs = append(exitAttrs, observability.ErrorAttrs(fmt.Errorf("%s", job.Error.Message))...)
	}
	if persistErr != nil {
		exitAttrs = append(exitAttrs, "persistence_error", persistErr,
			"persistence_error_digest", observability.Digest([]byte(persistErr.Error())))
	}
	slog.InfoContext(ctx, "query job", exitAttrs...)
}

func (s *QueryService) executeQuery(ctx context.Context, job *domain.Job, prepared preparedQuery) (domain.QueryResult, error) {
	if prepared.analysisError != nil {
		return domain.QueryResult{}, prepared.analysisError
	}
	configuration := job.Configuration
	if configuration.Destination == nil {
		return s.executeAnalyzedStatement(ctx, prepared.statement, job.Reference.JobID)
	}
	if s.statementMaterializer == nil || s.destinations == nil {
		return domain.QueryResult{}, fmt.Errorf("%w: query destination requires analyzed statement materializer and destination catalog ports", domain.ErrPrecondition)
	}

	destination := *configuration.Destination
	var dataset domain.Dataset
	var err error
	if configuration.AnonymousDestination {
		dataset, err = s.destinations.EnsureAnonymousDataset(ctx, destination.ProjectID, destination.DatasetID, job.Reference.Location)
	} else {
		dataset, err = s.destinations.GetDataset(ctx, destination.ProjectID, destination.DatasetID)
	}
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

	materialized, err := s.statementMaterializer.MaterializeAnalyzedStatement(ctx, prepared.statement, ports.StatementMaterializationRequest{
		Destination: destination, DestinationExists: destinationExists,
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

	fields := domain.CloneFields(materialized.QueryResult.Columns)
	for _, field := range fields {
		if fieldErr := field.Validate(); fieldErr != nil {
			resultErr := fmt.Errorf("%w: query result lacks a canonical recursive schema; capability=%s: %v",
				domain.ErrPrecondition, domain.GapQueryComplexResultSchemaV1, fieldErr)
			if cleanupErr := s.compensateMaterializedDestination(ctx, destination); cleanupErr != nil {
				return domain.QueryResult{}, errors.Join(resultErr, cleanupErr)
			}
			return domain.QueryResult{}, resultErr
		}
	}
	table := domain.Table{
		ProjectID: destination.ProjectID, DatasetID: destination.DatasetID, ID: destination.TableID,
		Type: "TABLE", Schema: fields, Location: dataset.Location,
	}
	if configuration.AnonymousDestination {
		expires := s.clock.Now().Add(s.anonymousTTL)
		table.ExpirationTime = &expires
	}
	if publishErr := s.destinations.PublishMaterializedTable(ctx, table); publishErr != nil {
		cleanupErr := s.compensateMaterializedDestination(ctx, destination)
		if cleanupErr != nil {
			return domain.QueryResult{}, errors.Join(publishErr, cleanupErr)
		}
		return domain.QueryResult{}, publishErr
	}
	return materialized.QueryResult, nil
}

func (s *QueryService) compensateMaterializedDestination(ctx context.Context, destination domain.TableReference) error {
	cleanupBase, cancelCleanupBase := s.withRuntimeCancellation(context.WithoutCancel(ctx))
	defer cancelCleanupBase()
	cleanupCtx, cancelCleanup := context.WithTimeout(cleanupBase, s.compensationTimeout)
	defer cancelCleanup()
	err := s.statementMaterializer.DropMaterializedDestination(cleanupCtx, destination)
	if err != nil {
		return fmt.Errorf("compensate unpublished destination: %w", err)
	}
	return nil
}

func queryTerminalReason(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "stopped"
	case errors.Is(err, domain.ErrNotFound):
		return "notFound"
	case errors.Is(err, domain.ErrConflict):
		return "duplicate"
	case errors.Is(err, domain.ErrPrecondition):
		return "conditionNotMet"
	case errors.Is(err, domain.ErrInvalidQuery), errors.Is(err, domain.ErrInvalid):
		return "invalidQuery"
	case errors.Is(err, domain.ErrBackend):
		return "jobBackendError"
	default:
		return "invalidQuery"
	}
}
