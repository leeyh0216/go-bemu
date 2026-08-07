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
	"fmt"
	"log/slog"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type QueryInput struct {
	ProjectID      string
	JobID          string
	Location       string
	DefaultDataset string
	SQL            string
}

type QueryService struct {
	jobs            ports.JobRepository
	warehouse       ports.QueryEngine
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

func NewQueryService(jobs ports.JobRepository, warehouse ports.QueryEngine, clock ports.Clock, ids ports.IDGenerator, options ...QueryOption) *QueryService {
	service := &QueryService{jobs: jobs, warehouse: warehouse, clock: clock, ids: ids, defaultLocation: "US"}
	for _, option := range options {
		option(service)
	}
	return service
}

func (s *QueryService) RunSync(ctx context.Context, input QueryInput) (*domain.Job, error) {
	job, err := s.newJob(ctx, input)
	if err != nil {
		return nil, err
	}
	s.execute(ctx, job, input)
	return s.jobs.Get(ctx, job.Reference.ProjectID, job.Reference.JobID)
}

func (s *QueryService) Submit(ctx context.Context, input QueryInput) (*domain.Job, error) {
	job, err := s.newJob(ctx, input)
	if err != nil {
		return nil, err
	}
	executionJob := *job
	go s.execute(context.WithoutCancel(ctx), &executionJob, input)
	return job, nil
}

func (s *QueryService) Get(ctx context.Context, projectID, jobID string) (*domain.Job, error) {
	return s.jobs.Get(ctx, projectID, jobID)
}

func (s *QueryService) List(ctx context.Context, projectID string) ([]*domain.Job, error) {
	return s.jobs.List(ctx, projectID)
}

func (s *QueryService) newJob(ctx context.Context, input QueryInput) (*domain.Job, error) {
	if input.JobID == "" {
		input.JobID = "job_" + s.ids.NewID()
	}
	if input.Location == "" {
		input.Location = s.defaultLocation
	}
	job, err := domain.NewQueryJob(domain.JobReference{
		ProjectID: input.ProjectID,
		JobID:     input.JobID,
		Location:  input.Location,
	}, input.SQL, s.clock.Now())
	if err != nil {
		return nil, err
	}
	started := observability.LogSideEffectStart(ctx, "job_repository", "create_job",
		"project_id", job.Reference.ProjectID, "job_id", job.Reference.JobID,
		"location", job.Reference.Location, "job_type", "QUERY")
	if err := s.jobs.Create(ctx, job); err != nil {
		observability.LogSideEffectEnd(ctx, "job_repository", "create_job", started, err,
			"project_id", job.Reference.ProjectID, "job_id", job.Reference.JobID)
		return nil, err
	}
	observability.LogSideEffectEnd(ctx, "job_repository", "create_job", started, nil,
		"project_id", job.Reference.ProjectID, "job_id", job.Reference.JobID, "job_state", job.State)
	return job, nil
}

func (s *QueryService) execute(ctx context.Context, job *domain.Job, input QueryInput) {
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
	result, err := s.warehouse.Query(ctx, ports.QueryRequest{
		ProjectID:      input.ProjectID,
		DefaultDataset: input.DefaultDataset,
		SQL:            input.SQL,
	})
	if err != nil {
		_ = job.Fail("invalidQuery", err.Error(), s.clock.Now())
	} else {
		_ = job.Complete(result, s.clock.Now())
	}
	persistErr := s.jobs.Update(ctx, job)
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
