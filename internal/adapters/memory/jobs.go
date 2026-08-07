package memory

// JobRepository is a concurrency-safe development adapter for the BigQuery
// REST job lifecycle. It is intentionally replaceable; durable job metadata is
// tracked separately from the protocol model.
// https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type JobRepository struct {
	mu   sync.RWMutex
	jobs map[string]*domain.Job
}

var _ ports.JobRepository = (*JobRepository)(nil)

func NewJobRepository() *JobRepository {
	return &JobRepository{jobs: make(map[string]*domain.Job)}
}

func jobKey(reference domain.JobReference) (string, error) {
	if err := reference.Validate(); err != nil {
		return "", err
	}
	return reference.ProjectID + "\x00" + strings.ToUpper(reference.Location) + "\x00" + reference.JobID, nil
}

func cloneJob(job *domain.Job) *domain.Job {
	clone := *job
	if job.StartedAt != nil {
		startedAt := *job.StartedAt
		clone.StartedAt = &startedAt
	}
	if job.EndedAt != nil {
		endedAt := *job.EndedAt
		clone.EndedAt = &endedAt
	}
	if job.Result != nil {
		result := *job.Result
		result.Columns = append([]domain.Column(nil), job.Result.Columns...)
		result.Rows = make([][]any, len(job.Result.Rows))
		for rowIndex, row := range job.Result.Rows {
			result.Rows[rowIndex] = make([]any, len(row))
			for valueIndex, value := range row {
				result.Rows[rowIndex][valueIndex] = cloneJobValue(value)
			}
		}
		clone.Result = &result
	}
	clone.Configuration = job.Configuration
	if job.Configuration.Labels != nil {
		clone.Configuration.Labels = make(map[string]string, len(job.Configuration.Labels))
		for key, value := range job.Configuration.Labels {
			clone.Configuration.Labels[key] = value
		}
	}
	if job.Configuration.Destination != nil {
		destination := *job.Configuration.Destination
		clone.Configuration.Destination = &destination
	}
	if job.Error != nil {
		jobError := *job.Error
		clone.Error = &jobError
	}
	return &clone
}

func cloneJobValue(value any) any {
	switch typed := value.(type) {
	case []byte:
		return append([]byte(nil), typed...)
	case []any:
		clone := make([]any, len(typed))
		for index, nested := range typed {
			clone[index] = cloneJobValue(nested)
		}
		return clone
	case map[string]any:
		clone := make(map[string]any, len(typed))
		for key, nested := range typed {
			clone[key] = cloneJobValue(nested)
		}
		return clone
	default:
		return typed
	}
}

func (r *JobRepository) CreateOrGet(_ context.Context, job *domain.Job) (*domain.Job, bool, error) {
	if job == nil {
		return nil, false, fmt.Errorf("%w: query job is required", domain.ErrInvalid)
	}
	key, err := jobKey(job.Reference)
	if err != nil {
		return nil, false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.jobs[key]; ok {
		return cloneJob(existing), false, nil
	}
	r.jobs[key] = cloneJob(job)
	return cloneJob(job), true, nil
}

func (r *JobRepository) Update(_ context.Context, job *domain.Job) error {
	if job == nil {
		return fmt.Errorf("%w: query job is required", domain.ErrInvalid)
	}
	key, err := jobKey(job.Reference)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.jobs[key]; !ok {
		return fmt.Errorf("%w: job %s", domain.ErrNotFound, key)
	}
	r.jobs[key] = cloneJob(job)
	return nil
}

func (r *JobRepository) Get(_ context.Context, reference domain.JobReference) (*domain.Job, error) {
	key, err := jobKey(reference)
	if err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	job, ok := r.jobs[key]
	if !ok {
		return nil, fmt.Errorf("%w: query job %s", domain.ErrNotFound, reference.JobID)
	}
	return cloneJob(job), nil
}

func (r *JobRepository) List(_ context.Context, projectID, location string) ([]*domain.Job, error) {
	if err := domain.ValidateJobListScope(projectID, location); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	jobs := make([]*domain.Job, 0)
	for _, job := range r.jobs {
		if job.Reference.ProjectID == projectID && (location == "" || strings.EqualFold(job.Reference.Location, location)) {
			jobs = append(jobs, cloneJob(job))
		}
	}
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].CreatedAt.Equal(jobs[j].CreatedAt) {
			if jobs[i].Reference.JobID == jobs[j].Reference.JobID {
				return strings.ToUpper(jobs[i].Reference.Location) < strings.ToUpper(jobs[j].Reference.Location)
			}
			return jobs[i].Reference.JobID < jobs[j].Reference.JobID
		}
		return jobs[i].CreatedAt.After(jobs[j].CreatedAt)
	})
	return jobs, nil
}
