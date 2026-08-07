package memory

// JobRepository is a concurrency-safe development adapter for the BigQuery
// REST job lifecycle. It is intentionally replaceable; durable job metadata is
// tracked separately from the protocol model.
// https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs

import (
	"context"
	"fmt"
	"sort"
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

func jobKey(projectID, jobID string) string { return projectID + "/" + jobID }

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

func (r *JobRepository) Create(_ context.Context, job *domain.Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := jobKey(job.Reference.ProjectID, job.Reference.JobID)
	if _, ok := r.jobs[key]; ok {
		return fmt.Errorf("%w: job %s", domain.ErrConflict, key)
	}
	r.jobs[key] = cloneJob(job)
	return nil
}

func (r *JobRepository) Update(_ context.Context, job *domain.Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := jobKey(job.Reference.ProjectID, job.Reference.JobID)
	if _, ok := r.jobs[key]; !ok {
		return fmt.Errorf("%w: job %s", domain.ErrNotFound, key)
	}
	r.jobs[key] = cloneJob(job)
	return nil
}

func (r *JobRepository) Get(_ context.Context, projectID, jobID string) (*domain.Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	job, ok := r.jobs[jobKey(projectID, jobID)]
	if !ok {
		return nil, fmt.Errorf("%w: job %s/%s", domain.ErrNotFound, projectID, jobID)
	}
	return cloneJob(job), nil
}

func (r *JobRepository) List(_ context.Context, projectID string) ([]*domain.Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	jobs := make([]*domain.Job, 0)
	for _, job := range r.jobs {
		if job.Reference.ProjectID == projectID {
			jobs = append(jobs, cloneJob(job))
		}
	}
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].CreatedAt.Equal(jobs[j].CreatedAt) {
			return jobs[i].Reference.JobID < jobs[j].Reference.JobID
		}
		return jobs[i].CreatedAt.After(jobs[j].CreatedAt)
	})
	return jobs, nil
}
