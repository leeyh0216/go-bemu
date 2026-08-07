package memory

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
	if job.Result != nil {
		result := *job.Result
		result.Columns = append([]domain.Column(nil), job.Result.Columns...)
		result.Rows = append([][]any(nil), job.Result.Rows...)
		clone.Result = &result
	}
	if job.Error != nil {
		jobError := *job.Error
		clone.Error = &jobError
	}
	return &clone
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
