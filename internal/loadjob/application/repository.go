package application

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/leeyh0216/go-bemu/internal/loadjob/domain"
)

// MemoryJobRepository provides process-local idempotency. Persistence across
// emulator restarts is deliberately outside the P0 load-job slice.
type MemoryJobRepository struct {
	mu   sync.RWMutex
	jobs map[string]*domain.Job
}

func NewMemoryJobRepository() *MemoryJobRepository {
	return &MemoryJobRepository{jobs: make(map[string]*domain.Job)}
}

func jobKey(reference domain.JobReference) string {
	return reference.ProjectID + "\x00" + reference.Location + "\x00" + reference.JobID
}

func (r *MemoryJobRepository) CreateOrGet(_ context.Context, job *domain.Job) (*domain.Job, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := jobKey(job.Reference)
	if existing, ok := r.jobs[key]; ok {
		return existing.Clone(), false, nil
	}
	r.jobs[key] = job.Clone()
	return job.Clone(), true, nil
}

func (r *MemoryJobRepository) Update(_ context.Context, job *domain.Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := jobKey(job.Reference)
	if _, ok := r.jobs[key]; !ok {
		return fmt.Errorf("%w: load job %s", domain.ErrNotFound, job.Reference.JobID)
	}
	r.jobs[key] = job.Clone()
	return nil
}

func (r *MemoryJobRepository) Get(_ context.Context, reference domain.JobReference) (*domain.Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	job, ok := r.jobs[jobKey(reference)]
	if !ok {
		return nil, fmt.Errorf("%w: load job %s", domain.ErrNotFound, reference.JobID)
	}
	return job.Clone(), nil
}

func (r *MemoryJobRepository) List(_ context.Context, projectID, location string) ([]*domain.Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]*domain.Job, 0)
	for _, job := range r.jobs {
		if job.Reference.ProjectID == projectID && (location == "" || job.Reference.Location == location) {
			result = append(result, job.Clone())
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].Reference.JobID < result[j].Reference.JobID
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result, nil
}
