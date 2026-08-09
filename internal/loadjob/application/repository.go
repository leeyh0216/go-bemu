package application

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	"github.com/leeyh0216/go-bemu/internal/loadjob/ports"
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

func (r *MemoryJobRepository) ListInterrupted(_ context.Context) ([]*domain.Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]*domain.Job, 0)
	for _, job := range r.jobs {
		if job.State != domain.JobDone {
			result = append(result, job.Clone())
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return jobKey(result[i].Reference) < jobKey(result[j].Reference)
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result, nil
}

type MemoryMutationJournal struct {
	mu      sync.RWMutex
	records map[string]domain.MutationRecord
}

func NewMemoryMutationJournal() *MemoryMutationJournal {
	return &MemoryMutationJournal{records: make(map[string]domain.MutationRecord)}
}

func (journal *MemoryMutationJournal) Prepare(_ context.Context, record domain.MutationRecord) error {
	if err := record.Validate(); err != nil || record.Phase != domain.MutationPrepared {
		return fmt.Errorf("%w: prepared load mutation is invalid", domain.ErrInvalid)
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if existing, ok := journal.records[record.ID]; ok {
		if reflect.DeepEqual(existing, record) {
			return nil
		}
		return fmt.Errorf("%w: load mutation identity already exists", domain.ErrConflict)
	}
	journal.records[record.ID] = record.Clone()
	return nil
}

func (journal *MemoryMutationJournal) MarkPhysical(_ context.Context, id, planFingerprint string, result ports.LoadResult) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	record, ok := journal.records[id]
	if !ok {
		return fmt.Errorf("%w: load mutation", domain.ErrNotFound)
	}
	want := domain.MutationResult{
		OutputRows: result.OutputRows, CreatedDestination: result.CreatedDestination,
		UpdatedDestination: result.UpdatedDestination,
	}
	if record.Phase == domain.MutationPhysical || record.Phase == domain.MutationApplied {
		if record.PlanFingerprint == planFingerprint && record.Result != nil && *record.Result == want {
			return nil
		}
		return fmt.Errorf("%w: physical load mutation receipt differs", domain.ErrConflict)
	}
	if record.Phase != domain.MutationPrepared || record.PlanFingerprint != planFingerprint {
		return fmt.Errorf("%w: load mutation cannot become physical", domain.ErrConflict)
	}
	record.Phase = domain.MutationPhysical
	record.Result = &want
	if err := record.Validate(); err != nil {
		return err
	}
	journal.records[id] = record
	return nil
}

func (journal *MemoryMutationJournal) MarkApplied(_ context.Context, id string) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	record, ok := journal.records[id]
	if !ok {
		return fmt.Errorf("%w: load mutation", domain.ErrNotFound)
	}
	if record.Phase == domain.MutationApplied {
		return nil
	}
	if record.Phase != domain.MutationPhysical {
		return fmt.Errorf("%w: load mutation cannot become applied", domain.ErrConflict)
	}
	record.Phase = domain.MutationApplied
	journal.records[id] = record
	return nil
}

func (journal *MemoryMutationJournal) MarkAborted(_ context.Context, id string) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	record, ok := journal.records[id]
	if !ok {
		return fmt.Errorf("%w: load mutation", domain.ErrNotFound)
	}
	if record.Phase == domain.MutationAborted {
		return nil
	}
	if record.Phase != domain.MutationPrepared && record.Phase != domain.MutationPhysical {
		return fmt.Errorf("%w: physical load mutation cannot be aborted", domain.ErrConflict)
	}
	record.Phase = domain.MutationAborted
	record.Result = nil
	journal.records[id] = record
	return nil
}

func (journal *MemoryMutationJournal) ListRecoverable(_ context.Context) ([]domain.MutationRecord, error) {
	journal.mu.RLock()
	defer journal.mu.RUnlock()
	records := make([]domain.MutationRecord, 0, len(journal.records))
	for _, record := range journal.records {
		if record.Phase != domain.MutationAborted {
			records = append(records, record.Clone())
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
	return records, nil
}

var _ ports.MutationJournal = (*MemoryMutationJournal)(nil)
