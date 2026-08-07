package ports

import (
	"context"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

type JobRepository interface {
	CreateOrGet(context.Context, *domain.Job) (*domain.Job, bool, error)
	Update(context.Context, *domain.Job) error
	Get(context.Context, domain.JobReference) (*domain.Job, error)
	List(context.Context, string, string) ([]*domain.Job, error)
}
