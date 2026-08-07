package ports

import (
	"context"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

type JobRepository interface {
	Create(context.Context, *domain.Job) error
	Update(context.Context, *domain.Job) error
	Get(context.Context, string, string) (*domain.Job, error)
	List(context.Context, string) ([]*domain.Job, error)
}
