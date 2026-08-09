package ports

import (
	"context"
	"io"
	"time"

	"github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	catalogports "github.com/leeyh0216/go-bemu/internal/ports"
)

type ObjectInfo struct {
	URI        string
	Size       int64
	Generation string
	ETag       string
}

// ObjectStore deliberately separates metadata discovery from streaming data.
// Implementations must not buffer an object payload in memory.
type ObjectStore interface {
	List(context.Context, string) ([]ObjectInfo, error)
	Get(context.Context, string) (ObjectInfo, error)
	Open(context.Context, ObjectInfo) (io.ReadCloser, error)
}

type TableCatalog interface {
	GetTable(context.Context, domain.TableReference) (domain.Table, error)
	GetDataset(context.Context, string, string) (domain.Dataset, error)
	PublishTable(context.Context, domain.Table) error
	PublishSchemaUpdate(context.Context, domain.TableReference, []domain.Field, []domain.Field) error
}

type LocalObject struct {
	Path        string
	Fingerprint string
	Size        int64
}

type LoadResult struct {
	OutputRows         int64
	CreatedDestination bool
	UpdatedDestination bool
}

type ParquetSchemaOptions struct {
	EnableListInference bool
}

type Loader interface {
	catalogports.SchemaPlanner
	PlanLoad(context.Context, LoadPlanRequest) (LoadPlan, error)
	InferParquetSchema(context.Context, []LocalObject, ParquetSchemaOptions) ([]domain.Field, error)
	ExecuteLoad(context.Context, LoadPlan, []LocalObject) (LoadResult, error)
	DiscardLoadedTable(context.Context, domain.TableReference) error
}

type JobRepository interface {
	CreateOrGet(context.Context, *domain.Job) (job *domain.Job, created bool, err error)
	Update(context.Context, *domain.Job) error
	Get(context.Context, domain.JobReference) (*domain.Job, error)
	List(context.Context, string, string) ([]*domain.Job, error)
}

type Clock interface {
	Now() time.Time
}

type IDGenerator interface {
	NewID() string
}
