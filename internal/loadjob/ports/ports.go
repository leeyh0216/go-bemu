package ports

import (
	"context"
	"io"
	"time"

	"github.com/leeyh0216/go-bemu/internal/loadjob/domain"
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
}

type LocalObject struct {
	Path string
	Size int64
}

type LoadRequest struct {
	Destination      domain.Table
	Schema           []domain.Field
	Objects          []LocalObject
	SourceFormat     domain.SourceFormat
	WriteDisposition domain.WriteDisposition
}

type LoadResult struct {
	OutputRows int64
}

type Loader interface {
	Load(context.Context, LoadRequest) (LoadResult, error)
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
