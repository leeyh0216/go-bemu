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

// MediaUploadStore owns bytes received through the BigQuery media-upload
// endpoint.  Its identifiers and committed URIs are opaque: callers must
// never turn a client upload into a host file:// source.
type MediaUploadStore interface {
	Create(context.Context) (string, error)
	Append(context.Context, string, int64, io.Reader) (int64, error)
	Size(context.Context, string) (int64, error)
	Commit(context.Context, string) (ObjectInfo, error)
	Discard(context.Context, string) error
}

type TableCatalog interface {
	GetTable(context.Context, domain.TableReference) (domain.Table, error)
}

// SchemaEvolutionCatalog is the optional catalog capability needed when a
// load job explicitly requests an additive destination schema update. The
// adapter owns coordinating the canonical catalog and its physical warehouse
// so a successful load never observes divergent schemas.
type SchemaEvolutionCatalog interface {
	TableCatalog
	UpdateSchema(context.Context, domain.TableReference, []domain.Field) (domain.Table, error)
}

// DestinationTableCatalog is the optional lifecycle extension needed by a
// CREATE_IF_NEEDED load. Keeping it separate preserves source-only embedded
// adapters while allowing the production catalog to create and compensate a
// destination table around an asynchronous load.
type DestinationTableCatalog interface {
	TableCatalog
	CreateTable(context.Context, domain.Table) (domain.Table, error)
	DeleteTable(context.Context, domain.TableReference) error
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
