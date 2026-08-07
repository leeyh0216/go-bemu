package ports

import (
	"context"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

type CatalogRepository interface {
	CreateProject(context.Context, domain.Project) error
	GetProject(context.Context, string) (domain.Project, error)
	ListProjects(context.Context) ([]domain.Project, error)
	DeleteProject(context.Context, string) error

	CreateDataset(context.Context, domain.Dataset) error
	UpdateDataset(context.Context, domain.Dataset) error
	GetDataset(context.Context, string, string) (domain.Dataset, error)
	ListDatasets(context.Context, string) ([]domain.Dataset, error)
	DeleteDataset(context.Context, string, string) error

	CreateTable(context.Context, domain.Table) error
	UpdateTable(context.Context, domain.Table) error
	GetTable(context.Context, string, string, string) (domain.Table, error)
	ListTables(context.Context, string, string) ([]domain.Table, error)
	DeleteTable(context.Context, string, string, string) error
}

// HealthChecker is the minimal readiness dependency. Transports must not gain
// access to DDL or query operations merely to report backend availability.
type HealthChecker interface {
	Ping(context.Context) error
}

// WarehouseAdmin owns physical dataset/table lifecycle behind the application
// boundary. Domain and application packages never import DuckDB concepts.
type WarehouseAdmin interface {
	CreateDataset(context.Context, string, string) error
	DropDataset(context.Context, string, string) error
	CreateTable(context.Context, domain.Table) error
	ApplySchemaAdditions(context.Context, domain.Table, []domain.SchemaAddition) error
	DropTable(context.Context, string, string, string) error
}

// QueryEngine executes GoogleSQL-shaped requests against a replaceable backend.
type QueryEngine interface {
	Query(context.Context, QueryRequest) (domain.QueryResult, error)
}

// Warehouse remains as a composition boundary for wiring code that owns the
// complete backend lifecycle. Use cases depend on the narrower ports above.
type Warehouse interface {
	HealthChecker
	WarehouseAdmin
	QueryEngine
}

type QueryRequest struct {
	ProjectID      string
	DefaultDataset string
	SQL            string
}
