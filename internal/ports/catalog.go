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

// EngineCapabilities describes portable logical-schema bounds. Engine SQL and
// physical type names must not cross this boundary.
type EngineCapabilities struct {
	MaxDecimalPrecision int64
	MaxDecimalScale     int64
	SupportsStruct      bool
	SupportsRepeated    bool
}

type EngineCapabilityProvider interface {
	EngineCapabilities() EngineCapabilities
}

// SchemaPlanner verifies representability without performing a physical side
// effect. Detailed DDL planning remains a separate engine-SPI concern.
type SchemaPlanner interface {
	ValidateSchema([]domain.Field) error
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

// TableDataReader pages physical table rows without exposing backend query
// concepts to the application layer. Offset is a zero-based row ordinal and
// TotalRows describes the complete table, not only the returned page.
// https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
type TableDataReader interface {
	ListTableData(context.Context, TableDataReadRequest) (TableDataPage, error)
}

// TableDataMaxResults preserves the optional REST field's presence. The
// protocol defines maxResults as an optional uint32, so an explicit zero is a
// valid zero-row request and must not be collapsed into the omitted default.
// https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
type TableDataMaxResults struct {
	Value   int
	Present bool
}

// TableDataReadRequest is deliberately extensible: selected-field projection
// and snapshot/version preconditions can be added without changing the port's
// method signature when those REST capabilities are implemented.
type TableDataReadRequest struct {
	Reference domain.TableReference
	Schema    []domain.Field
	Offset    int64
	Limit     int
	// These local budgets are measured over the canonical adapter values. The
	// response budget trims a normal page, while the row budget is the hard
	// single-row exception ceiling. REST also checks exact f/v JSON bytes.
	MaxResponseBytes int64
	MaxRowBytes      int64
}

type TableDataPage struct {
	Rows      [][]any
	TotalRows int64
	// Schema is populated by the application from canonical catalog metadata,
	// not inferred by the physical row adapter.
	Schema []domain.Field
	// The application publishes its effective policy with the page so optional
	// transports cannot accidentally serialize it without the same hard bounds.
	MaxResponseBytes int64
	MaxRowBytes      int64
}

// QueryEngine executes GoogleSQL-shaped requests against a replaceable backend.
type QueryEngine interface {
	Query(context.Context, QueryRequest) (domain.QueryResult, error)
}

// QueryAnalyzer exposes backend-specific structural query analysis without
// leaking DuckDB parsing into the application layer. Location routing uses the
// referenced datasets and anonymous destinations are created only for
// row-producing statements.
// https://cloud.google.com/bigquery/docs/locations#specify_locations
type QueryAnalyzer interface {
	AnalyzeQuery(context.Context, QueryRequest) (QueryAnalysis, error)
}

// QueryMaterializer owns the atomic physical side of a query destination. The
// application publishes canonical table metadata only after this transaction
// succeeds. A compensating drop is available when metadata publication fails.
// https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery
type QueryMaterializer interface {
	MaterializeQuery(context.Context, QueryMaterializationRequest) (QueryMaterializationResult, error)
	DropMaterializedDestination(context.Context, domain.TableReference) error
}

// QueryDestinationCatalog is deliberately narrower than CatalogService. It
// lets query jobs validate location/existence and publish metadata for a table
// whose physical storage was already created by QueryMaterializer.
type QueryDestinationCatalog interface {
	GetDataset(context.Context, string, string) (domain.Dataset, error)
	GetTable(context.Context, string, string, string) (domain.Table, error)
	EnsureAnonymousDataset(context.Context, string, string, string) (domain.Dataset, error)
	PublishMaterializedTable(context.Context, domain.Table) error
}

// Warehouse remains as a composition boundary for wiring code that owns the
// complete backend lifecycle. Use cases depend on the narrower ports above.
type Warehouse interface {
	HealthChecker
	WarehouseAdmin
	QueryEngine
}

type QueryRequest struct {
	ProjectID        string
	DefaultProjectID string
	DefaultDataset   string
	SQL              string
}

type QueryAnalysis struct {
	ReferencedTables        []domain.TableReference
	MutationTargets         []domain.TableReference
	ProducesRows            bool
	RequiresCatalogMutation bool
}

type QueryMaterializationRequest struct {
	Query             QueryRequest
	Destination       domain.TableReference
	DestinationExists bool
	DestinationSchema []domain.Field
	WriteDisposition  domain.WriteDisposition
	CreateDisposition domain.CreateDisposition
}

type QueryMaterializationResult struct {
	QueryResult        domain.QueryResult
	DestinationCreated bool
}
