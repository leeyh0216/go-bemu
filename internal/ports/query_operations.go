package ports

// Connector-owned query operations stay separate from the generic QueryEngine
// contract. This lets the application bind canonical catalog metadata before a
// backend performs an operation whose correctness depends on that metadata.
//
// Spark 0.44.2 dynamic partition overwrite producer:
// https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryUtil.java#L796-L870

import (
	"context"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

const QueryOperationSparkDynamicTimePartitionOverwrite = "spark-dynamic-time-partition-overwrite"

// QueryOperation is a parsed semantic contract, never translated SQL. Fields
// are populated only after a source-pinned structural parser accepts the whole
// connector template.
type QueryOperation struct {
	Kind              string
	ModelVersion      string
	Destination       domain.TableReference
	Source            domain.TableReference
	PartitionFunction string
	PartitionField    string
	Granularity       string
	InsertFields      []string
}

// QueryOperationAnalyzer recognizes optional connector-specific semantic
// operations. matched=false means the generic query path remains authoritative.
type QueryOperationAnalyzer interface {
	AnalyzeQueryOperation(context.Context, QueryRequest) (operation QueryOperation, matched bool, err error)
}

// QueryOperationEngine receives canonical destination metadata from the
// application. Implementations must revalidate the operation against the raw
// request before opening a transaction, so forged or stale analyses cannot
// bypass the pinned profile.
type QueryOperationEngine interface {
	ExecuteQueryOperation(
		context.Context,
		QueryRequest,
		QueryOperation,
		domain.Table,
		domain.Table,
	) (domain.QueryResult, error)
}

// QueryOperationCatalog keeps canonical metadata stable across the backend
// transaction. The callback executes while the catalog's resource-mutation gate
// is held, preventing a delete/recreate race from substituting new partition
// metadata after validation.
type QueryOperationCatalog interface {
	WithCanonicalTables(
		context.Context,
		domain.TableReference,
		domain.TableReference,
		func(destination domain.Table, source domain.Table) (domain.QueryResult, error),
	) (domain.QueryResult, error)
}
