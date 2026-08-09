package ports

import (
	"context"

	"github.com/leeyh0216/go-bemu/internal/domain"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

// GoogleSQLCatalogReader is the analyzer-owned, read-only view of canonical
// metadata. One call returns one point-in-time revision whose slices, maps,
// pointers, and nested fields are not concurrently mutated by the provider;
// the gateway immediately makes its own recursive copy.
type GoogleSQLCatalogReader interface {
	GoogleSQLCatalogSnapshot(context.Context) (GoogleSQLCatalogSnapshot, error)
}

type GoogleSQLCatalogSnapshot struct {
	Projects []GoogleSQLProjectSnapshot
}

type GoogleSQLProjectSnapshot struct {
	Project  domain.Project
	Datasets []GoogleSQLDatasetSnapshot
}

type GoogleSQLDatasetSnapshot struct {
	Dataset domain.Dataset
	Tables  []domain.Table
	Views   []domain.View
}

// GoogleSQLGateway is the application's single syntax and semantic analysis
// boundary. Implementations parse once, analyze that parser AST, and return an
// owned engine-neutral statement without retaining SQL or foreign handles.
type GoogleSQLGateway interface {
	Analyze(context.Context, QueryRequest) (semantic.Statement, error)
}

// GoogleSQLAnalysisError identifies a user statement whose official syntax
// tree was classified but whose semantic analysis failed. Query jobs persist
// these failures as terminal job status instead of rejecting jobs.insert.
type GoogleSQLAnalysisError interface {
	error
	StatementKind() queryast.StatementKind
}

// StatementExecutor is the engine-neutral execution boundary. Implementations
// receive only an analyzed statement; client SQL and foreign parser handles
// never cross this port.
type StatementExecutor interface {
	ExecuteStatement(context.Context, semantic.Statement) (domain.QueryResult, error)
}

// StatementMaterializationRequest carries canonical destination policy beside
// the analyzed statement. Destination schema is owned recursively by the
// caller and must be copied by adapters that retain it.
type StatementMaterializationRequest struct {
	Destination       domain.TableReference
	DestinationExists bool
	DestinationSchema []domain.Field
	WriteDisposition  domain.WriteDisposition
	CreateDisposition domain.CreateDisposition
}

type StatementMaterializationResult struct {
	QueryResult        domain.QueryResult
	DestinationCreated bool
}

// StatementMaterializer owns the atomic physical destination transaction for
// an analyzed row-producing statement.
type StatementMaterializer interface {
	MaterializeAnalyzedStatement(
		context.Context,
		semantic.Statement,
		StatementMaterializationRequest,
	) (StatementMaterializationResult, error)
	DropMaterializedDestination(context.Context, domain.TableReference) error
}
