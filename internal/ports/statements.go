package ports

import (
	"context"

	"github.com/leeyh0216/go-bemu/internal/domain"
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
}

// GoogleSQLGateway is the application's single syntax and semantic analysis
// boundary. Implementations parse once, analyze that parser AST, and return an
// owned engine-neutral statement without retaining SQL or foreign handles.
type GoogleSQLGateway interface {
	Analyze(context.Context, QueryRequest) (semantic.Statement, error)
}
