package duckdb

import (
	"context"
	"fmt"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

var _ ports.LogicalViewStorage = (*Warehouse)(nil)

// CreateLogicalView renders only the typed, analyzed query subtree. It never
// parses persisted or client GoogleSQL text inside the DuckDB adapter.
func (w *Warehouse) CreateLogicalView(
	ctx context.Context,
	statement semantic.Statement,
	reference domain.TableReference,
	replace bool,
) error {
	create, ok := statement.Syntax().(*queryast.CreateViewStatement)
	if !ok || statement.Kind() != queryast.StatementCreateView || !statement.RelationsComplete() {
		return fmt.Errorf("%w: logical view statement is incomplete", domain.ErrPrecondition)
	}
	if create.OrReplace() != replace {
		return fmt.Errorf("%w: logical view replacement intent is inconsistent", domain.ErrPrecondition)
	}
	name, err := renderPhysicalTable(reference)
	if err != nil {
		return fmt.Errorf("%w: logical view reference is invalid", domain.ErrInvalid)
	}
	renderer, err := newDuckDBStatementRenderer(statement, create, nil)
	if err != nil {
		return err
	}
	query, err := renderer.renderQuery(create.Query())
	if err != nil {
		return err
	}
	prefix := "CREATE VIEW "
	if replace {
		prefix = "CREATE OR REPLACE VIEW "
	}
	if _, err := w.db.ExecContext(ctx, prefix+name+" AS "+query, renderer.arguments...); err != nil {
		return err
	}
	return nil
}

func (w *Warehouse) DropLogicalView(ctx context.Context, reference domain.TableReference) error {
	name, err := renderPhysicalTable(reference)
	if err != nil {
		return fmt.Errorf("%w: logical view reference is invalid", domain.ErrInvalid)
	}
	_, err = w.db.ExecContext(ctx, "DROP VIEW "+name)
	return err
}
