package duckdb

import (
	"context"
	"fmt"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/engine"
)

type duckDBSchemaAdapterPlanner struct{}

func (duckDBSchemaAdapterPlanner) ValidateSchemaIntent(ctx context.Context, intent engine.SchemaIntent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, field := range intent.AfterSchema() {
		if _, err := duckDBType(field); err != nil {
			return err
		}
	}
	return nil
}

func (w *Warehouse) PlanSchema(ctx context.Context, intent engine.SchemaIntent) (engine.SchemaPlan, error) {
	if w == nil || w.schemaPlanner == nil {
		return engine.SchemaPlan{}, fmt.Errorf("%w: DuckDB schema planner is not configured", domain.ErrPrecondition)
	}
	return w.schemaPlanner.Plan(ctx, intent)
}
