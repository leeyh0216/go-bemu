package duckdb

import (
	"testing"

	"github.com/leeyh0216/go-bemu/internal/enginetest"
)

func TestDuckDBPlanningConformance(t *testing.T) {
	enginetest.RunPlanningConformance(t, func(tb testing.TB) enginetest.PlanningAdapter {
		warehouse, err := New("")
		if err != nil {
			tb.Fatal(err)
		}
		tb.Cleanup(func() { _ = warehouse.Close() })
		return warehouse
	})
}
