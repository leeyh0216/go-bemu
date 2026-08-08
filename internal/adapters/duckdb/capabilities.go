package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/engine"
)

// Capabilities returns the immutable logical contract established when the
// physical engine was opened. It never includes DSNs, SQL, or physical types.
func (w *Warehouse) Capabilities() engine.Capabilities {
	if w == nil {
		return engine.Capabilities{}
	}
	return w.capabilities
}

func newDuckDBCapabilities(db *sql.DB) (engine.Capabilities, error) {
	var rawVersion string
	if err := db.QueryRowContext(context.Background(), "SELECT version()").Scan(&rawVersion); err != nil {
		return engine.Capabilities{}, fmt.Errorf("inspect DuckDB engine version: %w", err)
	}
	version := strings.TrimSpace(rawVersion)
	version = strings.TrimPrefix(strings.TrimPrefix(version, "v"), "V")
	identity, err := engine.NewIdentity("duckdb", version)
	if err != nil {
		return engine.Capabilities{}, fmt.Errorf("validate DuckDB engine identity: %w", err)
	}
	capabilities, err := engine.NewCapabilities(engine.CapabilitiesDescriptor{
		Identity: identity,
		Decimal: engine.DecimalCapabilities{
			Supported: true, MaxPrecision: domain.SparkDecimalMaxPrecision, MaxScale: domain.SparkDecimalMaxScale,
		},
		Composite: engine.CompositeCapabilities{MaxStructDepth: 15, MaxListDepth: 15},
		Transactions: map[engine.TransactionScope]bool{
			engine.TransactionScopeSingleTable: true,
			engine.TransactionScopeMultiTable:  true,
		},
		AtomicReplacements: map[engine.AtomicReplacementScope]bool{
			engine.AtomicReplacementTable:     true,
			engine.AtomicReplacementPartition: true,
		},
		Inspection: map[engine.InspectionScope]bool{
			engine.InspectionTableShape: true,
		},
		DDL: map[engine.DDLOperation]engine.DDLCapability{
			engine.DDLCreateTable: {
				Guarantee: engine.DDLGuaranteeAtomicPhysicalStatement,
			},
			engine.DDLDropTable: {
				Guarantee: engine.DDLGuaranteeAtomicPhysicalStatement,
			},
			engine.DDLAddColumn: {
				Guarantee: engine.DDLGuaranteeAtomicPhysicalTable, MaxFieldPathDepth: 15,
			},
			engine.DDLDropColumn: {
				Guarantee: engine.DDLGuaranteeAtomicPhysicalStatement, MaxFieldPathDepth: 1,
			},
			engine.DDLRenameColumn: {
				Guarantee: engine.DDLGuaranteeAtomicPhysicalStatement, MaxFieldPathDepth: 1,
			},
			engine.DDLChangeColumnType: {
				Guarantee: engine.DDLGuaranteeAtomicPhysicalStatement, MaxFieldPathDepth: 1,
			},
		},
	})
	if err != nil {
		return engine.Capabilities{}, fmt.Errorf("build DuckDB engine capabilities: %w", err)
	}
	return capabilities, nil
}
