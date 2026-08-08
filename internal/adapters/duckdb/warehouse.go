package duckdb

// DuckDB storage semantics and transactional ALTER provenance:
//   - https://duckdb.org/docs/stable/sql/statements/alter_table
//   - https://duckdb.org/docs/stable/sql/data_types/struct#updating-the-schema
//
// BigQuery resource semantics never leak through this adapter as DuckDB types;
// translation happens at the outbound port boundary.

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/engine"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type Warehouse struct {
	db            *sql.DB
	capabilities  engine.Capabilities
	schemaPlanner *engine.SchemaPlanner
	loadPlanner   *loadports.Planner
}

var (
	_ ports.HealthChecker  = (*Warehouse)(nil)
	_ ports.CatalogStorage = (*Warehouse)(nil)
)

func New(dsn string) (*Warehouse, error) {
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("open DuckDB: %w", err)
	}
	// A single connection keeps an in-memory DuckDB catalog coherent and avoids
	// concurrent DDL races. Storage stream work will introduce a write scheduler.
	db.SetMaxOpenConns(1)
	capabilities, err := newDuckDBCapabilities(db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	schemaPlanner, err := engine.NewSchemaPlanner(capabilities, duckDBSchemaAdapterPlanner{})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure DuckDB schema planner: %w", err)
	}
	loadPlanner, err := loadports.NewPlanner(capabilities, duckDBLoadAdapterPlanner{schemaPlanner: schemaPlanner})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure DuckDB load planner: %w", err)
	}
	warehouse := &Warehouse{
		db: db, capabilities: capabilities, schemaPlanner: schemaPlanner, loadPlanner: loadPlanner,
	}
	if err := warehouse.Ping(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return warehouse, nil
}

func (w *Warehouse) Close() error { return w.db.Close() }

func (w *Warehouse) Ping(ctx context.Context) error {
	if err := w.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping DuckDB: %w", err)
	}
	return nil
}

func physicalSchema(projectID, datasetID string) string {
	return "bq_" + hex.EncodeToString([]byte(projectID)) + "_" + hex.EncodeToString([]byte(datasetID))
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func (w *Warehouse) CreateDataset(ctx context.Context, projectID, datasetID string) error {
	started := observability.LogSideEffectStart(ctx, "duckdb", "create_dataset",
		"project_id", projectID, "dataset_id", datasetID, "transaction_mode", "autocommit")
	_, err := w.db.ExecContext(ctx, "CREATE SCHEMA "+quoteIdentifier(physicalSchema(projectID, datasetID)))
	if err != nil {
		err = fmt.Errorf("create dataset storage: %w", err)
	}
	observability.LogSideEffectEnd(ctx, "duckdb", "create_dataset", started, err,
		"project_id", projectID, "dataset_id", datasetID, "transaction_mode", "autocommit")
	return err
}

func (w *Warehouse) DropDataset(ctx context.Context, projectID, datasetID string) error {
	started := observability.LogSideEffectStart(ctx, "duckdb", "drop_dataset",
		"project_id", projectID, "dataset_id", datasetID, "transaction_mode", "autocommit")
	_, err := w.db.ExecContext(ctx, "DROP SCHEMA "+quoteIdentifier(physicalSchema(projectID, datasetID))+" CASCADE")
	if err != nil {
		err = fmt.Errorf("drop dataset storage: %w", err)
	}
	observability.LogSideEffectEnd(ctx, "duckdb", "drop_dataset", started, err,
		"project_id", projectID, "dataset_id", datasetID, "transaction_mode", "autocommit")
	return err
}

func (w *Warehouse) CreateTable(ctx context.Context, table domain.Table) error {
	intent, err := engine.NewSchemaIntent(engine.SchemaIntentDescriptor{
		Operation:   engine.SchemaOperationCreate,
		Target:      domain.TableReference{ProjectID: table.ProjectID, DatasetID: table.DatasetID, TableID: table.ID},
		AfterSchema: table.Schema,
	})
	if err != nil {
		return err
	}
	plan, err := w.PlanSchema(ctx, intent)
	if err != nil {
		return err
	}
	return w.CreatePlannedTable(ctx, plan, table)
}

func (w *Warehouse) CreatePlannedTable(ctx context.Context, plan engine.SchemaPlan, table domain.Table) error {
	intent, err := engine.NewSchemaIntent(engine.SchemaIntentDescriptor{
		Operation:   engine.SchemaOperationCreate,
		Target:      domain.TableReference{ProjectID: table.ProjectID, DatasetID: table.DatasetID, TableID: table.ID},
		AfterSchema: table.Schema,
	})
	if err != nil {
		return err
	}
	if err := w.schemaPlanner.ValidateBinding(plan, intent); err != nil {
		return err
	}
	return w.createTable(ctx, table)
}

func (w *Warehouse) createTable(ctx context.Context, table domain.Table) error {
	schemaSummary := fmt.Sprintf("%v", table.Schema)
	started := observability.LogSideEffectStart(ctx, "duckdb", "create_table",
		"project_id", table.ProjectID, "dataset_id", table.DatasetID, "table_id", table.ID,
		"schema_fingerprint", observability.Digest([]byte(schemaSummary)), "field_count", len(table.Schema),
		"transaction_mode", "autocommit")
	columns := make([]string, 0, len(table.Schema))
	for _, field := range table.Schema {
		fieldType, err := duckDBType(field)
		if err != nil {
			observability.LogSideEffectEnd(ctx, "duckdb", "create_table", started, err,
				"project_id", table.ProjectID, "dataset_id", table.DatasetID, "table_id", table.ID)
			return err
		}
		column := quoteIdentifier(field.Name) + " " + fieldType
		if strings.EqualFold(field.Mode, "REQUIRED") {
			column += " NOT NULL"
		}
		columns = append(columns, column)
	}
	statement := fmt.Sprintf("CREATE TABLE %s.%s (%s)",
		quoteIdentifier(physicalSchema(table.ProjectID, table.DatasetID)),
		quoteIdentifier(table.ID),
		strings.Join(columns, ", "),
	)
	_, err := w.db.ExecContext(ctx, statement)
	if err != nil {
		err = fmt.Errorf("create table storage: %w", err)
	}
	observability.LogSideEffectEnd(ctx, "duckdb", "create_table", started, err,
		"project_id", table.ProjectID, "dataset_id", table.DatasetID, "table_id", table.ID,
		"schema_fingerprint", observability.Digest([]byte(schemaSummary)), "field_count", len(table.Schema),
		"transaction_mode", "autocommit")
	return err
}

func (w *Warehouse) DropTable(ctx context.Context, projectID, datasetID, tableID string) error {
	started := observability.LogSideEffectStart(ctx, "duckdb", "drop_table",
		"project_id", projectID, "dataset_id", datasetID, "table_id", tableID, "transaction_mode", "autocommit")
	// IF EXISTS makes physical-first catalog compensation retryable after a
	// metadata deletion failure. The application still verifies the canonical
	// resource before user-initiated deletes.
	// https://duckdb.org/docs/stable/sql/statements/drop#examples
	statement := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s",
		quoteIdentifier(physicalSchema(projectID, datasetID)), quoteIdentifier(tableID))
	_, err := w.db.ExecContext(ctx, statement)
	if err != nil {
		err = fmt.Errorf("drop table storage: %w", err)
	}
	observability.LogSideEffectEnd(ctx, "duckdb", "drop_table", started, err,
		"project_id", projectID, "dataset_id", datasetID, "table_id", tableID, "transaction_mode", "autocommit")
	return err
}

// ApplySchemaAdditions executes every legal addition in one DuckDB transaction.
// DuckDB fills both top-level and nested STRUCT fields with NULL for existing
// rows, matching the observable result of an additive BigQuery schema update.
func (w *Warehouse) ApplyPlannedSchemaAdditions(
	ctx context.Context,
	plan engine.SchemaPlan,
	table domain.Table,
	additions []domain.SchemaAddition,
) error {
	if w == nil || w.schemaPlanner == nil {
		return fmt.Errorf("%w: DuckDB schema planner is not configured", domain.ErrPrecondition)
	}
	plannedIntent := plan.Intent()
	intent, err := engine.NewSchemaIntent(engine.SchemaIntentDescriptor{
		Operation:    engine.SchemaOperationAddColumns,
		Target:       domain.TableReference{ProjectID: table.ProjectID, DatasetID: table.DatasetID, TableID: table.ID},
		BeforeSchema: plannedIntent.BeforeSchema(), AfterSchema: table.Schema, Additions: additions,
	})
	if err != nil {
		return err
	}
	if err := w.schemaPlanner.ValidateBinding(plan, intent); err != nil {
		return err
	}
	return w.ApplySchemaAdditions(ctx, table, additions)
}

func (w *Warehouse) ApplySchemaAdditions(ctx context.Context, table domain.Table, additions []domain.SchemaAddition) (err error) {
	paths := make([]string, len(additions))
	for index, addition := range additions {
		paths[index] = strings.Join(addition.Path, ".")
	}
	summary := strings.Join(paths, ",")
	started := observability.LogSideEffectStart(ctx, "duckdb", "apply_schema_additions",
		"project_id", table.ProjectID, "dataset_id", table.DatasetID, "table_id", table.ID,
		"addition_count", len(additions), "shape_fingerprint", observability.Digest([]byte(summary)),
		"transaction_mode", "explicit")
	defer func() {
		observability.LogSideEffectEnd(ctx, "duckdb", "apply_schema_additions", started, err,
			"project_id", table.ProjectID, "dataset_id", table.DatasetID, "table_id", table.ID,
			"addition_count", len(additions), "shape_fingerprint", observability.Digest([]byte(summary)),
			"transaction_mode", "explicit")
	}()
	if len(additions) == 0 {
		return nil
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin schema update transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	tableName := quoteIdentifier(physicalSchema(table.ProjectID, table.DatasetID)) + "." + quoteIdentifier(table.ID)
	retypedRoots := make(map[string]struct{})
	for _, addition := range additions {
		if len(addition.Path) == 0 {
			return fmt.Errorf("%w: schema addition path is empty", domain.ErrInvalid)
		}
		// DuckDB's nested ADD syntax only traverses STRUCT, not STRUCT[]. When
		// any ancestor is REPEATED, cast the proposed root field once; DuckDB's
		// name-based STRUCT cast preserves existing members and fills additions
		// with NULL for each existing array element.
		if root, ok := repeatedAncestorRoot(table.Schema, addition.Path); ok {
			if _, alreadyRetyped := retypedRoots[root.Name]; alreadyRetyped {
				continue
			}
			fieldType, typeErr := duckDBType(root)
			if typeErr != nil {
				return typeErr
			}
			statement := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s", tableName, quoteIdentifier(root.Name), fieldType)
			if _, execErr := tx.ExecContext(ctx, statement); execErr != nil {
				return fmt.Errorf("apply repeated schema addition %s: %w", strings.Join(addition.Path, "."), execErr)
			}
			retypedRoots[root.Name] = struct{}{}
			continue
		}
		fieldType, typeErr := duckDBType(addition.Field)
		if typeErr != nil {
			return typeErr
		}
		path := make([]string, len(addition.Path))
		for index, component := range addition.Path {
			path[index] = quoteIdentifier(component)
		}
		statement := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", tableName, strings.Join(path, "."), fieldType)
		if _, execErr := tx.ExecContext(ctx, statement); execErr != nil {
			return fmt.Errorf("apply schema addition %s: %w", strings.Join(addition.Path, "."), execErr)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit schema update transaction: %w", err)
	}
	return nil
}

func repeatedAncestorRoot(schema []domain.Field, path []string) (domain.Field, bool) {
	if len(path) < 2 {
		return domain.Field{}, false
	}
	fields := schema
	var root domain.Field
	for index, component := range path[:len(path)-1] {
		var current *domain.Field
		for fieldIndex := range fields {
			if fields[fieldIndex].Name == component {
				current = &fields[fieldIndex]
				break
			}
		}
		if current == nil {
			return domain.Field{}, false
		}
		if index == 0 {
			root = *current
		}
		if strings.EqualFold(current.Mode, "REPEATED") {
			return root, true
		}
		fields = current.Fields
	}
	return domain.Field{}, false
}

func duckDBType(field domain.Field) (string, error) {
	if err := field.Validate(); err != nil {
		return "", err
	}
	var result string
	switch strings.ToUpper(field.Type) {
	case "BOOL", "BOOLEAN":
		result = "BOOLEAN"
	case "INT64", "INTEGER":
		result = "BIGINT"
	case "FLOAT64", "FLOAT":
		result = "DOUBLE"
	case "NUMERIC", "BIGNUMERIC":
		parameters, err := field.EffectiveDecimalParameters()
		if err != nil {
			return "", err
		}
		result = fmt.Sprintf("DECIMAL(%d,%d)", parameters.Precision, parameters.Scale)
	case "STRING":
		result = "VARCHAR"
	case "BYTES":
		result = "BLOB"
	case "DATE":
		result = "DATE"
	case "DATETIME":
		result = "TIMESTAMP"
	case "TIME":
		result = "TIME"
	case "TIMESTAMP":
		result = "TIMESTAMPTZ"
	case "JSON":
		result = "JSON"
	case "RECORD", "STRUCT":
		fields := make([]string, 0, len(field.Fields))
		for _, nested := range field.Fields {
			nestedType, err := duckDBType(nested)
			if err != nil {
				return "", err
			}
			fields = append(fields, quoteIdentifier(nested.Name)+" "+nestedType)
		}
		result = "STRUCT(" + strings.Join(fields, ", ") + ")"
	default:
		return "", fmt.Errorf("%w: cannot map BigQuery type %q", domain.ErrInvalid, field.Type)
	}
	if strings.EqualFold(field.Mode, "REPEATED") {
		result += "[]"
	}
	return result, nil
}
