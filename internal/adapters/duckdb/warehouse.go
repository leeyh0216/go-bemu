package duckdb

// DuckDB storage semantics and transactional ALTER provenance:
//   - https://duckdb.org/docs/stable/sql/statements/alter_table
//   - https://duckdb.org/docs/stable/sql/data_types/struct#updating-the-schema
//
// BigQuery resource semantics never leak through this adapter as DuckDB types;
// translation happens at the outbound port boundary.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type Warehouse struct {
	db *sql.DB
}

var (
	_ ports.HealthChecker            = (*Warehouse)(nil)
	_ ports.WarehouseAdmin           = (*Warehouse)(nil)
	_ ports.EngineCapabilityProvider = (*Warehouse)(nil)
	_ ports.SchemaPlanner            = (*Warehouse)(nil)
	_ ports.TableSchemaPlanner       = (*Warehouse)(nil)
	_ ports.TableSchemaMutator       = (*Warehouse)(nil)
	_ ports.CatalogStorageInspector  = (*Warehouse)(nil)
)

func (*Warehouse) EngineCapabilities() ports.EngineCapabilities {
	return ports.EngineCapabilities{
		MaxDecimalPrecision: domain.SparkDecimalMaxPrecision,
		MaxDecimalScale:     domain.SparkDecimalMaxScale,
		SupportsStruct:      true,
		SupportsRepeated:    true,
		TableSchemaChanges: ports.TableSchemaChangeCapabilities{
			AddColumn: true, DropColumn: true, RenameColumn: true, AlterColumnType: true,
			Transactional: true, InspectBeforeAfter: true,
		},
	}
}

func (*Warehouse) ValidateSchema(schema []domain.Field) error {
	for _, field := range schema {
		if _, err := duckDBType(field); err != nil {
			return err
		}
	}
	return nil
}

func New(dsn string) (*Warehouse, error) {
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("open DuckDB: %w", err)
	}
	// A single connection keeps an in-memory DuckDB catalog coherent and avoids
	// concurrent DDL races. Storage stream work will introduce a write scheduler.
	db.SetMaxOpenConns(1)
	warehouse := &Warehouse{db: db}
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
	if err := w.ValidateSchema(table.Schema); err != nil {
		return err
	}
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

// PlanTableChange accepts one top-level scalar add, rename, drop, or type
// change and binds it to DuckDB's physical type mapping.
func (w *Warehouse) PlanTableChange(before, after domain.Table) (ports.TableSchemaChangePlan, error) {
	if err := w.ValidateSchema(before.Schema); err != nil {
		return ports.TableSchemaChangePlan{}, err
	}
	if err := w.ValidateSchema(after.Schema); err != nil {
		return ports.TableSchemaChangePlan{}, err
	}
	if _, err := tableSchemaChangeStatement(before, after); err != nil {
		return ports.TableSchemaChangePlan{}, err
	}
	beforeFingerprint, err := physicalTableFingerprint(before)
	if err != nil {
		return ports.TableSchemaChangePlan{}, err
	}
	afterFingerprint, err := physicalTableFingerprint(after)
	if err != nil {
		return ports.TableSchemaChangePlan{}, err
	}
	return ports.TableSchemaChangePlan{
		Before: before, After: after,
		BeforePhysicalFingerprint: beforeFingerprint,
		AfterPhysicalFingerprint:  afterFingerprint,
	}, nil
}

func (w *Warehouse) ApplyTableSchemaChange(ctx context.Context, plan ports.TableSchemaChangePlan) (err error) {
	verified, err := w.PlanTableChange(plan.Before, plan.After)
	if err != nil {
		return err
	}
	if verified.BeforePhysicalFingerprint != plan.BeforePhysicalFingerprint ||
		verified.AfterPhysicalFingerprint != plan.AfterPhysicalFingerprint {
		return fmt.Errorf("%w: table schema plan does not match the engine mapping", domain.ErrInvalid)
	}
	statement, err := tableSchemaChangeStatement(plan.Before, plan.After)
	if err != nil {
		return err
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin table schema change: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	tableName := quoteIdentifier(physicalSchema(plan.Before.ProjectID, plan.Before.DatasetID)) + "." + quoteIdentifier(plan.Before.ID)
	if _, err = tx.ExecContext(ctx, "ALTER TABLE "+tableName+" "+statement); err != nil {
		return fmt.Errorf("apply table schema change: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit table schema change: %w", err)
	}
	return nil
}

func (w *Warehouse) TableSchemaMatches(ctx context.Context, expected domain.Table) (bool, error) {
	if err := w.ValidateSchema(expected.Schema); err != nil {
		return false, err
	}
	type physicalColumn struct {
		name, dataType, nullable string
	}
	rows, err := w.db.QueryContext(ctx, `SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position`,
		physicalSchema(expected.ProjectID, expected.DatasetID), expected.ID)
	if err != nil {
		return false, fmt.Errorf("inspect table schema: %w", err)
	}
	columns := make([]physicalColumn, 0, len(expected.Schema))
	for rows.Next() {
		var column physicalColumn
		if err := rows.Scan(&column.name, &column.dataType, &column.nullable); err != nil {
			_ = rows.Close()
			return false, fmt.Errorf("scan table schema: %w", err)
		}
		columns = append(columns, column)
	}
	if err := rows.Close(); err != nil {
		return false, fmt.Errorf("close table schema inspection: %w", err)
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate table schema inspection: %w", err)
	}
	if len(columns) != len(expected.Schema) {
		return false, nil
	}
	for index, field := range expected.Schema {
		if columns[index].name != field.Name {
			return false, nil
		}
		fieldType, err := duckDBType(field)
		if err != nil {
			return false, err
		}
		var normalizedType string
		if err := w.db.QueryRowContext(ctx, "SELECT typeof(CAST(NULL AS "+fieldType+"))").Scan(&normalizedType); err != nil {
			return false, fmt.Errorf("normalize expected table type: %w", err)
		}
		if !strings.EqualFold(columns[index].dataType, normalizedType) {
			return false, nil
		}
		expectedNullable := !strings.EqualFold(field.Mode, "REQUIRED")
		if (columns[index].nullable == "YES") != expectedNullable {
			return false, nil
		}
	}
	return true, nil
}

func (w *Warehouse) ValidateCatalogStorage(ctx context.Context, snapshot ports.CatalogStorageSnapshot) error {
	expectedSchemas := make(map[string]domain.Dataset, len(snapshot.Datasets))
	for _, dataset := range snapshot.Datasets {
		expectedSchemas[physicalSchema(dataset.ProjectID, dataset.ID)] = dataset
	}
	actualSchemas := make(map[string]struct{})
	rows, err := w.db.QueryContext(ctx, `SELECT schema_name FROM information_schema.schemata`)
	if err != nil {
		return fmt.Errorf("inspect physical dataset catalog: %w", err)
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan physical dataset catalog: %w", err)
		}
		if strings.HasPrefix(name, "bq_") {
			actualSchemas[name] = struct{}{}
		}
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close physical dataset catalog: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate physical dataset catalog: %w", err)
	}
	for name, dataset := range expectedSchemas {
		if _, ok := actualSchemas[name]; !ok {
			return fmt.Errorf("physical catalog drift: missing dataset storage for %s/%s", dataset.ProjectID, dataset.ID)
		}
	}
	for name := range actualSchemas {
		if _, ok := expectedSchemas[name]; !ok {
			return fmt.Errorf("physical catalog drift: unexpected dataset storage %s", physicalSchemaDisplay(name))
		}
	}

	type tableKey struct{ schema, table string }
	expectedTables := make(map[tableKey]domain.Table, len(snapshot.Tables))
	for _, table := range snapshot.Tables {
		expectedTables[tableKey{physicalSchema(table.ProjectID, table.DatasetID), table.ID}] = table
	}
	actualTables := make(map[tableKey]struct{})
	rows, err = w.db.QueryContext(ctx, `SELECT table_schema, table_name FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'`)
	if err != nil {
		return fmt.Errorf("inspect physical table catalog: %w", err)
	}
	for rows.Next() {
		var key tableKey
		if err := rows.Scan(&key.schema, &key.table); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan physical table catalog: %w", err)
		}
		if strings.HasPrefix(key.schema, "bq_") {
			actualTables[key] = struct{}{}
		}
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close physical table catalog: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate physical table catalog: %w", err)
	}
	for key, table := range expectedTables {
		if _, ok := actualTables[key]; !ok {
			return fmt.Errorf("physical catalog drift: missing table storage for %s/%s/%s", table.ProjectID, table.DatasetID, table.ID)
		}
		matches, err := w.TableSchemaMatches(ctx, table)
		if err != nil {
			return err
		}
		if !matches {
			return fmt.Errorf("physical catalog drift: table schema does not match canonical metadata for %s/%s/%s", table.ProjectID, table.DatasetID, table.ID)
		}
	}
	for key := range actualTables {
		if _, ok := expectedTables[key]; !ok {
			return fmt.Errorf("physical catalog drift: unexpected table storage %s/%s", physicalSchemaDisplay(key.schema), key.table)
		}
	}
	return nil
}

func physicalSchemaDisplay(name string) string {
	encoded := strings.TrimPrefix(name, "bq_")
	projectHex, datasetHex, ok := strings.Cut(encoded, "_")
	if !ok {
		return name
	}
	project, projectErr := hex.DecodeString(projectHex)
	dataset, datasetErr := hex.DecodeString(datasetHex)
	if projectErr != nil || datasetErr != nil {
		return name
	}
	return string(project) + "/" + string(dataset)
}

func physicalTableFingerprint(table domain.Table) (string, error) {
	var descriptor strings.Builder
	for _, field := range table.Schema {
		fieldType, err := duckDBType(field)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&descriptor, "%d:%s\x00%d:%s\x00%t\n",
			len(field.Name), field.Name, len(fieldType), fieldType, strings.EqualFold(field.Mode, "REQUIRED"))
	}
	digest := sha256.Sum256([]byte(descriptor.String()))
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func tableSchemaChangeStatement(before, after domain.Table) (string, error) {
	if before.ProjectID != after.ProjectID || before.DatasetID != after.DatasetID || before.ID != after.ID {
		return "", fmt.Errorf("%w: table schema change cannot move a table", domain.ErrInvalid)
	}
	if len(after.Schema) == len(before.Schema)+1 {
		for i := range before.Schema {
			if !reflect.DeepEqual(before.Schema[i], after.Schema[i]) {
				return "", fmt.Errorf("%w: only one appended top-level column is supported", domain.ErrUnsupported)
			}
		}
		added := after.Schema[len(before.Schema)]
		if added.Mode == "REPEATED" || len(added.Fields) != 0 {
			return "", fmt.Errorf("%w: ALTER TABLE supports top-level scalar columns only", domain.ErrUnsupported)
		}
		fieldType, err := duckDBType(added)
		if err != nil {
			return "", err
		}
		return "ADD COLUMN " + quoteIdentifier(added.Name) + " " + fieldType, nil
	}
	if len(before.Schema) == len(after.Schema)+1 {
		removedIndex := -1
		for candidate := range before.Schema {
			remaining := append(append([]domain.Field(nil), before.Schema[:candidate]...), before.Schema[candidate+1:]...)
			if reflect.DeepEqual(remaining, after.Schema) {
				removedIndex = candidate
				break
			}
		}
		if removedIndex < 0 {
			return "", fmt.Errorf("%w: only one top-level column drop is supported", domain.ErrUnsupported)
		}
		removed := before.Schema[removedIndex]
		return "DROP COLUMN " + quoteIdentifier(removed.Name), nil
	}
	if len(before.Schema) != len(after.Schema) {
		return "", fmt.Errorf("%w: unsupported table schema shape", domain.ErrUnsupported)
	}
	changed := -1
	for i := range before.Schema {
		if !reflect.DeepEqual(before.Schema[i], after.Schema[i]) {
			if changed >= 0 {
				return "", fmt.Errorf("%w: one column per ALTER TABLE statement is required", domain.ErrUnsupported)
			}
			changed = i
		}
	}
	if changed < 0 {
		return "", fmt.Errorf("%w: ALTER TABLE does not change the schema", domain.ErrInvalid)
	}
	oldField, newField := before.Schema[changed], after.Schema[changed]
	if oldField.Mode == "REPEATED" || newField.Mode == "REPEATED" || len(oldField.Fields) != 0 || len(newField.Fields) != 0 {
		return "", fmt.Errorf("%w: ALTER TABLE supports top-level scalar columns only", domain.ErrUnsupported)
	}
	oldWithoutName, newWithoutName := oldField, newField
	oldWithoutName.Name, newWithoutName.Name = "", ""
	if oldField.Name != newField.Name && reflect.DeepEqual(oldWithoutName, newWithoutName) {
		return "RENAME COLUMN " + quoteIdentifier(oldField.Name) + " TO " + quoteIdentifier(newField.Name), nil
	}
	if oldField.Name == newField.Name && oldField.Mode == newField.Mode {
		fieldType, err := duckDBType(newField)
		if err != nil {
			return "", err
		}
		return "ALTER COLUMN " + quoteIdentifier(oldField.Name) + " TYPE " + fieldType, nil
	}
	return "", fmt.Errorf("%w: unsupported table schema change", domain.ErrUnsupported)
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
	case "STRING", "GEOGRAPHY":
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
