package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	githubsqlite3 "github.com/mattn/go-sqlite3"
)

var _ ports.CatalogRepository = (*Store)(nil)

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) CreateProject(ctx context.Context, project domain.Project) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO projects
        (project_id, friendly_name, description, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)`,
		project.ID, project.FriendlyName, project.Description,
		encodeTime(project.CreatedAt), encodeTime(project.UpdatedAt),
	)
	return translateConstraint(err, "project "+project.ID)
}

func (s *Store) GetProject(ctx context.Context, projectID string) (domain.Project, error) {
	var project domain.Project
	var createdAt, updatedAt string
	err := s.db.QueryRowContext(ctx, `SELECT project_id, friendly_name, description, created_at, updated_at
        FROM projects WHERE project_id = ?`, projectID).Scan(
		&project.ID, &project.FriendlyName, &project.Description, &createdAt, &updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Project{}, fmt.Errorf("%w: project %s", domain.ErrNotFound, projectID)
	}
	if err != nil {
		return domain.Project{}, fmt.Errorf("get project %s: %w", projectID, err)
	}
	if project.CreatedAt, err = decodeTime(createdAt); err != nil {
		return domain.Project{}, fmt.Errorf("decode project %s created time: %w", projectID, err)
	}
	if project.UpdatedAt, err = decodeTime(updatedAt); err != nil {
		return domain.Project{}, fmt.Errorf("decode project %s updated time: %w", projectID, err)
	}
	return project, nil
}

func (s *Store) ListProjects(ctx context.Context) ([]domain.Project, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT project_id, friendly_name, description, created_at, updated_at
        FROM projects ORDER BY project_id`)
	if err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	defer rows.Close()
	projects := make([]domain.Project, 0)
	for rows.Next() {
		var project domain.Project
		var createdAt, updatedAt string
		if err := rows.Scan(&project.ID, &project.FriendlyName, &project.Description, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan project: %w", err)
		}
		if project.CreatedAt, err = decodeTime(createdAt); err != nil {
			return nil, fmt.Errorf("decode project %s created time: %w", project.ID, err)
		}
		if project.UpdatedAt, err = decodeTime(updatedAt); err != nil {
			return nil, fmt.Errorf("decode project %s updated time: %w", project.ID, err)
		}
		projects = append(projects, project)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate projects: %w", err)
	}
	return projects, nil
}

func (s *Store) DeleteProject(ctx context.Context, projectID string) error {
	result, err := s.db.ExecContext(ctx, "DELETE FROM projects WHERE project_id = ?", projectID)
	if err != nil {
		return fmt.Errorf("delete project %s: %w", projectID, err)
	}
	return requireAffected(result, "project "+projectID)
}

func (s *Store) CreateDataset(ctx context.Context, dataset domain.Dataset) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin create dataset: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireProject(ctx, tx, dataset.ProjectID); err != nil {
		return err
	}
	labels, err := encodeJSON(dataset.Labels)
	if err != nil {
		return fmt.Errorf("encode dataset labels: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO datasets
        (project_id, dataset_id, friendly_name, description, location, labels_json,
         default_table_expiration_ms, default_partition_expiration_ms, created_at, updated_at, hidden)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		dataset.ProjectID, dataset.ID, dataset.FriendlyName, dataset.Description, dataset.Location, labels,
		dataset.DefaultTableExpirationMs, dataset.DefaultPartitionExpirationMs,
		encodeTime(dataset.CreatedAt), encodeTime(dataset.UpdatedAt), boolInt(dataset.Hidden),
	)
	if err := translateConstraint(err, "dataset "+dataset.ProjectID+"/"+dataset.ID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit create dataset: %w", err)
	}
	return nil
}

func (s *Store) UpdateDataset(ctx context.Context, dataset domain.Dataset) error {
	labels, err := encodeJSON(dataset.Labels)
	if err != nil {
		return fmt.Errorf("encode dataset labels: %w", err)
	}
	result, err := s.db.ExecContext(ctx, `UPDATE datasets SET
        friendly_name = ?, description = ?, location = ?, labels_json = ?,
        default_table_expiration_ms = ?, default_partition_expiration_ms = ?,
        created_at = ?, updated_at = ?, hidden = ?
        WHERE project_id = ? AND dataset_id = ?`,
		dataset.FriendlyName, dataset.Description, dataset.Location, labels,
		dataset.DefaultTableExpirationMs, dataset.DefaultPartitionExpirationMs,
		encodeTime(dataset.CreatedAt), encodeTime(dataset.UpdatedAt), boolInt(dataset.Hidden),
		dataset.ProjectID, dataset.ID,
	)
	if err != nil {
		return fmt.Errorf("update dataset %s/%s: %w", dataset.ProjectID, dataset.ID, err)
	}
	return requireAffected(result, "dataset "+dataset.ProjectID+"/"+dataset.ID)
}

func (s *Store) GetDataset(ctx context.Context, projectID, datasetID string) (domain.Dataset, error) {
	dataset, err := scanDataset(s.db.QueryRowContext(ctx, datasetSelect+" WHERE project_id = ? AND dataset_id = ?", projectID, datasetID))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Dataset{}, fmt.Errorf("%w: dataset %s/%s", domain.ErrNotFound, projectID, datasetID)
	}
	if err != nil {
		return domain.Dataset{}, fmt.Errorf("get dataset %s/%s: %w", projectID, datasetID, err)
	}
	return dataset, nil
}

func (s *Store) ListDatasets(ctx context.Context, projectID string) ([]domain.Dataset, error) {
	if err := requireProject(ctx, s.db, projectID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, datasetSelect+" WHERE project_id = ? ORDER BY dataset_id", projectID)
	if err != nil {
		return nil, fmt.Errorf("list datasets for project %s: %w", projectID, err)
	}
	defer rows.Close()
	datasets := make([]domain.Dataset, 0)
	for rows.Next() {
		dataset, err := scanDataset(rows)
		if err != nil {
			return nil, fmt.Errorf("scan dataset for project %s: %w", projectID, err)
		}
		datasets = append(datasets, dataset)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate datasets for project %s: %w", projectID, err)
	}
	return datasets, nil
}

func (s *Store) DeleteDataset(ctx context.Context, projectID, datasetID string) error {
	result, err := s.db.ExecContext(ctx, "DELETE FROM datasets WHERE project_id = ? AND dataset_id = ?", projectID, datasetID)
	if err != nil {
		return fmt.Errorf("delete dataset %s/%s: %w", projectID, datasetID, err)
	}
	return requireAffected(result, "dataset "+projectID+"/"+datasetID)
}

const datasetSelect = `SELECT project_id, dataset_id, friendly_name, description, location, labels_json,
    default_table_expiration_ms, default_partition_expiration_ms, created_at, updated_at, hidden FROM datasets`

type rowScanner interface {
	Scan(...any) error
}

func scanDataset(scanner rowScanner) (domain.Dataset, error) {
	var dataset domain.Dataset
	var labels, createdAt, updatedAt string
	var tableExpiration, partitionExpiration sql.NullInt64
	var hidden int
	if err := scanner.Scan(
		&dataset.ProjectID, &dataset.ID, &dataset.FriendlyName, &dataset.Description, &dataset.Location, &labels,
		&tableExpiration, &partitionExpiration, &createdAt, &updatedAt, &hidden,
	); err != nil {
		return domain.Dataset{}, err
	}
	if err := decodeJSON(labels, &dataset.Labels); err != nil {
		return domain.Dataset{}, fmt.Errorf("decode labels: %w", err)
	}
	dataset.DefaultTableExpirationMs = nullableInt64Pointer(tableExpiration)
	dataset.DefaultPartitionExpirationMs = nullableInt64Pointer(partitionExpiration)
	var err error
	if dataset.CreatedAt, err = decodeTime(createdAt); err != nil {
		return domain.Dataset{}, fmt.Errorf("decode created time: %w", err)
	}
	if dataset.UpdatedAt, err = decodeTime(updatedAt); err != nil {
		return domain.Dataset{}, fmt.Errorf("decode updated time: %w", err)
	}
	dataset.Hidden = hidden != 0
	return dataset, nil
}

func (s *Store) CreateTable(ctx context.Context, table domain.Table) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin create table: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireDataset(ctx, tx, table.ProjectID, table.DatasetID); err != nil {
		return err
	}
	if err := insertTable(ctx, tx, table); err != nil {
		return err
	}
	if err := insertFields(ctx, tx, table); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit create table: %w", err)
	}
	return nil
}

func (s *Store) UpdateTable(ctx context.Context, table domain.Table) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin update table: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	result, err := updateTableMetadata(ctx, tx, table)
	if err != nil {
		return err
	}
	if err := requireAffected(result, "table "+tableKey(table.ProjectID, table.DatasetID, table.ID)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM table_fields
        WHERE project_id = ? AND dataset_id = ? AND table_id = ?`, table.ProjectID, table.DatasetID, table.ID); err != nil {
		return fmt.Errorf("delete old table fields: %w", err)
	}
	if err := insertFields(ctx, tx, table); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit update table: %w", err)
	}
	return nil
}

func (s *Store) GetTable(ctx context.Context, projectID, datasetID, tableID string) (domain.Table, error) {
	table, err := scanTable(s.db.QueryRowContext(ctx, tableSelect+` WHERE project_id = ? AND dataset_id = ? AND table_id = ?`, projectID, datasetID, tableID))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Table{}, fmt.Errorf("%w: table %s", domain.ErrNotFound, tableKey(projectID, datasetID, tableID))
	}
	if err != nil {
		return domain.Table{}, fmt.Errorf("get table %s: %w", tableKey(projectID, datasetID, tableID), err)
	}
	table.Schema, err = loadFields(ctx, s.db, projectID, datasetID, tableID)
	if err != nil {
		return domain.Table{}, err
	}
	return table, nil
}

func (s *Store) ListTables(ctx context.Context, projectID, datasetID string) ([]domain.Table, error) {
	if err := requireDataset(ctx, s.db, projectID, datasetID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, tableSelect+` WHERE project_id = ? AND dataset_id = ? ORDER BY table_id`, projectID, datasetID)
	if err != nil {
		return nil, fmt.Errorf("list tables for dataset %s/%s: %w", projectID, datasetID, err)
	}
	tables := make([]domain.Table, 0)
	for rows.Next() {
		table, err := scanTable(rows)
		if err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan table for dataset %s/%s: %w", projectID, datasetID, err)
		}
		tables = append(tables, table)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close tables for dataset %s/%s: %w", projectID, datasetID, err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tables for dataset %s/%s: %w", projectID, datasetID, err)
	}
	// Read nested field rows only after closing the table cursor. Store uses one
	// SQLite connection so a nested query while rows is open would deadlock.
	for index := range tables {
		tables[index].Schema, err = loadFields(ctx, s.db, projectID, datasetID, tables[index].ID)
		if err != nil {
			return nil, err
		}
	}
	return tables, nil
}

func (s *Store) DeleteTable(ctx context.Context, projectID, datasetID, tableID string) error {
	result, err := s.db.ExecContext(ctx, `DELETE FROM catalog_tables
        WHERE project_id = ? AND dataset_id = ? AND table_id = ?`, projectID, datasetID, tableID)
	if err != nil {
		return fmt.Errorf("delete table %s: %w", tableKey(projectID, datasetID, tableID), err)
	}
	return requireAffected(result, "table "+tableKey(projectID, datasetID, tableID))
}

const tableSelect = `SELECT project_id, dataset_id, table_id, friendly_name, description,
    labels_json, table_type, location, expiration_time,
    has_time_partitioning, time_partitioning_type, time_partitioning_field, time_partitioning_expiration_ms,
    has_range_partitioning, range_partitioning_field, range_start, range_end, range_interval,
    clustering_fields_json, created_at, updated_at FROM catalog_tables`

func insertTable(ctx context.Context, tx *sql.Tx, table domain.Table) error {
	labels, clustering, values, err := tableStorageValues(table)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO catalog_tables
        (project_id, dataset_id, table_id, friendly_name, description, labels_json, table_type, location,
         expiration_time, has_time_partitioning, time_partitioning_type, time_partitioning_field,
         time_partitioning_expiration_ms, has_range_partitioning, range_partitioning_field,
         range_start, range_end, range_interval, clustering_fields_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		table.ProjectID, table.DatasetID, table.ID, table.FriendlyName, table.Description, labels, table.Type, table.Location,
		values.expirationTime, values.hasTimePartitioning, values.timeType, values.timeField, values.timeExpiration,
		values.hasRangePartitioning, values.rangeField, values.rangeStart, values.rangeEnd, values.rangeInterval,
		clustering, encodeTime(table.CreatedAt), encodeTime(table.UpdatedAt),
	)
	return translateConstraint(err, "table "+tableKey(table.ProjectID, table.DatasetID, table.ID))
}

func updateTableMetadata(ctx context.Context, tx *sql.Tx, table domain.Table) (sql.Result, error) {
	labels, clustering, values, err := tableStorageValues(table)
	if err != nil {
		return nil, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE catalog_tables SET
        friendly_name = ?, description = ?, labels_json = ?, table_type = ?, location = ?, expiration_time = ?,
        has_time_partitioning = ?, time_partitioning_type = ?, time_partitioning_field = ?,
        time_partitioning_expiration_ms = ?, has_range_partitioning = ?, range_partitioning_field = ?,
        range_start = ?, range_end = ?, range_interval = ?, clustering_fields_json = ?, created_at = ?, updated_at = ?
        WHERE project_id = ? AND dataset_id = ? AND table_id = ?`,
		table.FriendlyName, table.Description, labels, table.Type, table.Location, values.expirationTime,
		values.hasTimePartitioning, values.timeType, values.timeField, values.timeExpiration,
		values.hasRangePartitioning, values.rangeField, values.rangeStart, values.rangeEnd, values.rangeInterval,
		clustering, encodeTime(table.CreatedAt), encodeTime(table.UpdatedAt), table.ProjectID, table.DatasetID, table.ID,
	)
	if err != nil {
		return nil, fmt.Errorf("update table %s: %w", tableKey(table.ProjectID, table.DatasetID, table.ID), err)
	}
	return result, nil
}

type storedTableValues struct {
	expirationTime                                  any
	hasTimePartitioning, hasRangePartitioning       int
	timeType, timeField, timeExpiration             any
	rangeField, rangeStart, rangeEnd, rangeInterval any
}

func tableStorageValues(table domain.Table) (string, string, storedTableValues, error) {
	labels, err := encodeJSON(table.Labels)
	if err != nil {
		return "", "", storedTableValues{}, fmt.Errorf("encode table labels: %w", err)
	}
	clustering, err := encodeJSON(table.ClusteringFields)
	if err != nil {
		return "", "", storedTableValues{}, fmt.Errorf("encode clustering fields: %w", err)
	}
	values := storedTableValues{}
	if table.ExpirationTime != nil {
		values.expirationTime = encodeTime(*table.ExpirationTime)
	}
	if table.TimePartitioning != nil {
		values.hasTimePartitioning = 1
		values.timeType = table.TimePartitioning.Type
		values.timeField = table.TimePartitioning.Field
		values.timeExpiration = table.TimePartitioning.ExpirationMs
	}
	if table.RangePartitioning != nil {
		values.hasRangePartitioning = 1
		values.rangeField = table.RangePartitioning.Field
		values.rangeStart = table.RangePartitioning.Range.Start
		values.rangeEnd = table.RangePartitioning.Range.End
		values.rangeInterval = table.RangePartitioning.Range.Interval
	}
	return labels, clustering, values, nil
}

func scanTable(scanner rowScanner) (domain.Table, error) {
	var table domain.Table
	var labels, clustering, createdAt, updatedAt string
	var expirationTime, timeType, timeField, rangeField sql.NullString
	var timeExpiration, rangeStart, rangeEnd, rangeInterval sql.NullInt64
	var hasTimePartitioning, hasRangePartitioning int
	if err := scanner.Scan(
		&table.ProjectID, &table.DatasetID, &table.ID, &table.FriendlyName, &table.Description,
		&labels, &table.Type, &table.Location, &expirationTime,
		&hasTimePartitioning, &timeType, &timeField, &timeExpiration,
		&hasRangePartitioning, &rangeField, &rangeStart, &rangeEnd, &rangeInterval,
		&clustering, &createdAt, &updatedAt,
	); err != nil {
		return domain.Table{}, err
	}
	if err := decodeJSON(labels, &table.Labels); err != nil {
		return domain.Table{}, fmt.Errorf("decode labels: %w", err)
	}
	if err := decodeJSON(clustering, &table.ClusteringFields); err != nil {
		return domain.Table{}, fmt.Errorf("decode clustering fields: %w", err)
	}
	if expirationTime.Valid {
		value, err := decodeTime(expirationTime.String)
		if err != nil {
			return domain.Table{}, fmt.Errorf("decode expiration time: %w", err)
		}
		table.ExpirationTime = &value
	}
	if hasTimePartitioning != 0 {
		table.TimePartitioning = &domain.TimePartitioning{
			Type: timeType.String, Field: timeField.String, ExpirationMs: timeExpiration.Int64,
		}
	}
	if hasRangePartitioning != 0 {
		table.RangePartitioning = &domain.RangePartitioning{
			Field: rangeField.String,
			Range: domain.Range{Start: rangeStart.Int64, End: rangeEnd.Int64, Interval: rangeInterval.Int64},
		}
	}
	var err error
	if table.CreatedAt, err = decodeTime(createdAt); err != nil {
		return domain.Table{}, fmt.Errorf("decode created time: %w", err)
	}
	if table.UpdatedAt, err = decodeTime(updatedAt); err != nil {
		return domain.Table{}, fmt.Errorf("decode updated time: %w", err)
	}
	return table, nil
}

func insertFields(ctx context.Context, tx *sql.Tx, table domain.Table) error {
	var walk func([]domain.Field, string) error
	walk = func(fields []domain.Field, parentPath string) error {
		for ordinal, field := range fields {
			path := strconv.Itoa(ordinal)
			var storedParent any
			if parentPath != "" {
				path = parentPath + "/" + path
				storedParent = parentPath
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO table_fields
                (project_id, dataset_id, table_id, field_path, parent_path, ordinal,
                 name, logical_type, mode, description, precision, scale)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				table.ProjectID, table.DatasetID, table.ID, path, storedParent, ordinal,
				field.Name, field.Type, field.Mode, field.Description, field.Precision, field.Scale,
			); err != nil {
				return fmt.Errorf("insert table field %s: %w", path, err)
			}
			if err := walk(field.Fields, path); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(table.Schema, "")
}

type storedField struct {
	path       string
	parentPath string
	ordinal    int
	field      domain.Field
}

func loadFields(ctx context.Context, query queryer, projectID, datasetID, tableID string) ([]domain.Field, error) {
	rows, err := query.QueryContext(ctx, `SELECT field_path, parent_path, ordinal, name, logical_type, mode,
        description, precision, scale FROM table_fields
        WHERE project_id = ? AND dataset_id = ? AND table_id = ?
        ORDER BY length(field_path), field_path`, projectID, datasetID, tableID)
	if err != nil {
		return nil, fmt.Errorf("load fields for table %s: %w", tableKey(projectID, datasetID, tableID), err)
	}
	defer rows.Close()
	children := make(map[string][]storedField)
	for rows.Next() {
		var record storedField
		var parent sql.NullString
		var precision, scale sql.NullInt64
		if err := rows.Scan(
			&record.path, &parent, &record.ordinal, &record.field.Name, &record.field.Type,
			&record.field.Mode, &record.field.Description, &precision, &scale,
		); err != nil {
			return nil, fmt.Errorf("scan field for table %s: %w", tableKey(projectID, datasetID, tableID), err)
		}
		if parent.Valid {
			record.parentPath = parent.String
		}
		record.field.Precision = nullableInt64Pointer(precision)
		record.field.Scale = nullableInt64Pointer(scale)
		children[record.parentPath] = append(children[record.parentPath], record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate fields for table %s: %w", tableKey(projectID, datasetID, tableID), err)
	}
	var build func(string) []domain.Field
	build = func(parentPath string) []domain.Field {
		records := children[parentPath]
		sort.Slice(records, func(i, j int) bool { return records[i].ordinal < records[j].ordinal })
		fields := make([]domain.Field, len(records))
		for index, record := range records {
			fields[index] = record.field
			fields[index].Fields = build(record.path)
		}
		return fields
	}
	return build(""), nil
}

func requireProject(ctx context.Context, query queryer, projectID string) error {
	var exists int
	if err := query.QueryRowContext(ctx, "SELECT 1 FROM projects WHERE project_id = ?", projectID).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: project %s", domain.ErrNotFound, projectID)
		}
		return fmt.Errorf("check project %s: %w", projectID, err)
	}
	return nil
}

func requireDataset(ctx context.Context, query queryer, projectID, datasetID string) error {
	var exists int
	if err := query.QueryRowContext(ctx, `SELECT 1 FROM datasets WHERE project_id = ? AND dataset_id = ?`, projectID, datasetID).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: dataset %s/%s", domain.ErrNotFound, projectID, datasetID)
		}
		return fmt.Errorf("check dataset %s/%s: %w", projectID, datasetID, err)
	}
	return nil
}

func translateConstraint(err error, resource string) error {
	if err == nil {
		return nil
	}
	var sqliteError githubsqlite3.Error
	if errors.As(err, &sqliteError) && sqliteError.Code == githubsqlite3.ErrConstraint {
		switch sqliteError.ExtendedCode {
		case githubsqlite3.ErrConstraintPrimaryKey, githubsqlite3.ErrConstraintUnique:
			return fmt.Errorf("%w: %s", domain.ErrConflict, resource)
		}
	}
	return fmt.Errorf("persist %s: %w", resource, err)
}

func requireAffected(result sql.Result, resource string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read affected rows for %s: %w", resource, err)
	}
	if affected == 0 {
		return fmt.Errorf("%w: %s", domain.ErrNotFound, resource)
	}
	return nil
}

func encodeJSON(value any) (string, error) {
	encoded, err := json.Marshal(value)
	return string(encoded), err
}

func decodeJSON(encoded string, destination any) error {
	return json.Unmarshal([]byte(encoded), destination)
}

func encodeTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.Format(time.RFC3339Nano)
}

func decodeTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, value)
}

func nullableInt64Pointer(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	return &value.Int64
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func tableKey(projectID, datasetID, tableID string) string {
	return strings.Join([]string{projectID, datasetID, tableID}, "/")
}
