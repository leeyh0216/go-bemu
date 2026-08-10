package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

type catalogRepository struct {
	db *sql.DB
}

var _ ports.CatalogRepository = (*catalogRepository)(nil)
var _ ports.ViewRepository = (*catalogRepository)(nil)

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type rowScanner interface {
	Scan(...any) error
}

func (r *catalogRepository) CreateProject(ctx context.Context, project domain.Project) error {
	if err := project.Validate(); err != nil {
		return err
	}
	return r.write(ctx, "create project", func(tx *sql.Tx) error {
		exists, err := projectExists(ctx, tx, project.ID)
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("%w: project %s", domain.ErrConflict, project.ID)
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO bqemu_projects
    (project_id, friendly_name, description, created_at, updated_at)
VALUES (?, ?, ?, ?, ?)`, project.ID, project.FriendlyName, project.Description,
			encodeTime(project.CreatedAt), encodeTime(project.UpdatedAt))
		return err
	})
}

func (r *catalogRepository) GetProject(ctx context.Context, projectID string) (domain.Project, error) {
	project, err := scanProject(r.db.QueryRowContext(ctx, `SELECT project_id, friendly_name, description,
    created_at, updated_at FROM bqemu_projects WHERE project_id = ?`, projectID))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Project{}, fmt.Errorf("%w: project %s", domain.ErrNotFound, projectID)
	}
	if err != nil {
		return domain.Project{}, repositoryError(ctx, "get project", err)
	}
	return project, nil
}

func (r *catalogRepository) ListProjects(ctx context.Context) ([]domain.Project, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT project_id, friendly_name, description,
    created_at, updated_at FROM bqemu_projects ORDER BY project_id`)
	if err != nil {
		return nil, repositoryError(ctx, "list projects", err)
	}
	defer rows.Close()
	projects := make([]domain.Project, 0)
	for rows.Next() {
		project, scanErr := scanProject(rows)
		if scanErr != nil {
			return nil, repositoryError(ctx, "list projects", scanErr)
		}
		projects = append(projects, project)
	}
	if err := rows.Err(); err != nil {
		return nil, repositoryError(ctx, "list projects", err)
	}
	return projects, nil
}

func (r *catalogRepository) DeleteProject(ctx context.Context, projectID string) error {
	return r.write(ctx, "delete project", func(tx *sql.Tx) error {
		exists, err := projectExists(ctx, tx, projectID)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("%w: project %s", domain.ErrNotFound, projectID)
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM bqemu_projects WHERE project_id = ?`, projectID)
		return err
	})
}

func (r *catalogRepository) CreateDataset(ctx context.Context, dataset domain.Dataset) error {
	if err := dataset.Validate(); err != nil {
		return err
	}
	return r.write(ctx, "create dataset", func(tx *sql.Tx) error {
		parentExists, err := projectExists(ctx, tx, dataset.ProjectID)
		if err != nil {
			return err
		}
		if !parentExists {
			return fmt.Errorf("%w: project %s", domain.ErrNotFound, dataset.ProjectID)
		}
		exists, err := datasetExists(ctx, tx, dataset.ProjectID, dataset.ID)
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("%w: dataset %s/%s", domain.ErrConflict, dataset.ProjectID, dataset.ID)
		}
		return insertDataset(ctx, tx, dataset)
	})
}

func (r *catalogRepository) UpdateDataset(ctx context.Context, dataset domain.Dataset) error {
	if err := dataset.Validate(); err != nil {
		return err
	}
	return r.write(ctx, "update dataset", func(tx *sql.Tx) error {
		exists, err := datasetExists(ctx, tx, dataset.ProjectID, dataset.ID)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("%w: dataset %s/%s", domain.ErrNotFound, dataset.ProjectID, dataset.ID)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE bqemu_datasets SET
    friendly_name = ?, description = ?, location = ?, labels_present = ?,
    default_table_expiration_ms = ?, default_partition_expiration_ms = ?,
    created_at = ?, updated_at = ?, hidden = ?
WHERE project_id = ? AND dataset_id = ?`,
			dataset.FriendlyName, dataset.Description, dataset.Location, boolInt(dataset.Labels != nil),
			optionalInt64(dataset.DefaultTableExpirationMs), optionalInt64(dataset.DefaultPartitionExpirationMs),
			encodeTime(dataset.CreatedAt), encodeTime(dataset.UpdatedAt), boolInt(dataset.Hidden),
			dataset.ProjectID, dataset.ID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM bqemu_dataset_labels
WHERE project_id = ? AND dataset_id = ?`, dataset.ProjectID, dataset.ID); err != nil {
			return err
		}
		return insertDatasetLabels(ctx, tx, dataset)
	})
}

func (r *catalogRepository) GetDataset(ctx context.Context, projectID, datasetID string) (domain.Dataset, error) {
	dataset, labelsPresent, err := scanDataset(r.db.QueryRowContext(ctx, datasetSelect+`
WHERE project_id = ? AND dataset_id = ?`, projectID, datasetID))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Dataset{}, fmt.Errorf("%w: dataset %s/%s", domain.ErrNotFound, projectID, datasetID)
	}
	if err != nil {
		return domain.Dataset{}, repositoryError(ctx, "get dataset", err)
	}
	if err := loadDatasetLabels(ctx, r.db, &dataset, labelsPresent); err != nil {
		return domain.Dataset{}, repositoryError(ctx, "get dataset labels", err)
	}
	return dataset, nil
}

func (r *catalogRepository) ListDatasets(ctx context.Context, projectID string) ([]domain.Dataset, error) {
	exists, err := projectExists(ctx, r.db, projectID)
	if err != nil {
		return nil, repositoryError(ctx, "list datasets", err)
	}
	if !exists {
		return nil, fmt.Errorf("%w: project %s", domain.ErrNotFound, projectID)
	}
	rows, err := r.db.QueryContext(ctx, datasetSelect+`
WHERE project_id = ? ORDER BY dataset_id`, projectID)
	if err != nil {
		return nil, repositoryError(ctx, "list datasets", err)
	}
	type result struct {
		dataset       domain.Dataset
		labelsPresent bool
	}
	var scanned []result
	for rows.Next() {
		dataset, labelsPresent, scanErr := scanDataset(rows)
		if scanErr != nil {
			rows.Close()
			return nil, repositoryError(ctx, "list datasets", scanErr)
		}
		scanned = append(scanned, result{dataset: dataset, labelsPresent: labelsPresent})
	}
	if err := rows.Close(); err != nil {
		return nil, repositoryError(ctx, "list datasets", err)
	}
	if err := rows.Err(); err != nil {
		return nil, repositoryError(ctx, "list datasets", err)
	}
	datasets := make([]domain.Dataset, 0, len(scanned))
	for _, item := range scanned {
		if err := loadDatasetLabels(ctx, r.db, &item.dataset, item.labelsPresent); err != nil {
			return nil, repositoryError(ctx, "list dataset labels", err)
		}
		datasets = append(datasets, item.dataset)
	}
	return datasets, nil
}

func (r *catalogRepository) DeleteDataset(ctx context.Context, projectID, datasetID string) error {
	return r.write(ctx, "delete dataset", func(tx *sql.Tx) error {
		exists, err := datasetExists(ctx, tx, projectID, datasetID)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("%w: dataset %s/%s", domain.ErrNotFound, projectID, datasetID)
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM bqemu_datasets
WHERE project_id = ? AND dataset_id = ?`, projectID, datasetID)
		return err
	})
}

func (r *catalogRepository) CreateTable(ctx context.Context, table domain.Table) error {
	if err := table.Validate(); err != nil {
		return err
	}
	if err := validateFieldPath(table.Schema); err != nil {
		return err
	}
	return r.write(ctx, "create table", func(tx *sql.Tx) error {
		parentExists, err := datasetExists(ctx, tx, table.ProjectID, table.DatasetID)
		if err != nil {
			return err
		}
		if !parentExists {
			return fmt.Errorf("%w: dataset %s/%s", domain.ErrNotFound, table.ProjectID, table.DatasetID)
		}
		exists, err := tableExists(ctx, tx, table.ProjectID, table.DatasetID, table.ID)
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("%w: table %s/%s/%s", domain.ErrConflict, table.ProjectID, table.DatasetID, table.ID)
		}
		return insertTable(ctx, tx, table)
	})
}

func (r *catalogRepository) UpdateTable(ctx context.Context, table domain.Table) error {
	if err := table.Validate(); err != nil {
		return err
	}
	if err := validateFieldPath(table.Schema); err != nil {
		return err
	}
	return r.write(ctx, "update table", func(tx *sql.Tx) error {
		exists, err := tableExists(ctx, tx, table.ProjectID, table.DatasetID, table.ID)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("%w: table %s/%s/%s", domain.ErrNotFound, table.ProjectID, table.DatasetID, table.ID)
		}
		if err := updateTableRecord(ctx, tx, table); err != nil {
			return err
		}
		for _, statement := range []string{
			`DELETE FROM bqemu_table_labels WHERE project_id = ? AND dataset_id = ? AND table_id = ?`,
			`DELETE FROM bqemu_table_clustering_fields WHERE project_id = ? AND dataset_id = ? AND table_id = ?`,
			`DELETE FROM bqemu_table_primary_key_columns WHERE project_id = ? AND dataset_id = ? AND table_id = ?`,
			`DELETE FROM bqemu_table_fields WHERE project_id = ? AND dataset_id = ? AND table_id = ?`,
		} {
			if _, err := tx.ExecContext(ctx, statement, table.ProjectID, table.DatasetID, table.ID); err != nil {
				return err
			}
		}
		return insertTableChildren(ctx, tx, table)
	})
}

func (r *catalogRepository) GetTable(ctx context.Context, projectID, datasetID, tableID string) (domain.Table, error) {
	table, presence, err := scanTable(r.db.QueryRowContext(ctx, tableSelect+`
WHERE project_id = ? AND dataset_id = ? AND table_id = ?`, projectID, datasetID, tableID))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Table{}, fmt.Errorf("%w: table %s/%s/%s", domain.ErrNotFound, projectID, datasetID, tableID)
	}
	if err != nil {
		return domain.Table{}, repositoryError(ctx, "get table", err)
	}
	if err := loadTableChildren(ctx, r.db, &table, presence); err != nil {
		return domain.Table{}, repositoryError(ctx, "get table metadata", err)
	}
	return table, nil
}

func (r *catalogRepository) ListTables(ctx context.Context, projectID, datasetID string) ([]domain.Table, error) {
	exists, err := datasetExists(ctx, r.db, projectID, datasetID)
	if err != nil {
		return nil, repositoryError(ctx, "list tables", err)
	}
	if !exists {
		return nil, fmt.Errorf("%w: dataset %s/%s", domain.ErrNotFound, projectID, datasetID)
	}
	rows, err := r.db.QueryContext(ctx, tableSelect+`
WHERE project_id = ? AND dataset_id = ? ORDER BY table_id`, projectID, datasetID)
	if err != nil {
		return nil, repositoryError(ctx, "list tables", err)
	}
	type result struct {
		table    domain.Table
		presence tablePresence
	}
	var scanned []result
	for rows.Next() {
		table, presence, scanErr := scanTable(rows)
		if scanErr != nil {
			rows.Close()
			return nil, repositoryError(ctx, "list tables", scanErr)
		}
		scanned = append(scanned, result{table: table, presence: presence})
	}
	if err := rows.Close(); err != nil {
		return nil, repositoryError(ctx, "list tables", err)
	}
	if err := rows.Err(); err != nil {
		return nil, repositoryError(ctx, "list tables", err)
	}
	tables := make([]domain.Table, 0, len(scanned))
	for _, item := range scanned {
		if err := loadTableChildren(ctx, r.db, &item.table, item.presence); err != nil {
			return nil, repositoryError(ctx, "list table metadata", err)
		}
		tables = append(tables, item.table)
	}
	return tables, nil
}

func (r *catalogRepository) DeleteTable(ctx context.Context, projectID, datasetID, tableID string) error {
	return r.write(ctx, "delete table", func(tx *sql.Tx) error {
		exists, err := tableExists(ctx, tx, projectID, datasetID, tableID)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("%w: table %s/%s/%s", domain.ErrNotFound, projectID, datasetID, tableID)
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM bqemu_tables
WHERE project_id = ? AND dataset_id = ? AND table_id = ?`, projectID, datasetID, tableID)
		return err
	})
}

const viewSelect = `SELECT project_id, dataset_id, view_id, friendly_name, description,
    labels_json, query_sql, use_legacy_sql, schema_json, dependencies_json,
    analysis_fingerprint, location, created_at, updated_at FROM bqemu_views`

func (r *catalogRepository) CreateView(ctx context.Context, view domain.View) error {
	if err := view.Validate(); err != nil {
		return err
	}
	return r.write(ctx, "create view", func(tx *sql.Tx) error {
		parentExists, err := datasetExists(ctx, tx, view.ProjectID, view.DatasetID)
		if err != nil {
			return err
		}
		if !parentExists {
			return fmt.Errorf("%w: dataset %s/%s", domain.ErrNotFound, view.ProjectID, view.DatasetID)
		}
		exists, err := viewExists(ctx, tx, view.ProjectID, view.DatasetID, view.ID)
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("%w: view %s/%s/%s", domain.ErrConflict, view.ProjectID, view.DatasetID, view.ID)
		}
		return insertView(ctx, tx, view)
	})
}

func (r *catalogRepository) ReplaceView(ctx context.Context, view domain.View) error {
	if err := view.Validate(); err != nil {
		return err
	}
	return r.write(ctx, "replace view", func(tx *sql.Tx) error {
		exists, err := viewExists(ctx, tx, view.ProjectID, view.DatasetID, view.ID)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("%w: view %s/%s/%s", domain.ErrNotFound, view.ProjectID, view.DatasetID, view.ID)
		}
		labels, schema, dependencies, err := encodeView(view)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE bqemu_views SET
    friendly_name = ?, description = ?, labels_json = ?, query_sql = ?, use_legacy_sql = ?,
    schema_json = ?, dependencies_json = ?, analysis_fingerprint = ?, location = ?,
    created_at = ?, updated_at = ?
WHERE project_id = ? AND dataset_id = ? AND view_id = ?`,
			view.FriendlyName, view.Description, labels, view.Query, boolInt(view.UseLegacySQL),
			schema, dependencies, view.AnalysisFingerprint, view.Location, encodeTime(view.CreatedAt), encodeTime(view.UpdatedAt),
			view.ProjectID, view.DatasetID, view.ID)
		return err
	})
}

func (r *catalogRepository) GetView(ctx context.Context, projectID, datasetID, viewID string) (domain.View, error) {
	view, err := scanView(r.db.QueryRowContext(ctx, viewSelect+`
WHERE project_id = ? AND dataset_id = ? AND view_id = ?`, projectID, datasetID, viewID))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.View{}, fmt.Errorf("%w: view %s/%s/%s", domain.ErrNotFound, projectID, datasetID, viewID)
	}
	if err != nil {
		return domain.View{}, repositoryError(ctx, "get view", err)
	}
	return view, nil
}

func (r *catalogRepository) ListViews(ctx context.Context, projectID, datasetID string) ([]domain.View, error) {
	exists, err := datasetExists(ctx, r.db, projectID, datasetID)
	if err != nil {
		return nil, repositoryError(ctx, "list views", err)
	}
	if !exists {
		return nil, fmt.Errorf("%w: dataset %s/%s", domain.ErrNotFound, projectID, datasetID)
	}
	rows, err := r.db.QueryContext(ctx, viewSelect+`
WHERE project_id = ? AND dataset_id = ? ORDER BY view_id`, projectID, datasetID)
	if err != nil {
		return nil, repositoryError(ctx, "list views", err)
	}
	defer rows.Close()
	views := make([]domain.View, 0)
	for rows.Next() {
		view, scanErr := scanView(rows)
		if scanErr != nil {
			return nil, repositoryError(ctx, "list views", scanErr)
		}
		views = append(views, view)
	}
	if err := rows.Err(); err != nil {
		return nil, repositoryError(ctx, "list views", err)
	}
	return views, nil
}

func (r *catalogRepository) DeleteView(ctx context.Context, projectID, datasetID, viewID string) error {
	return r.write(ctx, "delete view", func(tx *sql.Tx) error {
		exists, err := viewExists(ctx, tx, projectID, datasetID, viewID)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("%w: view %s/%s/%s", domain.ErrNotFound, projectID, datasetID, viewID)
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM bqemu_views
WHERE project_id = ? AND dataset_id = ? AND view_id = ?`, projectID, datasetID, viewID)
		return err
	})
}

func viewExists(ctx context.Context, q queryer, projectID, datasetID, viewID string) (bool, error) {
	var value int
	err := q.QueryRowContext(ctx, `SELECT 1 FROM bqemu_views
WHERE project_id = ? AND dataset_id = ? AND view_id = ?`, projectID, datasetID, viewID).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func insertView(ctx context.Context, tx *sql.Tx, view domain.View) error {
	labels, schema, dependencies, err := encodeView(view)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO bqemu_views
    (project_id, dataset_id, view_id, friendly_name, description, labels_json, query_sql,
     use_legacy_sql, schema_json, dependencies_json, analysis_fingerprint, location, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		view.ProjectID, view.DatasetID, view.ID, view.FriendlyName, view.Description, labels, view.Query,
		boolInt(view.UseLegacySQL), schema, dependencies, view.AnalysisFingerprint, view.Location,
		encodeTime(view.CreatedAt), encodeTime(view.UpdatedAt))
	return err
}

func encodeView(view domain.View) (labels, schema, dependencies string, err error) {
	labelsBytes, err := json.Marshal(view.Labels)
	if err != nil {
		return "", "", "", err
	}
	schemaBytes, err := json.Marshal(view.Schema)
	if err != nil {
		return "", "", "", err
	}
	dependenciesBytes, err := json.Marshal(view.Dependencies)
	if err != nil {
		return "", "", "", err
	}
	return string(labelsBytes), string(schemaBytes), string(dependenciesBytes), nil
}

func scanView(scanner rowScanner) (domain.View, error) {
	var view domain.View
	var labels, schema, dependencies, createdAt, updatedAt string
	var legacy int
	if err := scanner.Scan(&view.ProjectID, &view.DatasetID, &view.ID, &view.FriendlyName, &view.Description,
		&labels, &view.Query, &legacy, &schema, &dependencies, &view.AnalysisFingerprint, &view.Location, &createdAt, &updatedAt); err != nil {
		return domain.View{}, err
	}
	if legacy != 0 && legacy != 1 {
		return domain.View{}, errors.New("view legacy SQL presence marker is inconsistent")
	}
	view.UseLegacySQL = legacy == 1
	if err := json.Unmarshal([]byte(labels), &view.Labels); err != nil {
		return domain.View{}, err
	}
	if err := json.Unmarshal([]byte(schema), &view.Schema); err != nil {
		return domain.View{}, err
	}
	if err := json.Unmarshal([]byte(dependencies), &view.Dependencies); err != nil {
		return domain.View{}, err
	}
	var err error
	if view.CreatedAt, err = decodeTime(createdAt); err != nil {
		return domain.View{}, err
	}
	if view.UpdatedAt, err = decodeTime(updatedAt); err != nil {
		return domain.View{}, err
	}
	if err := view.Validate(); err != nil {
		return domain.View{}, err
	}
	return domain.CloneView(view), nil
}

func (r *catalogRepository) write(ctx context.Context, operation string, mutate func(*sql.Tx) error) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return repositoryError(ctx, operation, err)
	}
	defer tx.Rollback()
	if err := mutate(tx); err != nil {
		if errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrConflict) ||
			errors.Is(err, domain.ErrInvalid) || errors.Is(err, domain.ErrUnsupported) {
			return err
		}
		return repositoryError(ctx, operation, err)
	}
	if err := tx.Commit(); err != nil {
		return repositoryError(ctx, operation, err)
	}
	return nil
}

func scanProject(scanner rowScanner) (domain.Project, error) {
	var project domain.Project
	var createdAt, updatedAt string
	if err := scanner.Scan(&project.ID, &project.FriendlyName, &project.Description, &createdAt, &updatedAt); err != nil {
		return domain.Project{}, err
	}
	var err error
	if project.CreatedAt, err = decodeTime(createdAt); err != nil {
		return domain.Project{}, err
	}
	if project.UpdatedAt, err = decodeTime(updatedAt); err != nil {
		return domain.Project{}, err
	}
	return project, nil
}

const datasetSelect = `SELECT project_id, dataset_id, friendly_name, description, location,
    labels_present, default_table_expiration_ms, default_partition_expiration_ms,
    created_at, updated_at, hidden FROM bqemu_datasets `

func scanDataset(scanner rowScanner) (domain.Dataset, bool, error) {
	var dataset domain.Dataset
	var labelsPresent, hidden int
	var tableExpiration, partitionExpiration sql.NullInt64
	var createdAt, updatedAt string
	if err := scanner.Scan(&dataset.ProjectID, &dataset.ID, &dataset.FriendlyName, &dataset.Description,
		&dataset.Location, &labelsPresent, &tableExpiration, &partitionExpiration,
		&createdAt, &updatedAt, &hidden); err != nil {
		return domain.Dataset{}, false, err
	}
	var err error
	if dataset.CreatedAt, err = decodeTime(createdAt); err != nil {
		return domain.Dataset{}, false, err
	}
	if dataset.UpdatedAt, err = decodeTime(updatedAt); err != nil {
		return domain.Dataset{}, false, err
	}
	dataset.DefaultTableExpirationMs = fromNullInt64(tableExpiration)
	dataset.DefaultPartitionExpirationMs = fromNullInt64(partitionExpiration)
	dataset.Hidden = hidden == 1
	return dataset, labelsPresent == 1, nil
}

func insertDataset(ctx context.Context, tx *sql.Tx, dataset domain.Dataset) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO bqemu_datasets (
    project_id, dataset_id, friendly_name, description, location, labels_present,
    default_table_expiration_ms, default_partition_expiration_ms,
    created_at, updated_at, hidden
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		dataset.ProjectID, dataset.ID, dataset.FriendlyName, dataset.Description, dataset.Location,
		boolInt(dataset.Labels != nil), optionalInt64(dataset.DefaultTableExpirationMs),
		optionalInt64(dataset.DefaultPartitionExpirationMs), encodeTime(dataset.CreatedAt),
		encodeTime(dataset.UpdatedAt), boolInt(dataset.Hidden)); err != nil {
		return err
	}
	return insertDatasetLabels(ctx, tx, dataset)
}

func insertDatasetLabels(ctx context.Context, tx *sql.Tx, dataset domain.Dataset) error {
	for _, key := range sortedKeys(dataset.Labels) {
		if _, err := tx.ExecContext(ctx, `INSERT INTO bqemu_dataset_labels
    (project_id, dataset_id, label_key, label_value) VALUES (?, ?, ?, ?)`,
			dataset.ProjectID, dataset.ID, key, dataset.Labels[key]); err != nil {
			return err
		}
	}
	return nil
}

func loadDatasetLabels(ctx context.Context, q queryer, dataset *domain.Dataset, present bool) error {
	labels, err := loadLabels(ctx, q, `SELECT label_key, label_value FROM bqemu_dataset_labels
WHERE project_id = ? AND dataset_id = ? ORDER BY label_key`, dataset.ProjectID, dataset.ID)
	if err != nil {
		return err
	}
	if !present && len(labels) != 0 {
		return errors.New("dataset label presence marker is inconsistent")
	}
	if present {
		dataset.Labels = labels
	}
	return nil
}

type tablePresence struct {
	labels     bool
	clustering bool
}

const tableSelect = `SELECT project_id, dataset_id, table_id, friendly_name, description,
    labels_present, table_type, location, expiration_time,
    time_partitioning_present, time_partitioning_type, time_partitioning_field,
    time_partitioning_expiration_ms, range_partitioning_present, range_partitioning_field,
    range_start, range_end, range_interval, clustering_present, created_at, updated_at
FROM bqemu_tables `

func scanTable(scanner rowScanner) (domain.Table, tablePresence, error) {
	var table domain.Table
	var labelsPresent, clusteringPresent int
	var expiration sql.NullString
	var timePresent int
	var timeType, timeField sql.NullString
	var timeExpiration sql.NullInt64
	var rangePresent int
	var rangeField sql.NullString
	var rangeStart, rangeEnd, rangeInterval sql.NullInt64
	var createdAt, updatedAt string
	if err := scanner.Scan(
		&table.ProjectID, &table.DatasetID, &table.ID, &table.FriendlyName, &table.Description,
		&labelsPresent, &table.Type, &table.Location, &expiration,
		&timePresent, &timeType, &timeField, &timeExpiration,
		&rangePresent, &rangeField, &rangeStart, &rangeEnd, &rangeInterval,
		&clusteringPresent, &createdAt, &updatedAt,
	); err != nil {
		return domain.Table{}, tablePresence{}, err
	}
	var err error
	if table.CreatedAt, err = decodeTime(createdAt); err != nil {
		return domain.Table{}, tablePresence{}, err
	}
	if table.UpdatedAt, err = decodeTime(updatedAt); err != nil {
		return domain.Table{}, tablePresence{}, err
	}
	if expiration.Valid {
		value, parseErr := decodeTime(expiration.String)
		if parseErr != nil {
			return domain.Table{}, tablePresence{}, parseErr
		}
		table.ExpirationTime = &value
	}
	if timePresent == 1 {
		if !timeType.Valid || !timeField.Valid || !timeExpiration.Valid {
			return domain.Table{}, tablePresence{}, errors.New("time partitioning presence marker is inconsistent")
		}
		table.TimePartitioning = &domain.TimePartitioning{
			Type: timeType.String, Field: timeField.String, ExpirationMs: timeExpiration.Int64,
		}
	}
	if rangePresent == 1 {
		if !rangeField.Valid || !rangeStart.Valid || !rangeEnd.Valid || !rangeInterval.Valid {
			return domain.Table{}, tablePresence{}, errors.New("range partitioning presence marker is inconsistent")
		}
		table.RangePartitioning = &domain.RangePartitioning{
			Field: rangeField.String,
			Range: domain.Range{Start: rangeStart.Int64, End: rangeEnd.Int64, Interval: rangeInterval.Int64},
		}
	}
	return table, tablePresence{labels: labelsPresent == 1, clustering: clusteringPresent == 1}, nil
}

func insertTable(ctx context.Context, tx *sql.Tx, table domain.Table) error {
	values := tableColumnValues(table)
	if _, err := tx.ExecContext(ctx, `INSERT INTO bqemu_tables (
    project_id, dataset_id, table_id, friendly_name, description, labels_present,
    table_type, location, expiration_time, time_partitioning_present,
    time_partitioning_type, time_partitioning_field, time_partitioning_expiration_ms,
    range_partitioning_present, range_partitioning_field, range_start, range_end,
    range_interval, clustering_present, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, values...); err != nil {
		return err
	}
	return insertTableChildren(ctx, tx, table)
}

func updateTableRecord(ctx context.Context, tx *sql.Tx, table domain.Table) error {
	values := tableColumnValues(table)
	// The primary identity is used only by the WHERE clause for updates.
	_, err := tx.ExecContext(ctx, `UPDATE bqemu_tables SET
    friendly_name = ?, description = ?, labels_present = ?, table_type = ?, location = ?,
    expiration_time = ?, time_partitioning_present = ?, time_partitioning_type = ?,
    time_partitioning_field = ?, time_partitioning_expiration_ms = ?,
    range_partitioning_present = ?, range_partitioning_field = ?, range_start = ?, range_end = ?,
    range_interval = ?, clustering_present = ?, created_at = ?, updated_at = ?
WHERE project_id = ? AND dataset_id = ? AND table_id = ?`,
		values[3], values[4], values[5], values[6], values[7], values[8], values[9], values[10],
		values[11], values[12], values[13], values[14], values[15], values[16], values[17],
		values[18], values[19], values[20], values[0], values[1], values[2])
	return err
}

func tableColumnValues(table domain.Table) []any {
	var expiration any
	if table.ExpirationTime != nil {
		expiration = encodeTime(*table.ExpirationTime)
	}
	var timePresent int
	var timeType, timeField, timeExpiration any
	if table.TimePartitioning != nil {
		timePresent = 1
		timeType = table.TimePartitioning.Type
		timeField = table.TimePartitioning.Field
		timeExpiration = table.TimePartitioning.ExpirationMs
	}
	var rangePresent int
	var rangeField, rangeStart, rangeEnd, rangeInterval any
	if table.RangePartitioning != nil {
		rangePresent = 1
		rangeField = table.RangePartitioning.Field
		rangeStart = table.RangePartitioning.Range.Start
		rangeEnd = table.RangePartitioning.Range.End
		rangeInterval = table.RangePartitioning.Range.Interval
	}
	return []any{
		table.ProjectID, table.DatasetID, table.ID, table.FriendlyName, table.Description,
		boolInt(table.Labels != nil), table.Type, table.Location, expiration,
		timePresent, timeType, timeField, timeExpiration,
		rangePresent, rangeField, rangeStart, rangeEnd, rangeInterval,
		boolInt(table.ClusteringFields != nil), encodeTime(table.CreatedAt), encodeTime(table.UpdatedAt),
	}
}

func insertTableChildren(ctx context.Context, tx *sql.Tx, table domain.Table) error {
	for _, key := range sortedKeys(table.Labels) {
		if _, err := tx.ExecContext(ctx, `INSERT INTO bqemu_table_labels
    (project_id, dataset_id, table_id, label_key, label_value) VALUES (?, ?, ?, ?, ?)`,
			table.ProjectID, table.DatasetID, table.ID, key, table.Labels[key]); err != nil {
			return err
		}
	}
	for ordinal, field := range table.ClusteringFields {
		if _, err := tx.ExecContext(ctx, `INSERT INTO bqemu_table_clustering_fields
    (project_id, dataset_id, table_id, ordinal, field_name) VALUES (?, ?, ?, ?, ?)`,
			table.ProjectID, table.DatasetID, table.ID, ordinal, field); err != nil {
			return err
		}
	}
	for ordinal, field := range table.PrimaryKey {
		if _, err := tx.ExecContext(ctx, `INSERT INTO bqemu_table_primary_key_columns
    (project_id, dataset_id, table_id, ordinal, field_name) VALUES (?, ?, ?, ?, ?)`,
			table.ProjectID, table.DatasetID, table.ID, ordinal, field); err != nil {
			return err
		}
	}
	return insertFields(ctx, tx, table, "", table.Schema)
}

func insertFields(ctx context.Context, tx *sql.Tx, table domain.Table, parentPath string, fields []domain.Field) error {
	for ordinal, field := range fields {
		path := field.Name
		var parent any
		if parentPath != "" {
			path = parentPath + "." + field.Name
			parent = parentPath
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO bqemu_table_fields (
    project_id, dataset_id, table_id, field_path, parent_path, ordinal,
    field_name, field_type, field_mode, description, precision, scale, rounding_mode
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			table.ProjectID, table.DatasetID, table.ID, path, parent, ordinal,
			field.Name, field.Type, field.Mode, field.Description,
			optionalInt64(field.Precision), optionalInt64(field.Scale), string(field.RoundingMode)); err != nil {
			return err
		}
		if err := insertFields(ctx, tx, table, path, field.Fields); err != nil {
			return err
		}
	}
	return nil
}

func loadTableChildren(ctx context.Context, q queryer, table *domain.Table, presence tablePresence) error {
	labels, err := loadLabels(ctx, q, `SELECT label_key, label_value FROM bqemu_table_labels
WHERE project_id = ? AND dataset_id = ? AND table_id = ? ORDER BY label_key`,
		table.ProjectID, table.DatasetID, table.ID)
	if err != nil {
		return err
	}
	if !presence.labels && len(labels) != 0 {
		return errors.New("table label presence marker is inconsistent")
	}
	if presence.labels {
		table.Labels = labels
	}

	rows, err := q.QueryContext(ctx, `SELECT ordinal, field_name FROM bqemu_table_clustering_fields
WHERE project_id = ? AND dataset_id = ? AND table_id = ? ORDER BY ordinal`,
		table.ProjectID, table.DatasetID, table.ID)
	if err != nil {
		return err
	}
	var clustering []string
	expectedOrdinal := 0
	for rows.Next() {
		var ordinal int
		var fieldName string
		if err := rows.Scan(&ordinal, &fieldName); err != nil {
			rows.Close()
			return err
		}
		if ordinal != expectedOrdinal {
			rows.Close()
			return errors.New("table clustering ordinals are not contiguous")
		}
		expectedOrdinal++
		clustering = append(clustering, fieldName)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if !presence.clustering && len(clustering) != 0 {
		return errors.New("table clustering presence marker is inconsistent")
	}
	if presence.clustering {
		if clustering == nil {
			clustering = []string{}
		}
		table.ClusteringFields = clustering
	}

	primaryKeyRows, err := q.QueryContext(ctx, `SELECT ordinal, field_name FROM bqemu_table_primary_key_columns
WHERE project_id = ? AND dataset_id = ? AND table_id = ? ORDER BY ordinal`,
		table.ProjectID, table.DatasetID, table.ID)
	if err != nil {
		return err
	}
	for expectedOrdinal := 0; primaryKeyRows.Next(); expectedOrdinal++ {
		var ordinal int
		var fieldName string
		if err := primaryKeyRows.Scan(&ordinal, &fieldName); err != nil {
			primaryKeyRows.Close()
			return err
		}
		if ordinal != expectedOrdinal {
			primaryKeyRows.Close()
			return errors.New("table primary-key ordinals are not contiguous")
		}
		table.PrimaryKey = append(table.PrimaryKey, fieldName)
	}
	if err := primaryKeyRows.Close(); err != nil {
		return err
	}
	if err := primaryKeyRows.Err(); err != nil {
		return err
	}

	table.Schema, err = loadFields(ctx, q, *table)
	if err != nil {
		return err
	}
	if err := table.Validate(); err != nil {
		return fmt.Errorf("persisted table metadata is invalid: %w", err)
	}
	return nil
}

type fieldRecord struct {
	path    string
	parent  string
	ordinal int
	field   domain.Field
}

func loadFields(ctx context.Context, q queryer, table domain.Table) ([]domain.Field, error) {
	rows, err := q.QueryContext(ctx, `SELECT field_path, parent_path, ordinal, field_name,
    field_type, field_mode, description, precision, scale, rounding_mode
FROM bqemu_table_fields
WHERE project_id = ? AND dataset_id = ? AND table_id = ?
ORDER BY ifnull(parent_path, ''), ordinal`, table.ProjectID, table.DatasetID, table.ID)
	if err != nil {
		return nil, err
	}
	children := make(map[string][]fieldRecord)
	paths := make(map[string]struct{})
	count := 0
	for rows.Next() {
		var record fieldRecord
		var parent sql.NullString
		var precision, scale sql.NullInt64
		if err := rows.Scan(&record.path, &parent, &record.ordinal, &record.field.Name,
			&record.field.Type, &record.field.Mode, &record.field.Description,
			&precision, &scale, &record.field.RoundingMode); err != nil {
			rows.Close()
			return nil, err
		}
		if parent.Valid {
			record.parent = parent.String
		}
		record.field.Precision = fromNullInt64(precision)
		record.field.Scale = fromNullInt64(scale)
		children[record.parent] = append(children[record.parent], record)
		paths[record.path] = struct{}{}
		count++
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, errors.New("table schema has no fields")
	}

	visited := make(map[string]bool, count)
	var build func(string) ([]domain.Field, error)
	build = func(parent string) ([]domain.Field, error) {
		records := children[parent]
		sort.Slice(records, func(i, j int) bool { return records[i].ordinal < records[j].ordinal })
		result := make([]domain.Field, 0, len(records))
		for expectedOrdinal, record := range records {
			if record.ordinal != expectedOrdinal {
				return nil, fmt.Errorf("field ordinals under %q are not contiguous", parent)
			}
			if visited[record.path] {
				return nil, fmt.Errorf("field hierarchy contains a cycle at %q", record.path)
			}
			visited[record.path] = true
			nested, err := build(record.path)
			if err != nil {
				return nil, err
			}
			if len(nested) != 0 {
				record.field.Fields = nested
			}
			result = append(result, record.field)
		}
		return result, nil
	}
	fields, err := build("")
	if err != nil {
		return nil, err
	}
	if len(visited) != len(paths) {
		return nil, errors.New("table schema contains an unreachable nested field")
	}
	return fields, nil
}

func loadLabels(ctx context.Context, q queryer, statement string, args ...any) (map[string]string, error) {
	rows, err := q.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	labels := make(map[string]string)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		labels[key] = value
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return labels, nil
}

func projectExists(ctx context.Context, q queryer, projectID string) (bool, error) {
	return rowExists(ctx, q, `SELECT EXISTS(
    SELECT 1 FROM bqemu_projects WHERE project_id = ?)`, projectID)
}

func datasetExists(ctx context.Context, q queryer, projectID, datasetID string) (bool, error) {
	return rowExists(ctx, q, `SELECT EXISTS(
    SELECT 1 FROM bqemu_datasets WHERE project_id = ? AND dataset_id = ?)`, projectID, datasetID)
}

func tableExists(ctx context.Context, q queryer, projectID, datasetID, tableID string) (bool, error) {
	return rowExists(ctx, q, `SELECT EXISTS(
    SELECT 1 FROM bqemu_tables WHERE project_id = ? AND dataset_id = ? AND table_id = ?)`,
		projectID, datasetID, tableID)
}

func rowExists(ctx context.Context, q queryer, statement string, args ...any) (bool, error) {
	var exists int
	if err := q.QueryRowContext(ctx, statement, args...).Scan(&exists); err != nil {
		return false, err
	}
	return exists == 1, nil
}

func repositoryError(ctx context.Context, operation string, err error) error {
	if err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return fmt.Errorf("%w: SQLite catalog %s: %v", domain.ErrBackend, operation, err)
}

func encodeTime(value time.Time) string {
	return value.Round(0).UTC().Format(time.RFC3339Nano)
}

func decodeTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("decode persisted timestamp: %w", err)
	}
	return parsed.UTC(), nil
}

func optionalInt64(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func fromNullInt64(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	result := value.Int64
	return &result
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// Ensure field paths cannot collide with the separator used by the normalized
// hierarchy. Domain validation currently permits only alphanumeric/underscore
// names, so this check protects future extensions from silently corrupting the
// durable representation.
func validateFieldPath(fields []domain.Field) error {
	for _, field := range fields {
		if strings.Contains(field.Name, ".") {
			return fmt.Errorf("%w: field name %q cannot contain a dot", domain.ErrInvalid, field.Name)
		}
		if err := validateFieldPath(field.Fields); err != nil {
			return err
		}
	}
	return nil
}
