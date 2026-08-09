-- +goose Up

CREATE TABLE bqemu_projects (
    project_id TEXT PRIMARY KEY,
    friendly_name TEXT NOT NULL,
    description TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE bqemu_datasets (
    project_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    friendly_name TEXT NOT NULL,
    description TEXT NOT NULL,
    location TEXT NOT NULL,
    labels_present INTEGER NOT NULL CHECK (labels_present IN (0, 1)),
    default_table_expiration_ms INTEGER,
    default_partition_expiration_ms INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    hidden INTEGER NOT NULL CHECK (hidden IN (0, 1)),
    PRIMARY KEY (project_id, dataset_id),
    FOREIGN KEY (project_id) REFERENCES bqemu_projects(project_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE bqemu_dataset_labels (
    project_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    label_key TEXT NOT NULL,
    label_value TEXT NOT NULL,
    PRIMARY KEY (project_id, dataset_id, label_key),
    FOREIGN KEY (project_id, dataset_id)
        REFERENCES bqemu_datasets(project_id, dataset_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE bqemu_tables (
    project_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    table_id TEXT NOT NULL,
    friendly_name TEXT NOT NULL,
    description TEXT NOT NULL,
    labels_present INTEGER NOT NULL CHECK (labels_present IN (0, 1)),
    table_type TEXT NOT NULL,
    location TEXT NOT NULL,
    expiration_time TEXT,
    time_partitioning_present INTEGER NOT NULL CHECK (time_partitioning_present IN (0, 1)),
    time_partitioning_type TEXT,
    time_partitioning_field TEXT,
    time_partitioning_expiration_ms INTEGER,
    range_partitioning_present INTEGER NOT NULL CHECK (range_partitioning_present IN (0, 1)),
    range_partitioning_field TEXT,
    range_start INTEGER,
    range_end INTEGER,
    range_interval INTEGER,
    clustering_present INTEGER NOT NULL CHECK (clustering_present IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (project_id, dataset_id, table_id),
    FOREIGN KEY (project_id, dataset_id)
        REFERENCES bqemu_datasets(project_id, dataset_id) ON DELETE CASCADE,
    CHECK ((expiration_time IS NULL) OR length(expiration_time) > 0),
    CHECK ((time_partitioning_present = 0 AND time_partitioning_type IS NULL
            AND time_partitioning_field IS NULL AND time_partitioning_expiration_ms IS NULL)
        OR (time_partitioning_present = 1 AND time_partitioning_type IS NOT NULL
            AND time_partitioning_field IS NOT NULL AND time_partitioning_expiration_ms IS NOT NULL)),
    CHECK ((range_partitioning_present = 0 AND range_partitioning_field IS NULL
            AND range_start IS NULL AND range_end IS NULL AND range_interval IS NULL)
        OR (range_partitioning_present = 1 AND range_partitioning_field IS NOT NULL
            AND range_start IS NOT NULL AND range_end IS NOT NULL AND range_interval IS NOT NULL))
) STRICT;

CREATE TABLE bqemu_table_labels (
    project_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    table_id TEXT NOT NULL,
    label_key TEXT NOT NULL,
    label_value TEXT NOT NULL,
    PRIMARY KEY (project_id, dataset_id, table_id, label_key),
    FOREIGN KEY (project_id, dataset_id, table_id)
        REFERENCES bqemu_tables(project_id, dataset_id, table_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE bqemu_table_clustering_fields (
    project_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    table_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    field_name TEXT NOT NULL,
    PRIMARY KEY (project_id, dataset_id, table_id, ordinal),
    FOREIGN KEY (project_id, dataset_id, table_id)
        REFERENCES bqemu_tables(project_id, dataset_id, table_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE bqemu_table_fields (
    project_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    table_id TEXT NOT NULL,
    field_path TEXT NOT NULL,
    parent_path TEXT,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    field_name TEXT NOT NULL,
    field_type TEXT NOT NULL,
    field_mode TEXT NOT NULL,
    description TEXT NOT NULL,
    precision INTEGER,
    scale INTEGER,
    rounding_mode TEXT NOT NULL,
    PRIMARY KEY (project_id, dataset_id, table_id, field_path),
    FOREIGN KEY (project_id, dataset_id, table_id)
        REFERENCES bqemu_tables(project_id, dataset_id, table_id) ON DELETE CASCADE,
    FOREIGN KEY (project_id, dataset_id, table_id, parent_path)
        REFERENCES bqemu_table_fields(project_id, dataset_id, table_id, field_path) ON DELETE CASCADE,
    CHECK (length(field_path) > 0),
    CHECK (length(field_name) > 0),
    CHECK (precision IS NULL OR (precision >= 1 AND precision <= 38)),
    CHECK (scale IS NULL OR (scale >= 0 AND scale <= 38)),
    CHECK (rounding_mode IN ('', 'ROUNDING_MODE_UNSPECIFIED',
        'ROUND_HALF_AWAY_FROM_ZERO', 'ROUND_HALF_EVEN'))
) STRICT;

CREATE UNIQUE INDEX bqemu_table_fields_sibling_ordinal
ON bqemu_table_fields (
    project_id, dataset_id, table_id, ifnull(parent_path, ''), ordinal
);

-- +goose StatementBegin
CREATE TRIGGER bqemu_table_fields_parent_insert
BEFORE INSERT ON bqemu_table_fields
WHEN NEW.parent_path IS NOT NULL
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM bqemu_table_fields AS parent
        WHERE parent.project_id = NEW.project_id
          AND parent.dataset_id = NEW.dataset_id
          AND parent.table_id = NEW.table_id
          AND parent.field_path = NEW.parent_path
          AND upper(parent.field_type) IN ('RECORD', 'STRUCT')
    ) THEN RAISE(ABORT, 'nested field requires a RECORD or STRUCT parent') END;
END;
-- +goose StatementEnd
