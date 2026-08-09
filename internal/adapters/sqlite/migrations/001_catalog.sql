CREATE TABLE projects (
    project_id TEXT PRIMARY KEY,
    friendly_name TEXT NOT NULL,
    description TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE datasets (
    project_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    friendly_name TEXT NOT NULL,
    description TEXT NOT NULL,
    location TEXT NOT NULL,
    labels_json TEXT NOT NULL CHECK (json_valid(labels_json)),
    default_table_expiration_ms INTEGER,
    default_partition_expiration_ms INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    hidden INTEGER NOT NULL CHECK (hidden IN (0, 1)),
    PRIMARY KEY (project_id, dataset_id),
    FOREIGN KEY (project_id) REFERENCES projects (project_id) ON DELETE CASCADE
);

CREATE TABLE catalog_tables (
    project_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    table_id TEXT NOT NULL,
    friendly_name TEXT NOT NULL,
    description TEXT NOT NULL,
    labels_json TEXT NOT NULL CHECK (json_valid(labels_json)),
    table_type TEXT NOT NULL,
    location TEXT NOT NULL,
    expiration_time TEXT,
    has_time_partitioning INTEGER NOT NULL CHECK (has_time_partitioning IN (0, 1)),
    time_partitioning_type TEXT,
    time_partitioning_field TEXT,
    time_partitioning_expiration_ms INTEGER,
    has_range_partitioning INTEGER NOT NULL CHECK (has_range_partitioning IN (0, 1)),
    range_partitioning_field TEXT,
    range_start INTEGER,
    range_end INTEGER,
    range_interval INTEGER,
    clustering_fields_json TEXT NOT NULL CHECK (json_valid(clustering_fields_json)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (project_id, dataset_id, table_id),
    FOREIGN KEY (project_id, dataset_id)
        REFERENCES datasets (project_id, dataset_id) ON DELETE CASCADE
);

CREATE TABLE table_fields (
    project_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    table_id TEXT NOT NULL,
    field_path TEXT NOT NULL,
    parent_path TEXT,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    name TEXT NOT NULL,
    logical_type TEXT NOT NULL,
    mode TEXT NOT NULL,
    description TEXT NOT NULL,
    precision INTEGER,
    scale INTEGER,
    PRIMARY KEY (project_id, dataset_id, table_id, field_path),
    FOREIGN KEY (project_id, dataset_id, table_id)
        REFERENCES catalog_tables (project_id, dataset_id, table_id) ON DELETE CASCADE,
    CHECK (precision IS NULL OR precision > 0),
    CHECK (scale IS NULL OR scale >= 0)
);

CREATE INDEX table_fields_parent_order
    ON table_fields (project_id, dataset_id, table_id, parent_path, ordinal);
