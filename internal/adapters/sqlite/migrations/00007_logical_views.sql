-- +goose Up

CREATE TABLE IF NOT EXISTS bqemu_views (
    project_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    view_id TEXT NOT NULL,
    friendly_name TEXT NOT NULL,
    description TEXT NOT NULL,
    labels_json TEXT NOT NULL,
    query_sql TEXT NOT NULL,
    use_legacy_sql INTEGER NOT NULL CHECK (use_legacy_sql IN (0, 1)),
    schema_json TEXT NOT NULL,
    dependencies_json TEXT NOT NULL,
    analysis_fingerprint TEXT NOT NULL,
    location TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (project_id, dataset_id, view_id),
    FOREIGN KEY (project_id, dataset_id)
        REFERENCES bqemu_datasets(project_id, dataset_id) ON DELETE CASCADE,
    CHECK (length(query_sql) > 0),
    CHECK (length(analysis_fingerprint) = 64)
) STRICT;
