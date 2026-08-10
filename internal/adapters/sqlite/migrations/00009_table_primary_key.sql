-- +goose Up

CREATE TABLE bqemu_table_primary_key_columns (
    project_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    table_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    field_name TEXT NOT NULL,
    PRIMARY KEY (project_id, dataset_id, table_id, ordinal),
    FOREIGN KEY (project_id, dataset_id, table_id)
        REFERENCES bqemu_tables(project_id, dataset_id, table_id) ON DELETE CASCADE
) STRICT;
