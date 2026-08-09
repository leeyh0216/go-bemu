CREATE TABLE tabledata_insert_ids (
    project_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    table_id TEXT NOT NULL,
    insert_id TEXT NOT NULL,
    created_at_ns INTEGER NOT NULL,
    PRIMARY KEY (project_id, dataset_id, table_id, insert_id),
    FOREIGN KEY (project_id, dataset_id, table_id)
        REFERENCES catalog_tables (project_id, dataset_id, table_id) ON DELETE CASCADE,
    CHECK (length(insert_id) BETWEEN 1 AND 1024)
);

CREATE INDEX tabledata_insert_ids_bound
    ON tabledata_insert_ids (project_id, dataset_id, table_id, created_at_ns DESC);
