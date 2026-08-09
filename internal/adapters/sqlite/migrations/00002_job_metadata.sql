-- +goose Up

CREATE TABLE bqemu_query_jobs (
    project_id TEXT NOT NULL,
    location_key TEXT NOT NULL,
    location TEXT NOT NULL,
    job_id TEXT NOT NULL,
    configuration_version INTEGER NOT NULL CHECK (configuration_version = 1),
    configuration_json TEXT NOT NULL CHECK (json_valid(configuration_json)),
    configuration_digest TEXT NOT NULL CHECK (length(configuration_digest) = 64),
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'RUNNING', 'DONE')),
    error_reason TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    ended_at TEXT,
    result_present INTEGER NOT NULL CHECK (result_present IN (0, 1)),
    result_schema_json TEXT,
    result_row_count INTEGER NOT NULL CHECK (result_row_count >= 0),
    affected_rows INTEGER NOT NULL,
    PRIMARY KEY (project_id, location_key, job_id),
    CHECK (length(location_key) > 0 AND length(location) > 0 AND length(job_id) > 0),
    CHECK ((error_reason IS NULL) = (error_message IS NULL)),
    CHECK (result_schema_json IS NULL OR json_valid(result_schema_json)),
    CHECK ((result_present = 0 AND result_schema_json IS NULL
            AND result_row_count = 0 AND affected_rows = 0)
        OR (result_present = 1 AND result_schema_json IS NOT NULL)),
    CHECK ((state = 'PENDING' AND started_at IS NULL AND ended_at IS NULL)
        OR (state = 'RUNNING' AND started_at IS NOT NULL AND ended_at IS NULL)
        OR (state = 'DONE' AND ended_at IS NOT NULL))
) STRICT;

CREATE INDEX bqemu_query_jobs_list
ON bqemu_query_jobs (project_id, location_key, created_at DESC, job_id);

CREATE TABLE bqemu_load_jobs (
    project_id TEXT NOT NULL,
    location_key TEXT NOT NULL,
    location TEXT NOT NULL,
    job_id TEXT NOT NULL,
    configuration_version INTEGER NOT NULL CHECK (configuration_version = 1),
    configuration_json TEXT NOT NULL CHECK (json_valid(configuration_json)),
    configuration_digest TEXT NOT NULL CHECK (length(configuration_digest) = 64),
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'RUNNING', 'DONE')),
    error_reason TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    ended_at TEXT,
    input_files INTEGER NOT NULL CHECK (input_files >= 0),
    input_bytes INTEGER NOT NULL CHECK (input_bytes >= 0),
    output_rows INTEGER NOT NULL CHECK (output_rows >= 0),
    output_bytes INTEGER NOT NULL CHECK (output_bytes >= 0),
    PRIMARY KEY (project_id, location_key, job_id),
    CHECK (length(location_key) > 0 AND length(location) > 0 AND length(job_id) > 0),
    CHECK ((error_reason IS NULL) = (error_message IS NULL)),
    CHECK ((state = 'PENDING' AND started_at IS NULL AND ended_at IS NULL)
        OR (state = 'RUNNING' AND started_at IS NOT NULL AND ended_at IS NULL)
        OR (state = 'DONE' AND ended_at IS NOT NULL))
) STRICT;

CREATE INDEX bqemu_load_jobs_list
ON bqemu_load_jobs (project_id, location_key, created_at DESC, job_id);
