-- +goose Up

CREATE TABLE bqemu_load_mutations (
    mutation_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    location_key TEXT NOT NULL,
    location TEXT NOT NULL,
    job_id TEXT NOT NULL,
    configuration_digest TEXT NOT NULL,
    plan_fingerprint TEXT NOT NULL,
    destination_json TEXT NOT NULL CHECK (json_valid(destination_json)),
    before_schema_json TEXT NOT NULL CHECK (json_valid(before_schema_json)),
    publication TEXT NOT NULL CHECK (publication IN ('NONE', 'CREATE', 'SCHEMA_UPDATE')),
    phase TEXT NOT NULL CHECK (phase IN ('PREPARED', 'PHYSICAL', 'APPLIED', 'ABORTED')),
    input_files INTEGER NOT NULL CHECK (input_files >= 0),
    input_bytes INTEGER NOT NULL CHECK (input_bytes >= 0),
    output_rows INTEGER,
    created_destination INTEGER CHECK (created_destination IN (0, 1)),
    updated_destination INTEGER CHECK (updated_destination IN (0, 1)),
    revision INTEGER NOT NULL CHECK (revision > 0),
    FOREIGN KEY (project_id, location_key, job_id)
        REFERENCES bqemu_load_jobs(project_id, location_key, job_id) ON DELETE RESTRICT,
    CHECK (length(mutation_id) = 64 AND mutation_id NOT GLOB '*[^0-9a-f]*'),
    CHECK (length(configuration_digest) = 64 AND configuration_digest NOT GLOB '*[^0-9a-f]*'),
    CHECK (length(plan_fingerprint) = 64 AND plan_fingerprint NOT GLOB '*[^0-9a-f]*'),
    CHECK ((phase IN ('PREPARED', 'ABORTED') AND output_rows IS NULL
            AND created_destination IS NULL AND updated_destination IS NULL)
        OR (phase IN ('PHYSICAL', 'APPLIED') AND output_rows IS NOT NULL
            AND created_destination IS NOT NULL AND updated_destination IS NOT NULL))
) STRICT;

CREATE INDEX bqemu_load_mutations_recovery
ON bqemu_load_mutations (phase, mutation_id);

-- +goose StatementBegin
CREATE TRIGGER bqemu_load_mutation_identity_immutable
BEFORE UPDATE ON bqemu_load_mutations
WHEN OLD.mutation_id <> NEW.mutation_id
    OR OLD.project_id <> NEW.project_id
    OR OLD.location_key <> NEW.location_key
    OR OLD.location <> NEW.location
    OR OLD.job_id <> NEW.job_id
    OR OLD.configuration_digest <> NEW.configuration_digest
    OR OLD.plan_fingerprint <> NEW.plan_fingerprint
    OR OLD.destination_json <> NEW.destination_json
    OR OLD.before_schema_json <> NEW.before_schema_json
    OR OLD.publication <> NEW.publication
    OR OLD.input_files <> NEW.input_files
    OR OLD.input_bytes <> NEW.input_bytes
BEGIN
    SELECT RAISE(ABORT, 'load mutation identity is immutable');
END;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE TRIGGER bqemu_load_mutation_transition
BEFORE UPDATE ON bqemu_load_mutations
WHEN NEW.revision <> OLD.revision + 1
    OR NOT ((OLD.phase = 'PREPARED' AND NEW.phase IN ('PHYSICAL', 'ABORTED'))
        OR (OLD.phase = 'PHYSICAL' AND NEW.phase IN ('APPLIED', 'ABORTED')))
BEGIN
    SELECT RAISE(ABORT, 'load mutation lifecycle transition is invalid');
END;
-- +goose StatementEnd
