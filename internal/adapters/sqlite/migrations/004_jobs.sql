CREATE TABLE job_identities (
    project_id TEXT NOT NULL,
    location_key TEXT NOT NULL,
    location TEXT NOT NULL,
    job_id TEXT NOT NULL,
    job_kind TEXT NOT NULL CHECK (job_kind IN ('QUERY', 'LOAD')),
    configuration_digest TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'RUNNING', 'DONE')),
    error_reason TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    ended_at TEXT,
    PRIMARY KEY (project_id, location_key, job_id),
    FOREIGN KEY (project_id) REFERENCES projects (project_id) ON DELETE CASCADE,
    CHECK (location_key = upper(location)),
    CHECK (length(location) BETWEEN 1 AND 1024),
    CHECK (location NOT GLOB '*[^A-Za-z0-9-]*'),
    CHECK (length(job_id) BETWEEN 1 AND 1024),
    CHECK (job_id NOT GLOB '*[^A-Za-z0-9_-]*'),
    CHECK (
        length(configuration_digest) = 64
        AND configuration_digest NOT GLOB '*[^0-9a-f]*'
    ),
    CHECK ((error_reason IS NULL) = (error_message IS NULL)),
    CHECK (
        (state = 'PENDING' AND started_at IS NULL AND ended_at IS NULL AND error_reason IS NULL)
        OR
        (state = 'RUNNING' AND started_at IS NOT NULL AND ended_at IS NULL AND error_reason IS NULL)
        OR
        (state = 'DONE' AND started_at IS NOT NULL AND ended_at IS NOT NULL)
    )
);

CREATE TABLE query_job_details (
    project_id TEXT NOT NULL,
    location_key TEXT NOT NULL,
    job_id TEXT NOT NULL,
    configuration_json TEXT NOT NULL CHECK (json_valid(configuration_json)),
    has_result INTEGER NOT NULL CHECK (has_result IN (0, 1)),
    result_columns_json TEXT NOT NULL CHECK (json_valid(result_columns_json)),
    affected_rows INTEGER NOT NULL CHECK (affected_rows >= 0),
    total_rows INTEGER NOT NULL CHECK (total_rows >= 0),
    PRIMARY KEY (project_id, location_key, job_id),
    FOREIGN KEY (project_id, location_key, job_id)
        REFERENCES job_identities (project_id, location_key, job_id) ON DELETE CASCADE,
    CHECK (
        has_result = 1
        OR (result_columns_json = '[]' AND affected_rows = 0 AND total_rows = 0)
    )
);

CREATE TABLE load_job_details (
    project_id TEXT NOT NULL,
    location_key TEXT NOT NULL,
    job_id TEXT NOT NULL,
    configuration_json TEXT NOT NULL CHECK (json_valid(configuration_json)),
    input_files INTEGER NOT NULL CHECK (input_files >= 0),
    input_bytes INTEGER NOT NULL CHECK (input_bytes >= 0),
    output_bytes INTEGER NOT NULL CHECK (output_bytes >= 0),
    output_rows INTEGER NOT NULL CHECK (output_rows >= 0),
    PRIMARY KEY (project_id, location_key, job_id),
    FOREIGN KEY (project_id, location_key, job_id)
        REFERENCES job_identities (project_id, location_key, job_id) ON DELETE CASCADE
);

CREATE INDEX job_identities_list_order
    ON job_identities (project_id, job_kind, created_at DESC, job_id, location_key);

CREATE TRIGGER job_identity_immutable
BEFORE UPDATE ON job_identities
WHEN OLD.project_id <> NEW.project_id
    OR OLD.location_key <> NEW.location_key
    OR OLD.location <> NEW.location
    OR OLD.job_id <> NEW.job_id
    OR OLD.job_kind <> NEW.job_kind
    OR OLD.configuration_digest <> NEW.configuration_digest
    OR OLD.created_at <> NEW.created_at
BEGIN
    SELECT RAISE(ABORT, 'job identity and configuration are immutable');
END;

CREATE TRIGGER job_identity_state_transition
BEFORE UPDATE OF state ON job_identities
WHEN OLD.state <> NEW.state
    AND NOT (
        (OLD.state = 'PENDING' AND NEW.state IN ('RUNNING', 'DONE'))
        OR (OLD.state = 'RUNNING' AND NEW.state = 'DONE')
    )
BEGIN
    SELECT RAISE(ABORT, 'job state transition is invalid');
END;

CREATE TRIGGER query_job_configuration_immutable
BEFORE UPDATE OF configuration_json ON query_job_details
WHEN OLD.configuration_json <> NEW.configuration_json
BEGIN
    SELECT RAISE(ABORT, 'query job configuration is immutable');
END;

CREATE TRIGGER load_job_configuration_immutable
BEFORE UPDATE OF configuration_json ON load_job_details
WHEN OLD.configuration_json <> NEW.configuration_json
BEGIN
    SELECT RAISE(ABORT, 'load job configuration is immutable');
END;
