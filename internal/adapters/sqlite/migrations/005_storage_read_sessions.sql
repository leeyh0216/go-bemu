CREATE TABLE storage_read_sessions (
    session_name TEXT PRIMARY KEY,
    table_reference TEXT NOT NULL,
    data_format TEXT NOT NULL CHECK (data_format IN ('ARROW', 'AVRO')),
    selected_fields_json TEXT NOT NULL
        CHECK (json_valid(selected_fields_json) AND json_type(selected_fields_json) = 'array'),
    row_restriction_digest TEXT NOT NULL,
    row_restriction_bytes INTEGER NOT NULL CHECK (row_restriction_bytes >= 0),
    filter_predicate_count INTEGER NOT NULL CHECK (filter_predicate_count >= 0),
    filter_logical_operator_count INTEGER NOT NULL CHECK (filter_logical_operator_count >= 0),
    stream_count INTEGER NOT NULL CHECK (stream_count > 0),
    created_at_ns INTEGER NOT NULL,
    expires_at_ns INTEGER NOT NULL CHECK (expires_at_ns > created_at_ns),
    snapshot_time_ns INTEGER,
    retained_row_count INTEGER NOT NULL CHECK (retained_row_count >= 0),
    retained_bytes INTEGER NOT NULL CHECK (retained_bytes >= 0),
    estimated_bytes_scanned INTEGER NOT NULL CHECK (estimated_bytes_scanned >= 0),
    schema_fingerprint TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL CHECK (lifecycle_state IN ('ACTIVE', 'EXPIRED', 'UNAVAILABLE')),
    lifecycle_updated_at_ns INTEGER NOT NULL CHECK (lifecycle_updated_at_ns >= created_at_ns),
    CHECK (length(session_name) BETWEEN 1 AND 2048),
    CHECK (length(table_reference) BETWEEN 1 AND 2048),
    CHECK (
        length(row_restriction_digest) = 71
        AND substr(row_restriction_digest, 1, 7) = 'sha256:'
        AND substr(row_restriction_digest, 8) NOT GLOB '*[^0-9a-f]*'
    ),
    CHECK (
        length(schema_fingerprint) = 71
        AND substr(schema_fingerprint, 1, 7) = 'sha256:'
        AND substr(schema_fingerprint, 8) NOT GLOB '*[^0-9a-f]*'
    )
);

CREATE TABLE storage_read_streams (
    stream_name TEXT PRIMARY KEY,
    session_name TEXT NOT NULL,
    stream_index INTEGER NOT NULL CHECK (stream_index >= 0),
    start_offset INTEGER NOT NULL CHECK (start_offset >= 0),
    end_offset INTEGER NOT NULL CHECK (end_offset >= start_offset),
    FOREIGN KEY (session_name) REFERENCES storage_read_sessions (session_name) ON DELETE CASCADE,
    UNIQUE (session_name, stream_index),
    CHECK (length(stream_name) BETWEEN 1 AND 2304)
);

CREATE INDEX storage_read_sessions_lifecycle
    ON storage_read_sessions (lifecycle_state, expires_at_ns, session_name);
CREATE INDEX storage_read_streams_session
    ON storage_read_streams (session_name, stream_index);

CREATE TRIGGER storage_read_session_identity_immutable
BEFORE UPDATE ON storage_read_sessions
WHEN OLD.session_name <> NEW.session_name
    OR OLD.table_reference <> NEW.table_reference
    OR OLD.data_format <> NEW.data_format
    OR OLD.selected_fields_json <> NEW.selected_fields_json
    OR OLD.row_restriction_digest <> NEW.row_restriction_digest
    OR OLD.row_restriction_bytes <> NEW.row_restriction_bytes
    OR OLD.filter_predicate_count <> NEW.filter_predicate_count
    OR OLD.filter_logical_operator_count <> NEW.filter_logical_operator_count
    OR OLD.stream_count <> NEW.stream_count
    OR OLD.created_at_ns <> NEW.created_at_ns
    OR OLD.expires_at_ns <> NEW.expires_at_ns
    OR OLD.snapshot_time_ns IS NOT NEW.snapshot_time_ns
    OR OLD.retained_row_count <> NEW.retained_row_count
    OR OLD.retained_bytes <> NEW.retained_bytes
    OR OLD.estimated_bytes_scanned <> NEW.estimated_bytes_scanned
    OR OLD.schema_fingerprint <> NEW.schema_fingerprint
BEGIN
    SELECT RAISE(ABORT, 'Storage Read session identity and structural metadata are immutable');
END;

CREATE TRIGGER storage_read_session_lifecycle_transition
BEFORE UPDATE OF lifecycle_state ON storage_read_sessions
WHEN OLD.lifecycle_state <> NEW.lifecycle_state
    AND NOT (
        OLD.lifecycle_state = 'ACTIVE'
        AND NEW.lifecycle_state IN ('EXPIRED', 'UNAVAILABLE')
    )
BEGIN
    SELECT RAISE(ABORT, 'Storage Read session lifecycle transition is invalid');
END;

CREATE TRIGGER storage_read_stream_immutable
BEFORE UPDATE ON storage_read_streams
BEGIN
    SELECT RAISE(ABORT, 'Storage Read stream metadata is immutable');
END;
