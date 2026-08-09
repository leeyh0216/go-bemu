-- +goose Up

CREATE TABLE bqemu_write_streams (
    stream_name TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    table_id TEXT NOT NULL,
    stream_type TEXT NOT NULL CHECK (stream_type IN ('DEFAULT', 'PENDING')),
    stream_state TEXT NOT NULL CHECK (stream_state IN ('OPEN', 'FINALIZED', 'COMMITTED')),
    create_time_ns INTEGER NOT NULL,
    commit_time_ns INTEGER,
    location TEXT NOT NULL,
    schema_json TEXT NOT NULL CHECK (json_valid(schema_json)),
    row_count INTEGER NOT NULL CHECK (row_count >= 0),
    next_offset INTEGER NOT NULL CHECK (next_offset = row_count),
    writer_schema_fingerprint TEXT NOT NULL,
    last_activity_ns INTEGER NOT NULL,
    operation_kind TEXT NOT NULL CHECK (operation_kind IN ('NONE', 'APPEND', 'COMMIT')),
    operation_phase TEXT NOT NULL CHECK (operation_phase IN ('NONE', 'PREPARED', 'UNRESOLVED')),
    operation_token TEXT NOT NULL,
    cleanup_phase TEXT NOT NULL CHECK (cleanup_phase IN ('ACTIVE', 'PENDING')),
    cleanup_attempts INTEGER NOT NULL CHECK (cleanup_attempts >= 0),
    revision INTEGER NOT NULL CHECK (revision > 0),
    CHECK (length(stream_name) BETWEEN 1 AND 2304),
    CHECK (length(project_id) > 0 AND length(dataset_id) > 0 AND length(table_id) > 0),
    CHECK (length(location) > 0),
    CHECK (writer_schema_fingerprint = '' OR
        (length(writer_schema_fingerprint) = 71 AND substr(writer_schema_fingerprint, 1, 7) = 'sha256:'
         AND substr(writer_schema_fingerprint, 8) NOT GLOB '*[^0-9a-f]*')),
    CHECK ((operation_kind = 'NONE' AND operation_phase = 'NONE' AND operation_token = '')
        OR (operation_kind <> 'NONE' AND operation_phase IN ('PREPARED', 'UNRESOLVED')
            AND length(operation_token) > 0)),
    CHECK ((stream_type = 'DEFAULT' AND stream_state = 'COMMITTED' AND commit_time_ns IS NOT NULL)
        OR stream_type = 'PENDING'),
    CHECK ((stream_state = 'COMMITTED') = (commit_time_ns IS NOT NULL))
) STRICT;

CREATE TABLE bqemu_write_append_receipts (
    stream_name TEXT NOT NULL,
    start_offset INTEGER NOT NULL CHECK (start_offset >= 0),
    row_count INTEGER NOT NULL CHECK (row_count > 0),
    schema_fingerprint TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    receipt_phase TEXT NOT NULL CHECK (receipt_phase IN ('PREPARED', 'UNRESOLVED', 'APPLIED')),
    created_at_ns INTEGER NOT NULL,
    updated_at_ns INTEGER NOT NULL CHECK (updated_at_ns >= created_at_ns),
    PRIMARY KEY (stream_name, start_offset),
    FOREIGN KEY (stream_name) REFERENCES bqemu_write_streams(stream_name) ON DELETE CASCADE,
    CHECK (length(schema_fingerprint) = 71 AND substr(schema_fingerprint, 1, 7) = 'sha256:'
        AND substr(schema_fingerprint, 8) NOT GLOB '*[^0-9a-f]*'),
    CHECK (length(payload_digest) = 71 AND substr(payload_digest, 1, 7) = 'sha256:'
        AND substr(payload_digest, 8) NOT GLOB '*[^0-9a-f]*')
) STRICT;

CREATE INDEX bqemu_write_receipts_stream
ON bqemu_write_append_receipts (stream_name, start_offset);

CREATE TABLE bqemu_write_commit_groups (
    group_id TEXT PRIMARY KEY,
    parent_reference TEXT NOT NULL,
    member_count INTEGER NOT NULL CHECK (member_count > 0),
    expected_row_count INTEGER NOT NULL CHECK (expected_row_count >= 0),
    commit_phase TEXT NOT NULL CHECK (commit_phase IN ('PREPARED', 'UNRESOLVED', 'APPLIED', 'ABORTED')),
    created_at_ns INTEGER NOT NULL,
    updated_at_ns INTEGER NOT NULL CHECK (updated_at_ns >= created_at_ns),
    commit_time_ns INTEGER,
    CHECK (length(group_id) BETWEEN 1 AND 256),
    CHECK (length(parent_reference) BETWEEN 1 AND 2048),
    CHECK ((commit_phase = 'APPLIED') = (commit_time_ns IS NOT NULL))
) STRICT;

CREATE TABLE bqemu_write_commit_members (
    group_id TEXT NOT NULL,
    member_index INTEGER NOT NULL CHECK (member_index >= 0),
    stream_name TEXT NOT NULL,
    expected_row_count INTEGER NOT NULL CHECK (expected_row_count >= 0),
    PRIMARY KEY (group_id, member_index),
    UNIQUE (group_id, stream_name),
    FOREIGN KEY (group_id) REFERENCES bqemu_write_commit_groups(group_id) ON DELETE CASCADE,
    FOREIGN KEY (stream_name) REFERENCES bqemu_write_streams(stream_name)
) STRICT;

CREATE INDEX bqemu_write_commit_members_stream
ON bqemu_write_commit_members (stream_name, group_id);

CREATE INDEX bqemu_write_streams_cleanup
ON bqemu_write_streams (operation_phase, cleanup_phase, last_activity_ns, stream_name);

-- +goose StatementBegin
CREATE TRIGGER bqemu_write_stream_identity_immutable
BEFORE UPDATE ON bqemu_write_streams
WHEN OLD.stream_name <> NEW.stream_name
    OR OLD.project_id <> NEW.project_id
    OR OLD.dataset_id <> NEW.dataset_id
    OR OLD.table_id <> NEW.table_id
    OR OLD.stream_type <> NEW.stream_type
    OR OLD.create_time_ns <> NEW.create_time_ns
    OR OLD.location <> NEW.location
    OR OLD.schema_json <> NEW.schema_json
BEGIN
    SELECT RAISE(ABORT, 'Storage Write stream identity is immutable');
END;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE TRIGGER bqemu_write_stream_transition
BEFORE UPDATE ON bqemu_write_streams
WHEN NEW.revision <> OLD.revision + 1
    OR (OLD.stream_state <> NEW.stream_state
        AND NOT (OLD.stream_state = 'OPEN' AND NEW.stream_state = 'FINALIZED')
        AND NOT (OLD.stream_state = 'FINALIZED' AND NEW.stream_state = 'COMMITTED'))
    OR (OLD.writer_schema_fingerprint <> NEW.writer_schema_fingerprint
        AND NOT (OLD.writer_schema_fingerprint = '' AND NEW.writer_schema_fingerprint <> ''))
BEGIN
    SELECT RAISE(ABORT, 'Storage Write stream transition is invalid');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bqemu_write_receipt_identity_immutable
BEFORE UPDATE ON bqemu_write_append_receipts
WHEN OLD.stream_name <> NEW.stream_name
    OR OLD.start_offset <> NEW.start_offset
    OR OLD.row_count <> NEW.row_count
    OR OLD.schema_fingerprint <> NEW.schema_fingerprint
    OR OLD.payload_digest <> NEW.payload_digest
    OR OLD.created_at_ns <> NEW.created_at_ns
BEGIN
    SELECT RAISE(ABORT, 'Storage Write receipt identity is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bqemu_write_receipt_transition
BEFORE UPDATE OF receipt_phase ON bqemu_write_append_receipts
WHEN OLD.receipt_phase <> NEW.receipt_phase
    AND NOT (OLD.receipt_phase = 'PREPARED' AND NEW.receipt_phase IN ('UNRESOLVED', 'APPLIED'))
    AND NOT (OLD.receipt_phase = 'UNRESOLVED' AND NEW.receipt_phase = 'APPLIED')
BEGIN
    SELECT RAISE(ABORT, 'Storage Write receipt transition is invalid');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bqemu_write_commit_identity_immutable
BEFORE UPDATE ON bqemu_write_commit_groups
WHEN OLD.group_id <> NEW.group_id
    OR OLD.parent_reference <> NEW.parent_reference
    OR OLD.member_count <> NEW.member_count
    OR OLD.expected_row_count <> NEW.expected_row_count
    OR OLD.created_at_ns <> NEW.created_at_ns
BEGIN
    SELECT RAISE(ABORT, 'Storage Write commit group identity is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bqemu_write_commit_transition
BEFORE UPDATE OF commit_phase ON bqemu_write_commit_groups
WHEN OLD.commit_phase <> NEW.commit_phase
    AND NOT (OLD.commit_phase = 'PREPARED' AND NEW.commit_phase IN ('UNRESOLVED', 'APPLIED', 'ABORTED'))
    AND NOT (OLD.commit_phase = 'UNRESOLVED' AND NEW.commit_phase IN ('APPLIED', 'ABORTED'))
BEGIN
    SELECT RAISE(ABORT, 'Storage Write commit group transition is invalid');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bqemu_write_commit_member_immutable
BEFORE UPDATE ON bqemu_write_commit_members
BEGIN
    SELECT RAISE(ABORT, 'Storage Write commit membership is immutable');
END;
-- +goose StatementEnd
