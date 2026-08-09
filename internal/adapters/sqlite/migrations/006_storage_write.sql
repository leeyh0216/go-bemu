CREATE TABLE storage_write_streams (
    stream_name TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    table_id TEXT NOT NULL,
    stream_type TEXT NOT NULL CHECK (stream_type IN ('DEFAULT', 'PENDING')),
    stream_state TEXT NOT NULL CHECK (stream_state IN ('OPEN', 'FINALIZED', 'COMMITTED', 'FAILED')),
    location TEXT NOT NULL,
    schema_json TEXT NOT NULL,
    table_fingerprint TEXT NOT NULL,
    writer_descriptor BLOB,
    writer_fingerprint TEXT NOT NULL DEFAULT '',
    row_count INTEGER NOT NULL CHECK (row_count >= 0),
    next_offset INTEGER NOT NULL CHECK (next_offset >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    committed_at TEXT,
    failure_code TEXT NOT NULL DEFAULT '',
    failure_digest TEXT NOT NULL DEFAULT '',
    operation_kind TEXT NOT NULL DEFAULT '' CHECK (operation_kind IN ('', 'APPEND', 'COMMIT')),
    operation_token TEXT NOT NULL DEFAULT '',
    cleanup_state TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (cleanup_state IN ('ACTIVE', 'PENDING')),
    cleanup_attempts INTEGER NOT NULL DEFAULT 0 CHECK (cleanup_attempts >= 0),
    revision INTEGER NOT NULL CHECK (revision > 0),
    CHECK (row_count = next_offset),
    CHECK ((stream_state = 'COMMITTED' AND committed_at IS NOT NULL) OR stream_state <> 'COMMITTED'),
    CHECK ((operation_kind = '' AND operation_token = '') OR (operation_kind <> '' AND operation_token <> ''))
);

CREATE INDEX storage_write_streams_pending_idx
    ON storage_write_streams(stream_type, stream_state, cleanup_state, updated_at);

CREATE INDEX storage_write_streams_table_idx
    ON storage_write_streams(project_id, dataset_id, table_id);

CREATE TABLE storage_write_append_receipts (
    stream_name TEXT NOT NULL,
    start_offset INTEGER NOT NULL CHECK (start_offset >= 0),
    row_count INTEGER NOT NULL CHECK (row_count > 0),
    staged_bytes INTEGER NOT NULL CHECK (staged_bytes > 0),
    schema_fingerprint TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    receipt_state TEXT NOT NULL CHECK (receipt_state IN ('PREPARED', 'APPLIED')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (stream_name, start_offset),
    FOREIGN KEY (stream_name) REFERENCES storage_write_streams(stream_name) ON DELETE CASCADE
);

CREATE INDEX storage_write_append_receipts_state_idx
    ON storage_write_append_receipts(receipt_state, updated_at);
