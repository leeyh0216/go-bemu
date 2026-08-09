CREATE TABLE mutation_journal (
    mutation_id TEXT PRIMARY KEY,
    resource_key TEXT NOT NULL,
    mutation_kind TEXT NOT NULL,
    expected_canonical_revision INTEGER NOT NULL
        CHECK (expected_canonical_revision >= 0),
    before_physical_fingerprint TEXT NOT NULL,
    after_physical_fingerprint TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('PREPARED', 'APPLIED', 'FAILED')),
    failure_code TEXT NOT NULL DEFAULT '',
    failure_digest TEXT NOT NULL DEFAULT '',
    prepared_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    CHECK (length(mutation_id) BETWEEN 1 AND 256),
    CHECK (length(resource_key) BETWEEN 1 AND 4096),
    CHECK (length(mutation_kind) BETWEEN 1 AND 128),
    CHECK (length(failure_code) <= 128),
    CHECK (mutation_kind NOT GLOB '*[^a-z0-9._-]*'),
    CHECK (failure_code = '' OR failure_code NOT GLOB '*[^a-z0-9._-]*'),
    CHECK (
        (before_physical_fingerprint = '')
        OR (
            length(before_physical_fingerprint) = 71
            AND substr(before_physical_fingerprint, 1, 7) = 'sha256:'
            AND substr(before_physical_fingerprint, 8) NOT GLOB '*[^0-9a-f]*'
        )
    ),
    CHECK (
        (after_physical_fingerprint = '')
        OR (
            length(after_physical_fingerprint) = 71
            AND substr(after_physical_fingerprint, 1, 7) = 'sha256:'
            AND substr(after_physical_fingerprint, 8) NOT GLOB '*[^0-9a-f]*'
        )
    ),
    CHECK (before_physical_fingerprint <> '' OR after_physical_fingerprint <> ''),
    CHECK (
        (failure_digest = '')
        OR (
            length(failure_digest) = 71
            AND substr(failure_digest, 1, 7) = 'sha256:'
            AND substr(failure_digest, 8) NOT GLOB '*[^0-9a-f]*'
        )
    ),
    CHECK (
        (state = 'PREPARED' AND failure_code = '' AND failure_digest = '' AND completed_at IS NULL)
        OR
        (state = 'APPLIED' AND failure_code = '' AND failure_digest = '' AND completed_at IS NOT NULL)
        OR
        (state = 'FAILED' AND failure_code <> '' AND failure_digest <> '' AND completed_at IS NOT NULL)
    )
);

CREATE INDEX mutation_journal_pending_order
    ON mutation_journal (state, prepared_at, mutation_id);

CREATE TRIGGER mutation_journal_immutable_intent
BEFORE UPDATE ON mutation_journal
WHEN OLD.mutation_id <> NEW.mutation_id
    OR OLD.resource_key <> NEW.resource_key
    OR OLD.mutation_kind <> NEW.mutation_kind
    OR OLD.expected_canonical_revision <> NEW.expected_canonical_revision
    OR OLD.before_physical_fingerprint <> NEW.before_physical_fingerprint
    OR OLD.after_physical_fingerprint <> NEW.after_physical_fingerprint
    OR OLD.prepared_at <> NEW.prepared_at
BEGIN
    SELECT RAISE(ABORT, 'mutation intent is immutable');
END;

CREATE TRIGGER mutation_journal_terminal_state
BEFORE UPDATE OF state ON mutation_journal
WHEN OLD.state <> NEW.state
    AND NOT (OLD.state = 'PREPARED' AND NEW.state IN ('APPLIED', 'FAILED'))
BEGIN
    SELECT RAISE(ABORT, 'mutation state transition is invalid');
END;

CREATE TRIGGER mutation_journal_terminal_immutable
BEFORE UPDATE ON mutation_journal
WHEN OLD.state IN ('APPLIED', 'FAILED')
BEGIN
    SELECT RAISE(ABORT, 'terminal mutation is immutable');
END;

CREATE TRIGGER mutation_journal_delete_forbidden
BEFORE DELETE ON mutation_journal
BEGIN
    SELECT RAISE(ABORT, 'mutation journal records cannot be deleted');
END;
