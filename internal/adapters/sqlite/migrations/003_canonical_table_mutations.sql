-- Version 2 was a generic fingerprint-only journal and was never wired to a
-- physical mutation. Refuse to invent recovery payloads if a development
-- database nevertheless contains such records.
CREATE TABLE mutation_v3_empty_guard (
    record_count INTEGER NOT NULL CHECK (record_count = 0)
);
INSERT INTO mutation_v3_empty_guard (record_count)
SELECT count(*) FROM mutation_journal;
DROP TABLE mutation_v3_empty_guard;

ALTER TABLE mutation_journal
    ADD COLUMN canonical_before_json TEXT NOT NULL DEFAULT '{}'
    CHECK (json_valid(canonical_before_json));
ALTER TABLE mutation_journal
    ADD COLUMN canonical_after_json TEXT NOT NULL DEFAULT '{}'
    CHECK (json_valid(canonical_after_json));

CREATE TRIGGER mutation_journal_immutable_canonical_intent
BEFORE UPDATE ON mutation_journal
WHEN OLD.canonical_before_json <> NEW.canonical_before_json
    OR OLD.canonical_after_json <> NEW.canonical_after_json
BEGIN
    SELECT RAISE(ABORT, 'mutation canonical intent is immutable');
END;
