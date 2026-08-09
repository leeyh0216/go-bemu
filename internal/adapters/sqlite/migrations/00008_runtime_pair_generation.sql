-- +goose Up
CREATE TABLE IF NOT EXISTS bqemu_runtime_pair_generation (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    generation TEXT NOT NULL,
    created_at TEXT NOT NULL
);

-- +goose Down
DROP TABLE bqemu_runtime_pair_generation;
