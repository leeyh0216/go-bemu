-- name: CreateLoadMutation :exec
INSERT INTO bqemu_load_mutations (
    mutation_id, project_id, location_key, location, job_id, configuration_digest,
    plan_fingerprint, destination_json, before_schema_json, publication, phase,
    input_files, input_bytes, output_rows, created_destination, updated_destination, revision
) VALUES (
    sqlc.arg(mutation_id), sqlc.arg(project_id), sqlc.arg(location_key), sqlc.arg(location),
    sqlc.arg(job_id), sqlc.arg(configuration_digest), sqlc.arg(plan_fingerprint),
    sqlc.arg(destination_json), sqlc.arg(before_schema_json), sqlc.arg(publication), 'PREPARED',
    sqlc.arg(input_files), sqlc.arg(input_bytes), NULL, NULL, NULL, 1
);

-- name: GetLoadMutation :one
SELECT mutation_id, project_id, location, job_id,
    configuration_digest, plan_fingerprint, destination_json, before_schema_json,
    publication, phase, input_files, input_bytes, output_rows,
    created_destination, updated_destination
FROM bqemu_load_mutations
WHERE mutation_id = sqlc.arg(mutation_id);

-- name: MarkLoadMutationPhysical :execrows
UPDATE bqemu_load_mutations
SET phase = 'PHYSICAL', output_rows = sqlc.arg(output_rows),
    created_destination = sqlc.arg(created_destination),
    updated_destination = sqlc.arg(updated_destination), revision = revision + 1
WHERE mutation_id = sqlc.arg(mutation_id)
  AND plan_fingerprint = sqlc.arg(plan_fingerprint)
  AND phase = 'PREPARED';

-- name: MarkLoadMutationApplied :execrows
UPDATE bqemu_load_mutations
SET phase = 'APPLIED', revision = revision + 1
WHERE mutation_id = sqlc.arg(mutation_id) AND phase = 'PHYSICAL';

-- name: MarkLoadMutationAborted :execrows
UPDATE bqemu_load_mutations
SET phase = 'ABORTED', output_rows = NULL, created_destination = NULL,
    updated_destination = NULL, revision = revision + 1
WHERE mutation_id = sqlc.arg(mutation_id) AND phase IN ('PREPARED', 'PHYSICAL');

-- name: ListRecoverableLoadMutations :many
SELECT mutation_id, project_id, location, job_id,
    configuration_digest, plan_fingerprint, destination_json, before_schema_json,
    publication, phase, input_files, input_bytes, output_rows,
    created_destination, updated_destination
FROM bqemu_load_mutations
WHERE phase IN ('PREPARED', 'PHYSICAL') OR (phase = 'APPLIED' AND EXISTS (
    SELECT 1 FROM bqemu_load_jobs AS job
    WHERE job.project_id = bqemu_load_mutations.project_id
      AND job.location_key = bqemu_load_mutations.location_key
      AND job.job_id = bqemu_load_mutations.job_id
      AND job.state <> 'DONE'
))
ORDER BY mutation_id;
