-- name: CreateLoadJob :execrows
INSERT INTO bqemu_load_jobs (
    project_id, location_key, location, job_id, configuration_version,
    configuration_json, configuration_digest, state, error_reason, error_message,
    created_at, started_at, ended_at, input_files, input_bytes, output_rows, output_bytes
) VALUES (
    sqlc.arg(project_id), sqlc.arg(location_key), sqlc.arg(location), sqlc.arg(job_id),
    sqlc.arg(configuration_version), sqlc.arg(configuration_json), sqlc.arg(configuration_digest),
    sqlc.arg(state), sqlc.arg(error_reason), sqlc.arg(error_message),
    sqlc.arg(created_at), sqlc.arg(started_at), sqlc.arg(ended_at),
    sqlc.arg(input_files), sqlc.arg(input_bytes), sqlc.arg(output_rows), sqlc.arg(output_bytes)
)
ON CONFLICT (project_id, location_key, job_id) DO NOTHING;

-- name: UpdateLoadJob :execrows
UPDATE bqemu_load_jobs SET
    location = sqlc.arg(location), configuration_version = sqlc.arg(configuration_version),
    configuration_json = sqlc.arg(configuration_json),
    configuration_digest = sqlc.arg(configuration_digest), state = sqlc.arg(state),
    error_reason = sqlc.arg(error_reason), error_message = sqlc.arg(error_message),
    created_at = sqlc.arg(created_at), started_at = sqlc.arg(started_at),
    ended_at = sqlc.arg(ended_at), input_files = sqlc.arg(input_files),
    input_bytes = sqlc.arg(input_bytes), output_rows = sqlc.arg(output_rows),
    output_bytes = sqlc.arg(output_bytes)
WHERE project_id = sqlc.arg(project_id)
  AND location_key = sqlc.arg(location_key)
  AND job_id = sqlc.arg(job_id);

-- name: GetLoadJob :one
SELECT project_id, location, job_id, configuration_version,
    configuration_json, configuration_digest, state, error_reason, error_message,
    created_at, started_at, ended_at, input_files, input_bytes, output_rows, output_bytes
FROM bqemu_load_jobs
WHERE project_id = sqlc.arg(project_id)
  AND location_key = sqlc.arg(location_key)
  AND job_id = sqlc.arg(job_id);

-- name: ListLoadJobs :many
SELECT project_id, location, job_id, configuration_version,
    configuration_json, configuration_digest, state, error_reason, error_message,
    created_at, started_at, ended_at, input_files, input_bytes, output_rows, output_bytes
FROM bqemu_load_jobs
WHERE project_id = sqlc.arg(project_id)
ORDER BY created_at, job_id, location_key;

-- name: ListLoadJobsAtLocation :many
SELECT project_id, location, job_id, configuration_version,
    configuration_json, configuration_digest, state, error_reason, error_message,
    created_at, started_at, ended_at, input_files, input_bytes, output_rows, output_bytes
FROM bqemu_load_jobs
WHERE project_id = sqlc.arg(project_id)
  AND location_key = sqlc.arg(location_key)
ORDER BY created_at, job_id;

-- name: ListInterruptedLoadJobs :many
SELECT project_id, location, job_id, configuration_version,
    configuration_json, configuration_digest, state, error_reason, error_message,
    created_at, started_at, ended_at, input_files, input_bytes, output_rows, output_bytes
FROM bqemu_load_jobs
WHERE state IN ('PENDING', 'RUNNING')
ORDER BY created_at, project_id, location_key, job_id;
