-- name: CreateQueryJob :execrows
INSERT INTO bqemu_query_jobs (
    project_id, location_key, location, job_id, configuration_version,
    configuration_json, configuration_digest, state, error_reason, error_message,
    created_at, started_at, ended_at, result_present, result_schema_json,
    result_row_count, affected_rows
) VALUES (
    sqlc.arg(project_id), sqlc.arg(location_key), sqlc.arg(location), sqlc.arg(job_id),
    sqlc.arg(configuration_version), sqlc.arg(configuration_json), sqlc.arg(configuration_digest),
    sqlc.arg(state), sqlc.arg(error_reason), sqlc.arg(error_message),
    sqlc.arg(created_at), sqlc.arg(started_at), sqlc.arg(ended_at),
    sqlc.arg(result_present), sqlc.arg(result_schema_json),
    sqlc.arg(result_row_count), sqlc.arg(affected_rows)
)
ON CONFLICT (project_id, location_key, job_id) DO NOTHING;

-- name: UpdateQueryJob :execrows
UPDATE bqemu_query_jobs SET
    location = sqlc.arg(location), configuration_version = sqlc.arg(configuration_version),
    configuration_json = sqlc.arg(configuration_json),
    configuration_digest = sqlc.arg(configuration_digest), state = sqlc.arg(state),
    error_reason = sqlc.arg(error_reason), error_message = sqlc.arg(error_message),
    created_at = sqlc.arg(created_at), started_at = sqlc.arg(started_at),
    ended_at = sqlc.arg(ended_at), result_present = sqlc.arg(result_present),
    result_schema_json = sqlc.arg(result_schema_json), result_row_count = sqlc.arg(result_row_count),
    affected_rows = sqlc.arg(affected_rows)
WHERE project_id = sqlc.arg(project_id)
  AND location_key = sqlc.arg(location_key)
  AND job_id = sqlc.arg(job_id);

-- name: GetQueryJob :one
SELECT project_id, location, job_id, configuration_version,
    configuration_json, configuration_digest, state, error_reason, error_message,
    created_at, started_at, ended_at, result_present, result_schema_json,
    result_row_count, affected_rows
FROM bqemu_query_jobs
WHERE project_id = sqlc.arg(project_id)
  AND location_key = sqlc.arg(location_key)
  AND job_id = sqlc.arg(job_id);

-- name: ListQueryJobs :many
SELECT project_id, location, job_id, configuration_version,
    configuration_json, configuration_digest, state, error_reason, error_message,
    created_at, started_at, ended_at, result_present, result_schema_json,
    result_row_count, affected_rows
FROM bqemu_query_jobs
WHERE project_id = sqlc.arg(project_id);

-- name: ListQueryJobsAtLocation :many
SELECT project_id, location, job_id, configuration_version,
    configuration_json, configuration_digest, state, error_reason, error_message,
    created_at, started_at, ended_at, result_present, result_schema_json,
    result_row_count, affected_rows
FROM bqemu_query_jobs
WHERE project_id = sqlc.arg(project_id)
  AND location_key = sqlc.arg(location_key);

-- name: ReconcileInterruptedQueryJobs :execrows
UPDATE bqemu_query_jobs
SET state = 'DONE', error_reason = 'stopped', error_message = sqlc.arg(error_message),
    ended_at = sqlc.arg(ended_at)
WHERE state IN ('PENDING', 'RUNNING');
