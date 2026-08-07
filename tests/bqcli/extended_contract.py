"""Extended public-process contract for bq CLI 2.1.31.

Official pinned CLI and REST sources:
  https://cloud.google.com/sdk/docs/release-notes#56600_2026-04-28
  https://cloud.google.com/bigquery/docs/reference/bq-cli-reference#bq_query
  https://cloud.google.com/bigquery/docs/reference/bq-cli-reference#bq_show
  https://cloud.google.com/bigquery/docs/reference/bq-cli-reference#bq_ls
  https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert
  https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/get
  https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults
  https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/list

The parent runner owns bounded subprocess execution and output fingerprinting.
This module never emits the access token, SQL text, raw output, or page tokens.
"""

from __future__ import annotations

from collections.abc import Callable
import json
from pathlib import Path
import subprocess
import tempfile
from typing import Any


BQCLI_LIFECYCLE_CONTRACT = "BQCLI-QUERY-LIFECYCLE-001"
RunProcess = Callable[..., subprocess.CompletedProcess[str]]
DecodeJSON = Callable[[subprocess.CompletedProcess[str], str], Any]
Require = Callable[[bool, str, str, str], None]


def run_extended_contract(
    *,
    base: list[str],
    project: str,
    dataset_ref: str,
    table: str,
    existing_schema_fields: list[dict[str, Any]],
    location: str,
    suffix: str,
    run_process: RunProcess,
    decode_json: DecodeJSON,
    require: Require,
) -> None:
    """Exercise metadata patches and the complete query-job lifecycle."""

    run_process(
        base
        + [
            "update",
            "--dataset",
            "--description=Patched CLI dataset",
            "--set_label=owner:contracts",
            dataset_ref,
        ],
        "patch_dataset_metadata",
    )
    patched_dataset = decode_json(
        run_process(base + ["show", dataset_ref], "get_patched_dataset"),
        "get_patched_dataset",
    )
    require(
        patched_dataset.get("description") == "Patched CLI dataset"
        and patched_dataset.get("labels") == {"owner": "contracts"},
        "get_patched_dataset",
        "dataset-description-labels",
        "compare-datasets-patch-response",
    )

    # bq 2.1.31 serializes an empty schema when metadata flags are combined
    # with `update --table`. Supplying the complete additive schema mirrors the
    # documented update form and prevents an accidental field-removal request.
    # https://cloud.google.com/bigquery/docs/managing-table-schemas#bq
    with tempfile.NamedTemporaryFile(
        mode="w", prefix="bqemu-bqcli-patch-", suffix=".json", delete=False
    ) as schema_file:
        json.dump(
            [
                *existing_schema_fields,
                {"name": "source", "type": "STRING", "mode": "NULLABLE"},
            ],
            schema_file,
        )
        schema_path = Path(schema_file.name)
    try:
        run_process(
            base
            + [
                "update",
                "--table",
                "--description=Patched CLI table",
                "--set_label=owner:contracts",
                table,
                str(schema_path),
            ],
            "patch_table_metadata_and_schema",
        )
    finally:
        schema_path.unlink(missing_ok=True)
    patched_table = decode_json(
        run_process(base + ["show", table], "get_patched_table"),
        "get_patched_table",
    )
    require(
        patched_table.get("description") == "Patched CLI table"
        and patched_table.get("labels") == {"owner": "contracts"}
        and [
            field.get("name")
            for field in patched_table.get("schema", {}).get("fields", [])
        ]
        == [field.get("name") for field in existing_schema_fields] + ["source"],
        "get_patched_table",
        "table-description-labels-additive-schema",
        "compare-tables-patch-response",
    )

    append_job_id = f"bqcli-append-{suffix}"
    append_query = base + [
        f"--job_id={append_job_id}",
        f"--location={location}",
        "query",
        "--use_legacy_sql=false",
        f"--destination_table={table}",
        "--append_table",
        "SELECT 10 AS id, 'ten' AS name, true AS active, 'append' AS source",
    ]
    run_process(append_query, "query_destination_append")

    shown_append = decode_json(
        run_process(
            base + ["show", "--job", f"{project}:{location}.{append_job_id}"],
            "get_destination_append_job",
        ),
        "get_destination_append_job",
    )
    query_config = shown_append.get("configuration", {}).get("query", {})
    destination = query_config.get("destinationTable", {})
    require(
        shown_append.get("jobReference", {}).get("location") == location
        and destination.get("projectId") == project
        and destination.get("tableId") == table.rsplit(".", 1)[-1]
        and query_config.get("writeDisposition") == "WRITE_APPEND",
        "get_destination_append_job",
        "location-destination-write-disposition",
        "compare-jobs-get-query-configuration",
    )

    duplicate = run_process(
        append_query,
        "duplicate_query_job",
        expected_codes=(2,),
    )
    require(
        "already exists" in (duplicate.stdout + duplicate.stderr).lower()
        or "duplicate" in (duplicate.stdout + duplicate.stderr).lower(),
        "duplicate_query_job",
        "http-409-duplicate",
        f"compare-jobs-insert-error-{BQCLI_LIFECYCLE_CONTRACT}",
    )

    wrong_location = run_process(
        base + ["show", "--job", f"{project}:EU.{append_job_id}"],
        "get_job_wrong_location",
        expected_codes=(2,),
    )
    require(
        "not found" in (wrong_location.stdout + wrong_location.stderr).lower(),
        "get_job_wrong_location",
        "location-aware-not-found",
        "compare-jobs-get-location-error",
    )

    truncate_job_id = f"bqcli-truncate-{suffix}"
    run_process(
        base
        + [
            f"--job_id={truncate_job_id}",
            f"--location={location}",
            "query",
            "--use_legacy_sql=false",
            f"--destination_table={table}",
            "--replace",
            "SELECT 11 AS id, 'eleven' AS name, false AS active, 'truncate' AS source",
        ],
        "query_destination_truncate",
    )
    shown_truncate = decode_json(
        run_process(
            base + ["show", "--job", f"{project}:{location}.{truncate_job_id}"],
            "get_destination_truncate_job",
        ),
        "get_destination_truncate_job",
    )
    require(
        shown_truncate.get("configuration", {})
        .get("query", {})
        .get("writeDisposition")
        == "WRITE_TRUNCATE",
        "get_destination_truncate_job",
        "write-truncate",
        "compare-jobs-get-query-configuration",
    )

    sql_table = table.replace(":", ".", 1)
    destination_rows = decode_json(
        run_process(
            base
            + [
                f"--location={location}",
                "query",
                "--use_legacy_sql=false",
                "--max_rows=1",
                f"SELECT id FROM `{sql_table}` ORDER BY id",
            ],
            "verify_truncated_destination",
        ),
        "verify_truncated_destination",
    )
    require(
        destination_rows == [{"id": "11"}],
        "verify_truncated_destination",
        "truncated-destination-row",
        "compare-get-query-results-row-codec",
    )

    paged_rows = decode_json(
        run_process(
            base
            + [
                "--max_rows_per_request=1",
                f"--location={location}",
                "query",
                "--use_legacy_sql=false",
                "--max_rows=3",
                "SELECT 1 AS ordinal UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY ordinal",
            ],
            "get_query_results_paginated",
        ),
        "get_query_results_paginated",
    )
    require(
        paged_rows == [{"ordinal": "1"}, {"ordinal": "2"}, {"ordinal": "3"}],
        "get_query_results_paginated",
        "three-rows-across-one-row-pages",
        "compare-get-query-results-pagination",
    )

    first_page = decode_json(
        run_process(
            base
            + [
                f"--location={location}",
                "ls",
                "--jobs",
                "--max_results=1",
                "--print_last_token",
            ],
            "list_jobs_first_page",
        ),
        "list_jobs_first_page",
    )
    require(
        isinstance(first_page, dict)
        and len(first_page.get("results", [])) == 1
        and isinstance(first_page.get("token"), str)
        and bool(first_page.get("token")),
        "list_jobs_first_page",
        "one-job-and-next-page-token",
        "compare-bq-print-last-token-shape",
    )
    second_page = decode_json(
        run_process(
            base
            + [
                f"--location={location}",
                "ls",
                "--jobs",
                "--max_results=1",
                f"--page_token={first_page['token']}",
                "--print_last_token",
            ],
            "list_jobs_second_page",
        ),
        "list_jobs_second_page",
    )
    second_results = second_page.get("results", []) if isinstance(second_page, dict) else []
    first_reference = first_page["results"][0].get("jobReference", {})
    first_job = first_reference.get("jobId")
    second_reference = second_results[0].get("jobReference", {}) if second_results else {}
    second_job = second_reference.get("jobId") if second_results else None
    require(
        len(second_results) == 1
        and bool(first_job)
        and bool(second_job)
        and first_job != second_job
        and first_reference.get("location") == location
        and second_reference.get("location") == location,
        "list_jobs_second_page",
        "stable-page-token-advance",
        "compare-jobs-list-pagination",
    )
