#!/usr/bin/env python3
"""Public-process Parquet load contracts against one external fake GCS."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import importlib.metadata
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from typing import Any, Callable, Mapping
import uuid
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.integration.framework.consumer_runtime import (  # noqa: E402
    ConsumerRuntimeError,
    NormalizedConsumerCase,
    file_digest,
    load_normalized_execution,
    require_artifact,
)

from runtime import (
    AuditEntry,
    AuditProxy,
    ContractError,
    EmulatorRuntime,
    FakeGCSRuntime,
    Settings,
    digest,
    ensure_fake_gcs_image,
    event,
    failure,
    read_locked_json,
    request_json,
    run_process,
    validate_infrastructure_lock,
    write_evidence,
)


MODEL_VERSION = "bqemu-load-public-process/v1"
CASES = ("python", "bq", "pyspark", "scala-spark")
STORAGE_WRITE_OPERATIONS = {
    "grpc.bigquery-write.create-write-stream",
    "grpc.bigquery-write.append-rows",
    "grpc.bigquery-write.get-write-stream",
    "grpc.bigquery-write.finalize-write-stream",
    "grpc.bigquery-write.batch-commit-write-streams",
    "grpc.bigquery-write.flush-rows",
}


@dataclass(frozen=True)
class CaseResult:
    case: str
    consumer: str
    version: str
    expected_files: int
    expected_rows: int
    public_operations: tuple[str, ...]
    gcs_operations: tuple[AuditEntry, ...]
    statistics: dict[str, int]


def require(condition: bool, *, case: str, operation: str, shape: str, observed: Any) -> None:
    if condition:
        return
    raise failure(
        stage="assert",
        service=case,
        model_version=MODEL_VERSION,
        operation=operation,
        shape=shape,
        observed=observed,
        fix_hint="compare-the-pinned-load-contract",
    )


def create_dataset(endpoint: str, settings: Settings, dataset: str) -> None:
    request_json(
        endpoint,
        "POST",
        f"/bigquery/v2/projects/{settings.project}/datasets",
        operation="bigquery.datasets.insert",
        service="bigquery",
        model_version="current-source",
        timeout=settings.http_timeout,
        payload={
            "datasetReference": {
                "projectId": settings.project,
                "datasetId": dataset,
            },
            "location": settings.location,
        },
    )


def create_table(
    endpoint: str,
    settings: Settings,
    dataset: str,
    table: str,
    *,
    partitioned: bool = False,
) -> None:
    fields = [
        {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "active", "type": "BOOLEAN", "mode": "NULLABLE"},
    ]
    if partitioned:
        fields.append(
            {"name": "partition_date", "type": "DATE", "mode": "NULLABLE"}
        )
    resource: dict[str, Any] = {
        "tableReference": {
            "projectId": settings.project,
            "datasetId": dataset,
            "tableId": table,
        },
        "schema": {"fields": fields},
    }
    if partitioned:
        resource["timePartitioning"] = {"type": "DAY", "field": "partition_date"}
    request_json(
        endpoint,
        "POST",
        f"/bigquery/v2/projects/{settings.project}/datasets/{dataset}/tables",
        operation="bigquery.tables.insert",
        service="bigquery",
        model_version="current-source",
        timeout=settings.http_timeout,
        payload=resource,
    )


def seed_dynamic_partition_destination(
    endpoint: str, settings: Settings, dataset: str, table: str
) -> None:
    response = request_json(
        endpoint,
        "POST",
        f"/bigquery/v2/projects/{settings.project}/queries",
        operation="bigquery.jobs.query",
        service="bigquery",
        model_version="current-source",
        timeout=settings.http_timeout,
        payload={
            "query": (
                f"INSERT INTO `{settings.project}.{dataset}.{table}` VALUES "
                "(-1, 'old-one', true, DATE '2026-01-01'), "
                "(-2, 'old-two', false, DATE '2026-01-01'), "
                "(-3, 'keep', true, DATE '2026-01-02')"
            ),
            "useLegacySql": False,
        },
    )
    require(
        isinstance(response, dict)
        and response.get("jobComplete") is True
        and "errors" not in response,
        case="spark-indirect-dynamic-overwrite",
        operation="seed-destination",
        shape="incomplete-query",
        observed=response,
    )


def load_statistics(
    endpoint: str,
    settings: Settings,
    dataset: str,
    table: str,
    *,
    allow_temporary_destination: bool = False,
) -> dict[str, int]:
    response = request_json(
        endpoint,
        "GET",
        f"/bigquery/v2/projects/{settings.project}/jobs?location={settings.location}&projection=full",
        operation="bigquery.jobs.list",
        service="bigquery",
        model_version="current-source",
        timeout=settings.http_timeout,
    )
    jobs = response.get("jobs") if isinstance(response, dict) else None
    matches: list[dict[str, Any]] = []
    if isinstance(jobs, list):
        for job in jobs:
            if not isinstance(job, dict):
                continue
            load = job.get("configuration", {}).get("load")
            destination = load.get("destinationTable") if isinstance(load, dict) else None
            status = job.get("status")
            destination_matches = destination == {
                "projectId": settings.project,
                "datasetId": dataset,
                "tableId": table,
            }
            if allow_temporary_destination and isinstance(destination, dict):
                destination_matches = (
                    destination.get("projectId") == settings.project
                    and destination.get("datasetId") == dataset
                    and isinstance(destination.get("tableId"), str)
                )
            if destination_matches and (
                isinstance(status, dict)
                and status.get("state") == "DONE"
                and "errorResult" not in status
            ):
                matches.append(job)
    require(
        len(matches) == 1,
        case="load-statistics",
        operation="bigquery.jobs.list",
        shape="load-job-cardinality",
        observed=response,
    )
    job = matches[0]
    status = job.get("status")
    load = job.get("statistics", {}).get("load")
    require(
        isinstance(status, dict)
        and status.get("state") == "DONE"
        and "errorResult" not in status
        and isinstance(load, dict),
        case="load-statistics",
        operation="bigquery.jobs.list",
        shape="terminal-load-statistics",
        observed=job,
    )
    result: dict[str, int] = {}
    for key in ("inputFiles", "inputFileBytes", "outputRows", "outputBytes"):
        value = load.get(key)
        require(
            isinstance(value, str) and value.isascii() and value.isdigit(),
            case="load-statistics",
            operation="bigquery.jobs.list",
            shape=f"invalid-{key}",
            observed=value,
        )
        result[key] = int(value)
    return result


def delete_dataset(endpoint: str, settings: Settings, dataset: str) -> None:
    try:
        request_json(
            endpoint,
            "DELETE",
            f"/bigquery/v2/projects/{settings.project}/datasets/{dataset}?deleteContents=true",
            operation="bigquery.datasets.delete",
            service="bigquery",
            model_version="current-source",
            timeout=settings.http_timeout,
        )
    except ContractError:
        # Cleanup must not replace the primary contract outcome. The emulator
        # process and its isolated DuckDB file are removed immediately after.
        event(
            boundary="cleanup",
            model_version=MODEL_VERSION,
            operation="bigquery.datasets.delete",
            service="bigquery",
            status="failed",
        )


def assert_connector_prefix_empty(
    gcs_endpoint: str, settings: Settings, *, model_version: str
) -> None:
    response = request_json(
        gcs_endpoint,
        "GET",
        "/storage/v1/b/bqemu-load-e2e/o?prefix=.spark-bigquery-",
        operation="objects.list",
        service="storage",
        model_version=model_version,
        timeout=settings.http_timeout,
    )
    items = response.get("items") if isinstance(response, dict) else None
    require(
        items in (None, []),
        case="spark-bigquery-connector",
        operation="temporary-object-cleanup",
        shape="connector-prefix-not-empty",
        observed=response,
    )


def assert_active_parquet_option_rejected(
    endpoint: str,
    settings: Settings,
    proxy: AuditProxy,
    dataset: str,
    table: str,
) -> dict[str, Any]:
    job_id = "unsupported-parquet-" + uuid.uuid4().hex[:12]
    rows_before = table_row_count(endpoint, settings, dataset, table)
    gcs_calls_before = len(proxy.entries)
    job = request_json(
        endpoint,
        "POST",
        f"/bigquery/v2/projects/{settings.project}/jobs",
        operation="bigquery.jobs.insert",
        service="bigquery",
        model_version="current-source",
        timeout=settings.http_timeout,
        payload={
            "jobReference": {
                "projectId": settings.project,
                "location": settings.location,
                "jobId": job_id,
            },
            "configuration": {
                "load": {
                    "sourceUris": ["gs://bqemu-load-e2e/python/*.parquet"],
                    "destinationTable": {
                        "projectId": settings.project,
                        "datasetId": dataset,
                        "tableId": table,
                    },
                    "sourceFormat": "PARQUET",
                    "writeDisposition": "WRITE_APPEND",
                    "createDisposition": "CREATE_NEVER",
                    "parquetOptions": {"enumAsString": True},
                }
            },
        },
    )
    deadline = time.monotonic() + settings.http_timeout
    while isinstance(job, dict):
        current_status = job.get("status")
        if isinstance(current_status, dict) and current_status.get("state") == "DONE":
            break
        if time.monotonic() >= deadline:
            break
        time.sleep(0.02)
        job = request_json(
            endpoint,
            "GET",
            f"/bigquery/v2/projects/{settings.project}/jobs/{job_id}?location={settings.location}",
            operation="bigquery.jobs.get",
            service="bigquery",
            model_version="current-source",
            timeout=settings.http_timeout,
        )
    status = job.get("status") if isinstance(job, dict) else None
    statistics_container = job.get("statistics") if isinstance(job, dict) else None
    statistics = (
        statistics_container.get("load")
        if isinstance(statistics_container, dict)
        else None
    )
    error_result = status.get("errorResult") if isinstance(status, dict) else None
    require(
        isinstance(status, dict)
        and status.get("state") == "DONE"
        and isinstance(error_result, dict)
        and error_result.get("reason") == "notImplemented"
        and isinstance(statistics, dict)
        and statistics.get("inputFiles") == "0"
        and statistics.get("outputRows") == "0"
        and "outputBytes" not in statistics,
        case="load-options",
        operation="parquetOptions.enumAsString",
        shape="unsupported-option-contract",
        observed=job,
    )
    rows_after = table_row_count(endpoint, settings, dataset, table)
    require(
        len(proxy.entries) == gcs_calls_before and rows_after == rows_before,
        case="load-options",
        operation="parquetOptions.enumAsString",
        shape="side-effect-before-rejection",
        observed={
            "gcsCallsBefore": gcs_calls_before,
            "gcsCallsAfter": len(proxy.entries),
            "rowsBefore": rows_before,
            "rowsAfter": rows_after,
        },
    )
    return {
        "option": "parquetOptions.enumAsString",
        "reason": "notImplemented",
        "gcsCalls": 0,
        "rowDelta": 0,
    }


def assert_invalid_parquet_rolls_back(
    endpoint: str,
    settings: Settings,
    load_proxy: AuditProxy,
    emulator: EmulatorRuntime,
    workspace_root: Path,
    dataset: str,
    table: str,
    *,
    name: str,
    source_uri: str,
) -> dict[str, Any]:
    job_id = f"{name}-" + uuid.uuid4().hex[:12]
    rows_before = table_row_count(endpoint, settings, dataset, table)
    gcs_before = len(load_proxy.entries)
    log_before = emulator.log_position()
    job = request_json(
        endpoint,
        "POST",
        f"/bigquery/v2/projects/{settings.project}/jobs",
        operation="bigquery.jobs.insert",
        service="bigquery",
        model_version="current-source",
        timeout=settings.http_timeout,
        payload={
            "jobReference": {
                "projectId": settings.project,
                "location": settings.location,
                "jobId": job_id,
            },
            "configuration": {
                "load": {
                    "sourceUris": [source_uri],
                    "destinationTable": {
                        "projectId": settings.project,
                        "datasetId": dataset,
                        "tableId": table,
                    },
                    "sourceFormat": "PARQUET",
                    "writeDisposition": "WRITE_APPEND",
                    "createDisposition": "CREATE_NEVER",
                }
            },
        },
    )
    deadline = time.monotonic() + settings.client_timeout
    while isinstance(job, dict):
        status = job.get("status")
        if isinstance(status, dict) and status.get("state") == "DONE":
            break
        if time.monotonic() >= deadline:
            break
        time.sleep(0.02)
        job = request_json(
            endpoint,
            "GET",
            f"/bigquery/v2/projects/{settings.project}/jobs/{job_id}?location={settings.location}",
            operation="bigquery.jobs.get",
            service="bigquery",
            model_version="current-source",
            timeout=settings.http_timeout,
        )
    status = job.get("status") if isinstance(job, dict) else None
    error_result = status.get("errorResult") if isinstance(status, dict) else None
    statistics = job.get("statistics", {}).get("load") if isinstance(job, dict) else None
    rows_after = table_row_count(endpoint, settings, dataset, table)
    runtime_events = emulator.runtime_events(log_before, emulator.log_position())
    internal = [
        event.evidence()
        for event in runtime_events
        if event.protocol == "internal"
    ]
    require(
        isinstance(status, dict)
        and status.get("state") == "DONE"
        and isinstance(error_result, dict)
        and error_result.get("reason")
        in {"invalid", "invalidQuery", "backendError"}
        and isinstance(statistics, dict)
        and statistics.get("outputRows") == "0"
        and "outputBytes" not in statistics,
        case="load-invalid-parquet",
        operation=name,
        shape="terminal-load-error",
        observed=job,
    )
    require(
        rows_after == rows_before
        and len(load_proxy.entries) > gcs_before
        and not list(workspace_root.glob("bqemu-load-*")),
        case="load-invalid-parquet",
        operation=name,
        shape="rollback-or-workspace-cleanup",
        observed={
            "rowsBefore": rows_before,
            "rowsAfter": rows_after,
            "gcsCalls": len(load_proxy.entries) - gcs_before,
            "workspaceCount": len(list(workspace_root.glob("bqemu-load-*"))),
        },
    )
    required_internal = {
        ("internal.commit_load_job", "rolled_back"),
        ("internal.load_parquet", "rolled_back"),
        ("internal.cleanup_load_workspace", "committed"),
    }
    observed_internal = {
        (str(item["operation"]), str(item["status"])) for item in internal
    }
    require(
        required_internal <= observed_internal,
        case="load-invalid-parquet",
        operation=name,
        shape="missing-rollback-events",
        observed=internal,
    )
    return {
        "name": name,
        "reason": error_result["reason"],
        "rowDelta": rows_after - rows_before,
        "gcsCalls": len(load_proxy.entries) - gcs_before,
        "workspaceCount": 0,
        "internalEvents": internal,
    }


def table_row_count(endpoint: str, settings: Settings, dataset: str, table: str) -> int:
    resource = request_json(
        endpoint,
        "GET",
        f"/bigquery/v2/projects/{settings.project}/datasets/{dataset}/tables/{table}/data?maxResults=100",
        operation="bigquery.tabledata.list",
        service="bigquery",
        model_version="current-source",
        timeout=settings.http_timeout,
    )
    if not isinstance(resource, dict):
        return -1
    rows = resource.get("rows", [])
    return len(rows) if isinstance(rows, list) else -1


def table_row_ids(
    endpoint: str, settings: Settings, dataset: str, table: str
) -> list[int]:
    resource = request_json(
        endpoint,
        "GET",
        f"/bigquery/v2/projects/{settings.project}/datasets/{dataset}/tables/{table}/data?maxResults=100",
        operation="bigquery.tabledata.list",
        service="bigquery",
        model_version="current-source",
        timeout=settings.http_timeout,
    )
    rows = resource.get("rows") if isinstance(resource, dict) else None
    require(
        isinstance(rows, list),
        case="spark-indirect-dynamic-overwrite",
        operation="destination-rows",
        shape="invalid-tabledata",
        observed=resource,
    )
    values: list[int] = []
    for row in rows:
        cells = row.get("f") if isinstance(row, dict) else None
        value = cells[0].get("v") if isinstance(cells, list) and cells else None
        require(
            isinstance(value, str) and value.lstrip("-").isdigit(),
            case="spark-indirect-dynamic-overwrite",
            operation="destination-rows",
            shape="invalid-id",
            observed=value,
        )
        values.append(int(value))
    return sorted(values)


def run_python_case(
    settings: Settings,
    endpoint: str,
    dataset: str,
    table: str,
    *,
    versions: Mapping[str, str],
) -> dict[str, int]:
    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import bigquery

    require(
        importlib.metadata.version("google-cloud-bigquery") == versions["client"],
        case="python",
        operation="consumer-identity",
        shape="client-version-drift",
        observed=importlib.metadata.version("google-cloud-bigquery"),
    )

    client = bigquery.Client(
        project=settings.project,
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint=endpoint),
    )
    try:
        job = client.load_table_from_uri(
            "gs://bqemu-load-e2e/python/*.parquet",
            f"{settings.project}.{dataset}.{table}",
            job_id="python-load-" + uuid.uuid4().hex[:12],
            location=settings.location,
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=False,
            ),
        )
        job.result(timeout=settings.client_timeout)
        statistics = {
            "inputFiles": int(job.input_files or 0),
            "inputFileBytes": int(job.input_file_bytes or 0),
            "outputRows": int(job.output_rows or 0),
            "outputBytes": int(job.output_bytes or 0),
        }
        return statistics
    finally:
        client.close()


def run_bq_case(
    settings: Settings,
    endpoint: str,
    dataset: str,
    table: str,
    *,
    versions: Mapping[str, str],
) -> dict[str, int]:
    environment = os.environ.copy()
    with tempfile.TemporaryDirectory(prefix="bqemu-load-gcloud-") as temporary:
        environment.update(
            {
                "CLOUDSDK_CONFIG": temporary,
                "CLOUDSDK_CORE_DISABLE_PROMPTS": "1",
            }
        )
        command = [
            settings.bq_binary,
            f"--api={endpoint}",
            f"--project_id={settings.project}",
            "--use_gcloud_config=false",
            "--oauth_access_token=bqemu-load-contract-token",
            "--format=json",
            "load",
            f"--location={settings.location}",
            "--source_format=PARQUET",
            "--replace=false",
            f"{settings.project}:{dataset}.{table}",
            "gs://bqemu-load-e2e/bq/*.parquet",
        ]
        run_process(
            command,
            operation="bq-load-parquet",
            service="bq-cli",
            model_version=versions["bq"],
            timeout=settings.client_timeout,
            environment=environment,
        )
    return {}


def run_pyspark_case(
    settings: Settings,
    endpoint: str,
    dataset: str,
    table: str,
    *,
    versions: Mapping[str, str],
    gcs_endpoint: str,
    connector: Path,
    hadoop_gcs: Path,
) -> dict[str, int]:
    result = run_process(
        [
            str(settings.spark_python),
            str(ROOT / "tests/integration/load/pyspark_indirect.py"),
            "--connector",
            str(connector),
            "--hadoop-gcs",
            str(hadoop_gcs),
            "--http-endpoint",
            endpoint,
            "--gcs-endpoint",
            gcs_endpoint,
            "--project",
            settings.project,
            "--bucket",
            "bqemu-load-e2e",
            "--destination",
            f"{settings.project}.{dataset}.{table}",
        ],
        operation="pyspark-indirect-write",
        service="spark-bigquery-connector",
        model_version=f"spark-{versions['spark']}+connector-{versions['connector']}",
        timeout=settings.client_timeout,
        expected_codes=(0, 1),
    )
    marker = None
    for encoded in result.stdout.splitlines():
        try:
            candidate = json.loads(encoded)
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict) and candidate.get("entrypoint") == "pyspark":
            marker = candidate
    if isinstance(marker, dict) and marker.get("status") == "failed":
        event(
            boundary="child",
            error=marker.get("error"),
            java_failures=marker.get("javaFailures"),
            model_version=f"spark-{versions['spark']}+connector-{versions['connector']}",
            operation="pyspark-indirect-write",
            service="spark-bigquery-connector",
            stage=marker.get("stage"),
            status="failed",
            traceback=marker.get("traceback"),
        )
    require(
        result.returncode == 0
        and marker
        == {
            "connector": versions["connector"],
            "entrypoint": "pyspark",
            "mode": "dynamic-overwrite",
            "partitions": 4,
            "rows": 8,
            "spark": versions["spark"],
            "status": "passed",
        },
        case="pyspark",
        operation="spark-marker",
        shape="marker-mismatch",
        observed=marker,
    )
    return {}


def run_scala_spark_case(
    settings: Settings,
    endpoint: str,
    dataset: str,
    table: str,
    *,
    versions: Mapping[str, str],
    gcs_endpoint: str,
    connector: Path,
    hadoop_gcs: Path,
) -> dict[str, int]:
    spark_shell = settings.spark_python.parent / "spark-shell"
    spark_home_result = run_process(
        [
            str(settings.spark_python),
            "-c",
            "import pathlib,pyspark; print(pathlib.Path(pyspark.__file__).parent)",
        ],
        operation="resolve-spark-home",
        service="spark-runtime",
        model_version=versions["spark"],
        timeout=settings.client_timeout,
    )
    try:
        spark_home = Path(
            spark_home_result.stdout.decode("utf-8", errors="strict").strip()
        ).resolve(strict=True)
    except (OSError, UnicodeDecodeError) as error:
        raise failure(
            stage="provenance",
            service="spark-runtime",
            model_version=versions["spark"],
            operation="resolve-spark-home",
            shape="invalid-installation-path",
            observed=spark_home_result.stdout,
            fix_hint="install-the-case-declared-pyspark-runtime",
        ) from error
    require(
        spark_home.is_dir() and (spark_home / "bin/spark-submit").is_file(),
        case="scala-spark",
        operation="resolve-spark-home",
        shape="incomplete-installation",
        observed=spark_home_result.stdout,
    )
    environment = os.environ.copy()
    environment.update(
        {
            "SPARK_HOME": str(spark_home),
            "SPARK_LOCAL_IP": "127.0.0.1",
            "BQEMU_LOAD_SPARK_PROJECT": settings.project,
            "BQEMU_LOAD_SPARK_DESTINATION": f"{settings.project}.{dataset}.{table}",
            "BQEMU_LOAD_SPARK_HTTP_ENDPOINT": endpoint,
            "BQEMU_LOAD_SPARK_BUCKET": "bqemu-load-e2e",
            "BQEMU_LOAD_EXPECTED_SPARK": versions["spark"],
            "BQEMU_LOAD_EXPECTED_SCALA_BINARY": versions["scalaBinary"],
        }
    )
    configuration = [
        "spark.driver.host=127.0.0.1",
        "spark.driver.bindAddress=127.0.0.1",
        "spark.ui.enabled=false",
        "spark.sql.shuffle.partitions=4",
        "spark.sql.session.timeZone=UTC",
        "spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        "spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        "spark.hadoop.fs.gs.auth.service.account.enable=false",
        "spark.hadoop.fs.gs.auth.null.enable=true",
        f"spark.hadoop.fs.gs.storage.root.url={gcs_endpoint}",
        "spark.hadoop.fs.gs.storage.service.path=storage/v1/",
        "spark.hadoop.fs.gs.copy.with.rewrite.enable=false",
        "spark.hadoop.fs.gs.http.max.retry=0",
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2",
    ]
    command = [
        str(spark_shell),
        "--master",
        "local[4]",
        "--jars",
        f"{connector},{hadoop_gcs}",
    ]
    for value in configuration:
        command.extend(("--conf", value))
    command.extend(("-i", str(ROOT / "tests/integration/load/scala_indirect.scala")))
    result = run_process(
        command,
        operation="scala-spark-indirect-write",
        service="spark-bigquery-connector",
        model_version=f"spark-{versions['spark']}+connector-{versions['connector']}",
        timeout=settings.client_timeout,
        environment=environment,
    )
    stages = [
        line.strip()
        for line in result.stdout.decode("utf-8", errors="replace").splitlines()
        if line.strip().startswith("BQEMU_LOAD_SCALA_STAGE=")
    ]
    require(
        stages and stages[-1] == "BQEMU_LOAD_SCALA_STAGE=complete",
        case="scala-spark",
        operation="spark-marker",
        shape="marker-mismatch",
        observed=stages,
    )
    return {}


def assert_flow(result: CaseResult) -> None:
    operations = list(result.public_operations)
    require(
        "bigquery.jobs.insert" in operations,
        case=result.case,
        operation="public-flow",
        shape="missing-jobs-insert",
        observed=operations,
    )
    require(
        "bigquery.jobs.get" in operations,
        case=result.case,
        operation="public-flow",
        shape="missing-jobs-get",
        observed=operations,
    )
    if result.case in {"pyspark", "scala-spark"}:
        require(
            operations.count("bigquery.tables.insert") == 1
            and operations.count("bigquery.jobs.insert") == 2
            and operations.count("bigquery.jobs.get") >= 2
            and operations.count("bigquery.jobs.getQueryResults") >= 1
            and operations.count("bigquery.tables.delete") == 1,
            case=result.case,
            operation="dynamic-partition-flow",
            shape="temporary-load-then-script",
            observed=operations,
        )
    write_calls = [operation for operation in operations if operation in STORAGE_WRITE_OPERATIONS]
    require(
        not write_calls,
        case=result.case,
        operation="public-flow",
        shape="unexpected-storage-write-rpc",
        observed=write_calls,
    )
    gcs_operations = [entry.operation for entry in result.gcs_operations]
    require(
        "unexpected" not in gcs_operations,
        case=result.case,
        operation="gcs-flow",
        shape="unclassified-gcs-operation",
        observed=gcs_operations,
    )
    require(
        gcs_operations.count("objects.list") >= 1,
        case=result.case,
        operation="gcs-flow",
        shape="missing-objects-list",
        observed=gcs_operations,
    )
    require(
        gcs_operations.count("objects.get.media") == result.expected_files,
        case=result.case,
        operation="gcs-flow",
        shape="media-count",
        observed=gcs_operations,
    )
    if result.statistics:
        require(
            result.statistics.get("inputFiles") == result.expected_files
            and result.statistics.get("inputFileBytes", 0) > 0
            and result.statistics.get("outputRows") == result.expected_rows
            and result.statistics.get("outputBytes", 0) > 0,
            case=result.case,
            operation="load-statistics",
            shape="statistics-mismatch",
            observed=result.statistics,
        )
    if result.case in {"pyspark", "scala-spark"}:
        normalizations = [
            entry.transport_normalization
            for entry in result.gcs_operations
            if entry.transport_normalization is not None
        ]
        require(
            gcs_operations.count("objects.copy") == result.expected_files
            and normalizations == ["empty-gzip-body"] * result.expected_files,
            case=result.case,
            operation="gcs-flow",
            shape="copy-normalization-mismatch",
            observed={"operations": gcs_operations, "normalizations": normalizations},
        )
        require(
            gcs_operations.count("objects.upload") >= result.expected_files,
            case=result.case,
            operation="gcs-flow",
            shape="missing-upload",
            observed=gcs_operations,
        )
        require(
            gcs_operations.count("objects.delete") >= result.expected_files,
            case=result.case,
            operation="gcs-flow",
            shape="missing-cleanup-delete",
            observed=gcs_operations,
        )


def unified_trace(
    runtime_events: list[Any],
    upload_entries: list[AuditEntry],
    load_entries: list[AuditEntry],
) -> list[dict[str, Any]]:
    events = [event.evidence() for event in runtime_events]
    for entry in (*upload_entries, *load_entries):
        events.append(
            {
                "timeUnixNanos": entry.time_unix_nanos,
                "actor": entry.actor,
                "protocol": "gcs-json",
                "phase": "response",
                "operation": "gcs." + entry.operation,
                "status": entry.status,
            }
        )
    events.sort(
        key=lambda value: (
            int(value["timeUnixNanos"]),
            str(value["actor"]),
            str(value["operation"]),
            str(value["phase"]),
        )
    )
    for sequence, item in enumerate(events, 1):
        item["sequence"] = sequence
    return events


def assert_trace(case: str, trace: list[dict[str, Any]]) -> None:
    positions: dict[tuple[str, str, str], list[int]] = {}
    for item in trace:
        key = (str(item["actor"]), str(item["operation"]), str(item["phase"]))
        positions.setdefault(key, []).append(int(item["sequence"]))

    def first(actor: str, operation: str, phase: str) -> int:
        values = positions.get((actor, operation, phase), [])
        require(
            bool(values),
            case=case,
            operation="cross-protocol-trace",
            shape=f"missing-{actor}-{operation}-{phase}",
            observed=trace,
        )
        return values[0]

    def optional_first(actor: str, operation: str, phase: str) -> int | None:
        values = positions.get((actor, operation, phase), [])
        return values[0] if values else None

    jobs_insert = first("consumer", "bigquery.jobs.insert", "request")
    object_media = first("bqemu-load", "gcs.objects.get.media", "response")
    object_list = optional_first("bqemu-load", "gcs.objects.list", "response")
    object_read_start = object_list if object_list is not None else object_media
    load_before = first("bqemu", "internal.load_parquet", "before")
    load_after = first("bqemu", "internal.load_parquet", "after")
    commit_after = first("bqemu", "internal.commit_load_job", "after")
    workspace_after = first(
        "bqemu", "internal.cleanup_load_workspace", "after"
    )
    committed = {
        str(item["operation"])
        for item in trace
        if item["actor"] == "bqemu"
        and item["phase"] == "after"
        and item["status"] == "committed"
    }
    require(
        jobs_insert
        < object_read_start
        <= object_media
        < load_before
        < load_after
        < commit_after
        < workspace_after
        and {
            "internal.load_parquet",
            "internal.commit_load_job",
            "internal.cleanup_load_workspace",
        }
        <= committed,
        case=case,
        operation="cross-protocol-trace",
        shape="load-order",
        observed=trace,
    )
    if case in {"pyspark", "scala-spark"}:
        upload = first("spark-upload", "gcs.objects.upload", "response")
        deletes = positions.get(
            ("spark-upload", "gcs.objects.delete", "response"), []
        )
        require(
            upload < jobs_insert < commit_after
            and any(delete > commit_after for delete in deletes),
            case=case,
            operation="cross-protocol-trace",
            shape="connector-order",
            observed=trace,
        )


def run_case(
    case: str,
    settings: Settings,
    artifact_provenance: dict[str, Any],
    image: dict[str, Any],
) -> CaseResult:
    started = time.monotonic()
    versions = artifact_provenance["versions"]
    settings.artifact_root.mkdir(parents=True, exist_ok=True)
    artifact_directory = settings.artifact_root / case
    artifact_directory.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f"bqemu-load-{case}-") as temporary:
        work = Path(temporary)
        seed = work / "seed"
        binary = work / "go-bemu"
        run_process(
            [settings.go_binary, "build", "-trimpath", "-o", str(binary), "./cmd/emulator"],
            operation="build-emulator",
            service="bqemu",
            model_version="current-source",
            timeout=settings.build_timeout,
        )
        run_process(
            [
                settings.go_binary,
                "run",
                "./tests/integration/load/fixturegen",
                "--manifest",
                str(settings.fixture_lock),
                "--output-root",
                str(seed),
                "--timeout",
                f"{int(settings.fixture_timeout)}s",
            ],
            operation="generate-fixtures",
            service="fixturegen",
            model_version="v2.10505.0",
            timeout=settings.fixture_timeout,
        )
        upload_proxy = AuditProxy(
            settings.http_timeout,
            settings.proxy_max_request_bytes,
            settings.proxy_max_response_bytes,
            actor="spark-upload",
            model_version=str(image["version"]),
        )
        load_proxy = AuditProxy(
            settings.http_timeout,
            settings.proxy_max_request_bytes,
            settings.proxy_max_response_bytes,
            actor="bqemu-load",
            model_version=str(image["version"]),
        )
        fake = FakeGCSRuntime(
            settings, image, seed, artifact_directory / "fake-gcs.safe.json"
        )
        emulator: EmulatorRuntime | None = None
        dataset = "load_" + uuid.uuid4().hex[:12]
        table = "events"
        endpoint = ""
        try:
            upstream = fake.start(upload_proxy.endpoint)
            upload_proxy.set_upstream(upstream)
            load_proxy.set_upstream(upstream)
            upload_proxy.start()
            load_proxy.start()
            emulator_work = work / "emulator"
            emulator_work.mkdir(mode=0o700)
            raw_log = emulator_work / "server.log"
            emulator = EmulatorRuntime(
                settings,
                binary,
                emulator_work,
                load_proxy.endpoint,
                raw_log,
                artifact_directory / "emulator.safe.json",
            )
            endpoint = emulator.start()
            create_dataset(endpoint, settings, dataset)
            spark_dynamic = case in {"pyspark", "scala-spark"}
            create_table(
                endpoint,
                settings,
                dataset,
                table,
                partitioned=spark_dynamic,
            )
            if spark_dynamic:
                seed_dynamic_partition_destination(
                    endpoint, settings, dataset, table
                )
            negative_option = None
            invalid_inputs: list[dict[str, Any]] = []
            if case == "python":
                negative_option = assert_active_parquet_option_rejected(
                    endpoint, settings, load_proxy, dataset, table
                )
                for name, source_uri in (
                    ("corrupt-parquet", "gs://bqemu-load-e2e/invalid/corrupt.parquet"),
                    (
                        "schema-mismatch",
                        "gs://bqemu-load-e2e/invalid/schema-mismatch.parquet",
                    ),
                ):
                    invalid_inputs.append(
                        assert_invalid_parquet_rolls_back(
                            endpoint,
                            settings,
                            load_proxy,
                            emulator,
                            emulator_work / "tmp",
                            dataset,
                            table,
                            name=name,
                            source_uri=source_uri,
                        )
                    )

            log_start = emulator.log_position()
            upload_start = len(upload_proxy.entries)
            load_start = len(load_proxy.entries)
            runners: dict[str, tuple[str, int, int, Callable[..., dict[str, int]]]] = {
                "python": ("google-cloud-bigquery", 1, 2, run_python_case),
                "bq": ("bq-cli", 2, 2, run_bq_case),
                "pyspark": ("spark-bigquery-connector", 4, 8, run_pyspark_case),
                "scala-spark": (
                    "spark-bigquery-connector",
                    4,
                    8,
                    run_scala_spark_case,
                ),
            }
            consumer, expected_files, expected_rows, runner = runners[case]
            try:
                runner_arguments: dict[str, Any] = {"versions": versions}
                if case in {"pyspark", "scala-spark"}:
                    runner_arguments.update(
                        {
                            "gcs_endpoint": upload_proxy.endpoint,
                            "connector": Path(artifact_provenance["connectorPath"]),
                            "hadoop_gcs": Path(
                                artifact_provenance["hadoopGCSPath"]
                            ),
                        }
                    )
                client_statistics = runner(
                    settings,
                    endpoint,
                    dataset,
                    table,
                    **runner_arguments,
                )
                log_end = emulator.log_position()
                upload_end = len(upload_proxy.entries)
                load_end = len(load_proxy.entries)
                consumer_events = emulator.runtime_events(log_start, log_end)
                consumer_upload = upload_proxy.entries[upload_start:upload_end]
                consumer_load = load_proxy.entries[load_start:load_end]

                row_count = table_row_count(endpoint, settings, dataset, table)
                statistics = load_statistics(
                    endpoint,
                    settings,
                    dataset,
                    table,
                    allow_temporary_destination=spark_dynamic,
                )
                if client_statistics:
                    require(
                        client_statistics == statistics,
                        case=case,
                        operation="load-statistics",
                        shape="client-wire-mismatch",
                        observed={
                            "client": client_statistics,
                            "wire": statistics,
                        },
                    )
                if case in {"pyspark", "scala-spark"}:
                    assert_connector_prefix_empty(
                        upload_proxy.endpoint,
                        settings,
                        model_version=str(image["version"]),
                    )
            except ContractError:
                entries = (
                    upload_proxy.entries[upload_start:]
                    + load_proxy.entries[load_start:]
                )
                counts: dict[str, int] = {}
                shapes: dict[str, int] = {}
                for entry in entries:
                    key = f"{entry.actor}:{entry.operation}:{entry.status}"
                    counts[key] = counts.get(key, 0) + 1
                    if entry.status >= 400:
                        shape_key = f"{entry.actor}:{entry.target_shape}:{entry.status}"
                        shapes[shape_key] = shapes.get(shape_key, 0) + 1
                event(
                    boundary="case-diagnostic",
                    gcs_counts=counts,
                    gcs_failure_shapes=shapes,
                    model_version=MODEL_VERSION,
                    operation="parquet-load",
                    public_operations=emulator.public_operations(log_start),
                    service=case,
                    status="failed",
                )
                raise
            destination_rows = expected_rows + 1 if spark_dynamic else expected_rows
            require(
                row_count == destination_rows,
                case=case,
                operation="destination-row-count",
                shape="row-count",
                observed=row_count,
            )
            if spark_dynamic:
                ids = table_row_ids(endpoint, settings, dataset, table)
                require(
                    ids == [-3, *range(8)],
                    case=case,
                    operation="dynamic-partition-result",
                    shape="touched-vs-untouched",
                    observed=ids,
                )
            result = CaseResult(
                case=case,
                consumer=consumer,
                version=(
                    versions["client"]
                    if case == "python"
                    else versions["bq"]
                    if case == "bq"
                    else versions["connector"]
                ),
                expected_files=expected_files,
                expected_rows=expected_rows,
                public_operations=tuple(
                    event.operation
                    for event in consumer_events
                    if event.protocol in {"rest", "grpc"}
                ),
                gcs_operations=tuple((*consumer_upload, *consumer_load)),
                statistics=statistics,
            )
            assert_flow(result)
            trace = unified_trace(
                consumer_events,
                consumer_upload,
                consumer_load,
            )
            assert_trace(case, trace)
            evidence = {
                "schemaVersion": MODEL_VERSION,
                "consumerCaseId": artifact_provenance["consumerCaseID"],
                "executionId": artifact_provenance["executionID"],
                "case": result.case,
                "consumer": result.consumer,
                "consumerVersion": result.version,
                "runtimeVersions": versions,
                "expectedFiles": result.expected_files,
                "expectedRows": result.expected_rows,
                "expectedDestinationRows": destination_rows,
                "consumerOperations": list(result.public_operations),
                "storageWriteCalls": sum(
                    operation in STORAGE_WRITE_OPERATIONS
                    for operation in result.public_operations
                ),
                "consumerGCSOperations": [
                    entry.evidence() for entry in result.gcs_operations
                ],
                "crossProtocolTrace": trace,
                "statistics": result.statistics,
                "infrastructureLockSha256": artifact_provenance[
                    "infrastructureLockSha256"
                ],
                "fixtureLockSha256": "sha256:"
                + digest(settings.fixture_lock.read_bytes()).removeprefix("sha256:"),
                "artifactEvidence": artifact_provenance["artifactEvidence"],
                "negativeActiveParquetOption": negative_option,
                "invalidParquetInputs": invalid_inputs,
            }
            write_evidence(artifact_directory / "evidence.json", evidence)
            event(
                boundary="case",
                consumer=result.consumer,
                consumer_version=result.version,
                duration_ms=round((time.monotonic() - started) * 1000),
                model_version=MODEL_VERSION,
                operation="parquet-load",
                service=case,
                status="passed",
            )
            return result
        finally:
            if endpoint:
                delete_dataset(endpoint, settings, dataset)
            if emulator is not None:
                emulator.stop()
            upload_proxy.stop(settings.shutdown_timeout)
            load_proxy.stop(settings.shutdown_timeout)
            fake.stop()


def write_junit(
    path: Path,
    results: list[CaseResult],
    error: ContractError | None,
    duration: float,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    suite = ET.Element(
        "testsuite",
        name="load-public-process",
        tests=str(len(results) + (1 if error is not None else 0)),
        failures="1" if error is not None else "0",
        time=f"{duration:.3f}",
    )
    for result in results:
        ET.SubElement(
            suite,
            "testcase",
            classname="load.public-process",
            name=result.case,
            time=f"{duration:.3f}",
        )
    if error is not None:
        testcase = ET.SubElement(
            suite,
            "testcase",
            classname="load.public-process",
            name=str(error.fields["service"]),
            time=f"{duration:.3f}",
        )
        failure_element = ET.SubElement(
            testcase,
            "failure",
            message=str(error),
            type="ContractError",
        )
        failure_element.text = json.dumps(
            error.fields,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
    encoded = ET.tostring(suite, encoding="utf-8", xml_declaration=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_bytes(encoded)
    temporary.replace(path)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--case",
        choices=CASES,
        default=os.getenv("BQEMU_LOAD_CASE"),
        required=os.getenv("BQEMU_LOAD_CASE") is None,
    )
    parser.add_argument(
        "--junit",
        type=Path,
        default=Path(os.getenv("BQEMU_LOAD_JUNIT", ".artifacts/load/junit.xml")),
    )
    return parser.parse_args()


def configured_artifact(
    consumer_case: NormalizedConsumerCase,
    usage: str,
    environment_name: str,
) -> tuple[Path, dict[str, Any]]:
    artifact = require_artifact(consumer_case, usage)
    raw_path = os.getenv(environment_name, "")
    path = Path(raw_path).expanduser().absolute()
    try:
        actual_digest, size = file_digest(path)
    except OSError as error:
        raise failure(
            stage="provenance",
            service="load-e2e",
            model_version=MODEL_VERSION,
            operation="validate-configured-artifact",
            shape=usage,
            observed={"type": type(error).__name__, "error": repr(error)},
            fix_hint="use-the-case-declared-artifact-materialized-by-the-runner",
        ) from error
    require(
        actual_digest == artifact.sha256,
        case="load-e2e",
        operation="validate-configured-artifact",
        shape=usage,
        observed={"bytes": size, "sha256": actual_digest},
    )
    return path, {
        "id": artifact.artifact_id,
        "usage": artifact.usage,
        "sha256": artifact.sha256,
        "bytes": size,
        "status": "digest-verified",
    }


def case_provenance(
    settings: Settings,
    requested_case: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest_path = Path(
        os.getenv(
            "BQEMU_CONSUMER_MANIFEST",
            str(ROOT / "tests/integration/contract/consumers.normalized.json"),
        )
    ).resolve()
    case_id = os.getenv("BQEMU_CONSUMER_CASE_ID", "")
    execution_id = os.getenv("BQEMU_CONSUMER_EXECUTION_ID", "")
    try:
        consumer_case, execution = load_normalized_execution(
            manifest_path, case_id, execution_id
        )
    except ConsumerRuntimeError as error:
        raise failure(
            stage="provenance",
            service="load-e2e",
            model_version=MODEL_VERSION,
            operation="load-normalized-consumer-execution",
            shape=type(error).__name__,
            observed={"type": type(error).__name__, "error": repr(error)},
            fix_hint="run-the-case-through-the-normalized-consumer-runner",
        ) from error
    expected_cases = {
        "python-indirect-load-v1": "python",
        "bq-indirect-load-v1": "bq",
        "spark-pyspark-indirect-load-v1": "pyspark",
        "spark-scala-indirect-load-v1": "scala-spark",
    }
    selectors = [
        selector
        for scenario in execution.scenarios
        for selector in scenario["selectors"]
    ]
    require(
        execution_id == "indirect-load"
        and expected_cases.get(execution.runner_adapter_id) == requested_case
        and selectors == [f"load:{requested_case}"],
        case=requested_case,
        operation="load-normalized-consumer-execution",
        shape="execution-binding",
        observed={
            "caseIdDigest": digest(case_id),
            "executionId": execution_id,
            "runnerAdapterId": execution.runner_adapter_id,
            "selectors": selectors,
        },
    )
    artifact_evidence = [
        {
            "id": artifact.artifact_id,
            "usage": artifact.usage,
            "sha256": artifact.sha256,
            "status": "declared-by-normalized-case",
        }
        for artifact in consumer_case.artifacts
        if artifact.usage in execution.required_artifact_usages
    ]
    provenance: dict[str, Any] = {
        "consumerCaseID": consumer_case.case_id,
        "executionID": execution.execution_id,
        "versions": dict(consumer_case.versions),
        "artifactEvidence": artifact_evidence,
    }
    if requested_case in {"pyspark", "scala-spark"}:
        connector, connector_evidence = configured_artifact(
            consumer_case,
            "spark-connector-dsv1-jar",
            "BQEMU_SPARK_CONNECTOR_JAR",
        )
        hadoop_gcs, hadoop_evidence = configured_artifact(
            consumer_case,
            "hadoop-gcs-connector-jar",
            "BQEMU_HADOOP_GCS_CONNECTOR_JAR",
        )
        provenance["connectorPath"] = str(connector)
        provenance["hadoopGCSPath"] = str(hadoop_gcs)
        provenance["artifactEvidence"] = [
            evidence
            for evidence in artifact_evidence
            if evidence["usage"]
            not in {"spark-connector-dsv1-jar", "hadoop-gcs-connector-jar"}
        ] + [connector_evidence, hadoop_evidence]
    infrastructure = read_locked_json(
        settings.infrastructure_lock, "bqemu-load-infrastructure/v1"
    )
    infrastructure_provenance = validate_infrastructure_lock(
        settings, infrastructure
    )
    provenance["infrastructureLockSha256"] = infrastructure_provenance[
        "infrastructureLockSha256"
    ]
    return provenance, infrastructure_provenance


def main() -> int:
    arguments = parse_arguments()
    started = time.monotonic()
    results: list[CaseResult] = []
    captured: ContractError | None = None
    try:
        settings = Settings.from_environment()
        provenance, infrastructure = case_provenance(settings, arguments.case)
        image = ensure_fake_gcs_image(settings, infrastructure)
        results.append(run_case(arguments.case, settings, provenance, image))
    except ContractError as error:
        captured = error
    except Exception as error:
        captured = failure(
            stage="runner",
            service="load-e2e",
            model_version=MODEL_VERSION,
            operation="unhandled-error",
            shape=type(error).__name__,
            observed={"type": type(error).__name__, "error": repr(error)},
            fix_hint="add-a-diagnostic-classification",
        )
    write_junit(arguments.junit, results, captured, time.monotonic() - started)
    if captured is not None:
        event(status="failed", suite="load-public-process", **captured.fields)
        return 1
    event(status="passed", suite="load-public-process", cases=len(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
