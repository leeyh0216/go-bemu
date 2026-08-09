"""Real Spark 3.5.8 normal-path acceptance for the one-class DSv2 overlay.

This test intentionally proves less than exactly-once. It covers two ordinary
epochs against a pre-existing table and a no-input restart from the same
checkpoint. Replay after an ambiguous commit, repeated commit for one epoch,
partial abort messages, and new-table abort remain explicit matrix gaps.

Official contracts:
https://spark.apache.org/docs/3.5.8/api/java/org/apache/spark/sql/connector/write/streaming/StreamingWrite.html
https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/719817782a214b8ca72be520870013a3e0253d92/spark-bigquery-dsv2/spark-3.1-bigquery-lib/src/main/java/com/google/cloud/spark/bigquery/v2/BigQueryStreamingWrite.java#L23-L41
https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.BatchCommitWriteStreams
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import uuid

import pytest

from artifact_variants import DSV2_OVERLAY_VARIANT
from conftest import (
    PublicEdge,
    REPOSITORY_ROOT,
    _json_request,
    _run,
    create_table,
    observe_dsv2_exact_streaming_flow,
    public_edge_log_position,
    query,
    record_capability,
)


CAPABILITY = "SBQ-DSV2-OVERLAY-STREAM-NORMAL-APPEND-V1"


def _run_overlay(
    *,
    connector_jar: Path,
    overlay_jar: Path,
    public_edge: PublicEdge,
    table_id: str,
    input_directory: Path,
    checkpoint_directory: Path,
    result_path: Path,
    config_path: Path,
    timeout: float,
) -> dict[str, object]:
    config_path.write_text(
        json.dumps(
            {
                "connectorClasspath": [str(connector_jar)],
                "overlayClasspath": [str(overlay_jar)],
                "httpEndpoint": public_edge.http_endpoint,
                "grpcEndpoint": public_edge.grpc_endpoint,
                "projectId": public_edge.project_id,
                "datasetId": public_edge.dataset_id,
                "tableId": table_id,
                "inputDirectory": str(input_directory),
                "checkpointDirectory": str(checkpoint_directory),
                "truststore": str(public_edge.truststore),
                "truststorePassword": "bqemu-test-only",
                "jvmLog": str(public_edge.jvm_log_path),
                "resultPath": str(result_path),
                "testTimeoutSeconds": timeout,
                "rpcTimeoutSeconds": float(
                    os.getenv("BQEMU_SPARK_RPC_TIMEOUT_SECONDS", "30")
                ),
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    config_path.chmod(0o600)
    _run(
        [
            os.sys.executable,
            str(REPOSITORY_ROOT / "tests" / "spark" / "run_dsv2_streaming.py"),
            "--config",
            str(config_path),
        ],
        cwd=REPOSITORY_ROOT,
        timeout=timeout,
        stage="dsv2-overlay-streaming",
    )
    return json.loads(result_path.read_text(encoding="utf-8"))


def _assert_runtime_identity(result: dict[str, object]) -> None:
    expected = {
        "variant": DSV2_OVERLAY_VARIANT,
        "sparkVersion": "3.5.8",
        "scalaVersion": "2.12.18",
        "provider": "Spark35BigQueryTableProvider",
        "serviceProviderCount": 1,
        "listedJarCount": 0,
        "providerCodeSourceMatches": True,
        "writerContextCodeSourceMatches": True,
        "streamingHookCount": 2,
        "runtimeClasspathOrderMatches": True,
    }
    if any(result.get(key) != value for key, value in expected.items()):
        pytest.fail("DSv2 overlay runtime identity mismatch shape=provider-or-codesource")


@pytest.mark.capability(CAPABILITY)
def test_overlay_commits_two_microbatches_and_no_input_restart_is_quiet(
    dsv2_connector_jar: Path,
    dsv2_overlay_jar: Path,
    public_edge: PublicEdge,
    test_timeout: float,
    tmp_path: Path,
) -> None:
    table_id = "dsv2_overlay_" + uuid.uuid4().hex[:8]
    create_table(
        public_edge,
        test_timeout,
        table_id,
        [
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "active", "type": "BOOLEAN", "mode": "REQUIRED"},
            {"name": "score", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "payload", "type": "STRING", "mode": "NULLABLE"},
        ],
    )
    input_directory = tmp_path / "input"
    checkpoint_directory = tmp_path / "checkpoint"
    input_directory.mkdir()
    for batch in range(2):
        rows = (
            {
                "id": batch * 2 + index,
                "active": index % 2 == 0,
                "score": batch * 2 + index + 0.25,
                "payload": f"value-{batch}-{index}",
            }
            for index in range(2)
        )
        (input_directory / f"batch-{batch:05d}.json").write_text(
            "".join(
                json.dumps(row, separators=(",", ":")) + "\n" for row in rows
            ),
            encoding="utf-8",
        )

    result_path = tmp_path / "result.json"
    config_path = tmp_path / "runner.json"
    log_position = public_edge_log_position(public_edge)
    result = _run_overlay(
        connector_jar=dsv2_connector_jar,
        overlay_jar=dsv2_overlay_jar,
        public_edge=public_edge,
        table_id=table_id,
        input_directory=input_directory,
        checkpoint_directory=checkpoint_directory,
        result_path=result_path,
        config_path=config_path,
        timeout=test_timeout,
    )
    _assert_runtime_identity(result)
    batches = result.get("batches")
    batch_shape = tuple(
        (int(item.get("batchId", -1)), int(item.get("inputRows", -1)))
        for item in batches
    ) if isinstance(batches, list) else ()
    if batch_shape != ((0, 2), (1, 2)):
        pytest.fail("overlay progress mismatch shape=epochs:2,rows-per-epoch:2")

    observation = observe_dsv2_exact_streaming_flow(public_edge, since=log_position)
    expected_counts = {
        "CreateWriteStream": 2,
        "GetWriteStream": 0,
        "AppendRows": 2,
        "FinalizeWriteStream": 2,
        "BatchCommitWriteStreams": 2,
    }
    counts = observation["counts"]
    if not isinstance(counts, dict) or any(
        counts.get(operation, 0) != expected
        for operation, expected in expected_counts.items()
    ):
        pytest.fail("overlay RPC count mismatch shape=two-single-partition-epochs")
    expected_observation = {
        "create_types": ("PENDING", "PENDING"),
        "get_views": (),
        "append_batches": 2,
        "append_rows": 4,
        "append_offsets": (0, 0),
        "batch_commit_calls": 2,
        "committed_rows": 4,
        "commit_transactions": ((1, 2, "committed"), (1, 2, "committed")),
        "stream_lifecycle_correlated": True,
    }
    for field, expected in expected_observation.items():
        if observation.get(field) != expected:
            pytest.fail(f"overlay observation mismatch shape={field}")

    count_result = query(
        public_edge,
        test_timeout,
        f"SELECT COUNT(*) AS rows FROM `{public_edge.project_id}.{public_edge.dataset_id}.{table_id}`",
    )
    if count_result["rows"][0]["f"][0]["v"] != "4":
        pytest.fail("overlay rows are not visible shape=expected-count:4")

    restart_position = public_edge_log_position(public_edge)
    restart = _run_overlay(
        connector_jar=dsv2_connector_jar,
        overlay_jar=dsv2_overlay_jar,
        public_edge=public_edge,
        table_id=table_id,
        input_directory=input_directory,
        checkpoint_directory=checkpoint_directory,
        result_path=result_path,
        config_path=config_path,
        timeout=test_timeout,
    )
    _assert_runtime_identity(restart)
    restart_batches = restart.get("batches")
    if not isinstance(restart_batches, list) or any(
        int(item.get("inputRows", -1)) != 0 for item in restart_batches
    ):
        pytest.fail("same-checkpoint restart consumed input shape=expected-input-rows:0")
    restart_observation = observe_dsv2_exact_streaming_flow(
        public_edge, since=restart_position
    )
    if any(restart_observation["counts"].values()):
        pytest.fail("no-input restart performed Storage Write RPCs shape=expected-calls:0")
    count_after_restart = query(
        public_edge,
        test_timeout,
        f"SELECT COUNT(*) AS rows FROM `{public_edge.project_id}.{public_edge.dataset_id}.{table_id}`",
    )
    if count_after_restart["rows"][0]["f"][0]["v"] != "4":
        pytest.fail("no-input restart changed visibility shape=expected-count:4")

    table_url = (
        public_edge.http_endpoint
        + f"/bigquery/v2/projects/{public_edge.project_id}/datasets/{public_edge.dataset_id}/tables/{table_id}"
    )
    _json_request(public_edge, table_url, "DELETE", test_timeout)
    missing = _json_request(
        public_edge,
        table_url,
        "GET",
        test_timeout,
        allowed_statuses=frozenset({404}),
    )
    if missing is not None:
        pytest.fail("overlay E2E destination cleanup failed shape=table-still-visible")

    record_capability(
        CAPABILITY,
        (
            "normal-path-only epochs:2 rows-per-epoch:2 create:2 append:2 "
            "finalize:2 batch-commit:2 visible-rows:4 commit-tx:2 "
            "same-checkpoint-no-input-rpcs:0 destination-cleanup:verified"
        ),
    )
