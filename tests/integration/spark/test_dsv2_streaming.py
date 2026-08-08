"""Released DSv2 connector Structured Streaming contracts.

Raw and patched variants always run in isolated Spark processes. The artifact
cache may hold both variants, but the JVM classpath boundary rejects zero,
duplicate, mixed, or checksum-drifting connector selections before Spark
starts.

Pinned provider and streaming sources:
https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/719817782a214b8ca72be520870013a3e0253d92/spark-bigquery-dsv2/spark-3.5-bigquery-lib/src/main/java/com/google/cloud/spark/bigquery/v2/Spark35BigQueryTableProvider.java
https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/719817782a214b8ca72be520870013a3e0253d92/spark-bigquery-dsv2/spark-3.1-bigquery-lib/src/main/java/com/google/cloud/spark/bigquery/v2/BigQueryStreamingWrite.java#L23-L30
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import uuid

import pytest

from artifact_variants import (
    ArtifactClasspathError,
    DSV1_VARIANT,
    DSV2_RAW_VARIANT,
    artifact_spec_from_json,
    enforce_connector_classpath,
)
from conftest import (
    PublicEdge,
    REPOSITORY_ROOT,
    _run,
    assert_ordered_operations,
    create_table,
    observe_dsv2_exact_streaming_flow,
    public_edge_log_position,
    query,
    record_capability,
    record_capability_gap,
    runtime_versions,
)


@pytest.mark.capability("SBQ-DSV2-ARTIFACT-CLASSPATH-GUARD-V1")
@pytest.mark.parametrize(
    ("case", "expected_stage", "expected_shape"),
    (
        ("zero", "connector-count", "connector-count:0"),
        ("two", "connector-count", "connector-count:2"),
        ("mixed", "connector-count", "mixed-variants:2"),
        ("hash", "artifact-hash", "maven-jar:size:"),
    ),
)
def test_dsv2_connector_classpath_fails_closed(
    case: str,
    expected_stage: str,
    expected_shape: str,
    connector_jar: Path,
    dsv2_connector_jar: Path,
    tmp_path: Path,
) -> None:
    recognized_specs = {
        DSV1_VARIANT: artifact_spec_from_json(
            os.environ["BQEMU_SPARK_CONNECTOR_SPEC_JSON"]
        ),
        DSV2_RAW_VARIANT: artifact_spec_from_json(
            os.environ["BQEMU_SPARK_DSV2_CONNECTOR_SPEC_JSON"]
        ),
    }
    if case == "zero":
        classpath: list[Path] = []
    elif case == "two":
        classpath = [dsv2_connector_jar, dsv2_connector_jar]
    elif case == "mixed":
        classpath = [connector_jar, dsv2_connector_jar]
    else:
        drifted = tmp_path / "drifted-spark-bigquery.jar"
        drifted.write_bytes(b"checksum-drift")
        classpath = [drifted]

    with pytest.raises(ArtifactClasspathError) as raised:
        enforce_connector_classpath(
            classpath,
            expected_variant=DSV2_RAW_VARIANT,
            repository_root=REPOSITORY_ROOT,
            recognized_specs=recognized_specs,
        )
    diagnostic = str(raised.value)
    versions = runtime_versions()
    for fragment in (
        f"version={versions['connector']}",
        "operation=spark-connector-classpath",
        f"stage={expected_stage}",
        f"shape={expected_shape}",
        "fingerprint=sha256:",
        "fix_hint=",
    ):
        assert fragment in diagnostic
    assert re.search(r"fingerprint=sha256:[a-f0-9]{64}(?:\s|$)", diagnostic)
    for path in classpath:
        assert str(path) not in diagnostic

    record_capability(
        "SBQ-DSV2-ARTIFACT-CLASSPATH-GUARD-V1",
        f"negative-case:{case} stage:{expected_stage}",
    )


@pytest.mark.capability("SBQ-DSV2-ARTIFACT-CLASSPATH-GUARD-V1")
def test_dsv2_diagnostics_retain_runtime_values() -> None:
    diagnostic = "\n".join(
        (
            "projects/private-project/datasets/private-dataset/"
            "tables/private-table/streams/private-stream",
            'project_id":"private-keyed-project",table_id="private-keyed-table"',
            "seeded_table='spark-contract-a1b2c3d4.private-dataset.private-seeded'",
            "-Djavax.net.ssl.trustStorePassword=private-password",
            'gcpAccessToken:"private-token"',
            'sql:"SELECT private-row FROM private-table"',
            "type=PENDING view=BASIC",
        )
    )
    for secret in (
        "private-project",
        "private-dataset",
        "private-table",
        "private-stream",
        "private-keyed-project",
        "private-keyed-table",
        "private-seeded",
        "private-password",
        "private-token",
        "private-row",
        "SELECT",
    ):
        assert secret in diagnostic
    record_capability(
        "SBQ-DSV2-ARTIFACT-CLASSPATH-GUARD-V1",
        "diagnostics:raw runtime-values:retained",
    )


@pytest.mark.capability("SBQ-DSV2-RAW-STREAM-EXACT-APPEND-V1")
def test_raw_dsv2_exact_streaming_finalizes_without_commit(
    dsv2_connector_jar: Path,
    public_edge: PublicEdge,
    test_timeout: float,
    tmp_path: Path,
) -> None:
    table_id = "dsv2_raw_" + uuid.uuid4().hex[:8]
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
    input_rows = tuple(
        {
            "id": index,
            "active": index % 2 == 0,
            "score": index + 0.25,
            "payload": f"value-{index}",
        }
        for index in range(4)
    )
    (input_directory / "batch-00000.json").write_text(
        "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in input_rows),
        encoding="utf-8",
    )
    result_path = tmp_path / "result.json"
    runner_config = tmp_path / "runner.json"
    runner_config.write_text(
        json.dumps(
            {
                "connectorClasspath": [str(dsv2_connector_jar)],
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
                "testTimeoutSeconds": test_timeout,
                "rpcTimeoutSeconds": float(
                    os.getenv("BQEMU_SPARK_RPC_TIMEOUT_SECONDS", "30")
                ),
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    runner_config.chmod(0o600)

    log_position = public_edge_log_position(public_edge)
    _run(
        [
            os.sys.executable,
            str(REPOSITORY_ROOT / "tests" / "integration" / "spark" / "run_dsv2_streaming.py"),
            "--config",
            str(runner_config),
        ],
        cwd=REPOSITORY_ROOT,
        timeout=test_timeout,
        stage="dsv2-raw-streaming",
    )
    result = json.loads(result_path.read_text(encoding="utf-8"))
    versions = runtime_versions()
    if (
        result.get("variant") != DSV2_RAW_VARIANT
        or result.get("sparkVersion") != versions["spark"]
        or result.get("scalaVersion") != versions["scala"]
        or result.get("javaVersion") != versions["java"]
        or result.get("provider") != "Spark35BigQueryTableProvider"
        or result.get("serviceProviderCount") != 1
        or result.get("listedJarCount") != 1
        or result.get("providerCodeSourceMatches") is not True
        or result.get("writerContextCodeSourceMatches") is not True
    ):
        pytest.fail("DSv2 runner identity mismatch shape=runtime-provider")
    batches = result.get("batches")
    if (
        not isinstance(batches, list)
        or len(batches) != 1
        or int(batches[0].get("inputRows", -1)) != 4
    ):
        pytest.fail("DSv2 input progress mismatch shape=expected-rows:4")

    observation = observe_dsv2_exact_streaming_flow(
        public_edge, since=log_position
    )
    assert_ordered_operations(
        observation,
        (
            "CreateWriteStream",
            "AppendRows",
            "FinalizeWriteStream",
        ),
    )
    counts = observation["counts"]
    expected_counts = {
        "CreateWriteStream": 1,
        "GetWriteStream": 0,
        "AppendRows": 1,
        "FinalizeWriteStream": 1,
        "BatchCommitWriteStreams": 0,
    }
    if not isinstance(counts, dict) or any(
        counts.get(operation, 0) != expected
        for operation, expected in expected_counts.items()
    ):
        pytest.fail("raw DSv2 RPC count mismatch shape=single-partition")
    expected_observation = {
        "create_types": ("PENDING",),
        "get_views": (),
        "append_batches": 1,
        "append_rows": 4,
        "append_offsets": (0,),
        "batch_commit_calls": 0,
        "committed_rows": 0,
        "stream_lifecycle_correlated": True,
    }
    for field, expected in expected_observation.items():
        if observation.get(field) != expected:
            pytest.fail(
                "raw DSv2 observation mismatch "
                f"shape={field}:expected:{expected}"
            )

    count_result = query(
        public_edge,
        test_timeout,
        f"SELECT COUNT(*) AS row_count FROM `{public_edge.project_id}.{public_edge.dataset_id}.{table_id}`",
    )
    if count_result["rows"][0]["f"][0]["v"] != "0":
        pytest.fail("raw DSv2 pending rows became visible shape=expected-count:0")

    record_capability_gap(
        "SBQ-DSV2-RAW-STREAM-EXACT-APPEND-V1",
        (
            f"provider:Spark35BigQueryTableProvider spark:{versions['spark']} "
            f"scala:{versions['scala']} "
            "partitions:1 pending-streams:1 get-write-stream-calls:0 append-batches:1 "
            "finalized-streams:1 batch-commit-calls:0 visible-rows:0"
        ),
        "build-version-locked-overlay-after-raw-contract-freeze",
    )
