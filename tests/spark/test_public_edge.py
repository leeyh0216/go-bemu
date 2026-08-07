"""First released-connector vertical slice across the public TLS edge.

The test capability marker is resolved against the machine-readable matrix.
Known gaps become strict xfails; an unexpected pass fails so the matrix cannot
silently remain stale.

Official connector source:
https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/719817782a214b8ca72be520870013a3e0253d92
"""

from __future__ import annotations

import hashlib
import json
import uuid

import pytest

from conftest import (
    PublicEdge,
    connector_options,
    create_table,
    load_connector_source,
    observe_default_append_offsets,
    public_edge_log_position,
    query,
    record_capability,
)


@pytest.fixture(scope="session")
def seeded_table(public_edge: PublicEdge, test_timeout: float) -> str:
    table_id = "read_rows"
    create_table(
        public_edge,
        test_timeout,
        table_id,
        [
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "label", "type": "STRING", "mode": "NULLABLE"},
            {"name": "score", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "active", "type": "BOOLEAN", "mode": "NULLABLE"},
        ],
    )
    query(
        public_edge,
        test_timeout,
        (
            f"INSERT INTO `{public_edge.project_id}.{public_edge.dataset_id}.{table_id}` "
            "VALUES (1, 'one', 1.5, true), (2, 'two', 2.5, false), "
            "(3, 'three', 3.5, true), (4, 'four', 4.5, false)"
        ),
    )
    return f"{public_edge.project_id}.{public_edge.dataset_id}.{table_id}"


@pytest.mark.parametrize(
    ("wire_format", "requested_streams"),
    [
        pytest.param(
            "ARROW",
            1,
            marks=pytest.mark.capability("SBQ-READ-STREAM-ONE-V1"),
            id="arrow-1",
        ),
        pytest.param(
            "ARROW",
            2,
            marks=pytest.mark.capability("SBQ-READ-STREAM-TWO-V1"),
            id="arrow-2",
        ),
        pytest.param(
            "ARROW",
            4,
            marks=pytest.mark.capability("SBQ-READ-ARROW-TABLE-V1"),
            id="arrow-4",
        ),
        pytest.param(
            "ARROW",
            16,
            marks=pytest.mark.capability("SBQ-READ-ARROW-STREAM-SIXTEEN-V1"),
            id="arrow-16",
        ),
        pytest.param(
            "AVRO",
            1,
            marks=pytest.mark.capability("SBQ-READ-AVRO-STREAM-ONE-V1"),
            id="avro-1",
        ),
        pytest.param(
            "AVRO",
            2,
            marks=pytest.mark.capability("SBQ-READ-AVRO-STREAM-TWO-V1"),
            id="avro-2",
        ),
        pytest.param(
            "AVRO",
            4,
            marks=pytest.mark.capability("SBQ-READ-AVRO-TABLE-V1"),
            id="avro-4",
        ),
        pytest.param(
            "AVRO",
            16,
            marks=pytest.mark.capability("SBQ-READ-AVRO-STREAM-SIXTEEN-V1"),
            id="avro-16",
        ),
    ],
)
def test_storage_read_arrow_and_avro(
    spark_session,
    public_edge: PublicEdge,
    seeded_table: str,
    wire_format: str,
    requested_streams: int,
) -> None:
    frame = load_connector_source(
        spark_session,
        public_edge,
        source=seeded_table,
        source_kind="table",
        wire_format=wire_format,
        requested_streams=requested_streams,
    )
    rows = frame.select("id", "label", "score", "active").orderBy("id").collect()
    actual = [(row.id, row.label, row.score, row.active) for row in rows]
    expected = [
        (1, "one", 1.5, True),
        (2, "two", 2.5, False),
        (3, "three", 3.5, True),
        (4, "four", 4.5, False),
    ]
    if actual != expected:
        fingerprint = hashlib.sha256(
            json.dumps(actual, default=str, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        pytest.fail(
            f"row mismatch shape=row-count:{len(actual)} fingerprint=sha256:{fingerprint}"
        )
    actual_partitions = frame.rdd.getNumPartitions()
    if actual_partitions != requested_streams:
        pytest.fail(
            f"partition mismatch shape=actual:{actual_partitions},expected:{requested_streams}"
        )
    capability_ids = {
        ("ARROW", 1): "SBQ-READ-STREAM-ONE-V1",
        ("ARROW", 2): "SBQ-READ-STREAM-TWO-V1",
        ("ARROW", 4): "SBQ-READ-ARROW-TABLE-V1",
        ("ARROW", 16): "SBQ-READ-ARROW-STREAM-SIXTEEN-V1",
        ("AVRO", 1): "SBQ-READ-AVRO-STREAM-ONE-V1",
        ("AVRO", 2): "SBQ-READ-AVRO-STREAM-TWO-V1",
        ("AVRO", 4): "SBQ-READ-AVRO-TABLE-V1",
        ("AVRO", 16): "SBQ-READ-AVRO-STREAM-SIXTEEN-V1",
    }
    record_capability(
        capability_ids[(wire_format, requested_streams)],
        f"{wire_format.lower()}-rows+streams:{requested_streams}",
    )


@pytest.mark.parametrize(
    "logical_partitions",
    [
        pytest.param(
            1,
            marks=pytest.mark.capability(
                "SBQ-WRITE-DIRECT-EXACT-APPEND-ONE-V1"
            ),
        ),
        pytest.param(
            2,
            marks=pytest.mark.capability(
                "SBQ-WRITE-DIRECT-EXACT-APPEND-TWO-V1"
            ),
        ),
        pytest.param(
            4,
            marks=pytest.mark.capability(
                "SBQ-WRITE-DIRECT-EXACT-APPEND-FOUR-V1"
            ),
        ),
    ],
)
def test_direct_pending_exact_append(
    spark_session,
    public_edge: PublicEdge,
    test_timeout: float,
    logical_partitions: int,
) -> None:
    table_id = f"pending_{logical_partitions}_{uuid.uuid4().hex[:8]}"
    create_table(
        public_edge,
        test_timeout,
        table_id,
        [
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "payload", "type": "STRING", "mode": "NULLABLE"},
        ],
    )
    frame = spark_session.createDataFrame(
        [(index, f"row-{index}") for index in range(8)], ["id", "payload"]
    ).repartition(logical_partitions)
    writer = frame.write.format("bigquery")
    for key, value in connector_options(public_edge).items():
        writer = writer.option(key, value)
    (
        writer.option("writeMethod", "direct")
        .option("writeAtLeastOnce", "false")
        .mode("append")
        .save(f"{public_edge.project_id}.{public_edge.dataset_id}.{table_id}")
    )
    result = query(
        public_edge,
        test_timeout,
        f"SELECT COUNT(*) AS rows FROM `{public_edge.project_id}.{public_edge.dataset_id}.{table_id}`",
    )
    if result["rows"][0]["f"][0]["v"] != "8":
        pytest.fail("committed row count mismatch shape=single-count")
    partition_name = {1: "ONE", 2: "TWO", 4: "FOUR"}[logical_partitions]
    record_capability(
        f"SBQ-WRITE-DIRECT-EXACT-APPEND-{partition_name}-V1",
        f"pending-streams:{logical_partitions}",
    )


@pytest.mark.parametrize(
    "logical_partitions",
    [
        pytest.param(
            1,
            marks=pytest.mark.capability("SBQ-WRITE-DIRECT-ALO-APPEND-ONE-V1"),
        ),
        pytest.param(
            2,
            marks=pytest.mark.capability("SBQ-WRITE-DIRECT-ALO-APPEND-TWO-V1"),
        ),
        pytest.param(
            4,
            marks=pytest.mark.capability("SBQ-WRITE-DIRECT-ALO-APPEND-FOUR-V1"),
        ),
    ],
)
def test_direct_default_stream_at_least_once(
    spark_session,
    public_edge: PublicEdge,
    test_timeout: float,
    logical_partitions: int,
) -> None:
    table_id = f"default_{logical_partitions}_{uuid.uuid4().hex[:8]}"
    create_table(
        public_edge,
        test_timeout,
        table_id,
        [
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "payload", "type": "STRING", "mode": "NULLABLE"},
        ],
    )
    frame = spark_session.createDataFrame([(1, "one"), (2, "two")], ["id", "payload"])
    partitioned = frame.repartition(logical_partitions)
    if partitioned.rdd.getNumPartitions() != logical_partitions:
        pytest.fail(
            "default stream source partition mismatch "
            f"shape=actual:{partitioned.rdd.getNumPartitions()},expected:{logical_partitions}"
        )
    writer = partitioned.write.format("bigquery")
    for key, value in connector_options(public_edge).items():
        writer = writer.option(key, value)
    log_position = public_edge_log_position(public_edge)
    (
        writer.option("writeMethod", "direct")
        .option("writeAtLeastOnce", "true")
        .mode("append")
        .save(f"{public_edge.project_id}.{public_edge.dataset_id}.{table_id}")
    )
    append_batches, appended_rows = observe_default_append_offsets(
        public_edge, since=log_position
    )
    if appended_rows != 2:
        pytest.fail(
            f"default append row mismatch shape=observed:{appended_rows},expected:2"
        )
    result = query(
        public_edge,
        test_timeout,
        f"SELECT COUNT(*) AS rows FROM `{public_edge.project_id}.{public_edge.dataset_id}.{table_id}`",
    )
    if result["rows"][0]["f"][0]["v"] != "2":
        pytest.fail("default stream row count mismatch shape=single-count")
    partition_name = {1: "ONE", 2: "TWO", 4: "FOUR"}[logical_partitions]
    record_capability(
        f"SBQ-WRITE-DIRECT-ALO-APPEND-{partition_name}-V1",
        (
            f"default-stream:partitions:{logical_partitions} "
            f"append-batches:{append_batches} offset-end:{appended_rows}"
        ),
    )
