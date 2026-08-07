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
    assert_ordered_operations,
    connector_options,
    create_table,
    load_connector_source,
    observe_default_append_offsets,
    observe_query_read_flow,
    public_edge_log_position,
    query,
    raise_known_gap,
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


def _row_fingerprint(rows: list[tuple[object, ...]]) -> str:
    return hashlib.sha256(
        json.dumps(rows, default=str, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _assert_rows(actual: list[tuple[object, ...]], expected: list[tuple[object, ...]]) -> None:
    if actual == expected:
        return
    fingerprint = _row_fingerprint(actual)
    pytest.fail(
        "query row mismatch "
        f"shape=row-count:{len(actual)} fingerprint=sha256:{fingerprint}"
    )


def _assert_operation_counts(
    observation: dict[str, object],
    *,
    exact: dict[str, int],
    minimum: dict[str, int] | None = None,
) -> None:
    counts = observation["counts"]
    if not isinstance(counts, dict):
        raise AssertionError("query operation counts have an invalid shape")
    mismatches = [
        f"{operation}:{counts.get(operation, 0)}!={expected}"
        for operation, expected in exact.items()
        if counts.get(operation, 0) != expected
    ]
    mismatches.extend(
        f"{operation}:{counts.get(operation, 0)}<{expected}"
        for operation, expected in (minimum or {}).items()
        if counts.get(operation, 0) < expected
    )
    if mismatches:
        raise AssertionError(
            "query operation count mismatch shape=" + ",".join(sorted(mismatches))
        )


def _assert_anonymous_query_counts(
    observation: dict[str, object], *, read_rows: int
) -> None:
    _assert_operation_counts(
        observation,
        exact={
            "jobs.insert": 1,
            "jobs.get": 1,
            "tables.get": 2,
            "tables.patch": 0,
            "jobs.query": 0,
            "tabledata.list": 0,
            "CreateReadSession": 1,
            "ReadRows": read_rows,
        },
        minimum={"jobs.getQueryResults": 1},
    )
    if observation["anonymous_destinations"] != 1:
        raise AssertionError("query did not produce exactly one anonymous destination")


def _assert_read_session_shape(
    observation: dict[str, object],
    *,
    wire_format: str,
    selected_field_count: int,
    has_row_restriction: bool,
) -> None:
    shapes = observation["read_session_shapes"]
    expected_restriction = (
        (lambda value: value > 0)
        if has_row_restriction
        else (lambda value: value == 0)
    )
    matching = [
        shape
        for shape in shapes
        if shape.get("format") == wire_format
        and shape.get("selected_field_count") == selected_field_count
        and expected_restriction(shape.get("row_restriction_bytes"))
    ]
    if len(matching) != 1:
        raise AssertionError(
            "read session shape mismatch "
            f"shape=sessions:{len(shapes)},matching:{len(matching)}"
        )


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
    ("wire_format", "capability_id", "predicate"),
    [
        pytest.param(
            "ARROW",
            "SBQ-READ-ARROW-QUERY-V1",
            "id >= 2",
            marks=pytest.mark.capability("SBQ-READ-ARROW-QUERY-V1"),
            id="query-arrow",
        ),
        pytest.param(
            "AVRO",
            "SBQ-READ-AVRO-QUERY-V1",
            "id BETWEEN 2 AND 4",
            marks=pytest.mark.capability("SBQ-READ-AVRO-QUERY-V1"),
            id="query-avro",
        ),
    ],
)
def test_query_source_arrow_and_avro(
    spark_session,
    public_edge: PublicEdge,
    seeded_table: str,
    wire_format: str,
    capability_id: str,
    predicate: str,
) -> None:
    source_query = (
        f"SELECT id, label, score, active FROM `{seeded_table}` "
        f"WHERE {predicate}"
    )
    log_position = public_edge_log_position(public_edge)
    frame = load_connector_source(
        spark_session,
        public_edge,
        source=source_query,
        source_kind="query",
        wire_format=wire_format,
        requested_streams=4,
    )
    rows = frame.select("id", "label", "score", "active").collect()
    actual = sorted((row.id, row.label, row.score, row.active) for row in rows)
    _assert_rows(
        actual,
        [
            (2, "two", 2.5, False),
            (3, "three", 3.5, True),
            (4, "four", 4.5, False),
        ],
    )
    observation = observe_query_read_flow(public_edge, since=log_position)
    assert_ordered_operations(
        observation,
        (
            "jobs.insert",
            "jobs.getQueryResults",
            "jobs.get",
            "tables.get",
            "tables.get",
            "CreateReadSession",
            "ReadRows",
        ),
    )
    _assert_anonymous_query_counts(observation, read_rows=4)
    _assert_read_session_shape(
        observation,
        wire_format=wire_format,
        selected_field_count=4,
        has_row_restriction=False,
    )
    record_capability(
        capability_id,
        (
            f"{wire_format.lower()}-query rows:{len(actual)} "
            "anonymous-destinations:1 job-poll:observed patch-calls:0 "
            f"storage-read:observed row-fingerprint:sha256:{_row_fingerprint(actual)}"
        ),
    )


@pytest.mark.capability("SBQ-READ-ARROW-QUERY-PROJECTION-V1")
def test_query_source_projection(
    spark_session,
    public_edge: PublicEdge,
    seeded_table: str,
) -> None:
    source_query = (
        f"SELECT id, label, score, active FROM `{seeded_table}` "
        "WHERE id >= 1 AND active IN (TRUE, FALSE)"
    )
    log_position = public_edge_log_position(public_edge)
    frame = load_connector_source(
        spark_session,
        public_edge,
        source=source_query,
        source_kind="query",
        wire_format="ARROW",
        requested_streams=4,
    )
    rows = frame.select("id", "label").collect()
    actual = sorted((row.id, row.label) for row in rows)
    _assert_rows(actual, [(1, "one"), (2, "two"), (3, "three"), (4, "four")])
    observation = observe_query_read_flow(public_edge, since=log_position)
    assert_ordered_operations(
        observation,
        (
            "jobs.insert",
            "jobs.getQueryResults",
            "jobs.get",
            "tables.get",
            "tables.get",
            "CreateReadSession",
            "ReadRows",
        ),
    )
    _assert_anonymous_query_counts(observation, read_rows=4)
    _assert_read_session_shape(
        observation,
        wire_format="ARROW",
        selected_field_count=2,
        has_row_restriction=False,
    )
    record_capability(
        "SBQ-READ-ARROW-QUERY-PROJECTION-V1",
        (
            "arrow-query projection-fields:2 rows:4 storage-read:observed "
            f"row-fingerprint:sha256:{_row_fingerprint(actual)}"
        ),
    )


@pytest.mark.capability("SBQ-READ-ARROW-QUERY-FILTER-V1")
def test_query_source_filter_pushdown(
    spark_session,
    public_edge: PublicEdge,
    seeded_table: str,
) -> None:
    source_query = (
        f"SELECT id, label, score, active FROM `{seeded_table}` "
        "WHERE id >= 1 AND score >= 0"
    )
    log_position = public_edge_log_position(public_edge)
    frame = load_connector_source(
        spark_session,
        public_edge,
        source=source_query,
        source_kind="query",
        wire_format="ARROW",
        requested_streams=4,
    )
    rows = frame.where("score >= 3").select("id").collect()
    actual = sorted((row.id,) for row in rows)
    _assert_rows(actual, [(3,), (4,)])
    observation = observe_query_read_flow(public_edge, since=log_position)
    assert_ordered_operations(
        observation,
        (
            "jobs.insert",
            "jobs.getQueryResults",
            "jobs.get",
            "tables.get",
            "tables.get",
            "CreateReadSession",
            "ReadRows",
        ),
    )
    _assert_anonymous_query_counts(observation, read_rows=4)
    _assert_read_session_shape(
        observation,
        wire_format="ARROW",
        selected_field_count=1,
        has_row_restriction=True,
    )
    record_capability(
        "SBQ-READ-ARROW-QUERY-FILTER-V1",
        (
            "arrow-query filter-pushdown:present selected-fields:1 rows:2 "
            f"row-fingerprint:sha256:{_row_fingerprint(actual)}"
        ),
    )


@pytest.mark.capability("SBQ-READ-ARROW-QUERY-MATERIALIZED-V1")
def test_query_source_explicit_materialization_patches_expiration(
    spark_session,
    public_edge: PublicEdge,
    seeded_table: str,
) -> None:
    source_query = (
        f"SELECT id, label, score, active FROM `{seeded_table}` "
        "WHERE id >= 3 AND active IN (TRUE, FALSE)"
    )
    log_position = public_edge_log_position(public_edge)
    frame = load_connector_source(
        spark_session,
        public_edge,
        source=source_query,
        source_kind="query",
        wire_format="ARROW",
        requested_streams=1,
        extra_options={
            "materializationProject": public_edge.project_id,
            "materializationDataset": public_edge.dataset_id,
        },
    )
    rows = frame.select("id").collect()
    actual = sorted((row.id,) for row in rows)
    _assert_rows(actual, [(3,), (4,)])
    observation = observe_query_read_flow(public_edge, since=log_position)
    assert_ordered_operations(
        observation,
        (
            "jobs.insert",
            "jobs.getQueryResults",
            "jobs.get",
            "tables.get",
            "tables.patch",
            "tables.get",
            "CreateReadSession",
            "ReadRows",
        ),
    )
    _assert_operation_counts(
        observation,
        exact={
            "jobs.insert": 1,
            "jobs.get": 1,
            "tables.get": 2,
            "tables.patch": 1,
            "jobs.query": 0,
            "tabledata.list": 0,
            "CreateReadSession": 1,
            "ReadRows": 1,
        },
        minimum={"jobs.getQueryResults": 1},
    )
    if observation["anonymous_destinations"] != 0:
        raise AssertionError("explicit materialization produced an anonymous destination")
    _assert_read_session_shape(
        observation,
        wire_format="ARROW",
        selected_field_count=1,
        has_row_restriction=False,
    )
    record_capability(
        "SBQ-READ-ARROW-QUERY-MATERIALIZED-V1",
        (
            "arrow-query explicit-destination patch-calls:1 rows:2 "
            f"storage-read:observed row-fingerprint:sha256:{_row_fingerprint(actual)}"
        ),
    )


@pytest.mark.capability("SBQ-READ-ARROW-COUNT-V1")
def test_query_result_count(
    spark_session,
    public_edge: PublicEdge,
    seeded_table: str,
) -> None:
    source_query = (
        f"SELECT id, active FROM `{seeded_table}` "
        "WHERE active IN (TRUE, FALSE) AND id >= 1"
    )
    log_position = public_edge_log_position(public_edge)
    frame = load_connector_source(
        spark_session,
        public_edge,
        source=source_query,
        source_kind="query",
        wire_format="ARROW",
        requested_streams=2,
    )
    try:
        row_count = frame.count()
    except Exception as error:
        raise_known_gap(
            "SBQ-READ-ARROW-COUNT-V1",
            error=error,
            expected_fragments=(
                'invalid field name "count_star()"',
                "BigQueryClient.getNumberOfRows",
            ),
            stage="query-empty-projection-count",
            shape="jobs.query aggregate-field-name-invalid",
            fix_hint="normalize-anonymous-aggregate-field-name",
        )
    if row_count != 4:
        pytest.fail(f"query count mismatch shape=actual:{row_count},expected:4")
    observation = observe_query_read_flow(public_edge, since=log_position)
    assert_ordered_operations(
        observation,
        (
            "jobs.insert",
            "jobs.getQueryResults",
            "jobs.get",
            "tables.get",
            "jobs.insert",
            "jobs.getQueryResults",
            "jobs.get",
            "tabledata.list",
        ),
    )
    _assert_operation_counts(
        observation,
        exact={
            "jobs.insert": 2,
            "jobs.get": 2,
            "tables.get": 1,
            "tables.patch": 0,
            "jobs.query": 0,
            "tabledata.list": 1,
            "CreateReadSession": 0,
            "ReadRows": 0,
        },
        minimum={"jobs.getQueryResults": 2},
    )
    if observation["anonymous_destinations"] != 2:
        raise AssertionError("optimized count did not produce two anonymous destinations")
    record_capability(
        "SBQ-READ-ARROW-COUNT-V1",
        "arrow-query-result count:4 jobs.insert:2 tabledata.list:1 storage-read-calls:0",
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
