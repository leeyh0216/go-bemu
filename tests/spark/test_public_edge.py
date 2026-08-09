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
from decimal import Decimal

import pytest

from conftest import (
    PublicEdge,
    assert_ordered_operations,
    connector_options,
    create_table,
    list_table_data,
    load_connector_source,
    observe_default_append_offsets,
    observe_direct_overwrite_flow,
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


def _assert_direct_overwrite_phase_order(observation: dict[str, object]) -> None:
    """Pin the serial phase boundaries while allowing parallel task interleaving."""

    sequence = observation["sequence"]
    if not isinstance(sequence, tuple):
        raise AssertionError("overwrite operation sequence has an invalid shape")

    def positions(operation: str) -> list[int]:
        return [index for index, observed in enumerate(sequence) if observed == operation]

    table_create = positions("tables.insert")
    stream_create = positions("CreateWriteStream")
    appends = positions("AppendRows")
    finalizes = positions("FinalizeWriteStream")
    batch_commit = positions("BatchCommitWriteStreams")
    job_insert = positions("jobs.insert")
    query_polls = positions("jobs.getQueryResults")
    job_reload = positions("jobs.get")
    table_delete = positions("tables.delete")
    groups = (
        table_create,
        stream_create,
        appends,
        finalizes,
        batch_commit,
        job_insert,
        query_polls,
        job_reload,
        table_delete,
    )
    if any(not group for group in groups):
        raise AssertionError("overwrite phase observation omitted an operation group")
    if not (
        table_create[0] < min(stream_create)
        and max(appends) < batch_commit[0]
        and max(finalizes) < batch_commit[0]
        and batch_commit[0] < job_insert[0]
        and job_insert[0] < min(query_polls)
        and max(query_polls) < job_reload[0]
        and job_reload[0] < table_delete[0]
    ):
        raise AssertionError("overwrite operation phase order mismatch")


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
    ("wire_format", "capability_id"),
    [
        pytest.param(
            "ARROW",
            "SBQ-READ-ARROW-DECIMAL-TYPES-V1",
            marks=pytest.mark.capability("SBQ-READ-ARROW-DECIMAL-TYPES-V1"),
            id="arrow",
        ),
        pytest.param(
            "AVRO",
            "SBQ-READ-DECIMAL-TYPES-V1",
            marks=pytest.mark.capability("SBQ-READ-DECIMAL-TYPES-V1"),
            id="avro",
        ),
    ],
)
@pytest.mark.operation("bigquery.tables.insert")
@pytest.mark.operation("grpc.bigquery-read.create-read-session")
@pytest.mark.operation("grpc.bigquery-read.read-rows")
def test_decimal_schema_through_public_storage_read_edge(
    spark_session,
    public_edge: PublicEdge,
    test_timeout: float,
    wire_format: str,
    capability_id: str,
) -> None:
    from pyspark.sql.types import ArrayType, DecimalType, StructType

    table_id = f"decimal_schema_{uuid.uuid4().hex[:8]}"
    create_table(
        public_edge,
        test_timeout,
        table_id,
        [
            {"name": "numeric_default", "type": "NUMERIC"},
            {"name": "bignumeric_default", "type": "BIGNUMERIC"},
            {
                "name": "numeric_explicit",
                "type": "NUMERIC",
                "precision": "20",
                "scale": "4",
            },
            {
                "name": "bignumeric_explicit",
                "type": "BIGNUMERIC",
                "precision": "10",
                "scale": "2",
            },
            {
                "name": "details",
                "type": "RECORD",
                "fields": [{"name": "amount", "type": "BIGNUMERIC"}],
            },
            {
                "name": "amounts",
                "type": "NUMERIC",
                "mode": "REPEATED",
                "precision": "12",
                "scale": "3",
            },
        ],
    )
    destination = f"{public_edge.project_id}.{public_edge.dataset_id}.{table_id}"
    frame = load_connector_source(
        spark_session,
        public_edge,
        source=destination,
        source_kind="table",
        wire_format=wire_format,
        requested_streams=1,
    )

    fields = {field.name: field.dataType for field in frame.schema.fields}
    expected_scalars = {
        "numeric_default": DecimalType(38, 9),
        "bignumeric_default": DecimalType(38, 18),
        "numeric_explicit": DecimalType(20, 4),
        "bignumeric_explicit": DecimalType(10, 2),
    }
    for name, expected in expected_scalars.items():
        if fields.get(name) != expected:
            pytest.fail(
                f"decimal schema mismatch shape=field:{name},actual:{fields.get(name)},expected:{expected}"
            )
    if not isinstance(fields.get("details"), StructType) or fields["details"][
        "amount"
    ].dataType != DecimalType(38, 18):
        pytest.fail("nested BIGNUMERIC schema mismatch shape=details.amount")
    if not isinstance(fields.get("amounts"), ArrayType) or fields[
        "amounts"
    ].elementType != DecimalType(12, 3):
        pytest.fail("repeated NUMERIC schema mismatch shape=amounts[]")
    if frame.collect() != []:
        pytest.fail("empty decimal schema fixture returned rows")
    record_capability(
        capability_id,
        f"{wire_format.lower()} decimal-defaults:38,9+38,18 explicit:20,4+10,2 nested:verified repeated:verified",
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
def test_query_source_explicit_materialization_reuses_destination_metadata(
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
            "CreateReadSession",
            "ReadRows",
        ),
    )
    counts = observation["counts"]
    if not isinstance(counts, dict):
        raise AssertionError("query operation counts have an invalid shape")
    metadata_gets = counts.get("tables.get", 0)
    metadata_patches = counts.get("tables.patch", 0)
    if (metadata_gets, metadata_patches) not in {(1, 0), (2, 0), (2, 1)}:
        raise AssertionError(
            "explicit materialization metadata flow mismatch "
            f"shape=tables.get:{metadata_gets},tables.patch:{metadata_patches}"
        )
    _assert_operation_counts(
        observation,
        exact={
            "jobs.insert": 1,
            "jobs.get": 1,
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
            "arrow-query explicit-destination metadata-patch:optional rows:2 "
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
    log_position = public_edge_log_position(public_edge)
    (
        writer.option("writeMethod", "direct")
        .option("writeAtLeastOnce", "false")
        .mode("append")
        .save(f"{public_edge.project_id}.{public_edge.dataset_id}.{table_id}")
    )
    observation = observe_direct_overwrite_flow(public_edge, since=log_position)
    assert_ordered_operations(
        observation,
        (
            "CreateWriteStream",
            "AppendRows",
            "FinalizeWriteStream",
            "BatchCommitWriteStreams",
        ),
    )
    _assert_operation_counts(
        observation,
        exact={
            "CreateWriteStream": logical_partitions,
            "GetWriteStream": 0,
            "AppendRows": logical_partitions,
            "FinalizeWriteStream": logical_partitions,
            "BatchCommitWriteStreams": 1,
        },
    )
    expected_observation = {
        "pending_stream_count": logical_partitions,
        "pending_stream_types_valid": True,
        "append_batch_count": logical_partitions,
        "append_row_count": 8,
        "commit_stream_count": logical_partitions,
        "commit_row_count": 8,
        "commit_succeeded": True,
        "stream_lifecycle_correlated": True,
    }
    for field, expected_value in expected_observation.items():
        if observation.get(field) != expected_value:
            pytest.fail(
                "direct exact append observation mismatch "
                f"shape={field}:expected:{expected_value}"
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
        (
            f"pending-streams:{logical_partitions} "
            f"append-calls:{logical_partitions} "
            f"finalize-calls:{logical_partitions} "
            "get-write-stream-calls:0 batch-commit-calls:1 committed-rows:8"
        ),
    )


@pytest.mark.operation("bigquery.tables.insert")
@pytest.mark.operation("grpc.bigquery-write.create-write-stream")
@pytest.mark.operation("grpc.bigquery-write.append-rows")
@pytest.mark.operation("grpc.bigquery-write.finalize-write-stream")
@pytest.mark.operation("grpc.bigquery-write.batch-commit-write-streams")
@pytest.mark.operation("bigquery.tabledata.list")
@pytest.mark.capability("SBQ-WRITE-DIRECT-DECIMAL-V1")
def test_direct_decimal_write_through_public_proto_rows_edge(
    spark_session,
    public_edge: PublicEdge,
    test_timeout: float,
) -> None:
    from pyspark.sql.types import DecimalType, StructField, StructType

    table_id = f"decimal_write_{uuid.uuid4().hex[:8]}"
    destination = f"{public_edge.project_id}.{public_edge.dataset_id}.{table_id}"
    create_table(
        public_edge,
        test_timeout,
        table_id,
        [
            {
                "name": "numeric_value",
                "type": "NUMERIC",
                "precision": "20",
                "scale": "4",
            },
            {
                "name": "bignumeric_value",
                "type": "BIGNUMERIC",
                "precision": "38",
                "scale": "18",
            },
        ],
    )
    schema = StructType(
        [
            StructField("numeric_value", DecimalType(20, 4), True),
            StructField("bignumeric_value", DecimalType(38, 18), True),
        ]
    )
    frame = spark_session.createDataFrame(
        [
            (
                Decimal("12.3400"),
                Decimal("12345678901234567890.123456789012345678"),
            )
        ],
        schema,
    ).repartition(1)
    writer = frame.write.format("bigquery")
    for key, value in connector_options(public_edge).items():
        writer = writer.option(key, value)
    (
        writer.option("writeMethod", "direct")
        .option("writeAtLeastOnce", "false")
        .mode("append")
        .save(destination)
    )

    result = list_table_data(public_edge, test_timeout, table_id)
    values = [Decimal(str(cell["v"])) for cell in result["rows"][0]["f"]]
    if values != [
        Decimal("12.3400"),
        Decimal("12345678901234567890.123456789012345678"),
    ]:
        pytest.fail("direct decimal value mismatch shape=single-row-two-decimals")
    record_capability(
        "SBQ-WRITE-DIRECT-DECIMAL-V1",
        "proto-rows numeric:20,4 bignumeric:38,18 committed-rows:1",
    )


@pytest.mark.capability("SBQ-WRITE-DIRECT-EXACT-OVERWRITE-V1")
def test_direct_pending_exact_static_overwrite(
    spark_session,
    public_edge: PublicEdge,
    test_timeout: float,
) -> None:
    """Exercise the connector-owned temporary-table and MERGE lifecycle."""

    table_id = f"overwrite_{uuid.uuid4().hex[:8]}"
    destination = f"{public_edge.project_id}.{public_edge.dataset_id}.{table_id}"
    create_table(
        public_edge,
        test_timeout,
        table_id,
        [
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "active", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "score", "type": "FLOAT", "mode": "NULLABLE"},
        ],
    )
    query(
        public_edge,
        test_timeout,
        f"INSERT INTO `{destination}` VALUES (-2, false, -1.5), (-1, true, -0.5)",
    )

    replacement = (
        spark_session.range(0, 8, numPartitions=4)
        .selectExpr(
            "id",
            "(id % 2 = 0) AS active",
            "CAST(id AS DOUBLE) + 0.5 AS score",
        )
        .cache()
    )
    try:
        partition_sizes = replacement.rdd.mapPartitions(
            lambda rows: [sum(1 for _ in rows)]
        ).collect()
        if len(partition_sizes) != 4 or any(size <= 0 for size in partition_sizes):
            pytest.fail("overwrite source partition mismatch shape=nonempty:4")
        writer = replacement.write.format("bigquery")
        for key, value in connector_options(public_edge).items():
            writer = writer.option(key, value)

        log_position = public_edge_log_position(public_edge)
        (
            writer.option("writeMethod", "direct")
            .option("writeAtLeastOnce", "false")
            .mode("overwrite")
            .save(destination)
        )
    finally:
        replacement.unpersist()

    observation = observe_direct_overwrite_flow(public_edge, since=log_position)
    assert_ordered_operations(
        observation,
        (
            "tables.insert",
            "CreateWriteStream",
            "AppendRows",
            "FinalizeWriteStream",
            "BatchCommitWriteStreams",
            "jobs.insert",
            "jobs.getQueryResults",
            "jobs.get",
            "tables.delete",
        ),
    )
    _assert_direct_overwrite_phase_order(observation)
    _assert_operation_counts(
        observation,
        exact={
            "tables.insert": 1,
            "CreateWriteStream": 4,
            "AppendRows": 4,
            "FinalizeWriteStream": 4,
            "BatchCommitWriteStreams": 1,
            "GetWriteStream": 0,
            "jobs.insert": 1,
            "jobs.get": 1,
            "tables.delete": 1,
        },
        minimum={"jobs.getQueryResults": 1},
    )
    if observation["static_adapter_matches"] != 1:
        pytest.fail("static overwrite adapter match mismatch shape=expected:1")
    expected_observation = {
        "pending_stream_count": 4,
        "pending_stream_types_valid": True,
        "append_batch_count": 4,
        "append_row_count": 8,
        "commit_stream_count": 4,
        "commit_row_count": 8,
        "commit_succeeded": True,
        "stream_lifecycle_correlated": True,
        "temporary_table_correlated": True,
    }
    for field, expected_value in expected_observation.items():
        if observation.get(field) != expected_value:
            pytest.fail(
                "static overwrite observation mismatch "
                f"shape={field}:expected:{expected_value}"
            )

    result = query(
        public_edge,
        test_timeout,
        f"SELECT id, active, score FROM `{destination}` ORDER BY id",
    )
    rows = result.get("rows", [])
    actual = [
        (
            int(row["f"][0]["v"]),
            str(row["f"][1]["v"]).lower() == "true",
            float(row["f"][2]["v"]),
        )
        for row in rows
    ]
    expected = [(index, index % 2 == 0, index + 0.5) for index in range(8)]
    _assert_rows(actual, expected)
    record_capability(
        "SBQ-WRITE-DIRECT-EXACT-OVERWRITE-V1",
        (
            "spark-partitions:4 nonempty-partitions:4 pending-streams:4 "
            "committed-rows:8 merge-jobs:1 "
            "temporary-table-create-delete:1 atomic-replacement-rows:8 "
            f"row-fingerprint:sha256:{_row_fingerprint(actual)}"
        ),
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
