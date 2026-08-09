"""Dataframe media-upload contract against the public BigQuery REST edge.

The external callers are intentionally exercised only from this integration
suite. Product packages receive a standard media upload and remain unaware of
the caller that created it.
"""

from __future__ import annotations

import base64
from decimal import Decimal
import os

from google.cloud import bigquery
import pandas as pd
import pandas_gbq
import pytest

from conftest import MediaRuntime, _phase_json_request


def _schema() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("active", "BOOLEAN"),
        bigquery.SchemaField("payload", "STRING"),
    ]


def _load_config(
    *,
    create_disposition: str,
    write_disposition: str,
) -> bigquery.LoadJobConfig:
    return bigquery.LoadJobConfig(
        schema=_schema(),
        source_format=bigquery.SourceFormat.PARQUET,
        create_disposition=create_disposition,
        write_disposition=write_disposition,
    )


def _pandas_schema() -> list[dict[str, str]]:
    return [
        {"name": field.name, "type": field.field_type, "mode": field.mode}
        for field in _schema()
    ]


def _large_frame() -> pd.DataFrame:
    # High-entropy values keep the Parquet payload above the client multipart
    # threshold without producing a 100 MiB resumable chunk in CI.
    payloads = [
        base64.b64encode(os.urandom(128 * 1024)).decode("ascii")
        for _ in range(48)
    ]
    return pd.DataFrame(
        {
            "id": list(range(100, 148)),
            "name": [f"large-{index}" for index in range(48)],
            "active": [index % 2 == 0 for index in range(48)],
            "payload": payloads,
        }
    )


def _rich_schema() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("nullable_int", "INTEGER"),
        bigquery.SchemaField("nullable_bool", "BOOLEAN"),
        bigquery.SchemaField("nullable_float", "FLOAT"),
        bigquery.SchemaField("event_time", "TIMESTAMP"),
        bigquery.SchemaField("amount", "NUMERIC", precision=20, scale=4),
        bigquery.SchemaField("payload", "BYTES"),
        bigquery.SchemaField(
            "profile",
            "RECORD",
            fields=(
                bigquery.SchemaField("source", "STRING"),
                bigquery.SchemaField("score", "INTEGER"),
            ),
        ),
        bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
    ]


def _rich_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "nullable_int": pd.Series([7, pd.NA], dtype="Int64"),
            "nullable_bool": pd.Series([True, pd.NA], dtype="boolean"),
            "nullable_float": pd.Series([1.5, pd.NA], dtype="Float64"),
            "event_time": pd.Series(
                [pd.Timestamp("2026-08-09T12:34:56Z"), pd.NaT],
                dtype="datetime64[ns, UTC]",
            ),
            "amount": [Decimal("12.3400"), None],
            "payload": [b"\\x00media-bytes", None],
            "profile": [
                {"source": "dataframe", "score": 3},
                None,
            ],
            "tags": [["alpha", "beta"], []],
        }
    )


def _rich_load_config() -> bigquery.LoadJobConfig:
    return bigquery.LoadJobConfig(
        schema=_rich_schema(),
        source_format=bigquery.SourceFormat.PARQUET,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )


@pytest.mark.operation("bigquery.datasets.insert")
@pytest.mark.operation("bigquery.datasets.delete")
@pytest.mark.operation("bigquery.tables.insert")
@pytest.mark.operation("bigquery.tables.get")
@pytest.mark.operation("bigquery.tabledata.list")
@pytest.mark.operation("bigquery.jobs.insert.upload")
@pytest.mark.operation("bigquery.jobs.insert.upload-resume")
@pytest.mark.operation("bigquery.jobs.get")
def test_dataframe_writers_use_bounded_parquet_media_upload(
    media_runtime: MediaRuntime,
) -> None:
    destination = (
        f"{media_runtime.project_id}.{media_runtime.dataset_id}.{media_runtime.table_id}"
    )
    small = pd.DataFrame(
        {
            "id": [1, 2],
            "name": ["small-one", "small-two"],
            "active": [True, False],
            "payload": [None, None],
        }
    )
    small_job = media_runtime.client.load_table_from_dataframe(
        small,
        destination,
        job_config=_load_config(
            create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ),
        location=media_runtime.location,
        timeout=media_runtime.timeout,
    )
    small_job.result(timeout=media_runtime.timeout)

    large = _large_frame()
    assert large["payload"].str.len().sum() > 5 * 1024 * 1024
    large_job = media_runtime.client.load_table_from_dataframe(
        large,
        destination,
        job_config=_load_config(
            create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ),
        location=media_runtime.location,
        timeout=media_runtime.timeout,
    )
    large_job.result(timeout=media_runtime.timeout)

    pandas_gbq.to_gbq(
        small,
        f"{media_runtime.dataset_id}.{media_runtime.table_id}",
        project_id=media_runtime.project_id,
        if_exists="append",
        api_method="load_parquet",
        bigquery_client=media_runtime.client,
        table_schema=_pandas_schema(),
        location=media_runtime.location,
        progress_bar=False,
    )

    pandas_gbq.to_gbq(
        small,
        f"{media_runtime.dataset_id}.{media_runtime.table_id}",
        project_id=media_runtime.project_id,
        if_exists="replace",
        api_method="load_parquet",
        bigquery_client=media_runtime.client,
        table_schema=_pandas_schema(),
        location=media_runtime.location,
        progress_bar=False,
    )

    created_table_id = "created_events"
    created_destination = (
        f"{media_runtime.project_id}.{media_runtime.dataset_id}.{created_table_id}"
    )
    created_job = media_runtime.client.load_table_from_dataframe(
        small,
        created_destination,
        job_config=_load_config(
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ),
        location=media_runtime.location,
        timeout=media_runtime.timeout,
    )
    created_job.result(timeout=media_runtime.timeout)

    rows = _phase_json_request(
        media_runtime.endpoint,
        "GET",
        (
            f"/bigquery/v2/projects/{media_runtime.project_id}/datasets/"
            f"{media_runtime.dataset_id}/tables/{media_runtime.table_id}/data?maxResults=100"
        ),
        media_runtime.timeout,
        phase="assertion",
    )
    assert isinstance(rows, dict)
    assert rows["totalRows"] == str(len(small))

    created_rows = _phase_json_request(
        media_runtime.endpoint,
        "GET",
        (
            f"/bigquery/v2/projects/{media_runtime.project_id}/datasets/"
            f"{media_runtime.dataset_id}/tables/{created_table_id}/data?maxResults=100"
        ),
        media_runtime.timeout,
        phase="assertion",
    )
    assert isinstance(created_rows, dict)
    assert created_rows["totalRows"] == str(len(small))


@pytest.mark.operation("bigquery.tables.get")
@pytest.mark.operation("bigquery.jobs.insert.upload")
@pytest.mark.operation("bigquery.jobs.get")
def test_dataframe_media_upload_preserves_explicit_parquet_schema(
    media_runtime: MediaRuntime,
) -> None:
    table_id = "typed_events"
    destination = f"{media_runtime.project_id}.{media_runtime.dataset_id}.{table_id}"
    frame = _rich_frame()

    job = media_runtime.client.load_table_from_dataframe(
        frame,
        destination,
        job_config=_rich_load_config(),
        location=media_runtime.location,
        timeout=media_runtime.timeout,
    )
    job.result(timeout=media_runtime.timeout)
    assert job.output_rows == len(frame)

    table = media_runtime.client.get_table(destination)
    observed = {field.name: field for field in table.schema}
    assert observed["amount"].field_type == "NUMERIC"
    assert observed["amount"].precision == 20
    assert observed["amount"].scale == 4
    assert observed["payload"].field_type == "BYTES"
    assert observed["profile"].field_type == "RECORD"
    assert [field.name for field in observed["profile"].fields] == ["source", "score"]
    assert observed["tags"].mode == "REPEATED"

    rows = _phase_json_request(
        media_runtime.endpoint,
        "GET",
        (
            f"/bigquery/v2/projects/{media_runtime.project_id}/datasets/"
            f"{media_runtime.dataset_id}/tables/{table_id}/data?maxResults=100"
        ),
        media_runtime.timeout,
        phase="assertion",
    )
    assert isinstance(rows, dict)
    assert rows["totalRows"] == str(len(frame))
