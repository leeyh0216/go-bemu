"""Dataframe media-upload contract against the public BigQuery REST edge.

The external callers are intentionally exercised only from this integration
suite. Product packages receive a standard media upload and remain unaware of
the caller that created it.
"""

from __future__ import annotations

import base64
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


@pytest.mark.operation("bigquery.tables.get")
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
