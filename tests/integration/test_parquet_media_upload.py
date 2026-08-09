# bqemu:operation bigquery.jobs.insert.media-upload scenario=parquet-media-upload
"""Pinned public Python clients exercising Parquet media upload end to end."""

from __future__ import annotations

from decimal import Decimal

from google.cloud import bigquery
import pandas as pd
import pandas_gbq


def _dataset(client: bigquery.Client) -> str:
    dataset_id = f"media_{client.project.replace('-', '_')}"
    dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
    dataset.location = "US"
    client.create_dataset(dataset)
    return dataset_id


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": pd.Series([1, 2], dtype="Int64"),
            "label": pd.Series(["first", None], dtype="string"),
            "event_time": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"], utc=True),
            "amount": [Decimal("12.34"), Decimal("56.78")],
            "payload": [b"one", b"two"],
        }
    )


def test_load_table_from_dataframe_create_append_replace(bq_client: bigquery.Client) -> None:
    destination = f"{_dataset(bq_client)}.events"
    schema = [
        bigquery.SchemaField("id", "INT64"),
        bigquery.SchemaField("label", "STRING"),
        bigquery.SchemaField("event_time", "TIMESTAMP"),
        bigquery.SchemaField("amount", "NUMERIC"),
        bigquery.SchemaField("payload", "BYTES"),
    ]
    create = bq_client.load_table_from_dataframe(
        _frame(), destination, job_config=bigquery.LoadJobConfig(schema=schema)
    )
    assert create.result().state == "DONE"

    append = bq_client.load_table_from_dataframe(
        pd.DataFrame({"id": pd.Series([3], dtype="Int64"), "label": ["third"], "event_time": pd.to_datetime(["2024-01-03T00:00:00Z"], utc=True), "amount": [Decimal("90.12")], "payload": [b"three"]}),
        destination,
        job_config=bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND"),
    )
    assert append.result().output_rows == 1

    replace = bq_client.load_table_from_dataframe(
        pd.DataFrame({"id": pd.Series([4], dtype="Int64"), "label": ["replacement"], "event_time": pd.to_datetime(["2024-01-04T00:00:00Z"], utc=True), "amount": [Decimal("10.00")], "payload": [b"four"]}),
        destination,
        job_config=bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE"),
    )
    assert replace.result().output_rows == 1
    rows = list(bq_client.query(f"SELECT id, label FROM `{bq_client.project}.{destination}` ORDER BY id").result())
    assert [(row["id"], row["label"]) for row in rows] == [(4, "replacement")]


def test_load_table_from_dataframe_preserves_pyarrow_complex_and_nullable_values(bq_client: bigquery.Client) -> None:
    destination = f"{_dataset(bq_client)}.complex_events"
    frame = pd.DataFrame(
        {
            "id": pd.Series([1, 2], dtype="Int64"),
            "nullable_text": pd.Series(["present", None], dtype="string"),
            "score": [1.5, float("nan")],
            "event_time": pd.to_datetime(["2024-02-01T00:00:00Z", "2024-02-02T00:00:00Z"], utc=True),
            "amount": [Decimal("1.25"), Decimal("2.50")],
            "payload": [b"first", b"second"],
            "tags": [["red", "blue"], ["green"]],
            "profile": [{"name": "one", "rank": 1}, {"name": "two", "rank": 2}],
        }
    )
    schema = [
        bigquery.SchemaField("id", "INT64"),
        bigquery.SchemaField("nullable_text", "STRING"),
        bigquery.SchemaField("score", "FLOAT64"),
        bigquery.SchemaField("event_time", "TIMESTAMP"),
        bigquery.SchemaField("amount", "NUMERIC"),
        bigquery.SchemaField("payload", "BYTES"),
        bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
        bigquery.SchemaField("profile", "RECORD", fields=[bigquery.SchemaField("name", "STRING"), bigquery.SchemaField("rank", "INT64")]),
    ]
    job = bq_client.load_table_from_dataframe(frame, destination, job_config=bigquery.LoadJobConfig(schema=schema))
    assert job.result().output_rows == 2
    rows = list(bq_client.query(f"SELECT id, nullable_text, score, profile.name AS profile_name, array_length(tags) AS tag_count FROM `{bq_client.project}.{destination}` ORDER BY id").result())
    assert rows[0]["nullable_text"] == "present"
    assert rows[1]["nullable_text"] is None
    assert rows[1]["score"] is None
    assert [(row["profile_name"], row["tag_count"]) for row in rows] == [("one", 2), ("two", 1)]


def test_pandas_gbq_to_gbq_create_append_replace(bq_client: bigquery.Client) -> None:
    destination = f"{_dataset(bq_client)}.pandas_events"
    create = pd.DataFrame({"id": pd.Series([1, 2], dtype="Int64"), "label": ["first", "second"]})
    pandas_gbq.to_gbq(
        create,
        destination,
        project_id=bq_client.project,
        if_exists="fail",
        progress_bar=False,
        bigquery_client=bq_client,
    )
    pandas_gbq.to_gbq(
        pd.DataFrame({"id": pd.Series([3], dtype="Int64"), "label": ["third"]}),
        destination,
        project_id=bq_client.project,
        if_exists="append",
        progress_bar=False,
        bigquery_client=bq_client,
    )
    pandas_gbq.to_gbq(
        pd.DataFrame({"id": pd.Series([4], dtype="Int64"), "label": ["replacement"]}),
        destination,
        project_id=bq_client.project,
        if_exists="replace",
        progress_bar=False,
        bigquery_client=bq_client,
    )
    rows = list(bq_client.query(f"SELECT id, label FROM `{bq_client.project}.{destination}` ORDER BY id").result())
    assert [(row["id"], row["label"]) for row in rows] == [(4, "replacement")]
