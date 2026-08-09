# bqemu:operation bigquery.tabledata.insert-all scenario=tabledata-insert-all
from __future__ import annotations

import pandas as pd
from google.cloud import bigquery


def test_official_client_insert_rows_json_and_dataframe_are_idempotent(
    bq_client: bigquery.Client,
) -> None:
    dataset = bigquery.Dataset(f"{bq_client.project}.streaming")
    dataset.location = "US"
    bq_client.create_dataset(dataset)
    table = bigquery.Table(
        f"{bq_client.project}.streaming.events",
        schema=[
            bigquery.SchemaField("id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("label", "STRING"),
            bigquery.SchemaField("score", "FLOAT64"),
        ],
    )
    table = bq_client.create_table(table)

    row = {"id": 1, "label": "json-client", "score": 1.25}
    assert bq_client.insert_rows_json(table, [row], row_ids=["stable-row"]) == []
    # Retry the exact official-client payload: durable insertId handling must
    # not append a duplicate row.
    assert bq_client.insert_rows_json(table, [row], row_ids=["stable-row"]) == []

    dataframe = pd.DataFrame(
        {"id": [2, 3], "label": ["pandas-a", "pandas-b"], "score": [2.5, 3.75]}
    )
    assert bq_client.insert_rows_from_dataframe(table, dataframe, chunk_size=1) == [[], []]

    rows = list(bq_client.list_rows(table))
    assert [(row["id"], row["label"], row["score"]) for row in rows] == [
        (1, "json-client", 1.25),
        (2, "pandas-a", 2.5),
        (3, "pandas-b", 3.75),
    ]
