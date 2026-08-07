"""google-cloud-bigquery 3.43.0 metadata contract against a real BQEMU process.

Official operations:
https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client
"""

from __future__ import annotations

import datetime
import uuid

from google.api_core import exceptions
from google.cloud import bigquery
import pytest


def _dataset_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def test_dataset_crud_update_pagination_location_and_errors(
    bq_client: bigquery.Client, project_id: str, test_timeout: float
) -> None:
    dataset_ids = [_dataset_id("admin"), _dataset_id("page"), _dataset_id("page")]
    try:
        dataset = bigquery.Dataset(f"{project_id}.{dataset_ids[0]}")
        dataset.location = "EU"
        dataset.description = "created"
        dataset.labels = {"tier": "test"}
        created = bq_client.create_dataset(dataset, timeout=test_timeout)
        assert created.location == "EU"
        assert created.etag

        with pytest.raises(exceptions.Conflict):
            bq_client.create_dataset(dataset, timeout=test_timeout)

        loaded = bq_client.get_dataset(created.reference, timeout=test_timeout)
        loaded.description = "updated"
        loaded.labels = {"tier": "gold", "owner": "contracts"}
        loaded.default_table_expiration_ms = 86_400_000
        loaded.default_partition_expiration_ms = 3_600_000
        updated = bq_client.update_dataset(
            loaded,
            [
                "description",
                "labels",
                "default_table_expiration_ms",
                "default_partition_expiration_ms",
            ],
            timeout=test_timeout,
        )
        assert updated.description == "updated"
        assert updated.labels == {"tier": "gold", "owner": "contracts"}
        assert updated.default_table_expiration_ms == 86_400_000
        assert updated.default_partition_expiration_ms == 3_600_000
        assert updated.etag != created.etag

        stale = bq_client.get_dataset(created.reference, timeout=test_timeout)
        fresh = bq_client.get_dataset(created.reference, timeout=test_timeout)
        fresh.description = "fresh mutation"
        bq_client.update_dataset(fresh, ["description"], timeout=test_timeout)
        stale.description = "stale mutation"
        with pytest.raises(exceptions.PreconditionFailed):
            bq_client.update_dataset(stale, ["description"], timeout=test_timeout)

        for dataset_id in dataset_ids[1:]:
            candidate = bigquery.Dataset(f"{project_id}.{dataset_id}")
            candidate.location = "EU"
            bq_client.create_dataset(candidate, timeout=test_timeout)
        iterator = bq_client.list_datasets(
            project=project_id, max_results=3, page_size=1, timeout=test_timeout
        )
        pages = list(iterator.pages)
        listed_ids = [item.dataset_id for page in pages for item in page]
        assert set(dataset_ids).issubset(set(listed_ids))
        assert len(pages) == 3

        with pytest.raises(exceptions.NotFound):
            bq_client.get_dataset(f"{project_id}.missing_dataset", timeout=test_timeout)
    finally:
        for dataset_id in dataset_ids:
            bq_client.delete_dataset(
                f"{project_id}.{dataset_id}",
                delete_contents=True,
                not_found_ok=True,
                timeout=test_timeout,
            )


def test_table_crud_metadata_etag_and_additive_schema(
    bq_client: bigquery.Client, project_id: str, test_timeout: float
) -> None:
    dataset_id = _dataset_id("tables")
    table_id = f"events_{uuid.uuid4().hex[:8]}"
    dataset_ref = f"{project_id}.{dataset_id}"
    table_ref = f"{dataset_ref}.{table_id}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    bq_client.create_dataset(dataset, timeout=test_timeout)
    try:
        schema = [
            bigquery.SchemaField("id", "INT64"),
            bigquery.SchemaField(
                "payload", "RECORD", fields=[bigquery.SchemaField("name", "STRING")]
            ),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table.description = "created"
        table.labels = {"tier": "test"}
        created = bq_client.create_table(table, timeout=test_timeout)
        assert created.location == "US"
        assert created.etag
        assert [field.name for field in created.schema] == ["id", "payload"]

        listed = list(bq_client.list_tables(dataset_ref, max_results=1, timeout=test_timeout))
        assert [item.table_id for item in listed] == [table_id]

        loaded = bq_client.get_table(table_ref, timeout=test_timeout)
        loaded.description = "metadata updated"
        loaded.labels = {"owner": "contracts"}
        loaded.expires = datetime.datetime(2030, 1, 1, tzinfo=datetime.timezone.utc)
        loaded = bq_client.update_table(
            loaded, ["description", "labels", "expires"], timeout=test_timeout
        )
        assert loaded.description == "metadata updated"
        assert loaded.labels == {"owner": "contracts"}
        assert loaded.expires == datetime.datetime(2030, 1, 1, tzinfo=datetime.timezone.utc)

        loaded.schema = [
            loaded.schema[0],
            bigquery.SchemaField(
                "payload",
                "RECORD",
                fields=[
                    bigquery.SchemaField("name", "STRING"),
                    bigquery.SchemaField("score", "FLOAT64"),
                ],
            ),
            bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
        ]
        evolved = bq_client.update_table(loaded, ["schema"], timeout=test_timeout)
        assert [field.name for field in evolved.schema] == ["id", "payload", "tags"]
        assert [field.name for field in evolved.schema[1].fields] == ["name", "score"]

        illegal = bq_client.get_table(table_ref, timeout=test_timeout)
        illegal.schema = [bigquery.SchemaField("id", "STRING")]
        with pytest.raises(exceptions.BadRequest, match="CAP-SCHEMA-ADDITIVE-V1"):
            bq_client.update_table(illegal, ["schema"], timeout=test_timeout)

        stale = bq_client.get_table(table_ref, timeout=test_timeout)
        fresh = bq_client.get_table(table_ref, timeout=test_timeout)
        fresh.description = "fresh"
        bq_client.update_table(fresh, ["description"], timeout=test_timeout)
        stale.description = "stale"
        with pytest.raises(exceptions.PreconditionFailed):
            bq_client.update_table(stale, ["description"], timeout=test_timeout)

        latest = bq_client.get_table(table_ref, timeout=test_timeout)
        latest.description = None
        latest.expires = None
        cleared = bq_client.update_table(
            latest, ["description", "expires"], timeout=test_timeout
        )
        assert cleared.description in (None, "")
        assert cleared.expires is None

        bq_client.delete_table(table_ref, timeout=test_timeout)
        with pytest.raises(exceptions.NotFound):
            bq_client.get_table(table_ref, timeout=test_timeout)
    finally:
        bq_client.delete_dataset(
            dataset_ref, delete_contents=True, not_found_ok=True, timeout=test_timeout
        )
