"""Official Python client query contracts against the public REST edge.

Official query APIs:
https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client#google_cloud_bigquery_client_Client_query
https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
"""

from google.api_core import exceptions
from google.cloud import bigquery
import pytest
import uuid


@pytest.mark.operation("bigquery.jobs.query")
@pytest.mark.operation("bigquery.jobs.getQueryResults")
def test_synchronous_and_polled_query_jobs(
    bq_client: bigquery.Client, test_timeout: float
) -> None:
    synchronous = bq_client.query_and_wait(
        "SELECT 1 AS answer",
        location="US",
        api_timeout=test_timeout,
        wait_timeout=test_timeout,
    )
    rows = list(synchronous)
    assert synchronous.total_rows == 1
    assert rows[0]["answer"] == 1

    job = bq_client.query("SELECT 2 AS answer", location="US", timeout=test_timeout)
    rows = list(job.result(timeout=test_timeout))
    assert job.state == "DONE"
    assert job.error_result is None
    assert rows[0]["answer"] == 2


def test_failed_query_is_done_with_invalid_query_error(
    bq_client: bigquery.Client, test_timeout: float
) -> None:
    job = bq_client.query(
        "SELECT missing_column",
        location="US",
        timeout=test_timeout,
        retry=None,
        job_retry=None,
    )
    with pytest.raises(exceptions.BadRequest):
        list(job.result(timeout=test_timeout, retry=None))
    assert job.state == "DONE"
    assert job.error_result is not None
    assert job.error_result["reason"] == "invalidQuery"


@pytest.mark.operation("bigquery.jobs.insert")
@pytest.mark.operation("bigquery.jobs.getQueryResults")
@pytest.mark.operation("bigquery.datasets.insert")
@pytest.mark.operation("bigquery.datasets.delete")
@pytest.mark.operation("bigquery.tables.get")
def test_google_sql_ddl_updates_the_official_python_client_view(
    bq_client: bigquery.Client, project_id: str, test_timeout: float
) -> None:
    """Exercise the public query path rather than internal DDL implementation APIs."""

    dataset_id = "ddl_" + uuid.uuid4().hex[:8]
    table_id = "events"
    dataset_ref = f"{project_id}.{dataset_id}"
    table_ref = f"{dataset_ref}.{table_id}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    bq_client.create_dataset(dataset, timeout=test_timeout)
    try:
        for statement in (
            f"CREATE TABLE `{table_ref}` (id INT64, label STRING)",
            f"ALTER TABLE `{table_ref}` ADD COLUMN active BOOL",
            f"ALTER TABLE `{table_ref}` RENAME COLUMN label TO message",
        ):
            job = bq_client.query(statement, location="US", timeout=test_timeout)
            assert list(job.result(timeout=test_timeout)) == []
            assert job.error_result is None

        table = bq_client.get_table(table_ref, timeout=test_timeout)
        assert [(field.name, field.field_type) for field in table.schema] == [
            ("id", "INT64"),
            ("message", "STRING"),
            ("active", "BOOL"),
        ]
    finally:
        bq_client.delete_dataset(
            dataset_ref, delete_contents=True, not_found_ok=True, timeout=test_timeout
        )
