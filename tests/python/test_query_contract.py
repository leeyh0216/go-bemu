"""Official Python client query contracts against the public REST edge.

Official query APIs:
https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client#google_cloud_bigquery_client_Client_query
https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
"""

from google.api_core import exceptions
from google.cloud import bigquery
import pytest


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
