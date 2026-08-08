"""Official Python client query lifecycle contracts over the public REST edge.

Pinned client implementation and protocol sources:
https://github.com/googleapis/google-cloud-python/blob/google-cloud-bigquery-v3.43.0/packages/google-cloud-bigquery/google/cloud/bigquery/_job_helpers.py#L420-L641
https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert
https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults
https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/list
https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery

Tests retain operation metadata and the raw intercepted request/response context
needed to diagnose a contract failure.
"""

from __future__ import annotations

from collections.abc import Callable
import json
import os
from typing import Any
import uuid

from google.api_core import exceptions
from google.api_core.retry import Retry, if_transient_error
from google.cloud import bigquery
import pytest


try:
    CLIENT_VERSION = json.loads(os.environ["BQEMU_RUNTIME_VERSIONS_JSON"])["client"]
except (KeyError, TypeError, json.JSONDecodeError) as error:
    raise RuntimeError(
        "BQEMU_RUNTIME_VERSIONS_JSON must come from a normalized consumer case"
    ) from error
if not isinstance(CLIENT_VERSION, str) or not CLIENT_VERSION:
    raise RuntimeError("normalized Python client version is invalid")
LIFECYCLE_CONTRACT = "PY-QUERY-LIFECYCLE-001"
REQUEST_RETRY_CONTRACT = "PY-QUERY-REQUEST-ID-001"
REQUEST_RETRY_GAP = "query.sync.request-controls-v1"


class RequestIDIdempotencyGap(AssertionError):
    """Only the known lost-response deduplication gap may be xfailed."""


class PublicEdgeContractError(RuntimeError):
    """Payload-safe unexpected public-edge failure."""


def _diagnostic(contract: str, shape: str, fix_hint: str) -> str:
    return (
        f"consumer=google-cloud-bigquery consumer_version={CLIENT_VERSION} "
        f"contract={contract} shape={shape} fix_hint={fix_hint}"
    )


def _query_config(
    destination: str, write_disposition: str
) -> bigquery.QueryJobConfig:
    config = bigquery.QueryJobConfig()
    config.destination = destination
    config.write_disposition = write_disposition
    config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    config.priority = bigquery.QueryPriority.INTERACTIVE
    config.labels = {"contract": "python-client"}
    return config


def _run_destination_query(
    client: bigquery.Client,
    sql: str,
    destination: str,
    disposition: str,
    job_id: str,
    timeout: float,
) -> bigquery.QueryJob:
    job = client.query(
        sql,
        job_config=_query_config(destination, disposition),
        job_id=job_id,
        location="US",
        retry=None,
        timeout=timeout,
        job_retry=None,
    )
    list(job.result(page_size=1, timeout=timeout, retry=None, job_retry=None))
    assert job.state == "DONE", f"{LIFECYCLE_CONTRACT}: destination job did not finish"
    assert job.error_result is None, f"{LIFECYCLE_CONTRACT}: destination job failed"
    return job


@pytest.mark.operation("bigquery.jobs.insert")
@pytest.mark.operation("bigquery.jobs.list")
@pytest.mark.operation("bigquery.jobs.get")
def test_query_destination_job_lifecycle_pagination_and_metadata_patch(
    bq_client: bigquery.Client, project_id: str, test_timeout: float
) -> None:
    suffix = uuid.uuid4().hex[:10]
    dataset_ref = f"{project_id}.query_lifecycle_{suffix}"
    destination_ref = f"{dataset_ref}.destination"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    dataset.description = "created"
    created_dataset = bq_client.create_dataset(dataset, timeout=test_timeout)

    try:
        created_dataset.description = "patched"
        created_dataset.labels = {"owner": "contracts"}
        patched_dataset = bq_client.update_dataset(
            created_dataset, ["description", "labels"], timeout=test_timeout
        )
        assert patched_dataset.description == "patched"
        assert patched_dataset.labels == {"owner": "contracts"}

        create_job_id = f"python-create-{suffix}"
        create_job = _run_destination_query(
            bq_client,
            "SELECT 1 AS id, 'one' AS name",
            destination_ref,
            bigquery.WriteDisposition.WRITE_EMPTY,
            create_job_id,
            test_timeout,
        )
        assert create_job.destination == bigquery.TableReference.from_string(
            destination_ref
        )
        assert create_job.write_disposition == bigquery.WriteDisposition.WRITE_EMPTY
        assert create_job.create_disposition == bigquery.CreateDisposition.CREATE_IF_NEEDED
        assert create_job.priority == bigquery.QueryPriority.INTERACTIVE
        assert create_job.labels == {"contract": "python-client"}

        # jobs.insert is intentionally not idempotent for an already-created job ID:
        # https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert
        with pytest.raises(exceptions.Conflict) as duplicate_error:
            bq_client.query(
                "SELECT 1 AS id, 'one' AS name",
                job_config=_query_config(
                    destination_ref, bigquery.WriteDisposition.WRITE_EMPTY
                ),
                job_id=create_job_id,
                location="US",
                retry=None,
                timeout=test_timeout,
                job_retry=None,
            )
        assert duplicate_error.value.code == 409

        append_job_id = f"python-append-{suffix}"
        _run_destination_query(
            bq_client,
            "SELECT 2 AS id, 'two' AS name",
            destination_ref,
            bigquery.WriteDisposition.WRITE_APPEND,
            append_job_id,
            test_timeout,
        )
        with pytest.raises(exceptions.NotFound) as location_error:
            bq_client.get_job(append_job_id, location="EU", timeout=test_timeout)
        assert location_error.value.code == 404
        append_job = bq_client.get_job(
            append_job_id, location="US", timeout=test_timeout
        )
        assert append_job.location == "US"
        assert append_job.write_disposition == bigquery.WriteDisposition.WRITE_APPEND

        truncate_job_id = f"python-truncate-{suffix}"
        truncate_job = _run_destination_query(
            bq_client,
            "SELECT 3 AS id, 'three' AS name",
            destination_ref,
            bigquery.WriteDisposition.WRITE_TRUNCATE,
            truncate_job_id,
            test_timeout,
        )
        assert truncate_job.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
        destination_rows = list(
            bq_client.query(
                f"SELECT id, name FROM `{destination_ref}` ORDER BY id",
                location="US",
                retry=None,
                timeout=test_timeout,
                job_retry=None,
            ).result(page_size=1, timeout=test_timeout, retry=None, job_retry=None)
        )
        assert [(row["id"], row["name"]) for row in destination_rows] == [
            (3, "three")
        ]

        destination = bq_client.get_table(destination_ref, timeout=test_timeout)
        destination.description = "patched"
        destination.labels = {"owner": "contracts"}
        destination.schema = [
            *destination.schema,
            bigquery.SchemaField("note", "STRING", mode="NULLABLE"),
        ]
        patched_table = bq_client.update_table(
            destination,
            ["description", "labels", "schema"],
            timeout=test_timeout,
        )
        assert patched_table.description == "patched"
        assert patched_table.labels == {"owner": "contracts"}
        assert [field.name for field in patched_table.schema] == ["id", "name", "note"]

        paged_job_id = f"python-pages-{suffix}"
        paged_job = bq_client.query(
            "SELECT 1 AS ordinal UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY ordinal",
            job_id=paged_job_id,
            location="US",
            retry=None,
            timeout=test_timeout,
            job_retry=None,
        )
        row_iterator = paged_job.result(
            page_size=1,
            max_results=3,
            timeout=test_timeout,
            retry=None,
            job_retry=None,
        )
        assert [row["ordinal"] for row in row_iterator] == [1, 2, 3]

        # page_size=1 forces distinct jobs.list requests while max_results bounds
        # the aggregate client result. Each HTTP request retains its API timeout.
        job_pages = list(
            bq_client.list_jobs(
                project=project_id,
                max_results=3,
                page_size=1,
                timeout=test_timeout,
            ).pages
        )
        page_jobs = [list(page) for page in job_pages]
        assert len(job_pages) == 3, f"{LIFECYCLE_CONTRACT}: jobs.list page shape"
        assert all(len(page) == 1 for page in page_jobs)
        assert all(job.location == "US" for page in page_jobs for job in page)
    finally:
        bq_client.delete_dataset(
            dataset_ref,
            delete_contents=True,
            not_found_ok=True,
            timeout=test_timeout,
        )


@pytest.mark.gap(REQUEST_RETRY_GAP)
@pytest.mark.xfail(
    strict=True,
    raises=RequestIDIdempotencyGap,
    reason=(
        f"google-cloud-bigquery {CLIENT_VERSION} / query.sync.request-controls-v1: the "
        "requestId idempotency ledger is not implemented; lost-response retries "
        "currently create a second query job"
    ),
)
def test_query_and_wait_retries_same_request_id_without_duplicate_execution(
    bq_client: bigquery.Client, test_timeout: float
) -> None:
    """Exercise the exact query_and_wait requestId/timeoutMs retry wire shape.

    The pinned client adds one requestId before its transport retry wrapper, so
    every retry must identify the same query. We inject a retryable failure only
    after the first public HTTP response to model a lost response at the edge.
    """

    original_call_api: Callable[..., dict[str, Any]] = bq_client._call_api
    request_ids: list[str] = []
    timeout_values: list[int | None] = []
    response_job_ids: list[str] = []

    def lose_first_query_response(*args: Any, **kwargs: Any) -> dict[str, Any]:
        response = original_call_api(*args, **kwargs)
        if kwargs.get("method") == "POST" and str(kwargs.get("path", "")).endswith(
            "/queries"
        ):
            data = kwargs.get("data")
            request_ids.append(data.get("requestId", "") if isinstance(data, dict) else "")
            timeout_values.append(
                data.get("timeoutMs") if isinstance(data, dict) else None
            )
            reference = response.get("jobReference", {})
            response_job_ids.append(reference.get("jobId", ""))
            if len(request_ids) == 1:
                raise exceptions.ServiceUnavailable("injected response-loss boundary")
        return response

    retry = Retry(
        predicate=if_transient_error,
        initial=0.01,
        maximum=0.01,
        multiplier=1.0,
        timeout=min(test_timeout, 5.0),
    )
    bq_client._call_api = lose_first_query_response
    try:
        try:
            rows = list(
                bq_client.query_and_wait(
                    "SELECT 7 AS answer",
                    location="US",
                    api_timeout=test_timeout,
                    wait_timeout=test_timeout,
                    retry=retry,
                    job_retry=None,
                )
            )
        except exceptions.GoogleAPICallError as error:
            status = getattr(error, "code", "unknown")
            raise PublicEdgeContractError(
                _diagnostic(
                    REQUEST_RETRY_CONTRACT,
                    f"query-and-wait-http-{status}",
                    "inspect-server-log",
                )
            ) from None
    finally:
        bq_client._call_api = original_call_api

    assert rows[0]["answer"] == 7, _diagnostic(
        REQUEST_RETRY_CONTRACT, "single-int64-row", "compare-query-row-codec"
    )
    assert len(request_ids) == 2, _diagnostic(
        REQUEST_RETRY_CONTRACT, "retry-attempt-count", "compare-client-retry-policy"
    )
    assert request_ids[0] != "", _diagnostic(
        REQUEST_RETRY_CONTRACT, "missing-request-id", "compare-query-request-shape"
    )
    assert len(set(request_ids)) == 1, _diagnostic(
        REQUEST_RETRY_CONTRACT, "request-id-drift", "compare-client-retry-policy"
    )
    assert len(timeout_values) == 2 and all(
        isinstance(value, int) and value >= 0 for value in timeout_values
    ), _diagnostic(
        REQUEST_RETRY_CONTRACT,
        "missing-or-invalid-timeout-ms",
        "compare-query-request-shape",
    )
    assert len(set(timeout_values)) == 1, _diagnostic(
        REQUEST_RETRY_CONTRACT, "timeout-ms-drift", "compare-client-retry-policy"
    )
    assert response_job_ids[0] != "", _diagnostic(
        REQUEST_RETRY_CONTRACT, "missing-job-reference", "compare-query-response-shape"
    )
    if len(set(response_job_ids)) != 1:
        raise RequestIDIdempotencyGap(
            _diagnostic(
                REQUEST_RETRY_GAP,
                "duplicate-execution-after-response-loss",
                "implement-15-minute-request-id-ledger",
            )
        )
