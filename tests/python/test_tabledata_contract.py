"""Official google-cloud-bigquery 3.43.0 tabledata.list contract.

Pinned client and protocol sources:
https://github.com/googleapis/google-cloud-python/blob/google-cloud-bigquery-v3.43.0/packages/google-cloud-bigquery/google/cloud/bigquery/client.py#L4118-L4237
https://github.com/googleapis/google-cloud-python/blob/google-cloud-bigquery-v3.43.0/packages/google-cloud-bigquery/google/cloud/bigquery/table.py#L1838-L1995
https://github.com/googleapis/google-cloud-python/blob/google-api-core-v2.34.0/packages/google-api-core/google/api_core/page_iterator.py#L397-L420
https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
https://cloud.google.com/bigquery/docs/reference/rest/v2/FormatOptions

Captured evidence contains only normalized method/path/query-key and
response/client-decoding shape metadata. Table names and response row values are
never retained.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
import uuid

from google.cloud import bigquery
import pytest


CLIENT_VERSION = "3.43.0"
TABLEDATA_CONTRACT = "PY-TABLEDATA-LIST-001"


def _diagnostic(shape: str, fix_hint: str) -> str:
    return (
        f"consumer=google-cloud-bigquery consumer_version={CLIENT_VERSION} "
        f"contract={TABLEDATA_CONTRACT} shape={shape} fix_hint={fix_hint}"
    )


@pytest.mark.operation("bigquery.tabledata.list")
def test_list_rows_decodes_nested_repeated_values_and_pages(
    bq_client: bigquery.Client, project_id: str, test_timeout: float
) -> None:
    suffix = uuid.uuid4().hex[:10]
    dataset_ref = f"{project_id}.tabledata_{suffix}"
    table_ref = f"{dataset_ref}.current_source"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    bq_client.create_dataset(dataset, timeout=test_timeout)
    try:
        table = bigquery.Table(
            table_ref,
            schema=[
                bigquery.SchemaField("ordinal", "INT64", mode="REQUIRED"),
                bigquery.SchemaField("label", "STRING"),
                bigquery.SchemaField(
                    "payload",
                    "RECORD",
                    fields=[
                        bigquery.SchemaField("score", "INT64"),
                        bigquery.SchemaField("name", "STRING"),
                    ],
                ),
                bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
                bigquery.SchemaField("event_time", "TIMESTAMP"),
            ],
        )
        table = bq_client.create_table(table, timeout=test_timeout)
        insert_job = bq_client.query(
            f"""
            INSERT INTO `{table_ref}` VALUES
            (1, 'first', {{'score': 3, 'name': 'nested-one'}}, ['alpha', 'beta'],
             TIMESTAMPTZ '2026-08-08 01:02:03.123456+00'),
            (2, NULL, {{'score': NULL, 'name': 'nested-two'}}, [],
             TIMESTAMPTZ '1969-12-31 23:59:59.000001+00'),
            (3, 'third', {{'score': 9, 'name': 'nested-three'}}, ['omega'],
             TIMESTAMPTZ '2000-01-01 00:00:00+00')
            """,
            location="US",
            retry=None,
            timeout=test_timeout,
            job_retry=None,
        )
        list(insert_job.result(timeout=test_timeout, retry=None, job_retry=None))

        calls: list[dict[str, Any]] = []
        original_call_api: Callable[..., dict[str, Any]] = bq_client._call_api

        def capture_tabledata_shape(*args: Any, **kwargs: Any) -> dict[str, Any]:
            response = original_call_api(*args, **kwargs)
            path = str(kwargs.get("path", ""))
            if kwargs.get("method") == "GET" and path.endswith("/data"):
                query = dict(kwargs.get("query_params") or {})
                calls.append(
                    {
                        "method": "GET",
                        "target": "/bigquery/v2/projects/{project}/datasets/{dataset}/tables/{table}/data",
                        "query_keys": sorted(query),
                        "max_results": query.get("maxResults"),
                        "has_page_token": bool(query.get("pageToken")),
                        "start_index": query.get("startIndex"),
                        "timestamp_option": query.get(
                            "formatOptions.useInt64Timestamp"
                        ),
                        "response_kind": response.get("kind"),
                        "response_total_rows": response.get("totalRows"),
                        "response_row_count": len(response.get("rows", [])),
                        "response_has_page_token": bool(response.get("pageToken")),
                    }
                )
            return response

        bq_client._call_api = capture_tabledata_shape
        try:
            page_iterator = bq_client.list_rows(
                table,
                max_results=2,
                page_size=1,
                retry=None,
                timeout=test_timeout,
            )
            pages = list(page_iterator.pages)
            rows = [row for page in pages for row in page]
            start_rows = list(
                bq_client.list_rows(
                    table,
                    start_index=1,
                    max_results=1,
                    page_size=1,
                    retry=None,
                    timeout=test_timeout,
                )
            )
            # google-api-core preserves max_results=0 as maxResults=0 on the
            # first request. The service must return the exact table count but
            # neither rows nor a continuation token for that zero-row budget.
            # https://github.com/googleapis/google-cloud-python/blob/google-api-core-v2.34.0/packages/google-api-core/google/api_core/page_iterator.py#L397-L420
            zero_iterator = bq_client.list_rows(
                table,
                max_results=0,
                page_size=1,
                retry=None,
                timeout=test_timeout,
            )
            zero_pages = list(zero_iterator.pages)
        finally:
            bq_client._call_api = original_call_api

        assert len(pages) == 2 and all(page.num_items == 1 for page in pages), (
            _diagnostic("two-single-row-pages", "compare-tabledata-page-token")
        )
        assert page_iterator.total_rows == 3
        assert [row["ordinal"] for row in rows] == [1, 2]
        assert rows[0]["label"] == "first"
        assert rows[1]["label"] is None
        assert rows[0]["payload"]["score"] == 3
        assert rows[0]["payload"]["name"] == "nested-one"
        assert rows[1]["payload"]["score"] is None
        assert rows[1]["payload"]["name"] == "nested-two"
        assert list(rows[0]["tags"]) == ["alpha", "beta"]
        assert list(rows[1]["tags"]) == []
        assert rows[0]["event_time"] == datetime(
            2026, 8, 8, 1, 2, 3, 123456, tzinfo=timezone.utc
        ), _diagnostic("timestamp-microseconds", "compare-use-int64-timestamp")
        assert rows[1]["event_time"] == datetime(
            1969, 12, 31, 23, 59, 59, 1, tzinfo=timezone.utc
        ), _diagnostic("timestamp-before-epoch", "compare-signed-epoch-microseconds")
        assert (
            len(start_rows) == 1
            and start_rows[0]["ordinal"] == 2
            and start_rows[0]["event_time"] == rows[1]["event_time"]
        )
        assert len(zero_pages) == 1 and zero_pages[0].num_items == 0, _diagnostic(
            "explicit-zero-single-empty-page", "preserve-max-results-presence"
        )
        assert zero_iterator.total_rows == 3
        assert zero_iterator.next_page_token is None

        assert len(calls) == 4, _diagnostic(
            "four-tabledata-requests", "compare-client-pagination"
        )
        common_keys = {"formatOptions.useInt64Timestamp", "maxResults"}
        assert all(
            call["method"] == "GET"
            and call["target"]
            == "/bigquery/v2/projects/{project}/datasets/{dataset}/tables/{table}/data"
            for call in calls
        )
        assert set(calls[0]["query_keys"]) == common_keys
        assert calls[0]["max_results"] == 1
        assert calls[0]["response_kind"] == "bigquery#tableDataList"
        assert calls[0]["response_total_rows"] == "3"
        assert calls[0]["response_row_count"] == 1
        assert calls[0]["response_has_page_token"] is True
        assert calls[1]["max_results"] == 1
        assert set(calls[1]["query_keys"]) == common_keys | {"pageToken"}
        assert calls[1]["has_page_token"] is True
        assert calls[1]["response_row_count"] == 1
        assert set(calls[2]["query_keys"]) == common_keys | {"startIndex"}
        assert calls[2]["start_index"] == 1
        assert calls[2]["response_row_count"] == 1
        assert set(calls[3]["query_keys"]) == common_keys
        assert calls[3]["max_results"] == 0
        assert calls[3]["response_kind"] == "bigquery#tableDataList"
        assert calls[3]["response_total_rows"] == "3"
        assert calls[3]["response_row_count"] == 0
        assert calls[3]["response_has_page_token"] is False
        assert all(call["timestamp_option"] is True for call in calls)
    finally:
        bq_client.delete_dataset(
            dataset_ref,
            delete_contents=True,
            not_found_ok=True,
            timeout=test_timeout,
        )
