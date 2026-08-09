<!-- doc-id: generated/integration-consumer-contract -->
<!-- lang: en -->

[English](.) | [한국어](../../ko/generated/integration-consumer-contract.md)

# Generated Integration Consumer Contract

<!-- section: operations -->
This generated inventory is derived from literal integration-test annotations.

| Operation | Scenario | Source annotation |
| --- | --- | --- |
| `bigquery.datasets.get.metadata-view` | `dataset-metadata-view` | `tests/integration/test_catalog_metadata.py:2` |
| `bigquery.datasets.list.filter` | `dataset-label-filter` | `tests/integration/test_catalog_metadata.py:1` |
| `bigquery.jobs.insert.media-upload` | `parquet-media-upload` | `tests/integration/test_parquet_media_upload.py:1` |
| `bigquery.jobs.query.parameters` | `query-parameters` | `tests/integration/test_query_parameters.py:1` |
| `bigquery.tabledata.insert-all` | `tabledata-insert-all` | `tests/integration/test_tabledata_insert_all.py:1` |

<!-- section: runner-only-exceptions -->
## Runner-only exceptions

No exceptions are declared.

<!-- section: provenance -->
Operation names follow the [BigQuery REST reference](https://cloud.google.com/bigquery/docs/reference/rest).
