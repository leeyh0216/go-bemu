<!-- doc-id: generated/integration-consumer-contract -->
<!-- lang: ko -->

[English](../../en/generated/integration-consumer-contract.md) | [한국어](.)

# 생성된 통합 소비자 계약

<!-- section: operations -->
이 생성 목록은 통합 테스트 원본의 literal annotation에서 파생됩니다.

| Operation | Scenario | 원본 annotation |
| --- | --- | --- |
| `bigquery.datasets.get.metadata-view` | `dataset-metadata-view` | `tests/integration/test_catalog_metadata.py:2` |
| `bigquery.datasets.list.filter` | `dataset-label-filter` | `tests/integration/test_catalog_metadata.py:1` |
| `bigquery.jobs.insert.media-upload` | `parquet-media-upload` | `tests/integration/test_parquet_media_upload.py:1` |
| `bigquery.jobs.query.parameters` | `query-parameters` | `tests/integration/test_query_parameters.py:1` |
| `bigquery.tabledata.insert-all` | `tabledata-insert-all` | `tests/integration/test_tabledata_insert_all.py:1` |

<!-- section: runner-only-exceptions -->
## runner 전용 예외

현재 선언된 예외가 없습니다.

<!-- section: provenance -->
Operation 이름은 [BigQuery REST reference](https://cloud.google.com/bigquery/docs/reference/rest)를 따릅니다.
