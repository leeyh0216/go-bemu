<!-- doc-id: integration-capability-coverage -->
<!-- lang: ko -->

[English](../en/capability-coverage.md) | [한국어](capability-coverage.md)

# Spark BigQuery Connector Coverage

이 표의 version-bound claim은 [고정 source revision](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/719817782a214b8ca72be520870013a3e0253d92)을 기준으로 합니다.

> 생성 파일입니다. 테스트의 `contract_case(...)` annotation을 수정한 뒤 `make integration-contract-generate`를 실행하세요.

아래 표에 없는 동작은 지원 claim이 아닙니다. `verified`는 해당 공개 테스트가 통과했고, `partial`은 표에 적힌 제한 안에서만 동작합니다.

<!-- section: claims -->
## 테스트 기반 Claim

| 상태 | 동작 | 검증 경로 |
| --- | --- | --- |
| `verified` | Arrow query result count | `SBQ-READ-ARROW-COUNT-V1` |
| `partial` | Arrow decimal schema read | `SBQ-READ-ARROW-DECIMAL-TYPES-V1` |
| 제한 | Spark 3.5.8 verifies connector 0.44.2 Arrow schemas for default, parameterized, nested, and repeated decimals. Precision above 38 is intentionally unsupported; nested and repeated values remain covered below the released-Spark boundary. ([issue](https://github.com/leeyh0216/go-bemu/issues/9)) |  |
| `partial` | Arrow filter pushdown | `SBQ-READ-ARROW-FILTER-V1` |
| 제한 | Comparisons, IN, null predicates, nested boolean logic, string LIKE filters, and temporal literals are implemented; function calls and subqueries remain unsupported. ([issue](https://github.com/leeyh0216/go-bemu/issues/6)) |  |
| `partial` | Arrow nested projection | `SBQ-READ-ARROW-PROJECTION-V1` |
| 제한 | Nested Spark projection is verified end to end; the DSv1 artifact requests its top-level parent while exact nested selected-field paths remain transport-tested. ([issue](https://github.com/leeyh0216/go-bemu/issues/6)) |  |
| `verified` | Arrow query source filter pushdown | `SBQ-READ-ARROW-QUERY-FILTER-V1` |
| `verified` | Arrow query with explicit materialization | `SBQ-READ-ARROW-QUERY-MATERIALIZED-V1` |
| `verified` | Arrow query source projection | `SBQ-READ-ARROW-QUERY-PROJECTION-V1` |
| `verified` | Arrow query source read | `SBQ-READ-ARROW-QUERY-V1` |
| `verified` | Arrow table read with sixteen requested streams | `SBQ-READ-ARROW-STREAM-SIXTEEN-V1` |
| `verified` | Arrow table read with four requested streams | `SBQ-READ-ARROW-TABLE-V1` |
| `partial` | Avro filter pushdown | `SBQ-READ-AVRO-FILTER-V1` |
| 제한 | Comparisons, IN, null predicates, nested boolean logic, string LIKE filters, and temporal literals are implemented; function calls and subqueries remain unsupported. ([issue](https://github.com/leeyh0216/go-bemu/issues/6)) |  |
| `verified` | Avro query source read | `SBQ-READ-AVRO-QUERY-V1` |
| `verified` | Avro table read with one requested stream | `SBQ-READ-AVRO-STREAM-ONE-V1` |
| `verified` | Avro table read with sixteen requested streams | `SBQ-READ-AVRO-STREAM-SIXTEEN-V1` |
| `verified` | Avro table read with two requested streams | `SBQ-READ-AVRO-STREAM-TWO-V1` |
| `verified` | Avro table read with four requested streams | `SBQ-READ-AVRO-TABLE-V1` |
| `partial` | Avro decimal schema read | `SBQ-READ-DECIMAL-TYPES-V1` |
| 제한 | Spark 3.5.8 verifies connector 0.44.2 AVRO schemas for default, parameterized, nested, and repeated decimals. Precision above 38 is intentionally unsupported; nested and repeated values remain covered below the released-Spark boundary. ([issue](https://github.com/leeyh0216/go-bemu/issues/9)) |  |
| `verified` | Arrow table read with one requested stream | `SBQ-READ-STREAM-ONE-V1` |
| `verified` | Arrow table read with two requested streams | `SBQ-READ-STREAM-TWO-V1` |
| `partial` | Arrow field-partition filter | `SBQ-READ-TIME-PARTITION-V1` |
| 제한 | Field-based time partition metadata and filters are implemented; ingestion-time pseudo columns and physical partition pruning remain unsupported. ([issue](https://github.com/leeyh0216/go-bemu/issues/6)) |  |
| `verified` | Direct at-least-once append with four partitions | `SBQ-WRITE-DIRECT-ALO-APPEND-FOUR-V1` |
| `verified` | Direct at-least-once append with one partition | `SBQ-WRITE-DIRECT-ALO-APPEND-ONE-V1` |
| `verified` | Direct at-least-once append with two partitions | `SBQ-WRITE-DIRECT-ALO-APPEND-TWO-V1` |
| `partial` | Direct decimal ProtoRows write | `SBQ-WRITE-DIRECT-DECIMAL-V1` |
| 제한 | Spark 3.5.8 verifies connector 0.44.2 direct ProtoRows for scalar NUMERIC(20,4) and BIGNUMERIC(38,18). Recursive decimal ProtoRows are covered below the released-Spark boundary. ([issue](https://github.com/leeyh0216/go-bemu/issues/9)) |  |
| `verified` | Direct exactly-once append with four partitions | `SBQ-WRITE-DIRECT-EXACT-APPEND-FOUR-V1` |
| `verified` | Direct exactly-once append with one partition | `SBQ-WRITE-DIRECT-EXACT-APPEND-ONE-V1` |
| `verified` | Direct exactly-once append with two partitions | `SBQ-WRITE-DIRECT-EXACT-APPEND-TWO-V1` |
| `verified` | Direct exactly-once dynamic partition overwrite | `SBQ-WRITE-DIRECT-EXACT-DYNAMIC-OVERWRITE-V1` |
| `verified` | Direct exactly-once static overwrite | `SBQ-WRITE-DIRECT-EXACT-OVERWRITE-V1` |
<!-- section: api-coverage -->
## 공개 API 범위

| API/RPC | 테스트 기반 claim 수 |
| --- | --- |
| `bigquery.jobs.get` | 14 |
| `bigquery.jobs.getQueryResults` | 14 |
| `bigquery.jobs.insert` | 14 |
| `bigquery.tabledata.list` | 2 |
| `bigquery.tables.delete` | 2 |
| `bigquery.tables.get` | 20 |
| `bigquery.tables.insert` | 11 |
| `bigquery.tables.patch` | 1 |
| `grpc.bigquery-read.create-read-session` | 20 |
| `grpc.bigquery-read.read-rows` | 20 |
| `grpc.bigquery-write.append-rows` | 9 |
| `grpc.bigquery-write.batch-commit-write-streams` | 6 |
| `grpc.bigquery-write.create-write-stream` | 6 |
| `grpc.bigquery-write.finalize-write-stream` | 6 |
| `grpc.bigquery-write.get-write-stream` | 3 |

전체 claim-to-API mapping은 `tests/integration/contract/capabilities.normalized.json`에 있습니다. 프로필과 golden은 source-reviewed wire 계약입니다. 현재 CI의 실제 실행 trace는 이를 자동 비교하지 않으며, 실행별 evidence artifact로만 보관됩니다.
