<!-- doc-id: integration-capability-coverage -->
<!-- lang: en -->

[English](capability-coverage.md) | [한국어](../ko/capability-coverage.md)

# Spark BigQuery Connector Coverage

Version-bound claims in this table use the [pinned source revision](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/719817782a214b8ca72be520870013a3e0253d92).

> Generated from test-local `contract_case(...)` annotations. Edit the test, then run `make integration-contract-generate`.

Only behaviors in this table are support claims. `verified` has a passing public test; `partial` works only within its stated limit.

<!-- section: claims -->
## Test-Backed Claims

| State | Behavior | Claim |
| --- | --- | --- |
| `verified` | Arrow query result count | `SBQ-READ-ARROW-COUNT-V1` |
| `partial` | Arrow decimal schema read | `SBQ-READ-ARROW-DECIMAL-TYPES-V1` |
| Limit | Spark 3.5.8 verifies connector 0.44.2 Arrow schemas for default, parameterized, nested, and repeated decimals. Precision above 38 is intentionally unsupported; nested and repeated values remain covered below the released-Spark boundary. ([issue](https://github.com/leeyh0216/go-bemu/issues/9)) |  |
| `partial` | Arrow filter pushdown | `SBQ-READ-ARROW-FILTER-V1` |
| Limit | Comparisons, IN, null predicates, nested boolean logic, string LIKE filters, and temporal literals are implemented; function calls and subqueries remain unsupported. ([issue](https://github.com/leeyh0216/go-bemu/issues/6)) |  |
| `partial` | Arrow nested projection | `SBQ-READ-ARROW-PROJECTION-V1` |
| Limit | Nested Spark projection is verified end to end; the DSv1 artifact requests its top-level parent while exact nested selected-field paths remain transport-tested. ([issue](https://github.com/leeyh0216/go-bemu/issues/6)) |  |
| `verified` | Arrow query source filter pushdown | `SBQ-READ-ARROW-QUERY-FILTER-V1` |
| `verified` | Arrow query with explicit materialization | `SBQ-READ-ARROW-QUERY-MATERIALIZED-V1` |
| `verified` | Arrow query source projection | `SBQ-READ-ARROW-QUERY-PROJECTION-V1` |
| `verified` | Arrow query source read | `SBQ-READ-ARROW-QUERY-V1` |
| `verified` | Arrow table read with sixteen requested streams | `SBQ-READ-ARROW-STREAM-SIXTEEN-V1` |
| `verified` | Arrow table read with four requested streams | `SBQ-READ-ARROW-TABLE-V1` |
| `verified` | Avro query source read | `SBQ-READ-AVRO-QUERY-V1` |
| `verified` | Avro table read with one requested stream | `SBQ-READ-AVRO-STREAM-ONE-V1` |
| `verified` | Avro table read with sixteen requested streams | `SBQ-READ-AVRO-STREAM-SIXTEEN-V1` |
| `verified` | Avro table read with two requested streams | `SBQ-READ-AVRO-STREAM-TWO-V1` |
| `verified` | Avro table read with four requested streams | `SBQ-READ-AVRO-TABLE-V1` |
| `partial` | Avro decimal schema read | `SBQ-READ-DECIMAL-TYPES-V1` |
| Limit | Spark 3.5.8 verifies connector 0.44.2 AVRO schemas for default, parameterized, nested, and repeated decimals. Precision above 38 is intentionally unsupported; nested and repeated values remain covered below the released-Spark boundary. ([issue](https://github.com/leeyh0216/go-bemu/issues/9)) |  |
| `verified` | Arrow table read with one requested stream | `SBQ-READ-STREAM-ONE-V1` |
| `verified` | Arrow table read with two requested streams | `SBQ-READ-STREAM-TWO-V1` |
| `partial` | Arrow field-partition filter | `SBQ-READ-TIME-PARTITION-V1` |
| Limit | Field-based time partition metadata and filters are implemented; ingestion-time pseudo columns and physical partition pruning remain unsupported. ([issue](https://github.com/leeyh0216/go-bemu/issues/6)) |  |
| `verified` | Direct at-least-once append with four partitions | `SBQ-WRITE-DIRECT-ALO-APPEND-FOUR-V1` |
| `verified` | Direct at-least-once append with one partition | `SBQ-WRITE-DIRECT-ALO-APPEND-ONE-V1` |
| `verified` | Direct at-least-once append with two partitions | `SBQ-WRITE-DIRECT-ALO-APPEND-TWO-V1` |
| `partial` | Direct decimal ProtoRows write | `SBQ-WRITE-DIRECT-DECIMAL-V1` |
| Limit | Spark 3.5.8 verifies connector 0.44.2 direct ProtoRows for scalar NUMERIC(20,4) and BIGNUMERIC(38,18). Recursive decimal ProtoRows are covered below the released-Spark boundary. ([issue](https://github.com/leeyh0216/go-bemu/issues/9)) |  |
| `verified` | Direct exactly-once append with four partitions | `SBQ-WRITE-DIRECT-EXACT-APPEND-FOUR-V1` |
| `verified` | Direct exactly-once append with one partition | `SBQ-WRITE-DIRECT-EXACT-APPEND-ONE-V1` |
| `verified` | Direct exactly-once append with two partitions | `SBQ-WRITE-DIRECT-EXACT-APPEND-TWO-V1` |
| `verified` | Direct exactly-once dynamic partition overwrite | `SBQ-WRITE-DIRECT-EXACT-DYNAMIC-OVERWRITE-V1` |
| `verified` | Direct exactly-once static overwrite | `SBQ-WRITE-DIRECT-EXACT-OVERWRITE-V1` |
<!-- section: api-coverage -->
## Public API Coverage

| API/RPC | Test-backed claims |
| --- | --- |
| `bigquery.jobs.get` | 14 |
| `bigquery.jobs.getQueryResults` | 14 |
| `bigquery.jobs.insert` | 14 |
| `bigquery.tabledata.list` | 2 |
| `bigquery.tables.delete` | 2 |
| `bigquery.tables.get` | 19 |
| `bigquery.tables.insert` | 11 |
| `bigquery.tables.patch` | 1 |
| `grpc.bigquery-read.create-read-session` | 19 |
| `grpc.bigquery-read.read-rows` | 19 |
| `grpc.bigquery-write.append-rows` | 9 |
| `grpc.bigquery-write.batch-commit-write-streams` | 6 |
| `grpc.bigquery-write.create-write-stream` | 6 |
| `grpc.bigquery-write.finalize-write-stream` | 6 |
| `grpc.bigquery-write.get-write-stream` | 3 |

The complete claim-to-API mapping is in `tests/integration/contract/capabilities.normalized.json`. Profiles and goldens are source-reviewed wire contracts. CI runtime traces are not compared to them automatically today; they are retained as per-run evidence artifacts.
