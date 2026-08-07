<!-- doc-id: compatibility -->
<!-- lang: en -->

[English](compatibility.md) | [한국어](../ko/compatibility.md)

# Compatibility Contract

<!-- section: status-language -->
## Status Language

| Status | Meaning |
| --- | --- |
| Verified | Implemented and exercised at the stated public or adapter boundary |
| Partial | A useful subset exists and every material limitation is named |
| Registered | Canonical service exists but the operation returns `UNIMPLEMENTED` |
| Planned | Design/provenance exists; callers must not depend on it |
| Unsupported | Absent or deliberately rejected |

These labels describe this repository, not equivalence with the [BigQuery
service](https://cloud.google.com/bigquery/docs/introduction).

<!-- section: rest-metadata -->
## REST Metadata

| Operation | Status | Contract boundary |
| --- | --- | --- |
| health/readiness | Verified | process and warehouse ping |
| emulator project lifecycle | Verified | emulator-only namespace |
| `projects.list` | Verified basic | emulator projects plus opaque page token |
| dataset insert/get | Verified basic | location/labels/default expirations retained |
| dataset list/delete | Verified basic | paging and `deleteContents`; filter/all remain unsupported |
| dataset patch/update | Verified | metadata fields plus ETag/HTTP 412 precondition |
| table insert/get/delete | Verified basic | standard table and canonical schema metadata |
| table list | Verified basic | paging; no view/storage statistics |
| table patch/update | Verified narrow | metadata plus additive schema and ETag precondition |
| `tabledata.list` / `insertAll` | Unsupported | no route |

Request/response shapes are compared with official
[`datasets`](https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets) and
[`tables`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables)
resources. Ignoring an unknown JSON field is forward-tolerant decoding, not
implementation of that field.

`CAP-REST-METADATA-PATCH-V1` and `CAP-SCHEMA-ADDITIVE-V1` are also exercised by
the official [Python client
`3.43.0`](https://pypi.org/project/google-cloud-bigquery/3.43.0/) against a real
process. Schema support is append-only `NULLABLE`/`REPEATED`, including nested
and repeated records; DDL conversion, relaxation, and job-driven evolution are
not implied.

<!-- section: jobs -->
## Query and Jobs

| Operation | Status | Limit |
| --- | --- | --- |
| `jobs.query` | Partial | Python 3.43.0 path verified; synchronous DuckDB-compatible SQL subset |
| query `jobs.insert` | Partial | Python 3.43.0 polling path verified; process-local asynchronous execution |
| `jobs.get` | Verified basic | `PENDING/RUNNING/DONE`, terminal errors |
| `jobs.list` | Partial | `maxResults` truncation only |
| `jobs.getQueryResults` | Partial | `startIndex`/max results, no opaque page token |
| destination table/dispositions | Unsupported | not represented in job domain |
| cancellation | Unsupported | no route/state |
| load/copy/extract | Unsupported | only query configuration accepted |
| durable job/result state | Unsupported | in-memory repository |
| same-ID idempotent replay | Unsupported | duplicate conflicts |

Canonical job state and error fields come from the official
[`Job`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job) resource.
Nested/repeated result cells and type-specific temporal values are not yet full
[`TableRow`](https://cloud.google.com/bigquery/docs/reference/rest/v2/TableRow)
encodings.

<!-- section: sql -->
## SQL and MERGE

| Behavior | Status | Limit |
| --- | --- | --- |
| fully qualified table reference | Verified narrow case | backtick table token translated |
| `SELECT`/`INSERT` | Partial | DuckDB syntax and functions |
| `UPDATE`/`DELETE` | Partial | DuckDB statement behavior |
| basic `MERGE` | Partial | one tested DuckDB-compatible form |
| connector static overwrite | Planned | requires exact template adapter |
| dynamic partition overwrite | Unsupported | scripts/arrays/partition semantics absent |
| parameters/scripts/views/UDFs | Unsupported | no semantic adapter |

The [GoogleSQL lexical
contract](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical)
distinguishes quoted identifiers by syntactic position. The current broad
backtick rewrite cannot safely classify quoted columns, comments, or strings;
therefore arbitrary backtick SQL is not supported. General `MERGE` must follow
the [official DML
rules](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement),
including source cardinality and atomic visibility.

<!-- section: types -->
## Types

| BigQuery type group | Physical table creation | REST query value | Overall |
| --- | --- | --- | --- |
| BOOL/INT64/FLOAT64/STRING/BYTES | basic mapping | scalar encoding | Partial |
| NUMERIC | `DECIMAL(38,9)` | driver-dependent | Partial |
| BIGNUMERIC | text preservation | loses engine type identity | Unsupported arithmetic |
| DATE/DATETIME/TIME/TIMESTAMP | engine mapping | temporal formatting incomplete | Partial |
| JSON/GEOGRAPHY | JSON/text mapping | incomplete semantics | Partial/Unsupported |
| RECORD/REPEATED | STRUCT/LIST mapping | composite REST shape incompatible | Partial |

Compatibility is assessed against [BigQuery data
types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types).
No type is yet verified end to end across REST, Arrow, Avro, direct Proto write,
and indirect load.

<!-- section: storage-read -->
## Storage Read

| RPC/behavior | Status |
| --- | --- |
| official service registration/reflection | Registered |
| read service health | `NOT_SERVING` |
| `CreateReadSession` / `ReadRows` application and protobuf adapter | Verified with fake snapshots |
| public `CreateReadSession` / `ReadRows` | Registered, returns `UNIMPLEMENTED` because no snapshot adapter is composed |
| public `SplitReadStream` | Registered, inherited `UNIMPLEMENTED` |
| Arrow/Avro schema and row payloads | bare wire pass-through verified; DuckDB encoders absent |
| projection/filter/snapshot | request forwarding verified; DuckDB semantics absent |
| multiple streams/offset resume | application range/offset behavior verified; public runtime unsupported |

These internal tests do not raise the public capability above Registered. A real
DuckDB `SnapshotMaterializer`, Arrow/Avro encoders, runtime composition, and
public-endpoint E2E must all pass first.

The target contract is the official
[`BigQueryRead`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryRead)
service and connector
[`ReadSessionCreator.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/ReadSessionCreator.java).

<!-- section: storage-write -->
## Storage Write

| RPC/behavior | Status |
| --- | --- |
| official service registration/reflection | Registered |
| write service health | `NOT_SERVING` |
| create/get/append/finalize/commit/flush | Registered, returns `UNIMPLEMENTED` |
| default stream | Planned |
| multiple pending streams and offsets | Planned |
| atomic batch commit | Planned |

The target contract is the official
[`BigQueryWrite`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite)
service and connector
[`BigQueryDirectDataWriterHelper.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryDirectDataWriterHelper.java).

<!-- section: load-auth -->
## Load, Object Storage, and Identity

| Capability | Status |
| --- | --- |
| filesystem object-store adapter | Verified structurally |
| GCS/fake-GCS adapter | Planned |
| Parquet/Avro/ORC/CSV/JSON load job | Unsupported |
| write dispositions and atomic staging | Unsupported |
| REST/gRPC TLS | Implemented when configured |
| authentication disabled | Current mode |
| static token, ADC, OAuth, STS/WIF | Planned |
| IAM authorization | Unsupported |

The load target is
[`JobConfigurationLoad`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad).
Identity claims are separated according to [Google Cloud
authentication](https://cloud.google.com/docs/authentication); local token
acquisition must never be described as IAM parity.

<!-- section: persistence-atomicity -->
## Persistence and Atomicity

DuckDB file storage can retain physical rows, but catalog and jobs are
process-local. Metadata DDL and physical DDL do not share one durable transaction.
Additive physical columns are applied in one DuckDB transaction, but publication
to the in-memory catalog is not crash-atomic with that transaction.
There is no read-snapshot ledger, write-stream ledger, or load staging
transaction. Restart durability, same-ID replay, atomic load disposition, and
atomic multi-stream commit are therefore unsupported.

<!-- section: client-coverage -->
## Client Coverage

The exact [`bq` CLI `2.1.31`](https://cloud.google.com/bigquery/docs/reference/bq-cli-reference)
from [Google Cloud SDK `566.0.0`](https://cloud.google.com/sdk/docs/release-notes#56600_2026-04-28)
runs in its own CI layer with UI disabled. It verifies project listing, dataset
and table lifecycle, additive nullable schema update, query polling, job/table
listing, cleanup, and the not-found exit contract. Four official [Python client
`3.43.0`](https://pypi.org/project/google-cloud-bigquery/3.43.0/) E2E tests verify
dataset administration, table metadata/schema administration, synchronous
[`jobs.query`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query),
and asynchronous [`jobs.insert`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert)
through [`jobs.getQueryResults`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults).
The corresponding [`python-query-sync`](../../contract/golden/python-query-sync-3.43.0.json)
and [`python-query-async`](../../contract/golden/python-query-async-3.43.0.json)
goldens pin those shapes. Load/copy/extract, `insertAll`, and `tabledata.list`
remain five strict expected-gap xfails. The connector profile records expected
calls for version `0.44.2`; it does not imply Storage flows succeed. Every
capability promotion needs a public-edge test and a negative/boundary test.

The [`bq-project-dataset-admin`](../../contract/golden/bq-project-dataset-admin-2.1.31.json),
[`bq-table-schema-admin`](../../contract/golden/bq-table-schema-admin-2.1.31.json),
[`bq-query-job`](../../contract/golden/bq-query-job-2.1.31.json), and
[`bq-not-found-error`](../../contract/golden/bq-not-found-error-2.1.31.json)
goldens pin the CLI wire stages. Load, copy, and extract remain Planned in that
profile and therefore keep issue #13 open.

<!-- section: removal-criteria -->
## Workaround Removal Criteria

A compatibility workaround may be removed only after its pinned upstream defect
is reproduced, the exact upstream version no longer exhibits it, golden wire
traces agree, and direct connector tests pass without the rule. Generalizing a
workaround requires a protocol or semantic source, not another regex example.
