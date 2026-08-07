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
| `jobs.list` | Partial | location-aware identity and opaque cursor token; process-local snapshot only |
| `jobs.getQueryResults` | Partial | location-aware lookup, `startIndex`, `maxResults`, and job/result-bound opaque page tokens |
| explicit destination table | Partial | scalar exact-schema `WRITE_EMPTY`/`WRITE_APPEND`/`WRITE_TRUNCATE`; capability `query.destination.exact-schema-v1` |
| connector query metadata | Verified basic | `INTERACTIVE`/`BATCH` priority and validated labels, including an explicitly empty label map, are fingerprinted and round-tripped |
| anonymous destination table | Unsupported | gap `query.destination.anonymous-v1` |
| `WRITE_TRUNCATE` schema replacement | Unsupported | exact-schema subset only; gap `query.destination.truncate-schema-replacement-v1` |
| cancellation | Unsupported | no route/state |
| Parquet load `jobs.insert` / `jobs.get` / `jobs.list` | Partial | opt-in, existing destination table, process-local state |
| copy/extract | Unsupported | configuration rejected |
| durable job/result state | Unsupported | in-memory repository |
| bounded query result retention | Unsupported | all result rows remain in Go memory; gap `query.results.unbounded-memory-v1` |
| bounded async query execution | Unsupported | no queue/execution deadline; gap `query.execution.unbounded-v1` |
| same-ID query insert | Verified basic | atomic `(project, location, jobId)` uniqueness; every reuse returns `409 duplicate`, fingerprint retained for diagnostics |
| exact-request replay extension | Unsupported | future opt-in only; gap `query.jobs.exact-replay-extension-v1` |
| query/load cross-type identity | Unsupported | separate repositories have a check/create race; gap `query.jobs.cross-repository-identity-v1` |
| synchronous request controls | Partial | validates the 36-byte ASCII `requestId` and accepts non-negative `timeoutMs`; bounded unfinished responses, mutating-query deduplication, and `jobTimeoutMs` remain gap `query.sync.request-controls-v1` |
| unsupported query options | Strict gap | parameters, `dryRun`, cache/billing controls, and `jobTimeoutMs` are explicitly rejected with `400`; gap `query.options.unsupported-v1` |
| omitted-location dataset inference | Unsupported | configured default wins; gap `query.location.dataset-inference-v1` |
| terminal persistence recovery | Unsupported | a failed terminal repository update can leave `RUNNING`; gap `query.terminal-persistence-v1` |

Canonical job state and error fields come from the official
[`Job`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job) resource.
Nested/repeated result cells and type-specific temporal values are not yet full
[`TableRow`](https://cloud.google.com/bigquery/docs/reference/rest/v2/TableRow)
encodings. Explicit destinations follow
[`JobConfigurationQuery`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery),
and result tokens follow
[`jobs.getQueryResults`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults).
Known-but-unimplemented fields from the official
[`QueryRequest`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#QueryRequest)
and `JobConfigurationQuery` are presence-aware at the REST boundary and fail
before execution; a zero value is never silently accepted as implemented.
BigQuery rejects every reused job ID with `409 duplicate` and recommends
`jobs.get` recovery; BQEMU follows that default and retains a configuration
fingerprint only for safe drift diagnostics. See the official
[retry guidance](https://cloud.google.com/bigquery/docs/reliability-intro#retry_failed_job_insertions).

<!-- section: sql -->
## SQL and MERGE

| Behavior | Status | Limit |
| --- | --- | --- |
| fully qualified table reference | Verified narrow case | backtick table token translated |
| `SELECT`/`INSERT` | Partial | DuckDB syntax and functions |
| `UPDATE`/`DELETE` | Partial | DuckDB statement behavior |
| basic `MERGE` | Partial | one tested DuckDB-compatible form |
| connector `0.44.2` static overwrite | Partial | source-derived token shape; atomic DuckDB `MERGE` |
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

The Static Partial adapter recognizes only the source-derived connector shape
orchestrated by
[`BigQueryClient.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java),
parses its identifiers and clauses as tokens, and executes one atomic [DuckDB
`MERGE INTO`](https://duckdb.org/docs/current/sql/statements/merge_into). Dynamic
time/range partition overwrite and general BigQuery `MERGE` parity remain gaps.

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
| official service registration/reflection | Verified |
| read service health | lifecycle-aware `SERVING` while enabled and not draining |
| public `CreateReadSession` / `ReadRows` | Partial; one bounded DuckDB materialization per session |
| public `SplitReadStream` | Unsupported; returns `UNIMPLEMENTED` |
| Arrow/Avro schema and row payloads | Partial; encoded from bounded DuckDB rows and response bytes |
| projection and row restriction | Partial; top-level fields and a bounded expression subset; nested projection unsupported |
| logical streams and offset resume | Partial; stable ranges and stream-relative offsets within a live session |
| historical snapshot and compression | Unsupported |

The public capability is Partial. Each live session owns one stable, bounded
DuckDB materialization and exposes configurable logical streams. Split RPC,
wire compression, historical `snapshot_time`, nested projection, and durable
session recovery after restart remain gaps.

The target contract is the official
[`BigQueryRead`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryRead)
service and connector
[`ReadSessionCreator.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/ReadSessionCreator.java).

<!-- section: storage-write -->
## Storage Write

| RPC/behavior | Status |
| --- | --- |
| official service registration/reflection | Verified |
| write service health | lifecycle-aware `SERVING` while enabled and not draining |
| PENDING create/get/append/finalize/commit | Partial; ProtoRows, exact offsets, hidden DuckDB staging, and finalized row count |
| default stream | Partial; official and connector `0.44.2` legacy aliases, immediate append |
| multiple logical streams | Partial; weighted in-flight/staged-byte admission over one serialized DuckDB coordinator |
| atomic batch commit | Verified for a validated group: destination insert and staging/receipt deletion share one transaction |
| ArrowRows, BUFFERED/explicit COMMITTED streams, and `FlushRows` | Unsupported |

CDC, missing-value default expressions, durable staging/recovery after restart,
and distributed write concurrency remain unsupported. PENDING rows no longer
accumulate as decoded Go objects, but the stable staged-byte charge is not an
exact DuckDB physical-size measurement. The serialized backend is an intentional
embedded-engine bound, not BigQuery throughput parity.

The target contract is the official
[`BigQueryWrite`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite)
service and connector
[`BigQueryDirectDataWriterHelper.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryDirectDataWriterHelper.java).

<!-- section: load-auth -->
## Load, Object Storage, and Identity

| Capability | Status |
| --- | --- |
| filesystem object-store adapter | Verified only behind explicit local opt-in |
| GCS/fake-GCS JSON adapter | Partial; bounded list/get/media and URI glob expansion |
| Parquet load into an existing table | Partial; explicit schema/cast validation |
| Avro/ORC/CSV/NDJSON load | Unsupported with terminal `notImplemented` job error |
| `WRITE_APPEND` / `WRITE_EMPTY` / `WRITE_TRUNCATE` | Verified in one DuckDB transaction |
| destination create, autodetect, `schemaUpdateOptions`, multipart/resumable download | Unsupported |
| REST/gRPC TLS | Implemented when configured |
| authentication disabled | Current mode |
| static token, ADC, OAuth, STS/WIF | Planned |
| IAM authorization | Unsupported |

The load target is
[`JobConfigurationLoad`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad).
The opt-in path downloads bounded immutable objects into a private temporary
workspace, then applies the selected disposition atomically. Download is outside
the destination transaction, and load jobs and idempotency records are
process-local.
Identity claims are separated according to [Google Cloud
authentication](https://cloud.google.com/docs/authentication); local token
acquisition must never be described as IAM parity.

<!-- section: persistence-atomicity -->
## Persistence and Atomicity

DuckDB file storage can retain physical rows, but catalog, jobs, read sessions,
write streams, and load idempotency records are process-local. Additive physical
columns use one DuckDB transaction, but in-memory catalog publication is not
crash-atomic with it. Load dispositions, default-stream appends, and a validated
PENDING-stream group commit each use a destination transaction. This provides
atomicity only within a live process; restart recovery and durable replay are
unsupported.

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
remain five strict expected-gap xfails. The connector `0.44.2` profile records
public Storage Read, Storage Write, and indirect load as Partial; it does not
claim complete Spark E2E compatibility. Every capability promotion needs a
public-edge test and a negative/boundary test.

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
