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
| health/readiness | Verified | process plus SQLite state and warehouse ping |
| emulator project lifecycle | Verified | emulator-only namespace |
| `projects.list` | Verified basic | emulator projects plus opaque page token |
| dataset insert/get | Verified basic | location/labels/default expirations retained |
| dataset list/delete | Verified basic | paging, `deleteContents`, hidden-dataset `all`, and ANDed label filters using `labels.<name>[:<value>]` |
| dataset patch/update | Verified | metadata fields plus ETag/HTTP 412 precondition |
| table insert/get/delete | Verified basic | standard table and canonical schema metadata; `tables.get` supports `BASIC` and top-level `selectedFields`, while storage-statistics views are explicitly unsupported |
| table list | Verified basic | paging; no view/storage statistics |
| table patch/update | Verified narrow | metadata plus additive schema and ETag precondition |
| `tabledata.list` | Partial | scalar/nested/repeated `f/v` rows, exact non-finite FLOAT64 tokens, `startIndex`, row/exact-byte bounded pages, scoped opaque tokens, ETag precondition, exact `totalRows`, and `useInt64Timestamp`; selected fields, ISO-8601 picosecond output, and mutation-aware page invalidation remain gaps |
| `tabledata.insertAll` | Partial | schema-aware scalar/nullable/temporal/decimal/bytes/nested/repeated JSON rows, atomic batches, row-indexed `insertErrors`, bounded SQLite `insertId` retry ledger, and official Python/pandas client coverage; `skipInvalidRows`, `ignoreUnknownValues`, and template tables remain unsupported |

Request/response shapes are compared with official
[`datasets`](https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets) and
[`tables`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables)
resources. Ignoring an unknown JSON field is forward-tolerant decoding, not
implementation of that field.

<!-- section: generated-integration-coverage -->
## Generated Integration Coverage

The normalized external-consumer operation list is generated from literal
integration-test annotations at
[`contract/generated/integration-consumer-contract.json`](../../contract/generated/integration-consumer-contract.json).
The rendered operation inventory is available as the [generated integration
consumer contract](generated/integration-consumer-contract.md).
Run `make integration-contract-check` after adding an annotation; CI rejects a
stale generated contract. Scenario order and cardinality remain explicit test
assertions rather than inferred compatibility claims.

The [`tabledata.list`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list)
adapter performs count and ordinal page selection in one DuckDB transaction,
after the same catalog TTL check used by `tables.get` and Storage Read. The
file-first `tableData.maxPageRows` cap can return fewer rows than requested.
`tableData.operationTimeout` covers catalog admission, TTL resolution, and the
physical operation. DuckDB streams at most the configured row count and
incrementally trims its canonical page; its backend JSON representation is never
used as a protocol byte gate because it includes column names that `f/v` rows omit.
REST then applies an exact JSON `maxResponseBytes` normal cap and `maxRowBytes`
single-row hard cap while streaming accepted fragments. These deterministic local
10,000,000/100,000,000-byte rules emulate Cloud's approximate [pagination
limits](https://cloud.google.com/bigquery/docs/paging-results#api-limits). Mutation-aware
page invalidation, `selectedFields`, and `timestampOutputFormat` remain explicit
gaps. `formatOptions.useInt64Timestamp=true` returns epoch-microsecond strings as
required by the pinned Python client; its E2E contract also pins explicit
`maxResults=0` as one empty page with exact `totalRows` and no continuation, and decodes both a post-epoch
microsecond value and a signed pre-epoch value as UTC datetimes. FLOAT64 cells
use JSON numbers when finite and the exact `NaN`, `Infinity`, and `-Infinity`
spellings otherwise, following the
official [`StandardSqlDataType`](https://cloud.google.com/bigquery/docs/reference/rest/v2/StandardSqlDataType)
contract.

`CAP-REST-METADATA-PATCH-V1` and `CAP-SCHEMA-ADDITIVE-V1` are also exercised by
the official [Python client
`3.43.0`](https://pypi.org/project/google-cloud-bigquery/3.43.0/) against a real
process. Schema support is append-only `NULLABLE`/`REPEATED`, including nested
and repeated records. That REST patch coverage does not imply relaxation or
job-driven evolution, and it does not widen the separately documented semantic
DDL subset.

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
| query/load job labels | Verified basic | validated BigQuery label keys and values are included in the job configuration identity, persisted with the job configuration, and emitted by `jobs.insert`, `jobs.get`, and `jobs.list` |
| anonymous destination table | Partial | row-producing query jobs publish a generated hidden-dataset destination with 24-hour lazy expiration; capability `query.destination.anonymous-v1` |
| `WRITE_TRUNCATE` schema replacement | Unsupported | exact-schema subset only; gap `query.destination.truncate-schema-replacement-v1` |
| semantic SQL DDL | Partial | `CREATE TABLE`, `DROP TABLE`, top-level scalar `ADD COLUMN`, and `RENAME COLUMN`; destructive column changes, `TRUNCATE`, and other clauses remain unsupported under `query.ddl.catalog-sync-v1` |
| multi-statement queries | Unsupported except one pinned profile | literal/comment-aware scanning permits one optional trailing semicolon and rejects general scripts before job or engine side effects; the Spark dynamic time-partition overwrite semantic adapter is the only exception; gap `query.scripts.unsupported-v1`; see the official [multi-statement query contract](https://cloud.google.com/bigquery/docs/multi-statement-queries) |
| cancellation | Partial | runtime shutdown rejects new work, cancels and drains admitted sync/async work before closing Storage or DuckDB; public [`jobs.cancel`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/cancel) and cancellation state remain unsupported |
| Parquet load `jobs.insert` / `jobs.get` / `jobs.list` | Partial | opt-in `gs://` and immutable multipart/resumable media sources; explicit-schema destination create and `WRITE_EMPTY`/`WRITE_APPEND`/`WRITE_TRUNCATE`; process-local state |
| copy/extract | Unsupported | configuration rejected |
| durable job/result state | Unsupported | in-memory repository |
| bounded query result retention | Unsupported | all result rows remain in Go memory; gap `query.results.unbounded-memory-v1` |
| complex query-result schema | Strict gap | ARRAY/STRUCT results fail before metadata publication rather than flattening mode/children; gap `query.results.complex-schema-v1` |
| bounded async query execution | Partial | file-configured `query.operationTimeout` bounds service-owned sync/async execution; shutdown admission/cancel/wait is implemented, while worker capacity and exact request `timeoutMs` remain gaps; capability `query.execution.bounded-v1` |
| same-ID query insert | Verified basic | atomic `(project, location, jobId)` uniqueness; every reuse returns `409 duplicate`, fingerprint retained for diagnostics |
| exact-request replay extension | Unsupported | future opt-in only; gap `query.jobs.exact-replay-extension-v1` |
| query/load cross-type identity | Unsupported | separate repositories have a check/create race; gap `query.jobs.cross-repository-identity-v1` |
| synchronous request controls | Partial | validates the 36-byte ASCII `requestId` and accepts non-negative `timeoutMs`; bounded unfinished responses, mutating-query deduplication, and `jobTimeoutMs` remain gap `query.sync.request-controls-v1` |
| query parameters | Partial | typed `NAMED` and `POSITIONAL` scalar parameters (`BOOL`, `INT64`, `FLOAT64`, `NUMERIC`, `STRING`, `DATE`, `DATETIME`, `JSON`, `TIME`, `TIMESTAMP`) are validated through the GoogleSQL AST boundary and passed to DuckDB as bound arguments; ARRAY, STRUCT, and GEOGRAPHY parameters remain unsupported |
| unsupported query options | Strict gap | `dryRun`, cache/billing controls, and `jobTimeoutMs` are explicitly rejected with `400`; gap `query.options.unsupported-v1` |
| omitted-location dataset inference | Partial | structurally referenced tables, cross-project `defaultDataset.projectId`, and explicit destination datasets are checked before insertion; capability `query.location.dataset-inference-v1` |
| terminal persistence recovery | Unsupported | a failed terminal repository update can leave `RUNNING`; gap `query.terminal-persistence-v1` |

Canonical job state and error fields come from the official
[`Job`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job) resource.
Nested/repeated result cells and type-specific temporal values are not yet full
[`TableRow`](https://cloud.google.com/bigquery/docs/reference/rest/v2/TableRow)
encodings. Scalar query and `tabledata.list` rows do share JSON-number finite
FLOAT64 values and the exact non-finite tokens defined by the official
[`StandardSqlDataType`](https://cloud.google.com/bigquery/docs/reference/rest/v2/StandardSqlDataType).
Explicit destinations follow
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

For a row-producing query without `destinationTable`, BQEMU generates the
destination before `JobRepository.CreateOrGet`, returns it in
`configuration.query.destinationTable`, and materializes the result with
`WRITE_EMPTY`/`CREATE_IF_NEEDED`. This is the contract used by connector
`0.44.2`'s
[`TempTableBuilder`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java#L1150-L1240).
The generated dataset starts with `_`, is omitted from `datasets.list` unless
[`all=true`](https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list),
and its tables expose an expiration 24 hours after publication, matching the
connector's default
[`MaterializationConfiguration`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/MaterializationConfiguration.java)
and BigQuery's approximate [anonymous-table
lifetime](https://cloud.google.com/bigquery/docs/cached-results#how_cached_results_are_stored).
Cleanup is lazy at `tables.get`, `tables.list`, and Storage Read resolution; the
hidden dataset is retained for later results. There is no cleanup goroutine or
`Close` ordering: each request completes its cleanup synchronously. A known
hidden dataset follows normal delete rules: live tables require
`deleteContents=true`; after lazy expiration empties it, a normal dataset delete
succeeds. There is no cache-hit reuse, background sweeper, or restart-durable TTL ledger.

Before a job is inserted, the structural analyzer resolves all supported
backtick table paths plus cross-project `defaultDataset.projectId` and explicit
destination dataset. Omitted location uses their common location; an explicit
or inferred cross-location mismatch fails before repository or engine side
effects, following BigQuery's [location
rules](https://cloud.google.com/bigquery/docs/locations#specify_locations).
Unquoted relation paths outside the current lexical adapter, connections,
remote functions, and dynamic SQL do
not yet participate in inference. When no supported candidate exists, the
configured default remains the fallback.

<!-- section: sql -->
## SQL and MERGE

| Behavior | Status | Limit |
| --- | --- | --- |
| fully qualified table reference | Verified narrow case | backtick table token translated |
| `SELECT`/`INSERT` | Partial | DuckDB syntax and functions |
| `UPDATE`/`DELETE` | Partial | DuckDB statement behavior |
| basic `MERGE` | Partial | one tested DuckDB-compatible form |
| connector `0.44.2` static overwrite | Verified narrow | released Spark temporary-table write, atomic DuckDB `MERGE`, polling, and cleanup |
| connector `0.44.2` dynamic time-partition overwrite | Partial | source-pinned semantic operation and raw REST E2E; released connector JAR evidence absent |
| dynamic range-partition overwrite | Unsupported | range expression profile is rejected explicitly |
| semantic DDL | Partial | exact create/drop/add/rename subset; see the boundary guide |
| ARRAY/STRUCT/GEOGRAPHY parameters, scripts, views, UDFs | Unsupported | scalar typed parameters are supported; the remaining forms have no semantic adapter |

The [GoogleSQL lexical
contract](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical)
distinguishes quoted identifiers by syntactic position. The current lexical
scanner preserves strings and comments and distinguishes a declared set of
relation positions, but it is not a complete parser. Arbitrary backtick SQL is
not supported. General `MERGE` must follow
the [official DML
rules](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement),
including source cardinality and atomic visibility.

The narrow static adapter recognizes only the source-derived connector shape
orchestrated by
[`BigQueryClient.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java),
parses its identifiers and clauses as tokens, and executes one atomic [DuckDB
`MERGE INTO`](https://duckdb.org/docs/current/sql/statements/merge_into). Exact
Spark `3.5.8` process evidence covers four PENDING streams, one group commit,
one MERGE job, replacement visibility, and temporary-table cleanup. The dynamic
time adapter parses a separate source-pinned connector script into a semantic
operation and applies one DuckDB transaction after canonical metadata
validation. Dynamic range overwrite and general BigQuery `MERGE` parity remain
unsupported. Exact transformations and DDL clauses are listed in the
[GoogleSQL boundary guide](google-sql-boundary.md).

<!-- section: types -->
## Types

| BigQuery type group | Physical table creation | REST query value | Overall |
| --- | --- | --- | --- |
| BOOL/INT64/FLOAT64/STRING/BYTES | basic mapping | scalar encoding | Partial |
| NUMERIC | native `DECIMAL`, precision at most 38; omitted parameters `(38,9)` | driver-dependent | Partial |
| BIGNUMERIC | native `DECIMAL`, precision at most 38; omitted parameters `(38,18)` | full BigQuery range deliberately unavailable | Partial, Spark-limited |
| DATE/DATETIME/TIME/TIMESTAMP | engine mapping | temporal formatting incomplete | Partial |
| JSON | native JSON mapping | incomplete semantics | Partial |
| GEOGRAPHY | rejected by canonical schema validation | unavailable | Unsupported |
| RECORD/REPEATED | native STRUCT/LIST mapping | tabledata/Storage paths partial; complex query-result schema rejected | Partial |

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
| projection and row restriction | Partial; requested-order recursive STRUCT/REPEATED field projection, plus an official-GoogleSQL-AST subset (`AND`/`OR`/`NOT`, comparisons, `IN`, `BETWEEN`, `IS NULL`, `CAST`, DATE/TIMESTAMP, `LOWER`, and `STARTS_WITH`) lowered with bound values; arbitrary functions, repeated-field predicates, and full GoogleSQL semantics remain unsupported |
| logical streams and offset resume | Partial; stable ranges and stream-relative offsets within a live session |
| historical snapshot and compression | Unsupported |

The public capability is Partial. Each live session owns one stable, bounded
DuckDB materialization and exposes configurable logical streams. Split RPC,
wire compression, historical `snapshot_time`, repeated-field predicates, and
durable session recovery after restart remain gaps.

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
| local `file://` load source | Unsupported; public load requests are limited to `gs://` |
| GCS/fake-GCS JSON adapter | Partial; bounded list/get/media and URI glob expansion |
| Parquet load from `gs://` or media upload | Partial; immutable multipart/resumable media source, explicit schema/cast validation, and explicit-schema destination creation |
| Avro/ORC/CSV/NDJSON load | Unsupported with terminal `notImplemented` job error |
| `WRITE_APPEND` / `WRITE_EMPTY` / `WRITE_TRUNCATE` | Verified in one DuckDB transaction |
| `schemaUpdateOptions` | Partial: `ALLOW_FIELD_ADDITION` with `WRITE_APPEND` only; existing fields must be preserved and new fields must be nullable or repeated. Relaxation is unsupported. |
| autodetect, multipart/resumable download | Unsupported |
| REST/gRPC TLS | Implemented when configured |
| BigQuery-compatible request authentication | Intentionally absent; credentials are ignored |
| local credential JSON generation | Implemented for service-account, authorized-user, and file-sourced WIF clients |
| local OAuth/STS token acquisition | Implemented by a separate loopback-only development command |
| IAM authorization | Unsupported |

The load target is
[`JobConfigurationLoad`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad).
The opt-in path downloads bounded immutable objects into a private temporary
workspace, then applies the selected disposition atomically. Download is outside
the destination transaction, and load jobs and idempotency records are
process-local.
Identity claims are separated according to [Google Cloud
authentication](https://cloud.google.com/docs/authentication). The local client
credential tools are documented in [Local client credentials and
TLS](client-credentials-and-tls.md). They do not protect BQEMU endpoints and
must never be described as IAM parity.

<!-- section: persistence-atomicity -->
## Persistence and Atomicity

SQLite at `/data/bqemu-state.sqlite` is the canonical metadata store. DuckDB at
`/data/bqemu.duckdb` retains physical objects and rows only. Query/load jobs,
read sessions, write-stream ledgers, and load idempotency records remain
process-local.

Additive physical columns, load dispositions, default-stream appends, and a
validated PENDING-stream group commit use DuckDB transactions. SQLite catalog
publication is a separate transaction. Durable mutation-journal records exist,
but catalog mutations do not yet journal every cross-store step and startup does
not recover pending intents. Cross-store crash atomicity, restart replay, and a
consistent live backup from either file alone are unsupported. See the [storage
engine adapter guide](storage-engine-adapter.md).

<!-- section: client-coverage -->
## Client Coverage

The exact [`bq` CLI `2.1.31`](https://cloud.google.com/bigquery/docs/reference/bq-cli-reference)
from [Google Cloud SDK `566.0.0`](https://cloud.google.com/sdk/docs/release-notes#56600_2026-04-28)
runs in its own CI layer with UI disabled. It verifies project listing, dataset
and table lifecycle, additive nullable schema update, query polling, job/table
listing, cleanup, and the not-found exit contract. Six passing official [Python
client `3.43.0`](https://pypi.org/project/google-cloud-bigquery/3.43.0/) E2E tests verify
dataset administration, table metadata/schema administration, `tabledata.list`
pagination with nested/repeated and post-/pre-epoch TIMESTAMP decoding, synchronous
[`jobs.query`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query),
and asynchronous [`jobs.insert`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert)
through [`jobs.getQueryResults`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults).
The corresponding [`python-query-sync`](../../contract/golden/python-query-sync-3.43.0.json)
[`python-query-async`](../../contract/golden/python-query-async-3.43.0.json), and
[`python-tabledata-list`](../../contract/golden/python-tabledata-list-3.43.0.json)
goldens pin those shapes. Load/copy/extract and `insertAll` remain four strict
unsupported xfails; lost-response `requestId` replay is one separate strict
partial-contract xfail. The exact connector `0.44.2` matrix now records 21 of 75
entries as verified, including Arrow/Avro multi-stream table and query reads,
projection/filter pushdown, explicit materialization, optimized count, exact
PENDING append, default-stream append, and unpartitioned direct static
overwrite. It still does not claim complete Spark compatibility. Every
promotion requires public-edge evidence and a negative or boundary test.

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
