<!-- doc-id: compatibility -->
<!-- lang: en -->

[English](compatibility.md) | [한국어](../ko/compatibility.md)

# Compatibility Contract

The exact REST method/path and Storage RPC surface is generated from the strict
operation manifest. See [API and RPC compatibility](api-rpc-compatibility.md)
for canonical `support` and `verification` values, limitations, issue links,
and the tests that collect each operation.

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
| `tabledata.list` | Partial | scalar/nested/repeated `f/v` rows, exact non-finite FLOAT64 tokens, `startIndex`, row/exact-byte bounded pages, scoped opaque tokens, ETag precondition, exact `totalRows`, and `useInt64Timestamp`; selected fields, ISO-8601 picosecond output, and mutation-aware page invalidation remain gaps |
| `tabledata.insertAll` | Unsupported | no route |

Request/response shapes are compared with official
[`datasets`](https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets) and
[`tables`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables)
resources. Ignoring an unknown JSON field is forward-tolerant decoding, not
implementation of that field.

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
gaps. `formatOptions.useInt64Timestamp=true` returns epoch-microsecond strings.
Explicit `maxResults=0` returns one empty page with exact `totalRows` and no
continuation. Both post-epoch microsecond values and signed pre-epoch values use
the same UTC representation. FLOAT64 cells
use JSON numbers when finite and the exact `NaN`, `Infinity`, and `-Infinity`
spellings otherwise, following the
official [`StandardSqlDataType`](https://cloud.google.com/bigquery/docs/reference/rest/v2/StandardSqlDataType)
contract.

`CAP-REST-METADATA-PATCH-V1` and `CAP-SCHEMA-ADDITIVE-V1` are exercised at the
public transport boundary. Schema support is append-only
`NULLABLE`/`REPEATED`, including nested and repeated records; DDL conversion,
relaxation, and job-driven evolution are not implied.

<!-- section: jobs -->
## Query and Jobs

| Operation | Status | Limit |
| --- | --- | --- |
| `jobs.query` | Partial | synchronous execution through the unified GoogleSQL AST subset |
| query `jobs.insert` | Partial | asynchronous polling; execution is process-local while configuration, status, errors, timestamps, and statistics are SQLite-durable |
| `jobs.get` | Verified basic | `PENDING/RUNNING/DONE`, terminal errors |
| `jobs.list` | Partial | location-aware SQLite metadata and opaque cursor token |
| `jobs.getQueryResults` | Partial | location-aware lookup, `startIndex`, `maxResults`, and job/result-bound opaque page tokens |
| explicit destination table | Partial | scalar exact-schema `WRITE_EMPTY`/`WRITE_APPEND`/`WRITE_TRUNCATE`; capability `query.destination.exact-schema-v1` |
| query metadata | Verified basic | `INTERACTIVE`/`BATCH` priority and validated labels, including an explicitly empty label map, are fingerprinted and round-tripped |
| anonymous destination table | Partial | row-producing query jobs publish a generated hidden-dataset destination with 24-hour lazy expiration; capability `query.destination.anonymous-v1` |
| `WRITE_TRUNCATE` schema replacement | Unsupported | exact-schema subset only; gap `query.destination.truncate-schema-replacement-v1` |
| semantic SQL DDL | Partial | GoogleSQL AST plans execute `CREATE TABLE`, `DROP TABLE`, `TRUNCATE TABLE`, top-level `ADD`/`RENAME`/`DROP COLUMN`, and `ALTER COLUMN SET DATA TYPE`; unsupported clauses fail before mutation under `query.ddl.catalog-sync-v1`, while crash recovery between SQLite and the engine remains #26 |
| multi-statement queries | Partial | transactional `DECLARE`, `SET`, and supported query/DML statements; control flow, dynamic SQL, and temporary routines remain unsupported under the official [multi-statement query contract](https://cloud.google.com/bigquery/docs/multi-statement-queries) |
| cancellation | Partial | runtime shutdown rejects new work, cancels and drains admitted sync/async work before closing Storage or DuckDB; public [`jobs.cancel`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/cancel) and cancellation state remain unsupported |
| Parquet load `jobs.insert` / `jobs.get` / `jobs.list` | Partial | always available; configuration, status, errors, timestamps, and statistics are SQLite-durable |
| copy/extract | Unsupported | configuration rejected |
| durable job/result state | Partial | query/load job metadata survives restart; query row payloads do not, and a restarted non-empty result returns an explicit `backendError` instead of an empty success |
| bounded query result retention | Unsupported | all result rows remain in Go memory; gap `query.results.unbounded-memory-v1` |
| complex query-result schema | Partial | ARRAY/STRUCT schemas and nested/repeated `TableRow` cells are preserved; ambiguous decimal subfields and unsupported physical shapes fail before metadata publication |
| bounded async query execution | Partial | file-configured `query.operationTimeout` bounds service-owned sync/async execution; shutdown admission/cancel/wait is implemented, while worker capacity and exact request `timeoutMs` remain gaps; capability `query.execution.bounded-v1` |
| same-ID query insert | Verified basic | atomic `(project, location, jobId)` uniqueness; every reuse returns `409 duplicate`, fingerprint retained for diagnostics |
| exact-request replay extension | Unsupported | future opt-in only; gap `query.jobs.exact-replay-extension-v1` |
| query/load cross-type identity | Unsupported | separate repositories have a check/create race; gap `query.jobs.cross-repository-identity-v1` |
| synchronous request controls | Partial | validates the 36-byte ASCII `requestId` and accepts non-negative `timeoutMs`; bounded unfinished responses, mutating-query deduplication, and `jobTimeoutMs` remain gap `query.sync.request-controls-v1` |
| unsupported query options | Strict gap | parameters, `dryRun`, cache/billing controls, and `jobTimeoutMs` are explicitly rejected with `400`; gap `query.options.unsupported-v1` |
| omitted-location dataset inference | Partial | structurally referenced tables, cross-project `defaultDataset.projectId`, and explicit destination datasets are checked before insertion; capability `query.location.dataset-inference-v1` |
| terminal persistence recovery | Partial | startup changes interrupted `PENDING`/`RUNNING` jobs to terminal errors; recovery from every cross-store failure point remains gap `query.terminal-persistence-v1` |

Canonical job state and error fields come from the official
[`Job`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job) resource.
Nested/repeated result cells use recursive
[`TableRow`](https://cloud.google.com/bigquery/docs/reference/rest/v2/TableRow)
encoding. Type-specific temporal values do not yet cover the complete BigQuery
surface. Scalar query and `tabledata.list` rows do share JSON-number finite
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
fingerprint alongside the raw configuration for drift diagnostics. See the official
[retry guidance](https://cloud.google.com/bigquery/docs/reliability-intro#retry_failed_job_insertions).

For a row-producing query without `destinationTable`, BQEMU generates the
destination before `JobRepository.CreateOrGet`, returns it in
`configuration.query.destinationTable`, and materializes the result with
`WRITE_EMPTY`/`CREATE_IF_NEEDED`.
The generated dataset starts with `_`, is omitted from `datasets.list` unless
[`all=true`](https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list),
and its tables expose an expiration 24 hours after publication, matching
BigQuery's approximate [anonymous-table
lifetime](https://cloud.google.com/bigquery/docs/cached-results#how_cached_results_are_stored).
Cleanup is lazy at `tables.get`, `tables.list`, and Storage Read resolution; the
hidden dataset is retained for later results. There is no cleanup goroutine or
`Close` ordering: each request completes its cleanup synchronously. A known
hidden dataset follows normal delete rules: live tables require
`deleteContents=true`; after lazy expiration empties it, a normal dataset delete
succeeds. There is no cache-hit reuse, background sweeper, or restart-durable TTL ledger.

Before a job is inserted, the official GoogleSQL analyzer resolves supported
quoted and unquoted table paths, cross-project `defaultDataset.projectId`, and
the explicit destination dataset. Omitted location uses their common location;
an explicit or inferred cross-location mismatch fails before repository or
engine side effects, following BigQuery's [location
rules](https://cloud.google.com/bigquery/docs/locations#specify_locations).
Connections, remote functions, table decorators, and dynamic SQL do not yet
participate in inference. When no supported relation exists, the configured
default remains the fallback.

<!-- section: sql -->
## SQL and MERGE

| Behavior | Status | Limit |
| --- | --- | --- |
| canonical table references | Verified | official analyzer binding; no engine-side path inference |
| `SELECT`/`INSERT`/`UPDATE`/`DELETE` | Partial | supported AST nodes, operators, functions, and types only |
| `MERGE` | Partial | ordered matched/not-matched actions, first-qualified-`WHEN`, source-cardinality errors, and constant-false replacement; unsupported nodes fail closed |
| multi-statement scripts | Partial | transactional `DECLARE`, `SET`, and supported query/DML children; no control flow or temporary routines |
| catalog DDL | Partial | create/drop/truncate and the documented column mutations |
| dynamic partition overwrite | Partial | transactional typed-array scripts support DATE/TIMESTAMP/DATETIME and integer-range partition replacement; additional expressions remain #8 |
| parameters/views/UDFs/procedures | Unsupported | tracked separately; no raw SQL fallback |

The [GoogleSQL lexical
contract](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical)
distinguishes identifiers, comments, strings, relations, and expressions by
syntax position. One official parse/analyze gateway maps those structures into
an immutable semantic statement. The DuckDB visitor then renders adapter-private
SQL and bind arguments; it never tokenizes or retries the original input.
General `MERGE` follows the [official DML
rules](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement).
The implemented subset preserves clause order and one transaction. It rejects
multiple source matches before a target UPDATE or DELETE and supports the typed
array/function shapes used by dynamic time and integer-range replacement.
Unsupported expressions or actions fail before an engine side effect.

<!-- section: types -->
## Types

| BigQuery type group | Physical table creation | REST query value | Overall |
| --- | --- | --- | --- |
| BOOL/INT64/FLOAT64/STRING/BYTES | basic mapping | scalar encoding | Partial |
| NUMERIC | `DECIMAL(P,S)`, default `DECIMAL(38,9)` | exact decimal string | Partial |
| BIGNUMERIC | `DECIMAL(P,S)`, default `DECIMAL(38,18)` | exact decimal string | Partial, precision limited to 38 |
| DATE/DATETIME/TIME/TIMESTAMP | engine mapping | temporal formatting incomplete | Partial |
| JSON | JSON mapping | incomplete semantics | Partial |
| GEOGRAPHY | rejected before storage mutation | not available | Unsupported |
| RECORD/REPEATED | recursive STRUCT/LIST mapping | schema-aware nested and repeated cells | Partial |

Compatibility is assessed against [BigQuery data
types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types).
Both decimal families retain omitted versus explicit parameters in catalog and
REST metadata. The same applies to `roundingMode`: omission,
`ROUNDING_MODE_UNSPECIFIED`, `ROUND_HALF_AWAY_FROM_ZERO`, and
`ROUND_HALF_EVEN` remain distinct canonical metadata values. Effective
parameters are applied only at an engine or wire
boundary. Precision above 38 is rejected before table, load-job, or row
mutation because the current runtime representation is limited to 38 digits.
`GEOGRAPHY` is also rejected before a physical side effect.

ProtoRows writes apply BigQuery's default half-away-from-zero rounding and the
explicit half-even mode with exact decimal arithmetic, including recursive
STRUCT and REPEATED values. Parquet load accepts source decimal shapes that fit
the destination without narrowing. A source that would need narrowing is
rejected before destination mutation with
`load.decimal-rounding.unsupported-v1` and is tracked by
[issue #5](https://github.com/leeyh0216/go-bemu/issues/5). Existing query
destinations require the exact effective decimal shape; a narrowing attempt is
rejected with `query.destination.decimal-rounding.unsupported-v1`, while other
shape differences use `query.destination.exact-schema-v1`. Query rounding and
decimal lineage remain tracked by
[issue #27](https://github.com/leeyh0216/go-bemu/issues/27).

The table-level `defaultRoundingMode` changes the effective mode of newly added
decimal fields. Until canonical table defaults and recovery are implemented by
[issue #21](https://github.com/leeyh0216/go-bemu/issues/21) and
[issue #26](https://github.com/leeyh0216/go-bemu/issues/26), tables.insert,
tables.patch, and tables.update reject that property before a catalog or engine
mutation with `schema.table-default-rounding-mode.unsupported-v1`. Omitting the
table default and using explicit field modes remains supported.

NUMERIC and the supported BIGNUMERIC subset are covered by REST table/query and
tabledata cells, Arrow/Avro Storage Read schemas and values, direct ProtoRows,
and scalar Parquet loads. Recursive STRUCT and REPEATED decimal metadata is
covered for REST, Storage Read, and Storage Write. Omitted BIGNUMERIC parameters
use the emulator default of precision 38 and scale 18 at physical and wire
boundaries.

One query limitation remains. A new DuckDB result typed only as
`DECIMAL(P,S)` cannot reveal whether a value inside NUMERIC's range originated
as NUMERIC or BIGNUMERIC. A supported existing destination restores scalar,
nested STRUCT, and REPEATED identity from its catalog schema. A physical shape
outside NUMERIC's range is unambiguously BIGNUMERIC. Other ambiguous query
results fail before metadata publication with
`query.results.decimal-lineage-v1` until issue #27 provides query lineage
metadata.

<!-- section: storage-read -->
## Storage Read

| RPC/behavior | Status |
| --- | --- |
| official service registration/reflection | Verified |
| read service health | lifecycle-aware `SERVING` while enabled and not draining |
| public `CreateReadSession` / `ReadRows` | Partial; one bounded DuckDB materialization per session |
| public `SplitReadStream` | Unsupported; returns `UNIMPLEMENTED` |
| Arrow/Avro schema and row payloads | Partial; encoded from bounded DuckDB rows and response bytes |
| projection and row restriction | Partial; recursive STRUCT/REPEATED projection preserves catalog order; row restrictions support a bounded expression subset |
| logical streams and offset resume | Partial; stable ranges and stream-relative offsets within a live session, with SQLite-durable lifecycle metadata |
| historical snapshot and compression | Unsupported |

The public capability is Partial. Each live session owns one stable, bounded
DuckDB materialization and exposes configurable logical streams. Split RPC,
wire compression, and historical `snapshot_time` remain gaps. After restart,
an unexpired stream returns `UNAVAILABLE` and an expired
stream returns `NOT_FOUND`; snapshot row bytes are not reconstructed.

The target contract is the official
[`BigQueryRead`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryRead)
service.

<!-- section: storage-write -->
## Storage Write

| RPC/behavior | Status |
| --- | --- |
| official service registration/reflection | Verified |
| write service health | lifecycle-aware `SERVING` while enabled and not draining |
| PENDING create/get/append/finalize/commit | Partial; ProtoRows, exact offsets, hidden DuckDB staging, and finalized row count |
| default stream | Partial; official resource name and immediate append |
| multiple logical streams | Partial; weighted in-flight/staged-byte admission over one serialized DuckDB coordinator |
| atomic batch commit | Verified for a validated group: destination insert and staging/receipt deletion share one transaction |
| ArrowRows, BUFFERED/explicit COMMITTED streams, and `FlushRows` | Unsupported |

CDC, missing-value default expressions, independent-store restore proof, and
distributed write concurrency remain unsupported. SQLite persists stream,
receipt, and commit-group phases; startup classifies interrupted operations as
unresolved before accepting requests. PENDING rows do not accumulate as decoded
Go objects, but the stable staged-byte charge is not an exact DuckDB
physical-size measurement. The serialized backend is an intentional
embedded-engine bound, not BigQuery throughput parity.

The target contract is the official
[`BigQueryWrite`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite)
service.

<!-- section: load-auth -->
## Load, Object Storage, and Public Access

| Capability | Status |
| --- | --- |
| accepted load source URI | `gs://` only; local paths and other schemes fail before job persistence |
| fake GCS service | Required by the default Compose project; the binary connects to the configured GCS-compatible JSON endpoint |
| GCS/fake-GCS JSON adapter | Partial; bounded list/get/media and URI glob expansion |
| Parquet load into an existing table | Partial; scalar fields only, with explicit schema/cast validation; nested or repeated fields fail before object access with `load.parquet.nested-repeated.unsupported-v1`, and decimal narrowing fails before destination mutation with `load.decimal-rounding.unsupported-v1` |
| Avro/ORC/CSV/NDJSON load | Unsupported with terminal `notImplemented` job error |
| `WRITE_APPEND` / `WRITE_EMPTY` / `WRITE_TRUNCATE` | Verified in one DuckDB transaction |
| destination create, autodetect, `schemaUpdateOptions`, multipart/resumable download | Unsupported |
| REST/gRPC TLS | Implemented when configured |
| BigQuery-compatible endpoint authentication | Intentionally absent; missing, arbitrary, and malformed credentials are ignored |
| Disposable service-account, authorized-user, WIF, and direct-token files | Implemented by the loopback-only `bqemu-auth-fixture` development helper |
| OAuth/STS token acquisition | Implemented only for generated local fixtures; Google identity and IAM control planes are not emulated |
| diagnostics admin token | Optional and limited to the separate admin listener |
| IAM authorization | Unsupported |

The load target is
[`JobConfigurationLoad`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad).
The load path downloads bounded immutable objects into a private temporary
workspace, then applies the selected disposition atomically. Download is outside
the destination transaction. Load job metadata and idempotency identity persist
in SQLite; downloaded objects and temporary workspaces do not. The uploading
process and BQEMU configure their GCS endpoints independently, and both must
resolve to the same object-store service. See [Getting
started](getting-started.md) for the Compose settings.
The public edge does not parse or validate `Authorization` header or metadata
values. Client credential requirements, TLS, the separate diagnostics admin
token, and IAM are distinct compatibility claims. See [Local client credentials
and TLS](client-credentials-and-tls.md) for the generated file shapes, listener
contract, and strict-client setup.

<!-- section: persistence-atomicity -->
## Persistence and Atomicity

DuckDB file storage retains physical rows. SQLite retains canonical catalog,
query/load job metadata, Storage Read lifecycle metadata, and the Storage Write
ledger. Query result rows and Storage Read snapshot bytes remain process-local.
Cross-store catalog mutations are not yet crash-atomic at every failure point.
Load dispositions, default-stream appends, and a validated PENDING-stream group
commit each use a destination transaction. Startup reconciles interrupted job
and write-ledger phases, while independent restore of only one state file is
still unsupported.

<!-- section: client-coverage -->
## Integration Coverage

Product compatibility is defined by the operation manifest and the capability
tables above. Exact external executable versions, immutable artifacts,
scenario IDs, expected calls, and generated evidence belong to the
[integration test framework](../../tests/integration/docs/en/framework.md) and
[consumer compatibility](../../tests/integration/docs/en/consumer-compatibility.md).
They verify callers against the public process without becoming product runtime
dependencies. Every compatibility promotion requires public-process evidence
and a negative or boundary test.

<!-- section: removal-criteria -->
## Workaround Removal Criteria

A compatibility workaround may be removed only after its original behavior is
reproduced, the affected integration cases no longer require it, and public
operation traces agree without the rule. Generalizing a workaround requires a
protocol or semantic source, not another regex example.
