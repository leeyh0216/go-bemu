<!-- doc-id: bigquery-internals -->
<!-- lang: en -->

[English](bigquery-internals.md) | [한국어](../ko/bigquery-internals.md)

# BigQuery Protocol Internals

<!-- section: mental-model -->
## Mental Model

BigQuery-compatible callers cross three distinct public boundaries:

1. BigQuery REST for table metadata, query/load jobs, polling, and overwrite
   coordination;
2. BigQuery Storage Read gRPC for session creation and parallel row streams;
3. BigQuery Storage Write gRPC for direct append, stream finalization, and
   pending-stream commit.

BigQuery's canonical service boundaries are the [REST
reference](https://cloud.google.com/bigquery/docs/reference/rest) and [Storage RPC
reference](https://cloud.google.com/bigquery/docs/reference/storage/rpc).
`go-bemu` exposes REST metadata/query plus Parquet load jobs, and public Partial
Storage Read and Storage Write slices. The sections below distinguish
those bounded runtime paths from the remaining BigQuery requirements.

<!-- section: read-planning -->
## Read Planning

A Storage Read caller first resolves a table or query through REST, derives the
selected columns, filter, snapshot time, and requested parallelism, then sends
`CreateReadSession`. The server returns one reference schema and zero or more
named streams. A reader can assign work per returned stream; requested max
parallelism is an upper bound, not a command to fabricate streams.

A correct emulator must bind every logical stream to one stable snapshot. It
must not rerun an unordered query independently for each range. Projection and
row restriction belong to the session snapshot, and an offset passed to
`ReadRows` is relative to the selected stream. These fields and semantics are
defined by the official
[`ReadSession`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readsession)
and [`ReadRowsRequest`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readrowsrequest)
messages.

The current public runtime materializes one bounded DuckDB result per live
session, divides it into a configured number of stable logical ranges, and
resumes from a stream-relative offset. Recursive projection preserves catalog
field order. Row restrictions support boolean logic, comparisons, `IN`,
`BETWEEN`, NULL checks, `LIKE`, and scalar casts; functions and subqueries are
rejected before materialization. `SplitReadStream`, historical `snapshot_time`,
compression, and restart recovery are unsupported.

<!-- section: read-wire -->
## Arrow and Avro Read Wire Formats

For Arrow, `serialized_schema` and `serialized_record_batch` contain Arrow IPC
messages in separate protobuf fields; they are not arbitrary full Arrow files.
The format source is the [Arrow IPC
specification](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc).

Avro uses one JSON schema plus consecutive binary-encoded row datums. Logical
types and null unions must follow the [Apache Avro
specification](https://avro.apache.org/docs/1.11.4/specification/), and BigQuery's
format-specific mappings are documented under [Storage API Avro schema
details](https://cloud.google.com/bigquery/docs/reference/storage#avro_schema_details).

For either format, row counts, schema, payload bytes, empty results, multiple
batches, nested/repeated values, compression, and offset resume must agree. A
decoder that succeeds on one scalar fixture does not establish wire
compatibility.

The current DuckDB adapter encodes bounded Arrow record-batch messages and Avro
binary rows for the public `ReadRows` path. Compression and complete
nested/repeated type mappings remain gaps, so this is Partial rather than full
wire compatibility.

<!-- section: direct-exact -->
## Direct Write: Pending Streams and Exact Offsets

An exact-offset batch writer creates one `PENDING` stream per producer shard. It
opens `AppendRows`, supplies a writer schema, sends serialized Proto rows with
the stream-relative starting offset, validates each response offset, and
finalizes the stream. The coordinator collects stream names and commits them
after every producer succeeds.

The official Write API requires exact offset behavior: the next offset is
accepted, a gap fails, and a replay at an accepted offset is either recognized as
a duplicate or rejected if the payload differs. `FinalizeWriteStream` prevents
later appends and fixes the row count. `BatchCommitWriteStreams` atomically makes
pending streams visible. The canonical RPC contract is
[`BigQueryWrite`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite),
and the operational sequence is in [batch loading with pending
streams](https://cloud.google.com/bigquery/docs/write-api-batch).

The current Partial implementation persists a SQLite ledger keyed by stream,
including schema fingerprint, next offset, accepted payload digest, final row
count, operation phase, and commit group. ProtoRows append, exact offsets,
finalize, and atomic commit of a validated PENDING group are public. DuckDB
mutations are serialized through one bounded coordinator. Startup reconciles
prepared intents as unresolved before admitting requests, and an exact retry can
complete the operation against the paired DuckDB staging state. Independent
state-file restore and complete physical-proof recovery remain gaps. A
process-global offset or arbitrary stream-map lookup would be incorrect under
concurrent producers.

<!-- section: direct-at-least-once -->
## Direct Write: Default Stream and At-least-once Mode

A default-stream writer targets the table's `_default` stream and omits exact
offsets. Rows become visible without finalize/batch commit, but a retry after an
ambiguous failure can duplicate rows. Google documents this distinction in
[Storage Write streaming
semantics](https://cloud.google.com/bigquery/docs/write-api-streaming).

Local tests must keep the two modes separate. Removing a response offset for the
default stream is not proof of at-least-once retry behavior; fault tests must
interrupt after the server side effect and before the client receives the
response.

The public Partial implementation accepts the official `/streams/_default`
name and applies rows immediately without an offset. Ambiguous-response retries
can duplicate rows by design. ArrowRows, BUFFERED and explicit COMMITTED
streams, and `FlushRows` remain unsupported.

<!-- section: overwrite-merge -->
## Direct Overwrite and MERGE

Direct overwrite is not simply an append flag. A caller can write a temporary
table, submit a `MERGE` that replaces destination rows, and finally clean up.

BigQuery `MERGE` combines source/target matching with ordered clauses and atomic
visibility. A constant-false predicate is a documented replace optimization, but
dynamic partition overwrite also depends on expressions, partition values,
scripts, and source-row cardinality. The authoritative rules are in the
[GoogleSQL DML `MERGE`
reference](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement).
Regex text substitution cannot implement general `MERGE`. BQEMU parses and
analyzes the statement through the same official GoogleSQL gateway used for
every query, binds source and destination relations to canonical metadata, and
passes only the immutable semantic AST to the engine adapter. The DuckDB visitor
renders one atomic [DuckDB `MERGE
INTO`](https://duckdb.org/docs/current/sql/statements/merge_into) with bound
literal values. Constant-false replacement, ordered actions, and dynamic
DATE/TIMESTAMP/DATETIME or integer-range partition replacement use this path.
The first qualified `WHEN` clause wins. A correlated precondition in the same
transaction rejects multiple source rows for one target row before an UPDATE or
DELETE can run. Unsupported expressions, actions, and script control flow fail
before execution; their remaining parity is tracked in #8.

<!-- section: indirect-write -->
## Indirect Write and Load Jobs

An indirect writer places intermediate files in GCS, submits `jobs.insert` with
a load configuration, polls the job, and cleans staging objects.

A correct emulator resolves every source URI through an object-store port,
loads immutable inputs into staging, validates schema/bad-record options, then
applies `CREATE_IF_NEEDED` plus `WRITE_APPEND`, `WRITE_TRUNCATE`, or
`WRITE_EMPTY` in one destination transaction. BigQuery defines the REST shape in
[`JobConfigurationLoad`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad)
and the format/type behavior in [batch loading
documentation](https://cloud.google.com/bigquery/docs/loading-data).
Opening a Parquet file is only one step; it does not prove BigQuery load
semantics, job errors, wildcard URI handling, or atomic visibility.

The public load path resolves bounded `gs://` list/get/media requests through
a fake-GCS-compatible JSON adapter, downloads objects to a private temporary
workspace, validates Parquet columns and casts against an existing table, and
applies `WRITE_APPEND`, `WRITE_EMPTY`, or `WRITE_TRUNCATE` in one DuckDB
transaction. With `CREATE_IF_NEEDED`, an explicit request schema creates the
physical destination and inserts its rows in that same transaction; catalog
metadata is published only after commit, with physical compensation on a
publication failure. When the request omits a schema, scalar and STRUCT fields
are inferred from the Parquet schema; LIST fields additionally require
`parquetOptions.enableListInference=true`. An existing `WRITE_APPEND`
destination accepts only explicitly enabled nullable field additions and
REQUIRED-to-NULLABLE relaxations. The physical schema update and appended rows
share the destination transaction; canonical SQLite metadata is published
after commit. A newly created destination also carries validated time-unit or
integer-range partition metadata, partition expiration, and ordered clustering
fields into the same immutable load plan. Ingestion-time destinations create
and populate `_PARTITIONTIME` and `_PARTITIONDATE`; a request cannot replace an
existing destination's layout. Partition expiration sweeping and physical
reclustering are not yet implemented. Local paths and non-GCS URI schemes are rejected before job
persistence. The separate `autodetect` request flag, query-job schema updates,
Avro/ORC/CSV/NDJSON, and
multipart/resumable download are unsupported. Job metadata and idempotency
identity persist in SQLite; downloaded objects and temporary staging workspaces
do not.

<!-- section: rest-jobs -->
## REST Jobs, Polling, and Paging

`jobs.query` is synchronous from the caller's perspective but still returns job
identity and can require result polling. `jobs.insert` persists a job first and
then executes asynchronously. Both success and failure reach `DONE`; failure is
carried in `errorResult` and `errors`. Result pages require a stable opaque
`pageToken`, total row count, schema, and BigQuery JSON cell shapes. The official
resources are
[`Job`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job) and
[`GetQueryResultsResponse`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults#response-body).

`startIndex` truncation alone is not page-token support. In-memory job state is
not persisted merely because result table data is held in a DuckDB file.

<!-- section: types -->
## Type Boundaries

Types cross four independent mappings: BigQuery metadata, engine storage,
REST JSON cells, and Arrow/Avro/Proto wire values. The canonical type definitions
and ranges are in [BigQuery data
types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types).
Important cases include NUMERIC/BIGNUMERIC precision, TIMESTAMP versus DATETIME,
TIME microseconds, special floating values, BYTES base64, JSON null versus SQL
NULL, nested STRUCT, repeated fields, empty arrays, and
nullability.

The current engine adapter stores both NUMERIC and the supported BIGNUMERIC
subset as `DECIMAL(P,S)`. Canonical metadata remains responsible for their
distinct logical identities and parameter-presence information. Precision is
limited to the runtime's current maximum of 38. GEOGRAPHY has no local semantic
implementation and is rejected before storage mutation. Query result encoding
uses schema-aware conversion; `fmt.Sprint` of lists or structs is not a
BigQuery REST row.

<!-- section: authentication -->
## Authentication and Authorization

Service-account JSON, authorized-user ADC, and external-account WIF differ in
how a token is acquired. The BigQuery REST/gRPC service ultimately receives a
Bearer token. ADC search order and credential file types are documented in
[Application Default
Credentials](https://cloud.google.com/docs/authentication/application-default-credentials),
and WIF exchange is documented in [Workload Identity
Federation](https://cloud.google.com/iam/docs/workload-identity-federation).

BQEMU does not authenticate or authorize its BigQuery-compatible endpoints.
REST and gRPC allow requests without credentials and ignore `Authorization`
values when present. Client token acquisition, TLS, the separate diagnostics
admin token, and IAM remain distinct capability claims. The public runtime does
not emulate signature trust, IAM roles, permission inheritance, federation
policy, token introspection, or production authorization.

<!-- section: implementation-map -->
## Implementation Map

| BigQuery stage | Required emulator boundary | Current state |
| --- | --- | --- |
| REST metadata | catalog use cases and JSON transport | basic lifecycle, patch/update, paging, ETag verified |
| additive schema | schema validator plus warehouse transaction | top-level/nested/repeated-record additions verified |
| query job | job repository plus GoogleSQL gateway and statement ports | public sync/async path verified; result payload remains process-local |
| CreateReadSession/ReadRows | snapshot/session ledger plus Arrow/Avro encoder | public Partial: bounded DuckDB snapshot, recursive projection, logical streams, stable offsets; split/compression/historical snapshot/recovery gaps |
| AppendRows/finalize/commit | durable per-stream ledger plus transaction coordinator | public Partial: PENDING/default ProtoRows, offsets, finalize, atomic commit, startup reconciliation; advanced stream kinds and independent-restore proof gaps |
| indirect load | object store, staging, load dispositions | public Partial: fake-GCS JSON plus Parquet into an existing or schema-inferred new table, with explicit LIST inference; other formats/evolution/download gaps |
| overwrite `MERGE` | official analyzer, immutable semantic AST, engine visitor | constant-false, dynamic time/range partition, ordered `WHEN`, and source-cardinality behavior verified; additional AST nodes remain #8 |
| BigQuery-compatible request authentication | REST/gRPC transport behavior | intentionally absent; credential values are ignored |
| ADC/WIF acquisition | client credential library | external to the public BQEMU runtime |

Capability changes require public-boundary tests and a compatibility update in
both documentation languages.
