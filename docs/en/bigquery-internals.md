<!-- doc-id: bigquery-internals -->
<!-- lang: en -->

[English](bigquery-internals.md) | [한국어](../ko/bigquery-internals.md)

# BigQuery and Spark Connector Internals

<!-- section: mental-model -->
## Mental Model

The Spark connector crosses three distinct public boundaries:

1. BigQuery REST for table metadata, query/load jobs, polling, and overwrite
   coordination;
2. BigQuery Storage Read gRPC for session creation and parallel row streams;
3. BigQuery Storage Write gRPC for direct append, stream finalization, and
   pending-stream commit.

The exact client behavior discussed here is anchored to [connector
`0.44.2`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2).
BigQuery's canonical service boundaries are the [REST
reference](https://cloud.google.com/bigquery/docs/reference/rest) and [Storage RPC
reference](https://cloud.google.com/bigquery/docs/reference/storage/rpc).
`go-bemu` currently implements only the REST metadata/query slice; the Storage
sections below are implementation requirements, not current support claims.

<!-- section: read-planning -->
## Read Planning

The connector first resolves a table or query through REST, derives the selected
columns, filter, snapshot time, and requested parallelism, then sends
`CreateReadSession`. The exact builder is
[`ReadSessionCreator.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/ReadSessionCreator.java).
The server returns one reference schema and zero or more named streams. Spark
creates an input partition per returned stream; requested max parallelism is an
upper bound, not a command to fabricate streams.

A correct emulator must bind every logical stream to one stable snapshot. It
must not rerun an unordered query independently for each range. Projection and
row restriction belong to the session snapshot, and an offset passed to
`ReadRows` is relative to the selected stream. These fields and semantics are
defined by the official
[`ReadSession`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readsession)
and [`ReadRowsRequest`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readrowsrequest)
messages.

<!-- section: read-wire -->
## Arrow and Avro Read Wire Formats

For Arrow, `serialized_schema` and `serialized_record_batch` contain Arrow IPC
messages in separate protobuf fields; they are not arbitrary full Arrow files.
The format source is the [Arrow IPC
specification](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc).
The connector-side decoding path begins in
[`ArrowReaderIterator.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/ArrowReaderIterator.java).

Avro uses one JSON schema plus consecutive binary-encoded row datums. Logical
types and null unions must follow the [Apache Avro
specification](https://avro.apache.org/docs/1.11.4/specification/), and BigQuery's
format-specific mappings are documented under [Storage API Avro schema
details](https://cloud.google.com/bigquery/docs/reference/storage#avro_schema_details).

For either format, row counts, schema, payload bytes, empty results, multiple
batches, nested/repeated values, compression, and offset resume must agree. A
decoder that succeeds on one scalar fixture does not establish wire
compatibility.

<!-- section: direct-exact -->
## Direct Write: Pending Streams and Exact Offsets

With `writeMethod=direct` and exactly-once mode, every Spark data partition
creates a `PENDING` stream. Connector `0.44.2` performs this in
[`BigQueryDirectDataWriterHelper.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryDirectDataWriterHelper.java).
It opens `AppendRows`, supplies a writer schema, sends serialized Proto rows with
the stream-relative starting offset, validates each response offset, and
finalizes the stream. The driver collects stream names and commits them after all
tasks succeed.

The official Write API requires exact offset behavior: the next offset is
accepted, a gap fails, and a replay at an accepted offset is either recognized as
a duplicate or rejected if the payload differs. `FinalizeWriteStream` prevents
later appends and fixes the row count. `BatchCommitWriteStreams` atomically makes
pending streams visible. The canonical RPC contract is
[`BigQueryWrite`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite),
and the operational sequence is in [batch loading with pending
streams](https://cloud.google.com/bigquery/docs/write-api-batch).

An emulator therefore needs a durable ledger keyed by stream, with schema
fingerprint, next offset, accepted payload digest, final row count, state, and
staging relation. A process-global offset or arbitrary stream-map lookup is
incorrect under concurrent Spark tasks.

<!-- section: direct-at-least-once -->
## Direct Write: Default Stream and At-least-once Mode

With `writeAtLeastOnce=true`, connector `0.44.2` targets the table's `_default`
stream and omits exact offsets. Rows become visible without finalize/batch
commit, but a retry after an ambiguous failure can duplicate rows. Google
documents this distinction in [Storage Write streaming
semantics](https://cloud.google.com/bigquery/docs/write-api-streaming).

Local tests must keep the two modes separate. Removing a response offset for the
default stream is not proof of at-least-once retry behavior; fault tests must
interrupt after the server side effect and before the client receives the
response.

<!-- section: overwrite-merge -->
## Direct Overwrite and MERGE

Direct overwrite is not simply an append flag. The connector can write a
temporary table, then submit a `MERGE` that replaces destination rows, and
finally clean up. Connector orchestration is in
[`BigQueryClient.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java).

BigQuery `MERGE` combines source/target matching with ordered clauses and atomic
visibility. A constant-false predicate is a documented replace optimization, but
dynamic partition overwrite also depends on expressions, partition values,
scripts, and source-row cardinality. The authoritative rules are in the
[GoogleSQL DML `MERGE`
reference](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement).
Regex text substitution cannot implement general `MERGE`; a compatibility rule
must recognize one exact connector template and pass unknown SQL unchanged or
report it unsupported.

<!-- section: indirect-write -->
## Indirect Write and Load Jobs

With `writeMethod=indirect`, executors write intermediate files to GCS, the
driver submits `jobs.insert` with a load configuration, polls the job, and
cleans staging objects. The connector orchestration lives in
[`BigQueryWriteHelper.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/spark-bigquery-connector-common/src/main/java/com/google/cloud/spark/bigquery/write/BigQueryWriteHelper.java).

A correct emulator resolves every source URI through an object-store port,
loads immutable inputs into staging, validates schema/bad-record options, then
applies `CREATE_IF_NEEDED` plus `WRITE_APPEND`, `WRITE_TRUNCATE`, or
`WRITE_EMPTY` in one destination transaction. BigQuery defines the REST shape in
[`JobConfigurationLoad`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad)
and the format/type behavior in [batch loading
documentation](https://cloud.google.com/bigquery/docs/loading-data).
Opening a Parquet file is only one step; it does not prove BigQuery load
semantics, job errors, wildcard URI handling, or atomic visibility.

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
NULL, GEOGRAPHY transport, nested STRUCT, repeated fields, empty arrays, and
nullability.

DuckDB may store BIGNUMERIC as text or GEOGRAPHY as text, but canonical metadata
must remain BIGNUMERIC or GEOGRAPHY. Query result encoding must use schema-aware
conversion; `fmt.Sprint` of lists or structs is not a BigQuery REST row.

<!-- section: authentication -->
## Authentication and Authorization

Service-account JSON, authorized-user ADC, and external-account WIF differ in
how a token is acquired. The BigQuery REST/gRPC service ultimately receives a
Bearer token. ADC search order and credential file types are documented in
[Application Default
Credentials](https://cloud.google.com/docs/authentication/application-default-credentials),
and WIF exchange is documented in [Workload Identity
Federation](https://cloud.google.com/iam/docs/workload-identity-federation).

A local OAuth/STS stub can test acquisition and propagation. It does not emulate
signature trust, IAM roles, permission inheritance, federation policy, token
introspection, or production authorization. TLS, authentication, and
authorization must remain separate capability claims.

<!-- section: implementation-map -->
## Implementation Map

| BigQuery/connector stage | Required emulator boundary | Current state |
| --- | --- | --- |
| REST metadata | catalog use cases and JSON transport | basic lifecycle, patch/update, paging, ETag verified |
| additive schema | schema validator plus warehouse transaction | top-level/nested/repeated-record additions verified |
| query job | job repository plus query-engine port | official Python sync/async path verified; process-local partial slice |
| CreateReadSession/ReadRows | snapshot/session ledger plus Arrow/Avro encoder | application/protobuf slice verified with fakes; public runtime unimplemented without DuckDB snapshot/encoder adapter |
| AppendRows/finalize/commit | per-stream ledger plus transaction coordinator | RPC registered, unimplemented |
| indirect load | object store, staging, load dispositions | planned |
| direct overwrite MERGE | structural connector-template adapter | planned |
| ADC/WIF | optional token stub plus auth middleware | planned |

Capability changes require public-boundary tests and a compatibility update in
both documentation languages.
