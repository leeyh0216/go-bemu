<!-- doc-id: architecture -->
<!-- lang: en -->

[English](architecture.md) | [한국어](../ko/architecture.md)

# Architecture

<!-- section: goals -->
## Goals and Non-goals

The service reproduces observable contracts needed by local clients while
keeping execution, metadata, object storage, identity, time, and IDs replaceable.
The contract source is the [BigQuery REST
API](https://cloud.google.com/bigquery/docs/reference/rest) and [Storage
RPC](https://cloud.google.com/bigquery/docs/reference/storage/rpc), not accidental
DuckDB behavior. Reproducing Dremel, Colossus, slots, quotas, billing, regional
placement, or production availability is out of scope.

<!-- section: dependency-rule -->
## Dependency Rule

```text
transport/rest, transport/grpc  ->  application  ->  domain + ports
                                                  ^
adapters/duckdb, memory, objectstore, system  ----|
```

The arrows are source dependencies. Domain values contain no Google JSON,
protobuf, SQL connection, or framework type. Application services coordinate
ports and own compensation/state transitions. Transport converts public wire
types. Adapters implement outbound side effects.

<!-- section: package-ownership -->
## Package Ownership

| Package | Owns | Must not own |
| --- | --- | --- |
| `internal/domain` | identities, canonical schemas, job states, domain errors | HTTP, protobuf, DuckDB |
| `internal/application` | use cases, ordering, compensation | routes, generated wire types, SQL syntax |
| `internal/ports` | inbound/outbound contracts | concrete clients |
| `internal/adapters/duckdb` | physical names, type mapping, SQL execution | REST resources, job lifecycle |
| `internal/adapters/memory` | process-local repositories | query semantics |
| `internal/transport/*` | public REST/gRPC boundary | database imports |
| `cmd/emulator` | composition and lifecycle | business rules |

Compile-time assertions prove that an adapter implements a port. Application
tests must also replace DuckDB with a fake; an assertion alone does not prove the
port is behaviorally usable.

<!-- section: control-data-planes -->
## Control and Data Planes

**BigQuery contract:** REST resources create metadata and jobs, while Storage
Read/Write RPCs move high-volume row data. A read session divides a table snapshot
into streams, as defined by
[`CreateReadSession`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryRead.CreateReadSession).
Write stream type controls visibility and commit behavior, as described by the
[Storage Write API](https://cloud.google.com/bigquery/docs/write-api).

**Current implementation:** REST metadata/query plus Parquet load jobs
form the public control plane. Public Storage Read materializes one bounded
DuckDB snapshot, encodes Arrow or Avro, and exposes deterministic logical ranges
with offset resume. Public Storage Write accepts ProtoRows on PENDING and default
streams, validates offsets, finalizes PENDING streams, and commits a validated
group through one serialized DuckDB transaction. Both data planes are Partial:
Read lacks split/compression/historical snapshots, restart recovery, and nested-field projection;
Write lacks CDC, ArrowRows, BUFFERED/explicit COMMITTED streams, FlushRows, and
durable staging.

<!-- section: catalog-physical-model -->
## Catalog and Physical Model

Canonical resources retain BigQuery project, dataset, table, field, partition,
and clustering metadata. The DuckDB adapter maps `project.dataset.table` to a
hex-encoded physical schema plus a quoted table identifier. DuckDB's catalog and
SQL behavior are documented in [DuckDB CREATE
SCHEMA](https://duckdb.org/docs/stable/sql/statements/create_schema) and [identifier
rules](https://duckdb.org/docs/stable/sql/dialect/keywords_and_identifiers).

The metadata repository and engine catalog are currently separate. Create uses
physical DDL followed by metadata persistence with compensation. Delete performs
physical deletion before metadata deletion. A crash between steps can create
drift. Durable metadata and one transaction boundary are required before restart
or atomic catalog claims.

Anonymous query results reuse one emulator-owned hidden dataset per
project/location and one collision-resistant table identity per job. Metadata
publication attaches the file-configured expiration (24 hours by default).
`CatalogService` serializes physical/metadata resource mutations in one process,
rechecks expiration under that boundary, drops physical storage first, and deletes metadata second
when `tables.get`, `tables.list`, or Storage Read resolves the table. This models
BigQuery's [anonymous result
storage](https://cloud.google.com/bigquery/docs/cached-results#how_cached_results_are_stored)
without claiming a durable background expiration service. `CatalogService` has
no cleanup goroutine and no `Close` phase; each boundary completes lazy cleanup
synchronously. Hidden datasets use ordinary delete-content checks when addressed
directly, while `datasets.list` hides them unless `all=true`.

<!-- section: query-jobs -->
## Query Job Lifecycle

```text
PENDING -> RUNNING -> DONE(result)
                   -> DONE(errorResult)
```

BigQuery reports both successful and failed jobs as `DONE`; clients inspect
`status.errorResult`, per the official [JobStatus
resource](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobStatus).
The current repository stores state and materialized results in memory.
Query job identity is `(project, location, jobId)` plus a canonical configuration
fingerprint. Every reused ID returns `409 duplicate`; the fingerprint only makes
same-versus-different configuration drift visible without logging SQL. This
follows BigQuery's documented retry behavior; see the official
[reliability guidance](https://cloud.google.com/bigquery/docs/reliability-intro#retry_failed_job_insertions).
`jobs.insert` executes in a service-owned background goroutine with the
file-configured `query.operationTimeout` hard ceiling. Shutdown rejects new
query work, cancels admitted synchronous and asynchronous work, and waits for
the service to become idle before Storage services or DuckDB close. Every query
result row still remains in Go memory. Bounded worker admission, durable
terminal state, result retention, and the public
[`jobs.cancel`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/cancel)
route remain gaps. Cross-type
query/load uniqueness and terminal-update recovery remain
`query.jobs.cross-repository-identity-v1` and `query.terminal-persistence-v1`.
REST DTOs preserve the presence of known unsupported query controls and reject
them before execution under `query.options.unsupported-v1`; diagnostics contain
field names, never parameter values, labels, SQL, or rows. This boundary follows
the official [`QueryRequest`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#QueryRequest)
and [`JobConfigurationQuery`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery)
field sets.
REST table browsing crosses a separate `TableDataReader` outbound port. The
application resolves live canonical metadata and lazy expiration under the
catalog mutation boundary. Its file-configured operation deadline starts before
context-aware admission, then covers TTL resolution and the DuckDB transaction.
Within that transaction DuckDB obtains the exact count, streams no more than the
configured row count, and incrementally trims canonical values. Backend JSON
size is deliberately not a gate because DuckDB includes field names that the
public schema-ordered `f/v` row does not. The application verifies the
replaceable adapter's non-negative total, effective row limit, page range, and
canonical byte budget before returning it. REST
alone owns the schema-driven nested `f/v` JSON representation and applies the
exact uncompressed response limit, including envelope and token. Accepted row
fragments stream without a second full payload copy. A normal 10,000,000-byte
page stops before a subsequent row; only its first row may cross that boundary,
up to the 100,000,000-byte hard response ceiling. These deterministic local
counts are stricter than Cloud's approximate internal representation described
by the official [pagination limits](https://cloud.google.com/bigquery/docs/paging-results#api-limits).
Resource-scoped opaque tokens advance by the exact emitted row count. This follows the official
[`tabledata.list`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list)
boundary without exposing DuckDB values to transport code.
The execution ceiling follows the same bounded-job intent as official
[`jobTimeoutMs`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfiguration.FIELDS.job_timeout_ms),
while exact request-level `timeoutMs` behavior remains a separate compatibility
gap.
Connector-required `configuration.query.priority` and `configuration.labels`
are domain data rather than scheduler policy: priority is enum-validated, labels
are validated and round-tripped (including an empty map), and both participate
in the configuration fingerprint. Logs expose only priority, label count, and a
sorted label-key fingerprint, never label values.

For the supported query subset, creation is preceded by an explicit
`QueryAnalyzer` port:

```text
GoogleSQL request
  -> structural relation analysis
  -> source/default/destination dataset location validation
  -> generated anonymous destination (row-producing, destination omitted)
  -> JobRepository.CreateOrGet
  -> DuckDB materialization
  -> catalog publication
```

The application never imports DuckDB parsing. The DuckDB adapter returns only
referenced table identities and `producesRows`; logs retain SQL length/digest,
statement type, model version, and counts, not SQL. This follows BigQuery's
[location inference](https://cloud.google.com/bigquery/docs/locations#specify_locations)
and `JobConfigurationQuery` [generated destination
contract](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery).
The exact connector consumer is `0.44.2`
[`TempTableBuilder`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java#L1150-L1240),
which reads the completed job's `destinationTable` when no materialization
dataset is configured.

<!-- section: transactions -->
## Transactions and Visibility

An engine statement transaction is not automatically a BigQuery operation
transaction. Metadata plus physical DDL still spans separate stores. An explicit
query destination evaluates once into a DuckDB staging table and applies
`WRITE_EMPTY`, `WRITE_APPEND`, or exact-schema `WRITE_TRUNCATE` in the same
transaction, following
[`JobConfigurationQuery`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery).
New-table metadata is published after CTAS commit and a publication failure
triggers a compensating physical drop. Anonymous destinations use the same CTAS
and compensation path after their hidden dataset has been created with
physical-first metadata publication. Cache reuse, durable TTL cleanup, and
schema-replacing truncate remain explicit gaps. A Parquet
load validates a temporary staging table and applies its destination disposition
inside one DuckDB transaction. Storage Write validates all named PENDING streams
before its serialized coordinator applies one DuckDB transaction, following the
atomic group contract of
[`BatchCommitWriteStreams`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.BatchCommitWriteStreams).
Query and load job metadata is SQLite-durable, but query result rows and the
Storage Write stream ledger are not yet restart-durable. Object download is
deliberately outside the load commit.

<!-- section: sql-boundary -->
## SQL Dialect Boundary

Catalog-mutating statements use a GoogleSQL AST adapter and immutable semantic
commands. General query reference rewriting remains a bounded adapter and is
not a complete implementation of scripts, table decorators, or every query
expression. The authoritative syntax is the
[GoogleSQL lexical structure](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical)
and [query syntax](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax).
Unknown or unsupported forms must fail explicitly rather than be approximately
rewritten.
The application executes `CREATE TABLE`, `DROP TABLE`, `TRUNCATE TABLE`,
top-level `ADD`, `RENAME`, and `DROP COLUMN`, and `ALTER COLUMN SET DATA TYPE`
through typed engine plans and the canonical catalog service. Unsupported DDL
fails before mutation under `query.ddl.catalog-sync-v1`. A process crash between
the engine change and SQLite publication still requires #26 reconciliation.
The same boundary permits only one statement plus an optional trailing
semicolon. A literal/comment-aware scan rejects all [multi-statement
queries](https://cloud.google.com/bigquery/docs/multi-statement-queries) before
job or engine side effects under `query.scripts.unsupported-v1`. Full script
support requires statement-by-statement semantic analysis, variables, control
flow, temporary objects, and job-level transaction semantics; passing an opaque
script to DuckDB is never an acceptable fallback.

The verified static unpartitioned overwrite path is intentionally structural and
versioned. Its released connector `0.44.2` public-edge E2E uses a token parser
that recognizes the source-derived shape from
[`BigQueryClient.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java),
applies the constant-false [BigQuery `MERGE`
contract](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement),
and executes one atomic [DuckDB `MERGE
INTO`](https://duckdb.org/docs/current/sql/statements/merge_into). It does not
generalize to dynamic time/range partition overwrite or arbitrary `MERGE`; those
remain explicit gaps.

<!-- section: runtime-security -->
## Runtime, TLS, and Public Access

The process composes one storage engine and a BQEMU-owned SQLite state store.
SQLite owns canonical catalog, query/load job metadata, and Storage Read
lifecycle metadata. System clock/ID adapters, application services, public
REST/gRPC listeners, and an optional admin listener are composed around those
ports. One certificate pair enables TLS on the public listeners and on admin
when enabled.

BigQuery-compatible REST and gRPC endpoints do not authenticate or authorize
callers. Missing, arbitrary, malformed, and expired-looking `Authorization`
values reach the same protocol handlers. The public runtime neither parses
credentials nor propagates a caller principal. Boundary observability records
only the redacted metadata key, never its value.

TLS protects transport without adding caller identity. Client-side token
acquisition remains outside the emulator runtime. `admin.tokenFile` is an
independent option that protects only the separate diagnostics listener; it is
not a public BigQuery authentication policy.

<!-- section: observability -->
## Capabilities and Observability

Boundary logs include operation, status, identifiers, counts, latency, and
digests. Authorization, credentials, tokens, raw SQL, row payloads, protobuf
JSON, HTTP bodies, and error text are excluded in every format, level, and
configuration mode. The deprecated `logging.unsafePayloads` input remains
parse-compatible but is an explicit no-op. Opaque values cross into logging only
as shape, byte/item count, and whole-value SHA-256 through the observability
adapter. This fail-closed boundary follows [Cloud Logging audit
guidance](https://cloud.google.com/logging/docs/audit/best-practices); regex
redaction is not considered proof that an unknown protocol value is safe.
Capability profiles are versioned observations, not feature negotiation or proof
that every flow succeeds.

<!-- section: replacement-roadmap -->
## Replacement Roadmap

1. Persist canonical metadata, jobs, read sessions, and write/load ledgers in
   transactional system tables.
2. Replace broad regex SQL translation with structural adapters without
   generalizing the pinned static-overwrite shape.
3. Add Storage Read split/compression, historical snapshot support, nested
   projection, and durable session recovery without weakening the current
   byte/row/session bounds.
4. Add Storage Write ArrowRows, BUFFERED/explicit COMMITTED streams, FlushRows,
   default expressions, CDC, and durable pending recovery.
5. Extend the load port with missing-table create, schema-update options,
   non-Parquet formats, and multipart/resumable transfer while retaining bounded
   staging.
6. Persist anonymous-result ownership/expiration and add a bounded background
   sweeper while preserving physical-first cleanup and retryable metadata.

These changes preserve the dependency rule; DuckDB remains replaceable rather
than becoming the application API.
