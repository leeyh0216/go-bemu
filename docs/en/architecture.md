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

**Current implementation:** REST metadata/query plus opt-in Parquet load jobs
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
`jobs.insert` executes on an unbounded background goroutine; worker admission,
durable terminal state, cancellation, location-aware keys, and idempotent replay
remain design work.

<!-- section: transactions -->
## Transactions and Visibility

An engine statement transaction is not automatically a BigQuery operation
transaction. Metadata plus physical DDL still spans separate stores. A Parquet
load validates a temporary staging table and applies its destination disposition
inside one DuckDB transaction. Storage Write validates all named PENDING streams
before its serialized coordinator applies one DuckDB transaction, following the
atomic group contract of
[`BatchCommitWriteStreams`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.BatchCommitWriteStreams).
Those atomic transactions do not make the process-local job or stream ledgers
restart-durable, and object download is deliberately outside the load commit.

<!-- section: sql-boundary -->
## SQL Dialect Boundary

Backtick reference rewriting is a temporary adapter concern. Regex replacement
cannot distinguish a table identifier from a quoted column, string, comment,
script, table decorator, or function argument. General compatibility requires a
structural GoogleSQL parser/semantic adapter. The authoritative syntax is the
[GoogleSQL lexical structure](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical)
and [query syntax](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax).
Unknown or unsupported forms must fail explicitly rather than be approximately
rewritten.

One Static Partial exception is intentionally structural and versioned. A token
parser recognizes the source-derived connector `0.44.2` shape from
[`BigQueryClient.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java),
applies the constant-false [BigQuery `MERGE`
contract](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement),
and executes one atomic [DuckDB `MERGE
INTO`](https://duckdb.org/docs/current/sql/statements/merge_into). It does not
generalize to dynamic time/range partition overwrite or arbitrary `MERGE`.

<!-- section: runtime-security -->
## Runtime, TLS, and Identity

The process composes one warehouse, process-local catalog/job repositories,
system clock/ID adapters, application services, public REST/gRPC listeners, and
the optional separate admin listener. One certificate pair enables TLS on the
public listeners and on admin when enabled. Authentication is currently
permissive. Transport security and identity are separate; the service does not
implement the ADC or IAM behavior in [Google Cloud
authentication](https://cloud.google.com/docs/authentication).

<!-- section: observability -->
## Capabilities and Observability

Boundary logs include operation, status, identifiers, counts, latency, and
digests. Authorization, credentials, tokens, raw SQL, and row payloads are
excluded unless an explicit unsafe local-only switch permits payload logging.
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

These changes preserve the dependency rule; DuckDB remains replaceable rather
than becoming the application API.
