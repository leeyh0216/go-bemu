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

**Current implementation:** REST metadata/query is the first public vertical
slice. Storage Read has a tested application service and protobuf adapter for
snapshot ownership, deterministic ranges, offset resume, and bare Arrow/Avro
payload pass-through. No DuckDB snapshot/encoder adapter is composed, so the
public Read service remains `NOT_SERVING` and returns `UNIMPLEMENTED`. Storage
Write is registration-only. Internal protocol tests are not data-plane support.

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
transaction. Metadata plus physical DDL, load staging plus destination
disposition, and multi-stream batch commit each need an application-owned
transaction port. BigQuery specifies that a group of pending streams is committed
atomically by
[`BatchCommitWriteStreams`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.BatchCommitWriteStreams).
No current code may claim that behavior merely because DuckDB can start a SQL
transaction.

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

1. Separate inbound metadata/query/read/write ports and outbound query-engine,
   transaction, auth, and stream-ledger ports.
2. Persist canonical metadata and jobs in transactional system tables.
3. Replace regex SQL translation with a structural adapter.
4. Implement the DuckDB read-snapshot/encoder adapter, compose it with the tested
   ranged-stream application service, and prove exact Arrow/Avro framing at the
   public endpoint.
5. Add per-stream write ledgers and atomic pending-stream commit.
6. Add staged load jobs through an endpoint-configurable object-store port.

These changes preserve the dependency rule; DuckDB remains replaceable rather
than becoming the application API.
