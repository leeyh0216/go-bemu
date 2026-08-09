<!-- doc-id: storage-engine-adapter -->
<!-- lang: en -->

[English](storage-engine-adapter.md) | [한국어](../ko/storage-engine-adapter.md)

# Storage Engine Adapter Implementation Guide

<!-- section: ownership -->
## State and Storage Ownership

BQEMU state and storage engines have different authority. The BQEMU state
adapter owns canonical projects, datasets, tables, logical schemas,
partitioning, clustering, expiration, and control-plane records. The production
state adapter is SQLite. The container configuration stores it at
`/data/bqemu-state.sqlite`.

A storage engine owns physical schemas, tables, rows, staging relations,
snapshots, and engine-local objects only. It must not become a metadata source,
reconstruct BigQuery resources from its catalog, or publish an engine type as a
canonical schema. The current engine adapter is DuckDB, stored at
`/data/bqemu.duckdb` in the container configuration.

```text
REST / Storage RPC
        |
  application use case
     /             \
SQLite state       storage-engine ports
(canonical)        (physical rows and objects)
```

This separation follows the public [BigQuery table resource
contract](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Table).
Changing engines must not change resource identity, job state transitions, or
wire DTOs.

SQLite currently persists catalog resources and mutation-journal primitives.
Query and load jobs, Storage Read sessions, Storage Write stream ledgers, and
load idempotency records remain process-local. Those records belong to the
BQEMU control plane even though their durable repositories are not implemented
yet; they must never be moved into an engine catalog merely to gain persistence.

<!-- section: ports -->
## Port Surface

Implement the smallest applicable ports. Do not add one backend interface that
exposes a driver handle or backend SQL to application code.

| Port | Adapter responsibility |
| --- | --- |
| `HealthChecker` | Verify that the engine can accept work; readiness composes this with the SQLite state check. |
| `EngineCapabilityProvider` | Report portable limits without engine type names or SQL. |
| `SchemaPlanner` | Current whole-schema check; reject an unrepresentable canonical schema before any physical side effect. |
| `WarehouseAdmin` | Create and drop physical dataset/table objects from canonical identities. |
| `TableSchemaPlanner` | Plan one canonical before/after table change without exposing physical SQL. |
| `TableSchemaMutator` | Apply only an application-approved schema-change plan and inspect whether physical storage matches an expected canonical schema. |
| `TableDataReader` | Page physical rows while returning values in canonical schema order. |
| `QueryAnalyzer` | Return structural table references and statement properties, not parser objects or SQL. |
| `QueryEngine` and `QueryMaterializer` | Execute a request and atomically apply one physical destination operation. |
| `QueryOperationAnalyzer` and `QueryOperationEngine` | Recognize and execute a versioned connector operation after canonical metadata is supplied. |
| Storage Read/Write and load ports | Materialize snapshots, coordinate staged rows, and load objects under their package-specific contracts. |

`CatalogRepository`, job repositories, and the mutation journal are state
ports, not storage-engine ports. Application services alone order calls across
state and engine boundaries. Compile-time interface assertions are required,
but behavioral contract tests remain authoritative.

<!-- section: planning -->
## Capabilities and Schema Planning

Capabilities describe portable facts such as maximum decimal precision and
support for nested or repeated fields. A planner validates the complete
canonical schema before `CREATE TABLE`, schema evolution, load staging, or a
query destination performs DDL. It must reject the operation instead of
silently widening, stringifying, flattening, or dropping a field.

**Current contract:** `EngineCapabilities` exposes maximum decimal precision and
scale plus native struct and repeated-field support. Its `TableSchemaChanges`
member reports
`AddColumn`, `DropColumn`, `RenameColumn`, `AlterColumnType`, `Transactional`,
and `InspectBeforeAfter` separately. `TableSchemaPlanner.PlanTableChange(before,
after)` returns a `TableSchemaChangePlan` containing canonical before/after
tables and physical fingerprints, never raw SQL. The application receives this
decision before choosing a mutation strategy. `TableSchemaMutator` applies the
plan and `TableSchemaMatches` compares physical storage with the expected
canonical schema during reconciliation. DuckDB advertises all four top-level
scalar changes and applies each in one physical transaction. Support must not be
inferred from a concrete adapter type or from the fact that a backend accepts an
`ALTER TABLE` statement.

`SchemaPlanner.ValidateSchema` remains the whole-schema representation check.
With the canonical SQLite repository, the application journals every planned
table-schema change before applying it. A state repository without a canonical
mutation journal may use bounded reverse-plan compensation for add and rename;
drop and type change are rejected in that composition.

The planner receives canonical fields. It may derive a physical plan, but the
derived plan stays inside the adapter. Omitted decimal parameters remain
omitted in SQLite metadata; only the physical plan resolves their defaults.
Unsupported capability combinations return a stable domain error before the
driver is called.

When a new engine needs a portable capability that the current port cannot
express, extend the port and its tests first. Detecting support by issuing
speculative DDL is not a capability contract.

<!-- section: types -->
## Canonical Type Policy

The shared policy is intentionally limited by Spark's maximum decimal precision
of 38. It is narrower than the full [BigQuery data type
range](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types).

| Canonical type | Required physical behavior |
| --- | --- |
| `BOOL`/`BOOLEAN`, `INT64`/`INTEGER`, `FLOAT64`/`FLOAT` | Map to native scalar types without changing canonical aliases in SQLite. |
| `STRING`, `BYTES`, `DATE`, `DATETIME`, `TIME`, `TIMESTAMP`, `JSON` | Use a native representation and preserve the public encoding contract at adapter boundaries. |
| `NUMERIC` | Use native decimal with precision at most 38; omitted parameters resolve to `(38,9)`. |
| `BIGNUMERIC` | Use native decimal with precision at most 38; omitted parameters resolve to `(38,18)`. |
| `RECORD`/`STRUCT` | Use a native struct and preserve nested field names and order. |
| `REPEATED` | Use a native list of the planned element type; repeated structs remain list-of-struct, not JSON or text. |
| `GEOGRAPHY` | Unsupported. Reject it during canonical validation; do not store it as text. |

DuckDB satisfies the nested requirements with native
[`STRUCT`](https://duckdb.org/docs/stable/sql/data_types/struct) and
[`LIST`](https://duckdb.org/docs/stable/sql/data_types/list), and uses native
[`DECIMAL`](https://duckdb.org/docs/stable/sql/data_types/numeric) for both
decimal families. Every alternative adapter must test nullability, repeated
values, nested order, decimal boundaries, and round trips through every port it
implements.

<!-- section: mutation-lifecycle -->
## Mutation Lifecycle and Atomicity

One engine transaction is atomic only inside that engine. One SQLite
transaction is atomic only inside BQEMU state. No distributed transaction joins
the two files.

For a cross-store mutation, the application owns this order:

1. Resolve and validate canonical state, capabilities, and preconditions.
2. Record durable intent when the operation participates in the mutation journal.
3. Apply the physical engine transaction.
4. Publish the canonical SQLite change.
5. Mark the intent terminal, or run a separately timed compensating action.

Create, delete, query destination publication, and schema changes may need
different physical-first or state-first compensation, but an adapter never
chooses the order. Compensation must be safe to retry and must not report
success when the engine commit outcome is unknown. Engine transactions follow
the backend's documented atomicity, such as [DuckDB
transactions](https://duckdb.org/docs/stable/sql/statements/transactions).

SQLite contains an immutable mutation-intent ledger with `PREPARED`, `APPLIED`,
and `FAILED` transitions. Table-schema DDL writes this ledger. Before creating
the default project or opening listeners, startup replans each pending change,
verifies its physical fingerprints, and inspects DuckDB. It publishes the
canonical `after` table when storage already matches it, reapplies the plan when
storage still matches `before`, and refuses startup when storage matches neither
side. A complete dataset/table drift check follows pending-record recovery.

This recovery is not yet general. Create/drop resource flows, query destination
publication, and other cross-store workflows do not write every intent. Their
crash recovery and replay remain incomplete, and no distributed transaction
provides cross-store atomicity. Neither file alone is a complete backup of a
live instance.

<!-- section: retry-errors -->
## Idempotency and Errors

A retry key identifies one immutable intent. Reusing the key with the same
resource, mutation kind, expected revision, and physical fingerprints returns
the existing intent. Reusing it with different content is a conflict. Terminal
journal transitions use compare-and-swap behavior and cannot be reversed.

Physical helpers used for cleanup must be repeatable, for example a compensating
drop that tolerates an already-absent staging object. Public create or delete
semantics do not become idempotent merely because cleanup is repeatable.

Adapters preserve `context.Canceled` and `context.DeadlineExceeded`. They map
validation, unsupported capability, missing resource, conflict, stale
precondition, invalid query, and backend failure to the repository's domain
error categories. Wrap driver failures with the operation stage, but do not let
driver text determine REST or gRPC status. An uncertain commit is a backend
failure requiring reconciliation, never an inferred success.

<!-- section: lifecycle-safety -->
## Lifecycle and Safe Diagnostics

Open the state store and verify embedded migrations before opening public
listeners. Open the engine before composing use cases. Readiness checks both
SQLite and the engine. During shutdown, stop admission, cancel and drain query
and Storage work, close engine-owned snapshots and coordinators, then close the
engine and state store. A failed drain must not race `Close` against active work.

Logs may contain stable operation names, model versions, counts, durations,
publicly safe identifiers, and whole-value SHA-256 fingerprints. They must not
contain SQL text, row or object payloads, credentials, serialized protobufs, or
raw driver errors. Journal failures store a stable code and digest only. Use the
shared observability helpers; pattern-based partial redaction is not proof that
an unknown value is safe.

<!-- section: conformance -->
## Adapter Conformance

A new adapter is ready for composition only when tests demonstrate all
applicable contracts:

- compile-time assertions for every advertised port and application tests with
  a non-engine fake;
- capability and schema-planner tests, including nested lists/structs, decimal
  limits/defaults, and explicit `GEOGRAPHY` rejection;
- create, read, update, drop, materialization, staging, and rollback behavior
  using canonical identities rather than backend discovery;
- transaction and fault-injection tests at each state/engine boundary, including
  compensation retry and uncertain commit handling;
- cancellation, deadlines, stable domain error mapping, and resource cleanup;
- restart tests with persistent state and engine files, with process-local
  ledgers still documented as non-recoverable;
- idempotency conflicts and mutation-journal compare-and-swap transitions; and
- captured-log tests proving that SQL, rows, object bytes, credentials, and raw
  errors are absent.

Run the adapter package, application contract tests, affected REST/gRPC tests,
and `go test ./...`. An engine-specific passing query is evidence for that case,
not a claim of general BigQuery compatibility. Record user-visible limits in
[Compatibility](compatibility.md) and SQL behavior in the [GoogleSQL boundary
guide](google-sql-boundary.md).
