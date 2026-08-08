<!-- doc-id: engine-adapter-guide -->
<!-- lang: en -->

[English](engine-adapter-guide.md) | [한국어](../ko/engine-adapter-guide.md)

# Storage Engine Adapter Guide

<!-- section: purpose -->
## Scope

This guide defines the internal contracts for connecting a storage engine to
BQEMU. An engine adapter stores and executes the BigQuery logical model. It
must not own canonical metadata or decide public API semantics.

The current contracts are split into narrow ports for schema planning, load
planning, query execution, and Storage Read/Write. Application services never
receive one aggregate engine interface.

<!-- section: dependency -->
## Dependency Direction

Domain and application packages do not import DuckDB or another concrete
engine. The package consuming a capability owns its port.

- Catalog ports are in [`internal/ports/catalog.go`](../../internal/ports/catalog.go).
- Load ports are in [`internal/loadjob/ports`](../../internal/loadjob/ports).
- Storage Read ports are in
  [`internal/storageread/ports`](../../internal/storageread/ports).
- Storage Write ports are in
  [`internal/storagewrite/ports`](../../internal/storagewrite/ports).
- Shared engine values and plan contracts are in
  [`internal/engine`](../../internal/engine).

An adapter may implement these ports, but it must not reverse that dependency.
Do not add engine SQL, physical type names, a DSN, or local paths to a port.
Application code must not reach into concrete adapter fields or connection
objects.

<!-- section: capabilities -->
## Capability Declaration

At startup, an engine creates an immutable `engine.Capabilities` snapshot. It
declares engine identity and version, Decimal precision and scale, maximum
STRUCT and LIST depth, transactions, atomic replacement, physical inspection,
and DDL scope.

Anything omitted is unsupported. For example, an engine without a single-table
transaction capability cannot issue a load plan. Planning `WRITE_TRUNCATE` also
requires table-level atomic replacement.

Do not put physical types or SQL in a capability. Physical type mapping remains
inside the adapter, and public plans carry only a fingerprint of that mapping.

<!-- section: composition -->
## Runtime Composition

Construct concrete adapters only in the executable composition root. The
current composition contract is
[`cmd/emulator/engine_runtime.go`](../../cmd/emulator/engine_runtime.go).

`engineRuntime` validates every required port and lifecycle object once. The
composition root then immediately splits it into narrow catalog, query, load,
and Storage Read/Write ports for injection. Never pass the aggregate runtime to
an application service or use it as a service locator.

For an implementation that requires shutdown, Storage Write separates the
application `Coordinator` from the composition-only `ManagedCoordinator`.
Apply the same pattern when another consumer port should not own lifecycle
control.

<!-- section: schema-plan -->
## Schema Planning and Execution

Process a schema operation in this order:

1. The application constructs `engine.SchemaIntent` from the actual request.
2. Adapter `PlanSchema` checks the logical schema and declared capabilities.
3. A pure adapter validator checks physical representability.
4. `engine.SchemaPlan` binds engine identity, capability fingerprint, logical
   input, and the issuing planner.
5. The execution method reconstructs `SchemaIntent` from its actual arguments.
6. The execution method calls `ValidateBinding` before building SQL.

`SchemaPlan` is a short-lived authorization value. It contains neither SQL nor
physical types and must not be persisted. A plan from another engine or planner,
a changed capability snapshot, or changed schema input fails before execution.

New catalog write paths call only plan-required ports such as
`CreatePlannedTable` and `ApplyPlannedSchemaAdditions`. Keep any unplanned
convenience method off application-owned ports. Retain one only for adapter
compatibility and tests.

<!-- section: load-plan -->
## Load Planning and Execution

A load binds both schema and source objects. The current order is:

1. Create the destination table `SchemaPlan`.
2. Resolve URI object metadata through the object store.
3. Fingerprint URI, generation, ETag, and declared size.
4. Create `LoadPlan` from `LoadPlanRequest`.
5. Download into a bounded temporary directory and verify the actual byte count.
6. `ExecuteLoad` revalidates the plan, object fingerprints, and sizes before it
   starts a transaction.

`LoadPlan` stores no source URI or local path. `ResolvedObject` contains only a
fingerprint and size. A downloaded `LocalObject` carries the same fingerprint
and actual size to bind the planned object to the execution artifact.

Adapter-specific load checks must finish before download. Only information that
requires opening the file, such as actual Parquet columns, remains inside the
execution transaction. Schema, object, capability, or adapter-mapping drift
fails without starting a physical write.

<!-- section: query-storage -->
## Query and Storage Ports

SQL enters through the GoogleSQL gateway once. The gateway returns an immutable,
engine-neutral semantic statement whose relations and expression types are
already bound to canonical catalog metadata. An engine adapter visits that
statement and creates a private physical plan; it must not tokenize, reparse, or
infer unresolved table paths from the original SQL.

The shared AST boundary currently exposes these execution roots:

| Root | Engine contract | Explicit boundary |
| --- | --- | --- |
| `SELECT` | relational visitor with canonical relation/type bindings | unknown relation, operator, function, expression, or type fails closed |
| `INSERT`, `UPDATE`, `DELETE` | one typed DML statement and bind arguments | unsupported source/action shape fails before execution |
| `MERGE` | ordered matched/not-matched clauses in one transaction | unsupported action, expression, or cardinality rule fails closed |
| script | ordered `DECLARE`, `SET`, and supported query/DML children in one transaction | control flow, dynamic SQL, temporary routines, and exception blocks are unsupported |
| catalog DDL | application-owned typed mutation plan | only the documented create/drop/truncate and column mutations are accepted |

Query parameters, views, UDFs, procedures, connections, remote functions,
table decorators, and `UNNEST` relations are not engine fallbacks. They require
an explicit AST, semantic-binding, and lowering implementation before support is
advertised.

Storage Read and Storage Write implementations also satisfy consumer-owned
resolver and factory ports. Keep the new engine concrete type from crossing
the composition functions in `cmd/emulator`.

<!-- section: errors -->
## Error Contract

Planning uses stable classifications. Invalid logical input maps to
`ErrInvalid`, an engine representability limit maps to `ErrUnsupported`, and a
stale or foreign runtime binding maps to `ErrPrecondition`.

Wrap the raw adapter error in the planning classification so callers retain the
physical table name, engine SQL, path, or other backend context through the
error text and `errors.Unwrap`. Preserve cancellation and deadline errors while
also attaching a stable error code and capability identifier.

<!-- section: implementation -->
## Implement a New Adapter

1. Define an immutable `engine.Capabilities` snapshot.
2. Implement pure `SchemaAdapterPlanner` and `LoadAdapterPlanner` checks.
3. Implement only the catalog, query, table-data, and Storage Read/Write ports
   the runtime requires.
4. Validate plan bindings before starting SQL generation, a transaction, or
   file access.
5. Wire every port explicitly in the `cmd/emulator` composition root.
6. Run the planning conformance suite from
   [`internal/enginetest`](../../internal/enginetest).
7. Test physical transactions, rollback, and type mappings separately in the
   adapter package.

See [`internal/adapters/duckdb`](../../internal/adapters/duckdb) for the current
adapter and [`internal/enginetest/fake.go`](../../internal/enginetest/fake.go)
for a test implementation. Verify DuckDB transaction behavior against the
[official transaction
documentation](https://duckdb.org/docs/stable/sql/statements/transactions).
Application code does not special-case the fake.

<!-- section: verification -->
## Verification

First add a test that calls `enginetest.RunPlanningConformance` for the new
adapter. Then run the race detector for the adapter and its consumers.

```bash
go test ./internal/enginetest ./internal/adapters/<engine>
go test -race ./internal/engine ./internal/loadjob/ports ./internal/adapters/<engine>
make check
```

During review, verify that no public port can execute without a plan and that
application packages do not import a concrete engine. Also verify that plans
and errors contain no SQL, physical type, URI, or local path.

If the engine stores an applied generation or marker, do not treat it as
canonical metadata. It is a receipt used to verify physical application. The
BQEMU metadata repository remains the source of truth for BigQuery logical
metadata.
