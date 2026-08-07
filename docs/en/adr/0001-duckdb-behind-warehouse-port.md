<!-- doc-id: adr-0001-duckdb-warehouse-port -->
<!-- lang: en -->

[English](0001-duckdb-behind-warehouse-port.md) | [한국어](../../ko/adr/0001-duckdb-behind-warehouse-port.md)

# ADR-0001: Keep DuckDB Behind a Warehouse Port

<!-- section: status -->
## Status

Accepted for the initial vertical slice.

<!-- section: context -->
## Context

The emulator needs local SQL execution and table storage without implementing a
database. DuckDB provides embedded SQL, nested/list types, transactions, and file
persistence, documented in the [DuckDB SQL
introduction](https://duckdb.org/docs/stable/sql/introduction). Its dialect,
catalog, types, and transaction behavior are not BigQuery contracts.

<!-- section: decision -->
## Decision

Domain/application code depends on a warehouse/query-engine port. The DuckDB
driver, physical schema naming, quoting, type mapping, and SQL execution stay in
an outbound adapter. Canonical BigQuery metadata remains independent of engine
types. Adapter assertions and application tests with a fake enforce the boundary.

<!-- section: consequences -->
## Consequences

DuckDB can be replaced without changing public REST/gRPC DTOs or job state.
However, metadata plus engine DDL currently needs compensation and can drift on
process failure. Durable system tables and a transaction port are required before
restart-atomic catalog claims.

<!-- section: alternatives -->
## Alternatives

Embedding DuckDB calls in application services was rejected because it couples
BigQuery lifecycle semantics to one engine. Implementing a database was rejected
as unrelated to the compatibility goal.
