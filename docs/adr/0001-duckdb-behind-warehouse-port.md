# ADR-0001: Put DuckDB behind a warehouse port

- Status: Accepted
- Date: 2026-08-08

## Context

The emulator needs SQL execution, nested/list types, Parquet integration, and
transactional `MERGE`, but it must not become a DuckDB-specific API. Storage
Read/Write and load-job semantics also require application-owned state that is
not the SQL engine's responsibility.

## Decision

Application services depend on `ports.Warehouse`. The DuckDB package implements
that port and exclusively owns `database/sql`, the DuckDB driver import,
physical identifiers, physical types, and dialect translation.

Catalog metadata and jobs have separate repository ports. Object access has an
`ObjectStorage` port. Clock and ID generation are injected.

## Consequences

- A different engine can replace DuckDB without changing domain/application.
- Tests can use a fake warehouse and deterministic clock/IDs.
- BigQuery-versus-DuckDB semantic differences have one explicit adapter home.
- Cross-resource atomicity requires a later unit-of-work design; separate
  in-memory repositories are only an M0 implementation.
- The warehouse port will evolve around BigQuery capabilities, not arbitrary
  DuckDB features.
