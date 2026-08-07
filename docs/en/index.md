<!-- doc-id: docs-index -->
<!-- lang: en -->

[English](index.md) | [한국어](../ko/index.md)

# Documentation Index

<!-- section: guides -->
## Guides

- [Architecture](architecture.md): dependency rules, runtime composition,
  persistence boundaries, and replacement points.
- [BigQuery and connector internals](bigquery-internals.md): REST jobs, Storage
  Read/Write, indirect load, MERGE, types, and authentication flows.
- [Compatibility](compatibility.md): implemented, partial, registered, planned,
  and unsupported behavior.
- [Schema evolution and CDC](schema-evolution-cdc.md): additive schema rules,
  Storage Write schema changes, CDC ordering, and explicit current limits.
- [Dynamic partition overwrite](dynamic-partition-overwrite.md): pinned Spark
  script semantics, atomic execution, type validation, and promotion gaps.
- [Maintainer guide](maintainer-guide.md): clone-to-run learning path, version
  onboarding, drift diagnosis, and release runbooks.
- [Configuration and operations](operations.md): precedence, container hardening,
  health/shutdown, test timeouts, and diagnostics endpoint design.
- [Architecture decisions](adr/): decisions that constrain implementation.

<!-- section: reading-contract -->
## How to Read These Documents

Statements prefixed with **BigQuery contract** describe the service contract in
the [official BigQuery documentation](https://cloud.google.com/bigquery/docs).
Statements prefixed with **Current implementation** describe this repository.
They are intentionally separate: registration of an RPC or successful DuckDB SQL
does not prove BigQuery semantic equivalence.

<!-- section: version-policy -->
## Version and Source Policy

Connector-dependent statements use the exact [Spark BigQuery connector `0.44.2`
tag](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2).
Historical emulator comparisons use the exact [goccy BigQuery emulator `v0.8.1`
tag](https://github.com/goccy/bigquery-emulator/tree/v0.8.1), without cloning or
building it as part of this project.
Wire contracts use the [BigQuery Storage RPC
reference](https://cloud.google.com/bigquery/docs/reference/storage/rpc), and
engine statements use the [DuckDB documentation](https://duckdb.org/docs/stable/).
Mutable upstream branch links are not accepted for version-bound claims.

<!-- section: maintenance -->
## Maintenance Contract

Every file under `docs/en` has the same relative path under `docs/ko`. Both files
carry the same `doc-id`, ordered `section` markers, and primary-source URLs.
`go test ./...` enforces this contract. See [Contributing](../../CONTRIBUTING.md).
