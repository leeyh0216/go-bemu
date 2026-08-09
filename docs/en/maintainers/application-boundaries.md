<!-- doc-id: application-boundaries -->
<!-- lang: en -->

[English](application-boundaries.md) | [한국어](../../ko/maintainers/application-boundaries.md)

<!-- section: ownership -->
# Application boundaries

Application handlers own one visible policy at a time: catalog metadata and physical compensation, query admission/execution/materialization, or Storage Read/Write state transitions. Transport packages depend on the smallest local use-case interface; they do not require a concrete application service.
The public boundary follows the [BigQuery REST API](https://cloud.google.com/bigquery/docs/reference/rest).

```text
REST / gRPC -> transport-local use-case interface -> application handler -> consumer-owned port -> adapter
```

`internal/application` must not import an adapter or transport package, nor expose SQLite or DuckDB implementation types. The package boundary test enforces that direction. Composition in `cmd/emulator` supplies the typed ports.
