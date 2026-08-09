# UI Architecture

## Dependency direction

```text
features/components -> BigQueryApi port <- HTTP adapter
                                      <- mock adapter
```

Feature components do not parse BigQuery JSON responses. The HTTP adapter converts BigQuery v2 resources into the models in `src/domain`. The mock adapter implements the same port for deterministic UI tests and design review.

## Modules

- `features/projects`: project selection and emulator project lifecycle.
- `features/explorer`: datasets, tables, schema, preview, and details.
- `features/query`: SQL editing, execution statistics, and typed results.
- `features/jobs`: filtering, status, details, and cancellation.
- `components`: layout, status, result tables, and request states shared by all features.
- `adapters`: transport-specific code. No feature imports an adapter directly.

The console intentionally has no private query or job implementation. Core actions use the same REST resources exercised by official clients and the `bq` CLI. `/emulator/v1` is limited to emulator administration that BigQuery v2 does not expose, such as creating a local project.

## Distribution

This directory is the canonical UI source. The Go emulator can serve `dist` when explicitly enabled. The compatibility lab consumes the published console container or bundle rather than copying components.
