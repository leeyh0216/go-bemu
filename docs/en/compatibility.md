<!-- doc-id: compatibility -->
<!-- lang: en -->

[English](compatibility.md) | [한국어](../ko/compatibility.md)

# What Works Today

Use this page to decide whether BQEMU is suitable for a local test. A feature is
**supported** only when the stated behavior is available now. A **limited**
feature works within the listed boundary. **Unavailable** means the request is
rejected or the RPC returns `UNIMPLEMENTED`; do not build a test around it.
The public shapes follow the [BigQuery REST API
reference](https://cloud.google.com/bigquery/docs/reference/rest).

<!-- section: use-now -->
## Use Now

| Area | Status | What you can rely on |
| --- | --- | --- |
| Emulator projects, datasets, and tables | Supported | Create, read, list, patch, and delete the documented metadata subset; schema includes nested and repeated fields. |
| Table rows and query jobs | Limited | Run the documented GoogleSQL subset, poll query jobs, page rows, and use explicit or generated result destinations. |
| Storage Read | Limited | Create a live read session and read Arrow or Avro row batches. |
| Storage Write | Limited | Append ProtoRows through default or PENDING streams, finalize PENDING streams, and batch commit a validated group. |
| Parquet load jobs | Limited | Load `gs://` Parquet objects through the configured GCS-compatible service with supported write dispositions and schema updates. |
| Local persistence | Limited | Catalog and job metadata survive restart. Query result rows and Storage Read snapshot bytes do not. |
| TLS | Supported | Enable REST and gRPC TLS with a certificate and key. |

Use [Getting started](getting-started.md) for the endpoint topology and
[Configuration](configuration.md) for startup resources and GCS settings.

<!-- section: limits -->
## Important Limits

| Area | Boundary |
| --- | --- |
| GoogleSQL | Only the implemented AST subset executes. Unsupported syntax fails before the engine is called. |
| Views | Unavailable. |
| Query results after restart | Job metadata remains, but a non-empty in-memory result is unavailable after restart. |
| Storage Read | No split RPC, compression, historical snapshot, or restored snapshot bytes after restart. |
| Storage Write | No ArrowRows, CDC, BUFFERED or explicit COMMITTED streams, or `FlushRows`. |
| Load source and format | `gs://` and Parquet only. Local paths, Avro, ORC, CSV, and NDJSON are unavailable. |
| Load behavior | No autodetect, multipart/resumable download, or unsupported schema/layout change. |
| Control plane | No IAM authorization, production identity service, copy job, extract job, or job cancellation endpoint. |

When a supported request includes an unsupported option, BQEMU rejects that
option instead of silently ignoring it.

<!-- section: reference -->
## Exact API And RPC Reference

The generated [API and RPC compatibility table](api-rpc-compatibility.md) is
the exact method-level source for a caller that depends on a path, RPC, option,
or response field. It includes conditions such as disabled Storage services and
registered-but-unimplemented RPCs. Implementation and integration evidence are
maintainer material, not user prerequisites.
