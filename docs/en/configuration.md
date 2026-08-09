<!-- doc-id: configuration -->
<!-- lang: en -->

[English](configuration.md) | [한국어](../ko/configuration.md)

# Configuration

BQEMU reads one versioned YAML file at startup. Change configuration, then
restart the process or Compose service. The public resource contract follows the
[BigQuery REST API reference](https://cloud.google.com/bigquery/docs/reference/rest).

<!-- section: precedence -->
## Precedence And Overrides

Configuration is applied in this order:

```text
compiled defaults < YAML file < mapped BQEMU_* environment variables < --set path=value
```

`BQEMU_CONFIG` selects a YAML file; `--config` overrides that selection.
Repeated `--set` overrides one scalar field, for example:

```bash
go run ./cmd/emulator --config configs/bqemu.yaml \
  --set server.http.publicUrl=http://bqemu:9050 \
  --set load.gcsEndpoint=http://fake-gcs:4443
```

Use YAML for structured `bootstrap.projects`; it is intentionally not assembled
from environment variables or `--set` flags.

<!-- section: bootstrap-resources -->
## Bootstrap Resources

Projects are emulator resources, not BigQuery datasets. Declare every project
and dataset that must exist as soon as the service is ready:

```yaml
defaults:
  projectId: local-project
  location: US

bootstrap:
  projects:
    - id: local-project
      friendlyName: Local development
      datasets:
        - id: analytics
          location: US
          labels:
            environment: local
    - id: integration-project
      datasets:
        - id: fixtures
          location: US
```

The reconciler runs before public readiness. It is safe to restart with the
same declarations; resources retain their persisted identity and metadata.

<!-- section: topology -->
## Endpoint Topology

| Setting | Default | Use it for |
| --- | --- | --- |
| `server.http.address` | `:9050` | HTTP listener bind address |
| `server.http.publicUrl` | `http://localhost:9050` | Public REST base URL advertised by the service |
| `server.grpc.address` | `:9060` | Storage gRPC listener bind address |
| `server.tls.certFile` and `server.tls.keyFile` | empty | Enable REST and gRPC TLS together |
| `admin.enabled` and `admin.address` | `false`, `127.0.0.1:9051` | Optional local diagnostics listener |

For Compose, keep the listener addresses unchanged and set `server.http.publicUrl`
to the address reachable by the calling process. The default `compose.yaml`
uses `BQEMU_PUBLIC_URL` for that value. See [Getting started](getting-started.md#use-the-right-endpoint)
for host, sibling-service, and development-container addresses.

<!-- section: runtime-settings -->
## Runtime Settings

| Group | Important settings | Effect |
| --- | --- | --- |
| Defaults | `defaults.projectId`, `defaults.location` | Required default project and location when a request omits them. |
| Engine data | `database.adapter`, `database.dsn`, `database.tempDirectory` | Selects the engine and its physical data and temporary paths. |
| BQEMU state | `state.dsn` | SQLite file for canonical catalog, job, and stream metadata. Keep it beside the engine data. |
| Shutdown | `runtime.shutdownTimeout`, `runtime.serverDrainTimeout`, `runtime.storageCloseTimeout` | Bounded drain and storage close sequence. |
| Query results | `query.operationTimeout`, `query.compensationTimeout`, `query.materialization.*` | Query execution limit, cleanup budget, and optional generated-result dataset. |
| Table pages | `tableData.operationTimeout`, `tableData.maxPageRows`, `tableData.maxResponseBytes`, `tableData.maxRowBytes` | Bounds `tabledata.list` admission and response size. |
| Logging | `logging.level`, `logging.format` | Structured process log level and output format. |
| UI | `ui.enabled`, `ui.directory` | Optional static UI serving. |

When `query.materialization.projectId` and `datasetId` are both set, that
bootstrap dataset must exist and share the query location. Leave both empty for
the internal result dataset.

<!-- section: storage-limits -->
## Storage Read And Write Limits

All Storage settings are loaded at startup. Keep the defaults unless a local
test needs a smaller bounded service.

| Area | Settings |
| --- | --- |
| Read availability and concurrency | `storage.read.enabled`, `maxStreams`, `defaultStreamCount`, `maxSessions` |
| Read response and snapshot budgets | `rowsPerResponse`, `maxResponseBytes`, `maxSchemaBytes`, `maxRowBytes`, `maxSnapshotBytes`, `maxTotalSnapshotBytes`, `maxSnapshotRows`, `spillThresholdBytes`, `tempFilePattern` |
| Write availability and concurrency | `storage.write.enabled`, `maxStreams`, `maxConcurrentAppendRequests`, `queueCapacity`, `queueWaitTimeout`, `operationTimeout` |
| Write request and staging budgets | `maxAppendRequestBytes`, `maxAppendEnvelopeBytes`, `maxInFlightBytes`, `maxInFlightBytesPerStream`, `maxStagedBytes`, `maxStagedBytesPerStream`, `orphanTtl`, `cleanupInterval` |

`protocolModelVersion` for both services is a protocol-model compatibility
setting and should not be changed for ordinary use.

<!-- section: load-jobs -->
## Load Jobs

Every load job uses the configured GCS-compatible JSON endpoint. There is no
local-file source mode.

| Setting | Default | Effect |
| --- | --- | --- |
| `load.gcsEndpoint` | `http://127.0.0.1:4443` | Object API endpoint used by BQEMU. In Compose this is overridden to `http://fake-gcs:4443`. |
| `load.operationTimeout` | `2m` | Entire bounded load operation. |
| `load.maxObjects` | `1000` | Maximum resolved source objects. |
| `load.maxObjectBytes` | `1GiB` | Maximum one downloaded object. |
| `load.maxTotalBytes` | `4GiB` | Maximum combined source bytes. |
| `load.maxMetadataBytes` | `8MiB` | Maximum object metadata response size. |
| `load.maxListedObjects` | `10000` | Maximum objects examined while expanding URI patterns. |
| `load.mediaUpload.bucket` | `bqemu-media` | Bucket used for completed direct-upload objects before the ordinary `gs://` Parquet load path starts. |
| `load.mediaUpload.maxSessions` | `8` | Maximum concurrent process-local resumable upload sessions. |
| `load.mediaUpload.maxBytes` | `256MiB` | Shared in-memory payload budget for a multipart upload in flight and all active resumable sessions. A request without available budget is rejected as retryable rather than allocating beyond it. |
| `load.mediaUpload.maxChunkBytes` | `128MiB` | Maximum resumable chunk size. It must be positive and no greater than `maxBytes`; the default accepts 100MiB chunks. |
| `load.mediaUpload.sessionTtl` | `1h` | Idle lifetime of an incomplete resumable upload session. Sessions do not survive a restart. |

The uploader and BQEMU have different network locations, so configure each
with the address it can reach. Metadata load jobs accept only `gs://` sources.
The media-upload endpoints store completed bytes in this same GCS-compatible
service before submitting the ordinary `gs://` Parquet load job; they do not
introduce a host-file source mode. The supported format is Parquet; see [What
works today](compatibility.md).

<!-- section: full-reference -->
## Full Default File

[`configs/bqemu.yaml`](../../configs/bqemu.yaml) is the complete executable
reference. It contains every scalar leaf, default value, and resource budget
used by the Compose image. Validate a changed file before startup:

```bash
go run ./cmd/emulator --config path/to/bqemu.yaml --print-effective-config
```

The command prints the resolved configuration after environment and `--set`
overrides, without starting the listeners.
