<!-- doc-id: operations -->
<!-- lang: en -->

[English](operations.md) | [한국어](../ko/operations.md)

# Configuration and Operations Runbook

<!-- section: configuration -->
## Configuration Contract

The implemented versioned loader uses this low-to-high precedence:

```text
compiled defaults < YAML file < mapped environment variables < repeated --set path=value
```

`BQEMU_CONFIG` selects the optional file and `--config` overrides that selector.
Every scalar runtime leaf exists in the `config.bqemu.dev/v1alpha1` model and
can be overridden with typed `--set`; common settings also have named `BQEMU_*`
environment mappings. Structured catalog bootstrap declarations are owned by
the YAML file. The complete Docker-oriented example is
[`configs/bqemu.yaml`](../../configs/bqemu.yaml).

| Layer | Selector | Contract |
| --- | --- | --- |
| compiled defaults | none | complete valid in-memory model |
| file | `BQEMU_CONFIG` or `--config` | one YAML document, at most 1 MiB |
| environment | documented `BQEMU_*` mappings | non-empty scalar overrides |
| CLI | repeatable `--set path=value` | typed override for scalar leaves |

The composition root consumes defaults, HTTP/gRPC/TLS limits, database/temp
paths, shutdown budgets, logging, admin, UI, both Storage services, and load
fields. `storage.read.maxSnapshotBytes` is reserved before each
materialization and settled to the adapter's retained bytes; the sum of live
sessions and in-flight reservations cannot exceed `maxTotalSnapshotBytes`.
`query.operationTimeout` (default `2m`, environment
`BQEMU_QUERY_OPERATION_TIMEOUT`) is the server hard ceiling for both synchronous
and service-owned asynchronous query execution. `query.materialization` owns
server-generated result destinations. `projectId` and `datasetId` must either
both be empty or both identify a public dataset reconciled by `bootstrap` (or
already present in the persistent catalog). An empty pair uses an internal
hidden dataset. `expiration` defaults to `24h` and maps to
`BQEMU_QUERY_MATERIALIZATION_EXPIRATION`; the optional target maps to
`BQEMU_QUERY_MATERIALIZATION_PROJECT_ID` and
`BQEMU_QUERY_MATERIALIZATION_DATASET_ID`. The configured target is checked
before listeners start and again before a job is persisted. Its location must
match every source and explicit job location. An explicit `destinationTable`
always takes precedence and does not inherit generated-result expiration.
Durations must be positive and every scalar can be overridden with `--set`.
Their protocol basis is official
[`jobTimeoutMs`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfiguration.FIELDS.job_timeout_ms)
and [anonymous cached-result lifetime](https://cloud.google.com/bigquery/docs/cached-results#how_cached_results_are_stored).
`query.compensationTimeout` (default `30s`, environment
`BQEMU_QUERY_COMPENSATION_TIMEOUT`) separately bounds physical cleanup after a
metadata publication failure; it is detached from the cancelled request but is
never deadline-free.
`tableData.operationTimeout` (default `30s`, environment
`BQEMU_TABLE_DATA_OPERATION_TIMEOUT`) starts before admission to the global
catalog mutation boundary and bounds that wait, live metadata/TTL resolution,
and the DuckDB count-and-page transaction behind
[`tabledata.list`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list).
`tableData.maxPageRows` (default `10000`, environment
`BQEMU_TABLE_DATA_MAX_PAGE_ROWS`) caps one response even when a caller asks for
more rows; it must stay between 1 and BigQuery's 100,000-row response quota.
`tableData.maxResponseBytes` (default `10000000`, environment
`BQEMU_TABLE_DATA_MAX_RESPONSE_BYTES`) is an exact serialized JSON page budget.
It must be at least 1024 bytes so even an empty metadata envelope fits. The
DuckDB adapter streams a row-count-bounded query and incrementally trims
canonical values; backend JSON sizes never decide public semantics because they
include schema names. REST applies the exact uncompressed wire limit. A continuation token points
at the first row not emitted. `tableData.maxRowBytes` (default `100000000`, environment
`BQEMU_TABLE_DATA_MAX_ROW_BYTES`) is the exact hard ceiling for BigQuery's
documented one-row exception. This local accounting is deliberately deterministic;
Cloud describes its [10 MB page and 100 MB single-row limits](https://cloud.google.com/bigquery/docs/paging-results#api-limits)
as approximate internal sizes. All four values are file-first and have typed
`--set` overrides. Accepted wire fragments stream without a second whole-page
copy. Logs retain the raw rows together with row count, canonical byte count,
incremental framed digest, and the digest of bytes actually written at the HTTP
boundary.
In-memory snapshots charge encoded row bytes, while spill files also charge the
eight-byte frame prefix for every row. `storage.write.maxInFlightBytes*` bounds
decoded requests waiting for the serialized DuckDB coordinator, and
`maxStagedBytes*` bounds deterministic serialized-row charges held in hidden
DuckDB PENDING tables. The configured append size must fit the per-stream limit,
which must fit the global limit. Staged-byte accounting is deliberately stable
and portable; it is not DuckDB's physical file/page size.
`storage.write.queueWaitTimeout` (default `5s`) bounds admission to the
serialized coordinator after byte admission, and
`storage.write.operationTimeout` (default `30s`) bounds the total residence of
an accepted operation in the serialized queue plus its backend execution. It
starts only after queue admission, so queue saturation remains independently
bounded as `RESOURCE_EXHAUSTED`. They map to `BQEMU_STORAGE_WRITE_QUEUE_WAIT_TIMEOUT` and
`BQEMU_STORAGE_WRITE_OPERATION_TIMEOUT`. Queue saturation is reported as the
retryable gRPC `RESOURCE_EXHAUSTED`; a server operation deadline is reported as
`DEADLINE_EXCEEDED`, while an earlier caller deadline still wins. A PENDING
append that was staged before an acknowledgement timeout must be replayed with
the same offset/schema/payload receipt before Finalize; that receipt is
idempotent. DEFAULT streams retain
their official at-least-once ambiguity. The official limit applies to the full
request. The compatibility setting `maxAppendRequestBytes` bounds the serialized
row payload, while transient admission charges the complete
`AppendRowsRequest`.
Startup also requires `server.grpc.maxReceiveMessageBytes`
to fit the configured payload plus the file-configured
`maxAppendEnvelopeBytes` (default 64 KiB). Its environment override is
`BQEMU_STORAGE_WRITE_MAX_APPEND_ENVELOPE_BYTES`. These rules follow the official
[`AppendRows` request and retry contract](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.AppendRows).
`maxConcurrentAppendRequests` (default `16`, environment
`BQEMU_STORAGE_WRITE_MAX_CONCURRENT_APPEND_REQUESTS`) is acquired before gRPC
`Recv`, bounding concurrent protobuf decode, clone, and digest memory across
bidi streams before weighted coordinator admission begins.
`load.gcsEndpoint` must be an absolute HTTP(S) URL, and object/list/download
limits are enforced. Load source URIs must use `gs://`; local paths and other
schemes are rejected before job persistence. Multipart and resumable media
uploads are first persisted as immutable objects in the configured
GCS-compatible service, then use the same `gs://` Parquet load path. Runtime contract-profile
negotiation remains uncomposed. A valid setting is not a claim beyond each
Partial capability.

### Catalog bootstrap

`bootstrap.projects` declares projects and datasets that must exist before any
public listener opens. The list is file-owned because its nested resource
identity cannot be represented safely as independent scalar environment or
`--set` overrides.

```yaml
defaults:
  projectId: local-project
  location: US
bootstrap:
  projects:
    - id: local-project
      friendlyName: Local project
      datasets:
        - id: analytics
          location: US
          labels:
            environment: local
    - id: secondary-project
      datasets:
        - id: staging
```

An omitted dataset location inherits `defaults.location`. Project and dataset
IDs must be unique within the declaration. Dataset descriptions, labels, and
default table or partition expiration values are retained in canonical state.

Startup first compares every declared resource with canonical state. An exact
match is a no-op, so restarting with the same file does not change metadata or
ETags. Any metadata drift fails startup before listeners open; missing resources
are then created in declaration order. When `bootstrap.projects` is absent or
empty, the runtime preserves the previous behavior by creating only
`defaults.projectId`. If that project is present in the list, its declared
metadata is authoritative while it remains the default request context.

Keep the SQLite state file and engine database from the same runtime generation.
Recovery from a process kill exactly between an engine mutation and its
canonical-state commit remains part of the [cross-store recovery
work](https://github.com/leeyh0216/go-bemu/issues/26).

The BigQuery-compatible REST and gRPC listeners do not authenticate or authorize
requests. They ignore missing, arbitrary, malformed, duplicate, and
expired-looking `Authorization` values. There is no public `auth.*` configuration
or `BQEMU_AUTH_*` environment contract. Unknown YAML and `--set` paths still fail
strict configuration validation.

TLS remains independent transport protection. Client libraries may require a
credential before making a request, but token acquisition is outside this
runtime. `admin.tokenFile` is a separate option for the diagnostics listener and
does not protect BigQuery-compatible endpoints.

The HTTP edge accepts `identity` and `gzip` request bodies.
`server.http.maxCompressedRequestBytes` bounds bytes read from the wire before
gzip decoding, while `server.http.maxUncompressedRequestBytes` independently
bounds decoded bytes; both default to 2 MiB. They map to
`BQEMU_HTTP_MAX_COMPRESSED_REQUEST_BYTES` and
`BQEMU_HTTP_MAX_UNCOMPRESSED_REQUEST_BYTES` and remain available through typed
`--set`. Enforcement reads the stream and therefore also covers chunked
requests whose `ContentLength` is unknown. An unsupported encoding returns
`415`, a malformed or multiple encoding returns `400`, and either byte budget
returns `413`. Boundary logs retain the bounded compressed and decoded request
body, headers, encoding, accepted/rejected outcome, byte counts, SHA-256
digests, status, and reason. This follows Go's [`Request` body
contract](https://pkg.go.dev/net/http#Request),
[`MaxBytesReader`](https://pkg.go.dev/net/http#MaxBytesReader), and
[`gzip.NewReader`](https://pkg.go.dev/compress/gzip#NewReader), and the official
[`tables.insert` method](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/insert).

Unknown YAML fields, multiple documents, ambiguous numeric durations, unknown
override paths, and invalid cross-field combinations fail before listeners
start. Errors include `stage`, `operation`, `model_version`, `field`, `shape`,
`fingerprint`, and `fix_hint`. `--print-effective-config` validates and prints
the merged model; source-file and effective-model SHA-256 fingerprints make
drift reproducible. The schema follows the [YAML 1.2.2
specification](https://yaml.org/spec/1.2.2/).

Secret bytes never belong in runtime-configuration YAML or environment
variables. TLS keys and a remote admin token are referenced by mounted file
path. Mount secret files read-only with
deployment-appropriate permissions. Effective configuration can contain those
reference paths but never reads or prints file contents. Treat the output as
operational metadata. TLS only secures transport; it does not implement [Google
Cloud authentication](https://cloud.google.com/docs/authentication).

<!-- section: logging-safety -->
## Raw Diagnostic Logging

Runtime logs retain the request and failure context needed to reproduce an
emulator interaction. There is no `logging.unsafePayloads` switch: raw diagnostic
context is the normal behavior.

| Boundary | Recorded context |
| --- | --- |
| REST | method/path, query and header names and values, bounded request body, status, duration, and handler errors |
| Storage gRPC | RPC name, metadata, protobuf messages, serialized row/schema bytes, stream events, status, and errors |
| SQL and storage engines | submitted SQL or predicate, rows and schemas involved in the operation, generated statement context, and the original backend error |
| side effects | component, operation, pre/post state, raw request/result context, success, and duration |

Size and count limits remain resource controls, not redaction rules. Structured
digests may accompany raw values for correlation. Because logs can contain
headers, credentials, SQL, and row data, operators must apply access, retention,
and export controls appropriate to their environment.

<!-- section: local-run -->
## Local Runbook

```bash
direnv allow
mkdir -p data "$BQEMU_TEMP_DIRECTORY"
make check
make run
curl --fail http://localhost:9050/healthz
curl --fail http://localhost:9050/readyz
```

Direnv is optional and never auto-loads secrets. The checked-in `.envrc` sources
`.envrc.example`, then loads the ignored `.envrc.local` when present. The example
selects `configs/bqemu.yaml`, overrides its container database/temp paths for the
host, and sets bounded Go, Python, and Docker test budgets. Put only
machine-specific non-production overrides in `.envrc.local`. Inspect the merge
without starting listeners with:

```bash
go run ./cmd/emulator --print-effective-config
go run ./cmd/emulator --set logging.level=debug --print-effective-config
```

Liveness proves the process can answer; readiness also pings the warehouse. gRPC
exposes the standard health service. Enabled Storage Read/Write services report
`SERVING`, disabled services report `NOT_SERVING`, and every gRPC health entry is
switched to `NOT_SERVING` before transport draining. Canonical Storage services
are listed in the [Storage RPC
reference](https://cloud.google.com/bigquery/docs/reference/storage/rpc).

<!-- section: container -->
## Container Contract

The image runs as a dedicated non-root `bqemu` user, declares `/data` as
its writable volume, and health-checks `/readyz`. By default Compose supplies a
repository-relative config path as a build argument. The image copies it to the
stable runtime path with mode `0440`:

```yaml
build:
  context: .
  args:
    BQEMU_CONFIG_SOURCE: configs/bqemu.yaml
environment:
  BQEMU_CONFIG: /etc/bqemu/bqemu.yaml
volumes:
  - bqemu-data:/data
tmpfs:
  - /tmp/bqemu:uid=65532,gid=65532,mode=0700
```

Set `BQEMU_CONFIG_SOURCE=configs/<profile>.yaml` and rebuild to select another
non-secret file inside the [Docker build
context](https://docs.docker.com/build/building/context/). This default avoids a
runtime host bind, including Docker Desktop host-sharing restrictions. An
explicit override may instead bind a readable file to
`/etc/bqemu/bqemu.yaml:ro`; its behavior follows the [bind mounts
documentation](https://docs.docker.com/engine/storage/bind-mounts/).

Persist the database only through the named volume owned by container UID
`65532`. A built config must contain paths and non-secret settings only. Mount
TLS/token files separately and read-only; never copy their contents into an
image layer.

The checked-in Compose profile sets a read-only root filesystem,
`no-new-privileges`, a dedicated `/tmp/bqemu` tmpfs, readiness health check, and
a 15-second stop grace period, longer than the configured 10-second application
budget. Dropping all Linux capabilities and explicit CPU/memory limits remain
deployment-specific additions. Docker's authoritative controls are [read-only
root filesystems](https://docs.docker.com/reference/cli/docker/container/run/#read-only)
and [Compose service configuration](https://docs.docker.com/reference/compose-file/services/).

<!-- section: shutdown -->
## Health and Graceful Shutdown

On SIGINT, SIGTERM, or a listener failure, the runtime first marks all gRPC health
entries `NOT_SERVING`. One context using `runtime.serverDrainTimeout` (default
`5s`) bounds public/admin HTTP shutdown and gRPC `GracefulStop`; expiry forces
`grpc.Stop`. A separate `runtime.storageCloseTimeout` (default `4s`) is the
shared resource-close budget. It first rejects new queries, cancels and waits
for admitted synchronous/asynchronous query work, then bounds Read snapshot
cleanup, Write orphan cleanup, and coordinator close. If query work does not
release DuckDB within that budget, Storage and DuckDB close are skipped instead
of racing an active query; process teardown owns those remaining resources.
`runtime.shutdownTimeout` (default `10s`) remains the fallback for deferred
Storage cleanup during startup or early-return paths. HTTP readiness does not
flip to false before draining, outstanding operation counts are not reported,
and a second signal has no dedicated immediate-exit path. Abrupt termination can
lose the process-local catalog, jobs, Read sessions, Write streams, and load
idempotency records even when the DuckDB file persists.

Tests cover query admission rejection, active sync/async cancellation, bounded
query drain, and query-before-Storage close order. Idle shutdown, an active REST
request, an active gRPC stream, deadline expiry, second-signal force exit, and
restart with a mounted volume remain required runtime scenarios.
The Storage Write visibility contract remains the official
[`BatchCommitWriteStreams` contract](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.BatchCommitWriteStreams);
shutdown must not invent commit success.

<!-- section: timeouts -->
## Configurable Test Timeouts

No test may depend on an unlabelled sleep. Current controls deliberately encode
their units and scope:

| Control | Format and default | Current scope |
| --- | --- | --- |
| `BQEMU_GO_TEST_TIMEOUT` | Go duration, `10m` | `make test`, race tests, and CI package budget |
| `BQEMU_STORAGE_READ_TEST_TIMEOUT` | Go duration, `5s` | one Storage Read application-test context |
| `BQEMU_STORAGE_WRITE_TEST_TIMEOUT` | Go duration, `5s` | Storage Write application, adapter, and public gRPC test contexts |
| `BQEMU_REST_TEST_TIMEOUT` | Go duration, `5s` | REST request, gzip boundary, pagination, and overwrite test contexts |
| `BQEMU_DOCKER_START_TIMEOUT_SECONDS` | positive seconds, `120` | `docker compose --wait` startup budget |

External process timeout controls belong to the [integration test
framework](../../tests/integration/docs/en/framework.md). A future common
per-phase split is:

| Control | Purpose | Diagnostic on expiry |
| --- | --- | --- |
| `BQEMU_TEST_STARTUP_TIMEOUT` | process/container readiness budget | ports, health bodies, last bounded raw logs |
| `BQEMU_TEST_REQUEST_TIMEOUT` | one REST/RPC operation budget | operation, capability, request fingerprint |
| `BQEMU_TEST_EVENTUALLY_TIMEOUT` | polling/eventual-state budget | last state and transition history |
| `BQEMU_TEST_SHUTDOWN_TIMEOUT` | graceful-stop budget | outstanding REST/RPC/job counts |

Defaults belong in one test configuration module and must be printed in failure
output. CI may raise budgets through environment variables; an individual test
may reduce a budget through an explicit fixture/option. A timeout failure reports
`version`, `operation`, `shape`, `fingerprint`, and `fix_hint`. The four
split controls are a design contract and are not wired yet.

<!-- section: diagnostics -->
## Diagnostics Admin Endpoint Design

The versioned model defines `admin.enabled`, `admin.address`,
`admin.tokenFile`, `admin.readHeaderTimeout`, and `admin.maxStackBytes`.
Admin is disabled by default at `127.0.0.1:9051`. The composition root starts its
separate listener only when enabled; it never shares the BigQuery REST namespace.
A non-loopback bind requires both a token file and configured server TLS, and the
admin listener uses that shared TLS identity.

| Method and path | Implemented response |
| --- | --- |
| `GET /healthz` | admin liveness and `admin.bqemu.dev/v1alpha1` |
| `GET /bqemu/v1/admin/diagnostics/runtime` | uptime, Go/build/process, goroutine, heap, and GC snapshot |
| `GET /bqemu/v1/admin/diagnostics/goroutines` | bounded text stack with SHA-256 and truncation headers |

If `tokenFile` is set, every route requires a Bearer token compared in constant
time. The trimmed token must contain at least 16 bytes and its file is bounded at
64 KiB. Goroutine output has a default 4 MiB cap and is copied into the
diagnostic log when requested. Remote access and log storage must be protected
accordingly.

A future `GET /bqemu/v1/admin/config` may return model version,
source/effective fingerprints, and the effective model. That config endpoint,
capability/operation counts, and recent drift summaries are still planned and
are not an IAM substitute.
