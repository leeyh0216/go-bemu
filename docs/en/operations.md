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
Every runtime leaf exists in the `config.bqemu.dev/v1alpha1` model and can be
overridden with typed `--set`; common settings also have named `BQEMU_*`
environment mappings. The complete Docker-oriented example is
[`configs/bqemu.yaml`](../../configs/bqemu.yaml).

| Layer | Selector | Contract |
| --- | --- | --- |
| compiled defaults | none | complete valid in-memory model |
| file | `BQEMU_CONFIG` or `--config` | one YAML document, at most 1 MiB |
| environment | documented `BQEMU_*` mappings | non-empty scalar overrides |
| CLI | repeatable `--set path=value` | typed override for every leaf |

The composition root consumes defaults, HTTP/gRPC/TLS limits, database/temp
paths, shutdown budgets, logging, admin, UI, both Storage services, and opt-in
load fields. `storage.read.maxSnapshotBytes` is reserved before each
materialization and settled to the adapter's retained bytes; the sum of live
sessions and in-flight reservations cannot exceed `maxTotalSnapshotBytes`.
`query.operationTimeout` (default `2m`, environment
`BQEMU_QUERY_OPERATION_TIMEOUT`) is the server hard ceiling for both synchronous
and service-owned asynchronous query execution. `query.anonymousResultTtl` (default
`24h`, environment `BQEMU_QUERY_ANONYMOUS_RESULT_TTL`) controls generated result
table expiration. Both values must be positive and can be overridden in the
configuration file or with `--set`. Their protocol basis is official
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
copy. Logs
retain only row count, canonical byte count, incremental framed digest, and the
digest of bytes actually written at the HTTP boundary, never raw rows.
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
request; the pinned Java 3.22.1 client batches by `ProtoData` size. The
compatibility setting `maxAppendRequestBytes` models that client-visible
payload, while transient admission charges the complete `AppendRowsRequest`.
Startup also requires `server.grpc.maxReceiveMessageBytes`
to fit the configured payload plus the file-configured
`maxAppendEnvelopeBytes` (default 64 KiB). Its environment override is
`BQEMU_STORAGE_WRITE_MAX_APPEND_ENVELOPE_BYTES`. These rules follow the official
[`AppendRows` request and retry contract](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.AppendRows).
The exact pinned client source is retained in the
[`google-cloud-bigquerystorage` 3.22.1 source artifact](https://repo.maven.apache.org/maven2/com/google/cloud/google-cloud-bigquerystorage/3.22.1/google-cloud-bigquerystorage-3.22.1-sources.jar).
`maxConcurrentAppendRequests` (default `16`, environment
`BQEMU_STORAGE_WRITE_MAX_CONCURRENT_APPEND_REQUESTS`) is acquired before gRPC
`Recv`, bounding concurrent protobuf decode, clone, and digest memory across
bidi streams before weighted coordinator admission begins.
`load.enabled`
requires an absolute `load.gcsEndpoint`; `load.allowFileSources` defaults false,
and object/list/download limits are enforced. Runtime contract-profile
negotiation remains uncomposed. A valid setting is not a claim beyond each
Partial capability.

Authentication is file-first and is composed before database or listener side
effects. `auth.mode` accepts `disabled`, `bearer-present`, and `static`.
`disabled` deliberately ignores malformed credentials and installs an anonymous
principal. `bearer-present` enforces syntax and presence only; it is a connector
compatibility gate, not identity verification. `static` verifies against one
strict `auth.bqemu.dev/v1alpha1` `StaticTokenSet` YAML file. One authentication
application service and one immutable verifier snapshot serve both REST and
gRPC, so duplicate headers/metadata, principal digests, and allow/deny decisions
have the same semantics. Parsing follows [RFC 6750 bearer
usage](https://www.rfc-editor.org/rfc/rfc6750#section-2.1).

```yaml
apiVersion: auth.bqemu.dev/v1alpha1
kind: StaticTokenSet
tokens:
  - principal: local-developer
    token: replace-with-mounted-secret
```

The token-set decoder rejects unknown/duplicate fields, custom tags, scalar
coercion, aliases in credential fields, multiple documents, duplicate tokens,
and values outside the configured bounds. The source principal is converted to
a digest before publication and never enters logs or request context as text.

The file, token, Authorization field, entry-count, and principal byte bounds are
all explicit `auth.*` leaves with matching `BQEMU_AUTH_*` environment mappings
and typed `--set` paths. `auth.staticTokensReloadInterval` defaults to `5s` and
must be positive. Invalid initial content aborts startup before storage is
opened. An invalid periodic reload atomically publishes a deny-all snapshot; a
later valid reload publishes a new active digest and recovers without restart.
Logs retain policy, reason, byte counts, and SHA-256 digests only. The REST
`/healthz` and `/readyz` paths and the gRPC health `Check`, `List`, and `Watch`
methods are public. REST discovery, gRPC reflection, and every data-plane method
remain protected. A denial is a generic REST `401` or gRPC `UNAUTHENTICATED`
response before body decoding, `RecvMsg`, routing, or application side effects.

Service-account, authorized-user, and external-account credential files remain
client-side token acquisition mechanisms described by [Application Default
Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
and [Workload Identity
Federation](https://cloud.google.com/iam/docs/workload-identity-federation).
BQEMU accepts the resulting bearer token according to the selected local policy;
it does not exchange credentials, validate Google signatures, or emulate IAM.

The HTTP edge accepts `identity` and `gzip` request bodies.
`server.http.maxCompressedRequestBytes` bounds bytes read from the wire before
gzip decoding, while `server.http.maxUncompressedRequestBytes` independently
bounds decoded bytes; both default to 2 MiB. They map to
`BQEMU_HTTP_MAX_COMPRESSED_REQUEST_BYTES` and
`BQEMU_HTTP_MAX_UNCOMPRESSED_REQUEST_BYTES` and remain available through typed
`--set`. Enforcement reads the stream and therefore also covers chunked
requests whose `ContentLength` is unknown. An unsupported encoding returns
`415`, a malformed or multiple encoding returns `400`, and either byte budget
returns `413`. Boundary logs retain only encoding, accepted/rejected outcome,
byte counts, SHA-256 digests, status, and reason, never the raw body or
credentials. This follows Go's [`Request` body
contract](https://pkg.go.dev/net/http#Request),
[`MaxBytesReader`](https://pkg.go.dev/net/http#MaxBytesReader), and
[`gzip.NewReader`](https://pkg.go.dev/compress/gzip#NewReader), the official
[`tables.insert` method](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/insert),
and the pinned connector's [`BigQueryClient.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java).

Unknown YAML fields, multiple documents, ambiguous numeric durations, unknown
override paths, and invalid cross-field combinations fail before listeners
start. Errors include `stage`, `operation`, `model_version`, `field`, `shape`,
`fingerprint`, and `fix_hint`. `--print-effective-config` validates and prints
the merged model; source-file and effective-model SHA-256 fingerprints make
drift reproducible. The schema follows the [YAML 1.2.2
specification](https://yaml.org/spec/1.2.2/).

Secret bytes never belong in runtime-configuration YAML or environment
variables. TLS keys, the dedicated StaticTokenSet document, and a remote admin
token are referenced by mounted file path. Mount secret files read-only with
deployment-appropriate permissions. Effective configuration can contain those
reference paths but never reads or prints file contents. Treat the output as
operational metadata. TLS only secures transport; it does not implement [Google
Cloud authentication](https://cloud.google.com/docs/authentication).

<!-- section: logging-safety -->
## Payload-Safe Logging Contract

`logging.unsafePayloads` and `BQEMU_LOG_UNSAFE_PAYLOADS` are deprecated
compatibility inputs in `config.bqemu.dev/v1alpha1`. Existing files,
environments, and `--set logging.unsafePayloads=true` continue to load, but the
value is a no-op and emits a structured deprecation event. It cannot enable raw
payload logging. Removing the key requires a future configuration API version;
changing its behavior does not.

The invariant applies to JSON and text output, every log level, and both values
of the legacy setting. It follows the official [Cloud Logging structured-log
model](https://cloud.google.com/logging/docs/structured-logging) and [audit-log
security guidance](https://cloud.google.com/logging/docs/audit/best-practices):

| Boundary | Recorded shape | Never recorded |
| --- | --- | --- |
| REST | method/path, query and header **names**, encoding, body bytes read and SHA-256, status, duration | header/query values, request or response body |
| Storage gRPC | RPC and protobuf full name, resource identifiers, offsets, item/row counts, wire/schema byte counts and SHA-256 | protobuf JSON, serialized rows, filter/SQL text, credential metadata values |
| errors | Go error type, message byte count, whole-message SHA-256 | `error`/`err.Error()` text, embedded SQL, values, paths, or credentials |
| side effects | component, operation, pre/post state, success, duration, safe identifiers/counts/digests | raw request/response or backend payload |

`PayloadAttrs`, `ProtoAttrs`, and `ErrorAttrs` are the only conversion boundary
for opaque data. Even the retained `RedactText` helper returns only an omitted
marker, byte count, and digest; pattern-based partial redaction is not treated as
a security boundary. SHA-256 fingerprints support same/different diagnostics,
not payload recovery. The Storage message shapes come from the official
[`google.cloud.bigquery.storage.v1` RPC model](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1).

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
| `BQEMU_AUTH_RUNTIME_TEST_TIMEOUT` | Go duration, `5s` | authentication composition, reload, recovery, and scheduler test contexts |
| `BQEMU_AUTH_TRANSPORT_TEST_TIMEOUT` | Go duration, `5s` | REST and gRPC authentication boundary test contexts |
| `BQEMU_PYTEST_TIMEOUT_SECONDS` | positive seconds, suite default `90`; direnv default `300` | official Python-client build, readiness, request, and shutdown budget |
| `BQEMU_BQCLI_TIMEOUT_SECONDS` | positive seconds, `300` | each bq CLI subprocess plus emulator readiness budget |
| `BQEMU_DOCKER_START_TIMEOUT_SECONDS` | positive seconds, `120` | `docker compose --wait` startup budget |

On Python startup failure the fixture includes only a bounded tail of the local
process log. The suite-wide Python budget is configurable but deliberately
classified as coarse. A future per-phase split is:

| Control | Purpose | Diagnostic on expiry |
| --- | --- | --- |
| `BQEMU_TEST_STARTUP_TIMEOUT` | process/container readiness budget | ports, health bodies, last sanitized logs |
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
64 KiB. Goroutine output is sensitive even though it excludes configuration by
design: its default cap is 4 MiB, it is never copied into logs, and remote access
must be protected.

A future `GET /bqemu/v1/admin/config` may return model version,
source/effective fingerprints, and a redacted effective model. It must never
include Authorization headers, token/key contents, raw SQL, row payloads, or
unbounded logs; secret file references reduce to configured/not-configured or a
non-reversible digest. That config endpoint, capability/operation counts, and
recent drift summaries are still planned and are not an IAM substitute.
