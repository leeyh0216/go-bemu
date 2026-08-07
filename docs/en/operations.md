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
In-memory snapshots charge encoded row bytes, while spill files also charge the
eight-byte frame prefix for every row. `storage.write.maxInFlightBytes*` bounds
decoded requests waiting for the serialized DuckDB coordinator, and
`maxStagedBytes*` bounds deterministic serialized-row charges held in hidden
DuckDB PENDING tables. The configured append size must fit the per-stream limit,
which must fit the global limit. Staged-byte accounting is deliberately stable
and portable; it is not DuckDB's physical file/page size. `load.enabled`
requires an absolute `load.gcsEndpoint`; `load.allowFileSources` defaults false,
and object/list/download limits are enforced. Authentication and runtime
contract-profile negotiation remain uncomposed. A valid setting is not a claim
beyond each Partial capability.

Unknown YAML fields, multiple documents, ambiguous numeric durations, unknown
override paths, and invalid cross-field combinations fail before listeners
start. Errors include `stage`, `operation`, `model_version`, `field`, `shape`,
`fingerprint`, and `fix_hint`. `--print-effective-config` validates and prints
the merged model; source-file and effective-model SHA-256 fingerprints make
drift reproducible. The schema follows the [YAML 1.2.2
specification](https://yaml.org/spec/1.2.2/).

Secret bytes never belong in YAML or environment variables. TLS keys, static
tokens, and a remote admin token are referenced by mounted file path. Effective
configuration can contain those reference paths but never reads or prints file
contents. Treat the output as operational metadata. TLS only secures transport;
it does not implement [Google Cloud
authentication](https://cloud.google.com/docs/authentication).

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
`grpc.Stop`. A separate `runtime.storageCloseTimeout` (default `4s`) bounds Read
snapshot cleanup, Write orphan cleanup, and coordinator close.
`runtime.shutdownTimeout` (default `10s`) remains the fallback for deferred
Storage cleanup during startup or early-return paths. HTTP readiness does not
flip to false before draining, outstanding operation counts are not reported,
and a second signal has no dedicated immediate-exit path. Abrupt termination can
lose the process-local catalog, jobs, Read sessions, Write streams, and load
idempotency records even when the DuckDB file persists.

Tests must cover idle shutdown, an active REST request, an active gRPC stream,
deadline expiry, second-signal force exit, and restart with a mounted volume.
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
