<!-- doc-id: maintainer-guide -->
<!-- lang: en -->

[English](maintainer-guide.md) | [한국어](../ko/maintainer-guide.md)

# Maintainer Guide

<!-- section: bootstrap -->
## Clone to Running Service

Requirements are Go 1.26+, a C/C++ compiler for DuckDB, and optional Docker and
direnv. No upstream emulator clone is required.

```bash
direnv allow
mkdir -p data "$BQEMU_TEMP_DIRECTORY"
make check
make run
```

The checked-in, credential-free `.envrc` sources `.envrc.example` and then the
optional ignored `.envrc.local`. The example selects `configs/bqemu.yaml`, host
database/temp paths, and bounded test budgets. Put only machine-specific,
non-production overrides in `.envrc.local`. Never put a token, private key,
credential JSON, or production endpoint in either checked-in file. Without
direnv, export the same values or pass `--config` and repeated `--set`
explicitly. The exact merge and validation rules are in [Configuration and
operations](operations.md). Confirm `GET /healthz`, then create the emulator
project described in the root README. REST shapes come from the [BigQuery REST
reference](https://cloud.google.com/bigquery/docs/reference/rest).

<!-- section: learning-path -->
## Learning Path

1. Read [Architecture](architecture.md), especially dependency and transaction
   boundaries.
2. Run the service and one project/dataset/table/query request from the README.
3. Run the nearest focused test, for example `go test ./internal/domain -run Schema`.
4. Trace the request from `internal/transport` through application/ports to an
   adapter.
5. Read [BigQuery and connector internals](bigquery-internals.md) before changing
   a Storage or connector-dependent contract.
6. Use [Compatibility](compatibility.md) to choose an existing capability or
   declare a new one.

The connector baseline is exact [Spark BigQuery connector
`0.44.2`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2).

<!-- section: first-change -->
## First Change Runbook

Start with one public behavior and one negative case. Add or update the domain
invariant, application test with fake ports, adapter behavior, public REST/gRPC
test, compatibility row, and both language documents. Run:

```bash
gofmt -w ./cmd ./internal docs/documentation_test.go
go test ./...
go vet ./...
```

Before review, verify that no test only asserts engine SQL, no unsupported field
is silently ignored as implemented, and errors name the capability and an
actionable fix.

<!-- section: new-version -->
## Add a Protocol or Client Version

Use the pipeline `protocol profile -> adapter -> capability -> golden -> E2E`.
Record exact artifact/tag, REST/RPC sequence, field-presence rules, wire format,
schema mapping, retry/offset semantics, and removal criteria. Do not edit an old
profile in place or link a mutable branch. Storage operations are compared with
the [official RPC reference](https://cloud.google.com/bigquery/docs/reference/storage/rpc).

<!-- section: diagnose-drift -->
## Diagnose Drift

1. Reproduce at the public endpoint and capture stage plus identifiers.
2. Emit `version`, `operation`, `shape`, `fingerprint`, and `fix_hint` without
   credentials, SQL text, or row payload.
3. Compare the request/response to the pinned profile and official contract.
4. Localize the mismatch to transport, application invariant, or outbound
   adapter.
5. Add a negative golden before applying a narrow fix.
6. Run focused tests, full tests, vet, and the released-client E2E lane.

Schema fingerprints are deterministic digests, not schema authorities. The
canonical type source remains [BigQuery data
types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types).

<!-- section: release -->
## Release and Documentation Runbook

Integration operation coverage lives next to executable integration sources as
literal `# bqemu:operation <id> scenario=<scenario>` annotations. Run
`make integration-contract-check` after adding one; it regenerates the
normalized consumer manifest and rejects drift. Runner-only/load scenarios may
use an explicit reviewed exception only when it states a non-empty reason.

Every CI test job writes one compact GitHub Job Summary with the suite result,
pass/fail/skip counts, duration, and the named readable report artifact. JUnit
lanes use XML only as the machine-readable input and upload a JUnit HTML report.
Go, static, CLI, Compose, and matrix lanes use the same payload-safe suite HTML
shape. Keep the compact report for seven days. Detailed Compose, service, Spark,
and raw diagnostics are retained only for failed jobs under an artifact name
ending in `-diagnostics-<run-id>`; they are not the primary CI interface.

Run `make check`, build the container when Docker behavior changed, inspect the
compatibility diff, and ensure every public claim names a tested boundary.
README, CONTRIBUTING, and all `docs/en/**` files need Korean counterparts with
identical markers and source URLs. Every issue body needs equivalent
`## English` and `## 한국어` scope, acceptance criteria, exclusions, and sources.
Do not close an issue until its acceptance criteria are exercised.
