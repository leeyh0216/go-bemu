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
7. Before changing a storage-engine port, read the planning and composition
   contracts in the [storage engine adapter guide](engine-adapter-guide.md).

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

Consumer releases are declared in `tests/integration/contract/cases/*.yaml`. A case selects a
`runtimeProfile`, `runnerAdapter`, `compatibilityProfile`, and `scenarioSet`, and
contains its exact versions and immutable artifact URI and SHA-256. Add one case
file when a release uses an existing runtime, invocation, and wire contract. Do
not infer an adapter from a semantic-version range.

Change `tests/integration/contract/consumers.yaml` only when the runtime shape, invocation method,
wire contract, or scenario set is new. Operation IDs and scenario IDs are
different namespaces. Test annotations contain only an operation ID; case YAML
selects scenarios. Run the following checks before committing:

```bash
make contract-generate
make ci-static
go run ./tests/integration/cmd/integrationctl matrix --root . --family spark --lane required
```

CI isolates every matrix row. For a local client executable, select the matching
case explicitly, for example `BQEMU_CONSUMER_CASE=case-id make bq-test`. A
family-only local target intentionally stops when more than one case would share
the same installed runtime.

The compiler rejects unknown fields and references, duplicate IDs, invalid
digests, overlapping operation IDs in one scenario set, ordering cycles, and
incompatible runtime/adapter combinations. Each scenario selector names the
tests executed by its typed adapter. Calls outside the scenario and declared
runner setup operations fail the case.

CI and typed runners read only `tests/integration/contract/consumers.normalized.json`. Wire-call
cardinality is aggregated across the case. Partial ordering is checked within
each individual server run, identified in evidence by a digest of its relative
log path. An `execution` artifact is used by the runtime after digest or hash-lock
verification. A `tool-provenance` artifact identifies a release but is not
claimed as the executable installed by CI. Artifact `usage` is a strict adapter
contract such as `python-wheel` or `spark-connector-dsv1-jar`; the compiler requires
exactly one artifact for every usage declared by the adapter.
Every required case is also emitted into the credential and TLS matrix. Its
runner adapter selects the typed auth process, and the same runtime versions and
artifact usages drive setup and identity verification.

The bq case is deliberately different: `cloud-sdk-release-provenance` is a
`tool-provenance` OCI digest and is not executed by the runner. CI installs the
declared Cloud SDK version through `setup-gcloud`, then the adapter requires the
exact Cloud SDK and bq component identities from structured `gcloud version`
output as well as the exact `bq version` identity. Evidence records this
boundary and never claims that the OCI image is the installed executable.

Required cases block image publication; preview and nightly cases are selected
by scheduled or manually dispatched workflows and are not part of the release
requirement. An empty preview or nightly matrix is valid. Storage operations
remain grounded in the [official RPC
reference](https://cloud.google.com/bigquery/docs/reference/storage/rpc).

<!-- section: diagnose-drift -->
## Diagnose Drift

1. Reproduce at the public endpoint and capture stage plus identifiers.
2. Emit `version`, `operation`, `shape`, `fingerprint`, `fix_hint`, and the raw
   request, response, SQL, row, and backend error context.
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

Run `make check`, build the container when Docker behavior changed, inspect the
compatibility diff, and ensure every public claim names a tested boundary.
README, CONTRIBUTING, and all `docs/en/**` files need Korean counterparts with
identical markers and source URLs. Every issue body needs equivalent
`## English` and `## 한국어` scope, acceptance criteria, exclusions, and sources.
Do not close an issue until its acceptance criteria are exercised.
