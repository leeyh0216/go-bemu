<!-- doc-id: integration-framework -->
<!-- lang: en -->

[English](framework.md) | [한국어](../ko/framework.md)

# Integration Test Framework

The framework executes version-pinned public processes against the [BigQuery
API](https://cloud.google.com/bigquery/docs/reference/rest). Product packages do
not import it or select behavior by client version.

<!-- section: manifests -->
## Manifests And Evidence

`tests/integration/contract/consumers.yaml` defines runtime profiles, runner
adapters, compatibility profiles, scenarios, and scenario sets. Each release in
`tests/integration/contract/cases/*.yaml` selects those IDs and pins every
version and artifact digest. `testEvidence` binds scenario operation IDs to the
operation markers in integration tests. The compiler rejects unknown fields,
duplicate IDs, missing markers, invalid digests, ordering cycles, and
runtime/adapter mismatches.

Generate and validate the fully expanded input with:

```bash
make integration-contract-generate
make integration-contract-check
make consumer-runner-test
```

<!-- section: executions -->
## Executions

CI reads only `tests/integration/contract/consumers.normalized.json`. A case can
contain multiple executions, such as a public API contract and an indirect
Parquet load contract. Every execution names one typed runner adapter. The
runner verifies exact tool identity and artifact SHA-256 before starting the
process, compares observed operation cardinality and ordering, and writes JSON
evidence, a structured diff, and JUnit output.

Required cases gate publication. Preview and nightly cases run only when their
workflow lane is selected. Use the generated matrix instead of copying versions
into workflow YAML:

```bash
go run ./tests/integration/cmd/integrationctl matrix \
  --root . --family spark --lane required --execution public
```

<!-- section: add-behavior -->
## Add A Behavior Step By Step

1. Add the external-process test and literal operation annotations first.
2. Add or narrow a scenario selector in `consumers.yaml`. A selector names the
   test file or command entrypoint that the typed runner may execute.
3. Record only order and cardinality that the runner must compare. These cannot
   be inferred from a marker: a marker says an operation is relevant, while an
   expectation says how often it must appear and what must happen before it.
4. Do not write `operationIds` or `testEvidence` in the scenario. For
   `trafficSource: {kind: annotations}`, the compiler derives both from the
   selected annotations. `runner-evidence` is reserved for a `load:` selector,
   needs a reason, and derives its operations from the explicit ordering
   expectations.
5. Add a release case YAML only for a new pinned executable/runtime/provenance
   combination. Reuse a runner adapter and scenario set when the wire contract
   is unchanged.
6. Run `make integration-contract-generate`, inspect generated claims and the
   normalized matrix, then run `make integration-contract-check` and the runner
   unit tests.

Load-only flows are selected by a `load:` adapter and prove their operation
sequence from structured runtime evidence rather than a selected test function.
The exception is explicit in `trafficSource` and cannot be used by another
scenario kind.

Dataframe media upload is a selected Python test, not a `runner-evidence`
exception. Its CI job installs
`tests/integration/python/dataframe-requirements.lock`; use
`make python-dataframe-setup` when preparing that environment locally. The
test owns a fake-GCS runtime, sends the external client only to BQEMU's public
endpoint, and verifies both multipart and resumable Parquet uploads. When
testing the dataframe helper, create the destination table through the same
endpoint-aware client before calling it: otherwise that helper may construct a
new default-endpoint client while creating a missing table.
The contract covers public-client creation and helper append/replace on that
pre-created destination.

Use the phase-aware harness helper for REST setup or assertion queries. It
marks the request log as `setup` or `assertion`, and the runner excludes those
responses from the caller's wire claim. Extend that mechanism before adding a
new harness helper; do not add a harness-only operation to
`contract_case(...)` just to satisfy the comparator.

<!-- section: extending -->
## Add Or Change A Case

Add one case YAML when a release reuses an existing runtime, invocation, and
wire contract. Change `consumers.yaml` only when one of those contracts changes.
Do not infer an adapter from a semantic-version range. Keep Python, CLI, and
Spark-specific setup inside their integration runner adapters and guides.

The current exact versions, immutable artifacts, executions, and scenario IDs
are generated in [Consumer compatibility](consumer-compatibility.md).
