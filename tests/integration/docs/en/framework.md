<!-- doc-id: integration-framework -->
<!-- lang: en -->

[English](framework.md) | [한국어](../ko/framework.md)

# Integration Test Framework

The framework executes version-pinned public processes against the [BigQuery
API](https://cloud.google.com/bigquery/docs/reference/rest). Product packages do
not import it or select behavior by client version.

The version-bound connector examples use the [pinned source
revision](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/719817782a214b8ca72be520870013a3e0253d92).

<!-- section: manifests -->
## Manifests And Evidence

There are four handwritten inputs. Everything else is generated or observed:

1. A test under `tests/integration/<family>` proves one caller-visible behavior
   and carries literal annotations.
2. `tests/integration/contract/consumers.yaml` declares runner ownership,
   selectors, scenario grouping, traffic source, and operation
   order/cardinality expectations.
3. One file under `tests/integration/contract/cases/` pins a release's runtime,
   provenance, and immutable artifacts.
4. A source-reviewed profile, golden, or lock changes only when its wire
   contract or byte identity changes. It is not regenerated from a test run.

Python tests use one `pytest.mark.operation` marker per public operation. The
command-line runner uses `@operation("...", scenario="...")`; the scenario
label makes one shared entrypoint unambiguous.

```python
@pytest.mark.operation("bigquery.tables.get")
@pytest.mark.operation("grpc.bigquery-read.create-read-session")
def test_reads_one_table(...):
    ...
```

Spark test claims use one `contract_case` annotation instead of separate
capability and operation lists:

```python
@contract_case(
    "SBQ-READ-ARROW-TABLE-V1",
    state="verified",
    category="read",
    summary="Arrow table read with four requested streams",
    profile="spark-bigquery-connector-dsv1-0.44.2",
    wire_flow="read-arrow",
    operations=(
        "bigquery.tables.get",
        "grpc.bigquery-read.create-read-session",
        "grpc.bigquery-read.read-rows",
    ),
)
def test_reads_one_table(...):
    ...
```

All metadata is literal and the compiler reads it with Python's standard AST.
`verified` requires a passing test. `partial` also requires an issue and a
limit. `gap` requires a strict expected-failure test, an issue, and a limit.
The compiler rejects unknown IDs, aliases, dynamic metadata, orphaned
annotations, or a selected test with no declared traffic.

`consumers.yaml` defines runtime profiles, runner adapters, compatibility
profiles, scenarios, and scenario sets. Each release in
`tests/integration/contract/cases/*.yaml` selects those IDs and pins every
version and artifact digest. The compiler rejects unknown fields, duplicate
IDs, invalid digests, ordering cycles, and runtime/adapter mismatches.

Generate and validate the fully expanded input with:

```bash
make integration-contract-generate
make integration-contract-check
make consumer-runner-test
```

`integration-contract-generate` writes the normalized execution input and the
EN/KO compatibility pages. Never hand-edit those outputs.

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

<!-- section: extending -->
## Add Or Change A Case

Add one case YAML when a release reuses an existing runtime, invocation, and
wire contract. Change `consumers.yaml` only when one of those contracts changes.
Do not infer an adapter from a semantic-version range. Keep Python, CLI, and
Spark-specific setup inside their integration runner adapters and guides.

The current exact versions, immutable artifacts, executions, and scenario IDs
are generated in [Consumer compatibility](consumer-compatibility.md).

<!-- section: generated-output -->
## Generated Output And CI

The compiler writes only these reviewable projections:

- `tests/integration/contract/consumers.normalized.json`: fully expanded input
  consumed by runners and workflow matrices.
- `tests/integration/docs/en/consumer-compatibility.md` and its Korean pair:
  rendered release/runtime/provenance view.
- `tests/integration/contract/capabilities.normalized.json`: test-derived
  capability-to-operation index.
- `tests/integration/docs/en/capability-coverage.md` and its Korean pair:
  compact, test-backed coverage claims.

Do not create another coverage file or edit these projections by hand. The
runner writes evidence, a diff, and JUnit only during execution. CI shows the
result in the job Summary, stores `index.html` with JUnit in `test-report-*`,
and uploads raw diagnostics only for failed jobs. Runtime traces are evidence;
they do not automatically rewrite or approve source-reviewed goldens.

<!-- section: failures -->
## Failure Guide

| Failure | Fix at the source |
| --- | --- |
| Unknown operation annotation | Use an existing public operation ID or add it through `contract/operations.yaml`. |
| Selected annotation has no traffic | Put literal operation IDs on the selected test, or use the typed `load:` runner-evidence exception. |
| Invalid `partial` or `gap` claim | Add the required issue, limitation, and strict expected failure where applicable. |
| Generated artifacts are stale | Run `make integration-contract-generate`; review and commit the resulting projections. |
| Runner reports an unexpected operation | Change the product behavior/test, or add a justified scenario expectation. Do not discard the observed event. |
| Artifact/version mismatch | Update the versioned case YAML and immutable provenance, not workflow YAML. |
