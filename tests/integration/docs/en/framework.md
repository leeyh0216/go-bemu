<!-- doc-id: integration-framework -->
<!-- lang: en -->

[English](framework.md) | [한국어](../ko/framework.md)

# Integration Test Framework

The framework executes version-pinned public processes against the [BigQuery
API](https://cloud.google.com/bigquery/docs/reference/rest). Product packages do
not import it or select behavior by client version.

<!-- section: manifests -->
## Manifests And Evidence

There are three handwritten inputs. Everything else is generated or observed:

1. A test under `tests/integration/<family>` proves one caller-visible behavior
   and carries literal operation annotations.
2. `tests/integration/contract/consumers.yaml` declares runner ownership,
   selectors, scenario grouping, and operation order/cardinality expectations.
3. One file under `tests/integration/contract/cases/` pins a release's runtime,
   provenance, and immutable artifacts.

For a Python-family test, put one marker per public operation immediately above
the test function:

```python
@pytest.mark.operation("bigquery.tables.get")
@pytest.mark.operation("grpc.bigquery-read.create-read-session")
def test_reads_one_table(...):
    ...
```

The command-line runner uses the equivalent `@operation("...")` decorator.
Markers take exactly one literal ID. The compiler rejects an unknown ID, a
marker not selected by scenario evidence, or a selected test with no marker.

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
4. Put the annotated test function in `testEvidence` while the current
   manifest schema still requires that explicit mapping. The compiler rejects
   disagreement with the source annotations.
5. Add a release case YAML only for a new pinned executable/runtime/provenance
   combination. Reuse a runner adapter and scenario set when the wire contract
   is unchanged.
6. Run `make integration-contract-generate`, inspect the normalized matrix,
   then run `make integration-contract-check` and the runner unit tests.

Load-only flows are currently selected by a `load:` adapter and prove their
operation sequence from structured runtime evidence rather than a Python test
function. Their order/cardinality remains explicit until the annotation-derived
scenario projection replaces that special path.

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

Do not create a second coverage JSON or edit these projections by hand. The
runner writes evidence, a diff, and JUnit only during execution. CI shows the
result in the job Summary, stores `index.html` with JUnit in `test-report-*`,
and uploads raw diagnostics only for failed jobs.

<!-- section: failures -->
## Failure Guide

| Failure | Fix at the source |
| --- | --- |
| Unknown operation annotation | Use an existing public operation ID or add it through `contract/operations.yaml`. |
| Test evidence has no marker | Put a literal marker on the selected test function. |
| Generated artifacts are stale | Run `make integration-contract-generate`; review and commit the resulting projections. |
| Runner reports an unexpected operation | Change the product behavior/test, or add a justified scenario expectation. Do not discard the observed event. |
| Artifact/version mismatch | Update the versioned case YAML and immutable provenance, not workflow YAML. |
