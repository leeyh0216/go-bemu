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

<!-- section: extending -->
## Add Or Change A Case

Add one case YAML when a release reuses an existing runtime, invocation, and
wire contract. Change `consumers.yaml` only when one of those contracts changes.
Do not infer an adapter from a semantic-version range. Keep Python, CLI, and
Spark-specific setup inside their integration runner adapters and guides.

The current exact versions, immutable artifacts, executions, and scenario IDs
are generated in [Consumer compatibility](consumer-compatibility.md).
