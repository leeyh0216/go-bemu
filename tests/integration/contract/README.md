# Integration Contract Framework

This package is the source model for version-pinned external-process tests. It
does not define product behavior and product runtime packages must not import
it. Product REST/gRPC behavior belongs in [`contract/`](../../../contract/);
this package proves how released external processes exercise that behavior.

## Start Here

| You are changing | Edit | Do not edit |
| --- | --- | --- |
| A test-local capability claim | A literal `contract_case(...)` on the selected integration test | `capabilities.normalized.json` |
| A public operation used by a Python or CLI test | The test-local literal operation annotation | Scenario `operationIds` or `testEvidence` |
| Invocation, selector, expected ordering, or cardinality | `consumers.yaml` | `consumers.normalized.json` |
| A pinned released runtime or artifact | One file in `cases/` | Workflow version literals |
| Wire behavior or byte identity | The reviewed file in `profiles/`, `golden/`, or a lock | Per-run evidence artifacts |

The only handwritten inputs are tests, `consumers.yaml`, versioned case YAML,
and deliberately reviewed profiles/goldens/locks. Normalized JSON and
compatibility pages are deterministic projections. Runtime traces, JUnit, and
diffs are observed execution evidence, not inputs to the generator.

## Add A Test-Backed Capability

1. Add the public-process test under `tests/integration/<family>/`.
2. Put literal operation IDs on the test. For a Spark capability, use one
   `contract_case` annotation:

   ```python
   @contract_case(
       "EXAMPLE-READ-V1",
       state="verified",
       category="read",
       summary="Reads one table",
       profile="example-profile",
       wire_flow="read-arrow",
       operations=(
           "bigquery.tables.get",
           "grpc.bigquery-read.create-read-session",
           "grpc.bigquery-read.read-rows",
       ),
   )
   def test_example_read(...):
       ...
   ```

   All metadata is literal. The compiler uses Python's standard AST and
   rejects aliases, dynamic values, unknown operation IDs, duplicate claims,
   and annotations outside a selected test.
3. Add or narrow the scenario selector in `consumers.yaml`. Declare only the
   runner-visible ordering/cardinality expectations there. Do not repeat
   operation IDs or test evidence: `trafficSource: {kind: annotations}` derives
   both from the selected test.
4. Use `runner-evidence` only for a `load:` scenario that runs outside a
   selected test function. It must explain why and derive its operations from
   explicit ordering expectations.
5. Add a file in `cases/` only when a new executable/runtime/provenance
   combination is required. Reuse an existing scenario set when its wire
   contract did not change.
6. Generate and validate the projections:

   ```bash
   make integration-contract-generate
   make integration-contract-check
   make consumer-runner-test
   ```

   Commit the source test, handwritten manifest change, and regenerated output
   together. Real external-process runs belong in CI unless the task explicitly
   requires a local run.

## Generated Outputs

- `consumers.normalized.json`: runner and workflow matrix input.
- `capabilities.normalized.json`: exact test-local capability claims and their
  public operations.
- `../docs/en/consumer-compatibility.md` and Korean counterpart: runtime and
  provenance projection.
- `../docs/en/capability-coverage.md` and Korean counterpart: compact,
  test-backed capability projection.

Never repair these files by hand. `make integration-contract-check` fails when
they are stale.

## Harness Traffic

Integration harness setup and assertion requests are not consumer behavior.
Use the phase-aware harness helper for such calls so their request log entries
are marked `setup` or `assertion`; the runner excludes those responses from the
consumer wire claim. Do not add a harness-only operation to an annotation to
make a comparison pass.

## More Detail

- [Integration framework guide](../docs/en/framework.md) and its
  [Korean counterpart](../docs/ko/framework.md): runner lifecycle, manifests,
  generated pages, and failure diagnosis.
- [Product operation contract](../../../contract/README.md): adding or changing
  a REST/gRPC operation.
- [Contribution workflow](../../../CONTRIBUTING.md): issue, branch, review,
  generation, and CI policy.
