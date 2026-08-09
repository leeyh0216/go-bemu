<!-- doc-id: maintainers/development-workflow -->
<!-- lang: en -->

[English](development-workflow.md) | [한국어](../../ko/maintainers/development-workflow.md)

# Contribution Framework

This is the detailed companion to [CONTRIBUTING](../../../CONTRIBUTING.md).
It is the framework for taking one behavior from implementation through a
public contract, generated artifacts, and CI evidence. The goal is one human
decision at the owning boundary and deterministic generated evidence everywhere
else. Public behavior is still defined against the [BigQuery REST API
reference](https://cloud.google.com/bigquery/docs/reference/rest).

<!-- section: choose-boundary -->
## 1. Choose The Owning Boundary

| Change | Start here | Do not start here |
| --- | --- | --- |
| Domain rule or engine behavior | `internal/domain`, `internal/application`, or the owning adapter | REST/gRPC handler |
| Public REST or gRPC method | Owning application path, transport test, then `contract/operations.yaml` | Generated route specs or API table |
| GoogleSQL behavior | `internal/querylang`, GoogleSQL gateway, and engine visitor | Raw SQL string rewriting |
| Durable state query | `internal/adapters/sqlite/queries/*.sql` and sqlc configuration | Hand-built SQL in a repository method |
| End-to-end caller behavior | `tests/integration/<family>` | Product runtime packages |

Make the focused implementation and its owning unit/application test first. A
public transport test is required when a caller can observe the behavior.

<!-- section: public-contract -->
## 2. Record A Public Operation

For a REST or gRPC behavior, add or update one entry in
[`contract/operations.yaml`](../../../contract/operations.yaml), then put a
literal `contracttest.Operation(t, "...")` annotation in each declared Go
transport/application test. The detailed rules and example are in
[`contract/README.md`](../../../contract/README.md).

Run:

```bash
make contract-generate
make contract-check
```

`contract-generate` writes these deterministic outputs. They are reviewed but
never hand-edited:

| Generated file | Why it exists |
| --- | --- |
| `contract/operations.normalized.json` | Canonical machine-readable public surface |
| `internal/contractspec/operations_gen.go` | Runtime REST/RPC route specifications |
| `docs/en/api-rpc-compatibility.md` | Exact English API/RPC inventory |
| `docs/ko/api-rpc-compatibility.md` | Exact Korean API/RPC inventory |

`contract-check` also rejects an unannotated declared test, an annotation for
an unknown operation, route/descriptor drift, and stale generated output.

<!-- section: integration -->
## 3. Add An Integration Behavior

Keep external callers under `tests/integration`. Add the test where its runtime
belongs and attach literal operation annotations to the test function. The
integration compiler verifies that every declared test/evidence link and every
public operation ID is real.

The runner/runtime/version/provenance declaration is the handwritten source in
`tests/integration/contract/consumers.yaml` plus one versioned case YAML. Run:

```bash
make integration-contract-generate
make integration-contract-check
```

The command generates the normalized execution matrix and bilingual integration
compatibility table. Do not edit either generated output.

Today, scenario selectors and order/cardinality expectations remain explicit
because they are behavioral assertions, not facts that can be inferred from an
annotation. The duplicated scenario operation/evidence lists are compiler
checked; the follow-up annotation-derived projection will remove those lists
where the test source can provide them. Until then, a test or manifest change
that does not agree fails `integration-contract-check`.

<!-- section: sql-state -->
## 4. Add SQL And State Resources

SQL regression cases use `dataset.json`, `case.json`, and `expected.json` under
`internal/sqltest/testdata/cases/<case-id>/`; the harness executes the same
GoogleSQL gateway and engine path as public jobs. Add a focused case instead of
embedding a large query result in a Go test.

SQLite repository queries live in `internal/adapters/sqlite/queries/*.sql`.
After changing them, run:

```bash
make sqlc-generate
make sqlc-check
```

The generated Go adapter is not hand-edited. `sqlc-check` verifies that the
checked-in generated source matches the SQL resources.

<!-- section: verify -->
## 5. Verify In The Right Order

Keep local work focused, then let CI execute expensive end-to-end matrices.

```bash
# The package that changed, for example:
go test ./contract
go test ./internal/transport/rest

# Required when the corresponding source changed:
make contract-check
make integration-contract-check
make sqlc-check

# Documentation or CI-report changes:
go test ./docs ./tests/integration/cipolicy
make ci-report-test
```

Commit source, test, and generated artifacts together. CI renders Job Summary
tables and downloadable JUnit HTML only after the committed runner produces its
JUnit output; see [CI reports](ci-reporting.md).

<!-- section: review -->
## Review Checklist

1. Does the change have one owning domain/application/adapter boundary?
2. Does each caller-visible behavior have a public operation entry and a
   literal test annotation?
3. Were generated files regenerated rather than edited by hand?
4. Does a focused test prove the new behavior and a compiler check prove the
   metadata is synchronized?
5. Are the user compatibility page and configuration reference still accurate?
