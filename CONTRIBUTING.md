<!-- doc-id: contributing -->
<!-- lang: en -->

[English](CONTRIBUTING.md) | [한국어](CONTRIBUTING.ko.md)

# Contributing to go-bemu

<!-- section: scope -->
## Scope First

Before implementing a BigQuery behavior, identify the public REST method, gRPC
RPC/message, SQL rule, or wire format being reproduced. Link the authoritative
contract next to the implementation and documentation. Start from the
[BigQuery REST reference](https://cloud.google.com/bigquery/docs/reference/rest),
[Storage RPC reference](https://cloud.google.com/bigquery/docs/reference/storage/rpc),
or [GoogleSQL reference](https://cloud.google.com/bigquery/docs/reference/standard-sql).

<!-- section: architecture -->
## Architecture Rules

- Keep `internal/domain` and `internal/application` independent of HTTP, gRPC,
  Google DTOs, DuckDB, and object-store SDKs.
- Add external systems behind ports and include compile-time adapter assertions.
- Put transaction, offset, idempotency, and visibility invariants beside the
  state transition they constrain.
- Refuse unsupported semantics explicitly; do not silently coerce them into a
  different BigQuery type or lifecycle.
- Add runtime settings to the versioned YAML model, defaults, validation, typed
  `--set` path, and sample together. Preserve
  `defaults < file < environment < --set`; do not add a hidden flag or magic
  test value.

<!-- section: provenance -->
## Provenance Rules

- Use the official REST, Storage RPC, and GoogleSQL contracts as product sources.
- Keep executable-version observations and immutable upstream revisions inside
  `tests/integration`; do not make them product runtime contracts.
- Historical emulator comparisons must link the exact [goccy BigQuery emulator
  `v0.8.1` tag](https://github.com/goccy/bigquery-emulator/tree/v0.8.1); do not
  copy its source or make it an upstream build dependency.
- Link exact upstream tags or commits in golden fixtures and compatibility notes.
- Paraphrase contracts. Keep unavoidable quotations short and attributed.
- Never add GitHub source links to `master` or `main` for version-bound claims.
- State the removal condition for every compatibility workaround.

<!-- section: bilingual-docs -->
## Bilingual Documentation

Every maintainer-facing Markdown change must update both language files in the
same pull request:

- `README.md` and `README.ko.md`;
- `CONTRIBUTING.md` and `CONTRIBUTING.ko.md`;
- matching paths under `docs/en/**` and `docs/ko/**`.

Keep `doc-id` and ordered `section` markers identical. Keep primary-source URLs
identical even when link text is translated. Every page must retain its English
/ Korean language switch.

<!-- section: tests -->
## Tests

```bash
make ci-static
make ci-test-all
```

Add domain state tests, application tests with fake outbound adapters, public
REST/gRPC contract tests, and malformed/boundary cases. A test that only proves
DuckDB accepts SQL is not proof that BigQuery semantics are reproduced.

Run the narrow package or integration case while developing, then run the required
repository checks before committing. Generated manifests, documentation, and
evidence belong in the same commit as their source. CI rejects stale generated
artifacts.

<!-- section: implementation-workflow -->
## Implement, Test, And Generate

Use this sequence for every caller-visible change. It keeps the human-owned
decision small and makes the repository check the rest.

1. **Implement at the owning boundary.** Add the domain/application/adapter
   behavior and a focused test. A public behavior also needs a REST or gRPC
   transport test; do not use an engine-only test as public evidence.
2. **Describe a public operation once.** Add or change the operation in
   `contract/operations.yaml`, then annotate each declared Go test with a
   literal `contracttest.Operation(t, "operation.id")`. Run
   `make contract-generate`. It regenerates normalized contract JSON, runtime
   route/RPC specifications, and EN/KO API tables. Never edit those outputs by
   hand.
3. **Keep SQL as a resource.** Add SQL regression data under
   `internal/sqltest/testdata/cases/<case-id>/` as `dataset.json`, `case.json`,
   and `expected.json`. Put SQLite repository queries in
   `internal/adapters/sqlite/queries/*.sql`, then run `make sqlc-generate`.
   Generated sqlc source is reviewed with the SQL resource, not edited directly.
4. **Add caller behavior through the integration framework.** Put the test in
   `tests/integration/<family>`, attach literal operation annotations to the
   test function, and add a versioned case only when a runtime/provenance or
   runner contract changes. Run `make integration-contract-generate` to write
   the expanded execution matrix and integration compatibility pages.
5. **Run the smallest useful checks locally.** Run the changed package plus
   the matching generator check: `make contract-check`,
   `make integration-contract-check`, or `make sqlc-check`. Run
   `go test ./docs ./tests/integration/cipolicy` and `make ci-report-test` for
   documentation or CI-report changes. CI owns the expensive real-process
   matrices.
6. **Commit the source, test, and generated output together.** A stale
   generated file is a failure, not a reviewer cleanup task.

The [contribution framework](docs/en/maintainers/development-workflow.md)
explains the inputs, generated outputs, annotation rules, and failure modes in
detail. The [integration framework guide](tests/integration/docs/en/framework.md)
shows how to create a versioned external-process case.

<!-- section: evolution-pipeline -->
## Compatibility Evolution Pipeline

Introduce behavior in this order:

```text
operation contract -> domain use case -> port/adapter -> product test
```

The operation manifest identifies the public contract. Domain and application
code own semantics, while ports isolate engines and external systems. Product
tests prove the public boundary and unsupported cases. Exact executable
versions, artifacts, scenarios, and process evidence are added separately to
the [integration framework](tests/integration/docs/en/framework.md). Neither
path replaces comparison with the [BigQuery API
contract](https://cloud.google.com/bigquery/docs/reference).

<!-- section: issue-workflow -->
## Issue-Scoped Workflow

Every implementation or refactoring change starts from one open issue with a
Korean title, a concrete scope, acceptance criteria, exclusions, and
dependencies. Keep one issue in one branch and worktree, for example
`issue/32-contribution-process`. Do not combine unrelated issue work to make a
test pass.

1. Confirm the issue and its acceptance criteria before editing files.
2. Create an issue-owned branch and worktree from the latest validated base.
3. Record ownership before editing a shared file. Rebase after a dependency is
   committed instead of copying its uncommitted implementation.
4. Implement one coherent change and its tests, generated artifacts, and
   maintainer documentation.
5. Run focused checks followed by the required repository checks.
6. Review the exact staged diff. In a dirty worktree, never use `git add .` or
   `git add -A`; stage only the issue-owned paths or patch hunks.
7. Commit with the issue number, push promptly, and link the commit and CI run
   from the issue.
8. Close the issue only after the commit is on the target branch and the
   required `validation-complete` job has succeeded.

Use `refs #N` while any acceptance criterion remains. Use `closes #N` only when
the commit completes the issue. If implementation uncovers more work, update
the open issue or create a separately scoped Korean issue. Do not publish
temporary progress or planned behavior in user documentation.

Parallel agents follow the same ownership rules. An agent reports its exact
files and verification results, does not commit another issue's work, and stops
editing shared files before another issue rebases onto its commit.

<!-- section: change-description -->
## Change Description

State the operation or capability ID, authoritative source, observed difference,
chosen boundary, failure behavior, and remaining limitations. An integration
case also states the exact executable version and immutable artifact. A pull
request must identify one primary issue and explain dependency commits without
absorbing their scope.
