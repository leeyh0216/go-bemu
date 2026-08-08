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
- Reference secret material by mounted file path. Never add secret bytes to the
  effective configuration, logs, fixtures, or examples.

<!-- section: provenance -->
## Provenance Rules

- Use primary sources. For connector-dependent behavior, link the exact
  [`0.44.2` tag](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2),
  not a mutable branch.
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

Run the narrow package or consumer test while developing, then run the required
repository checks before committing. Generated manifests, documentation, and
evidence belong in the same commit as their source. CI rejects stale generated
artifacts.

<!-- section: evolution-pipeline -->
## Compatibility Evolution Pipeline

Introduce behavior in this order:

```text
protocol profile -> adapter -> capability -> golden -> E2E
```

The profile pins a public client/version and observed wire contract. The adapter
contains the smallest translation. The capability names its exact support
level. A sanitized golden captures shape, and an end-to-end test proves the
released client reaches the public edge. Drift reports must include
`version`, `operation`, `shape`, `fingerprint`, and `fix_hint`. This does not
replace comparison with the [BigQuery API contract](https://cloud.google.com/bigquery/docs/reference).

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

State the supported connector/client version, capability ID, authoritative
source, observed difference, chosen boundary, failure behavior, and remaining
limitations. A pull request must identify one primary issue and explain any
dependency commits without absorbing their scope.
