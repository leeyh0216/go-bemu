<!-- doc-id: maintainers/index -->
<!-- lang: en -->

[English](index.md) | [한국어](../../ko/maintainers/index.md)

# Maintainer Documentation

This index is for implementing, reviewing, operating, and releasing BQEMU.
Public protocol decisions start from the [BigQuery REST API
reference](https://cloud.google.com/bigquery/docs/reference/rest).

<!-- section: architecture -->
## Architecture And Contracts

- [Public operation contract](../../../contract/README.md): how to add a REST
  or gRPC operation, attach test evidence, and regenerate runtime/documentation
  artifacts.
- [Architecture](../architecture.md): package boundaries, dependency direction,
  runtime composition, and persistence ownership.
- [BigQuery protocol internals](../bigquery-internals.md): protocol flows,
  types, and translation boundaries.
- [Architecture decisions](../adr/): decisions that constrain implementation.

<!-- section: implementation -->
## Implementation Guides

- [SQL regression cases](sql-regression.md): data-driven fixtures, typed
  expectations, focused execution, and required CI behavior.
- [Storage engine adapter guide](../engine-adapter-guide.md): capabilities,
  planning contracts, composition, and conformance tests.
- [Schema evolution and CDC](../schema-evolution-cdc.md): schema mutation and
  write-path design notes.
- [Configuration and operations](../operations.md): configuration precedence,
  health, shutdown, diagnostics, and container operation.

<!-- section: workflow -->
## Repository Workflow

- [Contribution framework](development-workflow.md): where to implement a
  behavior, annotate a public contract, regenerate artifacts, and run focused
  checks.
- [Maintainer guide](../maintainer-guide.md): local bootstrap, consumer-version
  onboarding, compatibility diagnosis, and release runbooks.
- [CI reports](ci-reporting.md): Actions summaries, downloadable JUnit HTML,
  structured evidence, and failure-only diagnostics.
- [Contributing](../../../CONTRIBUTING.md): issue-scoped changes, validation,
  commits, and review.
- [Compatibility manifest](../compatibility.md): the public contract that code,
  tests, and generated documentation must keep aligned.

Incomplete features remain in the compatibility manifest and linked issues.
They are not presented as user feature guides.
