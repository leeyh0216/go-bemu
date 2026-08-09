<!-- doc-id: integration-docs-index -->
<!-- lang: en -->

[English](index.md) | [한국어](../ko/index.md)

# Integration Test Guides

These guides describe version-pinned processes exercised by CI against the
public [BigQuery API](https://cloud.google.com/bigquery/docs/reference/rest).
They are integration-test assets, not product runtime dependencies.

<!-- section: guides -->
## Versioned Guides

- [Python BigQuery client](clients/python-bigquery.md)
- [`bq` CLI](clients/bq-cli.md)
- [PySpark and Scala Spark](clients/spark-bigquery-connector.md)

<!-- section: evidence -->
## Generated Evidence

- [Integration test framework](framework.md): manifest structure, runner
  contracts, CI lanes, and the procedure for adding a case.
- [Consumer compatibility](consumer-compatibility.md): exact versions,
  immutable artifacts, scenario IDs, and selectors used by CI.
- [Capability coverage](capability-coverage.md): compact, generated claims
  backed by test-local annotations.
