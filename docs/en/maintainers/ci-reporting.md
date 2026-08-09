<!-- doc-id: maintainers/ci-reporting -->
<!-- lang: en -->

[English](ci-reporting.md) | [한국어](../../ko/maintainers/ci-reporting.md)

# CI Test Reports

CI presents a decision first and detailed evidence second. The workflow uses
the official [GitHub Actions job-summary contract](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-commands#adding-a-job-summary)
and keeps the public API implementation reference at the [BigQuery REST API
reference](https://cloud.google.com/bigquery/docs/reference/rest).

<!-- section: decision -->
## What To Read First

Every integration job writes a short table into its GitHub Actions Summary:
test count, passed, failed, errors, skipped, and any missing JUnit producer.
The release-blocking aggregate job writes a second table with every required
job result and explicitly states whether publication is blocked.

Read those summaries before opening an artifact. A red job with no JUnit output
is reported as missing rather than presented as a passing empty report.

<!-- section: artifacts -->
## Artifact Decision

| Artifact | When it exists | Contents | Purpose |
| --- | --- | --- | --- |
| `test-report-*` | Every integration execution | `index.html`, `summary.md`, JUnit XML, structured evidence and mismatch data | Download `index.html` for a readable suite and failure report. |
| `failure-diagnostics-*` | Only a failed execution | Process, emulator, JVM, or load diagnostics selected by that workflow | Diagnose why a known failed test failed. |
| No report artifact | Static or Go-only job without a JUnit producer | The job outcome and aggregate summary remain in Actions | Do not create an empty XML artifact that looks like detailed test coverage. |

The renderer consumes only declared JUnit paths. It never recursively uploads
an entire artifact directory. Adding a new test runner therefore requires an
explicit decision: produce JUnit and join `test-report-*`, or document why the
job has only an Actions outcome.

<!-- section: inspect -->
## Inspect A Failure

1. Open the failed job's Summary and identify the failing suite or missing
   JUnit producer.
2. Download the matching `test-report-*` artifact and open `index.html` in a
   browser. It includes failure and error text from the JUnit source.
3. Download `failure-diagnostics-*` only when the report needs process or
   service detail.
4. Fix the smallest owning boundary, then add or update the runner's JUnit
   case so the same signal appears in future summaries.

The reporting script has standard-library regression tests. It is intentionally
separate from protocol and integration comparison logic.
