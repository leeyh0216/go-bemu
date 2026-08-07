<!-- doc-id: dynamic-partition-overwrite -->
<!-- lang: en -->

[English](dynamic-partition-overwrite.md) | [한국어](../ko/dynamic-partition-overwrite.md)

# Dynamic Partition Overwrite

<!-- section: upstream-contract -->
## Upstream Contract

The supported candidate is the exact script emitted by Spark connector `0.44.2`
in [`BigQueryUtil.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryUtil.java#L796-L870).
It declares an array of distinct source partitions with `IGNORE NULLS`, then
uses `MERGE ... ON FALSE` to delete rows in touched destination partitions and
insert every source row. The service rules come from [multi-statement
queries](https://cloud.google.com/bigquery/docs/multi-statement-queries),
[`MERGE`](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement),
and [DML transaction
semantics](https://cloud.google.com/bigquery/docs/data-manipulation-language#multi-statement_transactions).

This is a versioned semantic adapter, not a general script translator. A token,
alias, field list, relation, partition function, or trailing-statement drift
fails closed with model, capability or gap, token index, expected shape, query
digest, and fix hint. SQL text is never logged.

<!-- section: execution-contract -->
## Current Execution Contract

The application resolves both destination and source canonical tables while one
context-aware resource-mutation gate is held. The gate remains held through
schema validation and the DuckDB transaction, preventing delete/recreate races.
A canceled waiter exits without consuming the permit, and later mutations can
reuse it.

Before transaction begin, every selected source field must match the destination
field's canonical BigQuery type, mode, nested names, and nested order. Documented
aliases (`BOOL`/`BOOLEAN`, `INTEGER`/`INT64`, `FLOAT`/`FLOAT64`, and
`STRUCT`/`RECORD`) normalize to one type. Other DuckDB implicit casts are
rejected as `invalidQuery`; missing canonical resources remain `notFound`.
Partition fields support `DATE`, `TIMESTAMP`, and `DATETIME` with the connector's
matching truncation function and valid granularity. Type definitions are in
[BigQuery data types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types).

The adapter executes delete and insert in one explicit DuckDB transaction. A
source NULL partition is excluded only from the touched-partition set because
of `IGNORE NULLS`; the source row itself is still inserted. Logs record
begin/delete/insert/commit/rollback pre/post boundaries, exact transaction
state, affected-row counts, durations, schema fingerprints, and opaque resource
fingerprints. They never record raw SQL, rows, project, dataset, table, or field
values.

<!-- section: rest-contract -->
## REST Job Contract

An accepted operation crosses the normal `jobs.insert` and `jobs.get` lifecycle.
Its query statistic reports `statementType=SCRIPT`, and the available top-level
and query-level affected-row totals are populated. Error reasons follow the
documented [BigQuery error
table](https://cloud.google.com/bigquery/docs/error-messages): schema/query
violations are `invalidQuery`, missing resources are `notFound`, deadlines are
`timeout`, cancellation is `stopped`, and backend transaction failures are
`jobBackendError`.

<!-- section: stable-gaps -->
## Stable Gaps

BigQuery scripts expose child jobs and script-specific statistics. Child-job
enumeration, `scriptStatistics`, and per-statement `dmlStats` are not implemented;
their wire definitions remain governed by
[`JobStatistics2`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobStatistics2).
Dynamic range-partition overwrite is also a registered unsupported gap.

No released Spark connector JAR has yet proven direct-write or indirect-write
dynamic overwrite through the public emulator endpoints. Unit tests and raw REST
E2E prove the semantic adapter, atomicity, NULL behavior, types, drift rejection,
and job reasons, but they are not connector evidence. Therefore the connector
profile, golden fixture, compatibility matrix, and both direct/indirect E2E rows
must remain gaps until a test downloads the released artifact, records its URL,
version, size, and SHA-256, and captures sanitized endpoint evidence.

<!-- section: promotion-gates -->
## Promotion Gates

For a later connector release, add a new source-pinned parser model instead of
loosening the `0.44.2` model. Promotion requires positive and negative token
fixtures, destination/source schema drift cases, DATE/TIMESTAMP/DATETIME and NULL
cases, cancellation and lock reuse, rollback evidence, opaque-log assertions,
raw REST E2E, and released-JAR direct plus indirect E2E. Only then may the
versioned profile, golden, and compatibility matrix move from gap to verified.
