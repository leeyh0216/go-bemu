<!-- doc-id: maintainers/sql-regression -->
<!-- lang: en -->

[English](sql-regression.md) | [한국어](../../ko/maintainers/sql-regression.md)

# SQL Regression Cases

The data-driven SQL suite detects behavioral drift across the production
GoogleSQL analysis and engine execution boundary. Each case declares its
catalog fixture, statement, expected result, and optional post-execution table
state. The runner compares canonical BigQuery types and values rather than
backend-native representations.

Use the [GoogleSQL data type
reference](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)
when adding fixture fields or expected values.

<!-- section: layout -->
## Case Layout

Add one directory under `internal/sqltest/testdata/cases`. A case contains
exactly four files; unknown files and unknown JSON fields fail validation.

| File | Contract |
|---|---|
| `case.json` | Schema version, stable case ID, default project and dataset, and result row order. |
| `dataset.json` | Initial projects, datasets, tables, recursive schemas, and rows. |
| `query.sql` | Portable GoogleSQL executed through the production gateway and semantic statement executor. |
| `expected.json` | Expected rows, affected-row count, stable error, and optional table postconditions. |

Use `rowOrder: ordered` only when the statement defines a deterministic order.
Use `unordered` when row membership matters but order does not, and `none` for
outcomes without rows.

A fixture table may declare either `timePartitioning` (`type`, `field`, and
optional `expirationMs`) or `rangePartitioning` (`field` plus `range.start`,
`range.end`, and `range.interval`). The loader rejects missing or incompatible
partition fields and does not allow both partitioning modes on one table.

<!-- section: values -->
## Typed Values

Expected schemas use canonical field types, modes, precision, scale, rounding
mode, and recursive fields. Encode values as follows:

| Type | JSON representation |
|---|---|
| `INT64`, `FLOAT64`, `BOOL`, `STRING` | JSON scalar |
| `NUMERIC`, `BIGNUMERIC` | Decimal string, preserving exact value |
| `BYTES` | Base64 string |
| `DATE`, `DATETIME`, `TIME`, `TIMESTAMP` | Canonical ISO text |
| `RECORD` | Object keyed by child field name |
| `REPEATED` | JSON array of the element representation |

For `kind: rows`, declare both `schema` and `rows`. For `kind: affected`,
declare `affectedRows`. For `kind: error`, declare `error.phase` as `analyze` or
`execute` and use a stable error code. Add `tables` whenever a mutation or
failure must prove the resulting catalog and row state.

<!-- section: authoring -->
## Authoring Rules

Cases must remain independent of a particular client, CLI, connector, version,
or storage engine. Do not encode producer templates, backend SQL syntax, or
runtime-specific setup. Unsupported syntax should be represented as an
analysis or execution expectation only when that behavior is an intentional
product contract.

Prefer one behavior per case. Give every mutation case a table postcondition,
and give every fail-closed case a postcondition proving that no unintended
mutation occurred. The comparator reports the first schema, row, error, or
table-state difference with its field path.

<!-- section: commands -->
## Running Cases

Run a single case while implementing it:

```sh
go test ./internal/sqltest -run '^TestGoogleSQLRegressionCases/projection-filter$' -count=1
```

Run the complete SQL regression lane before submitting changes to the SQL
boundary:

```sh
make ci-test-sql-regression
```

CI runs this as an independent required job. A failed or skipped SQL regression
job prevents image publication.
