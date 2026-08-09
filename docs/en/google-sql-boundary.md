<!-- doc-id: google-sql-boundary -->
<!-- lang: en -->

[English](google-sql-boundary.md) | [한국어](../ko/google-sql-boundary.md)

# GoogleSQL Boundary and Support Guide

<!-- section: boundary -->
## Boundary, Not a Full Parser

BQEMU does not implement or claim a complete GoogleSQL parser. It has four
deliberately narrow paths:

1. a lexical scanner that admits one query/DML statement and rewrites supported
   backtick identifiers for DuckDB;
2. a token parser for one Spark connector `0.44.2` static-overwrite `MERGE`;
3. a source-pinned semantic parser for one Spark dynamic time-partition
   overwrite script; and
4. an application-owned parser for a small catalog-synchronized DDL subset.

The reference language remains the [GoogleSQL lexical
structure](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical)
and [query
syntax](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax).
Successful execution by DuckDB proves only the submitted case. It does not add
the surrounding grammar or GoogleSQL semantics to the compatibility contract.

<!-- section: admission -->
## Statement Admission

The generic path admits one leading statement class: `SELECT`, `WITH`, `VALUES`,
`INSERT`, `UPDATE`, `DELETE`, or `MERGE`. It allows one optional trailing
semicolon. A quote- and comment-aware scan rejects additional statements before
job or engine side effects.

`CREATE`, `ALTER`, `DROP`, and `TRUNCATE` are classified as catalog mutations.
They never fall through to generic DuckDB execution. The application DDL parser
accepts the exact subset below; `TRUNCATE` is unsupported. The sole supported
multi-statement form is the versioned dynamic time-partition overwrite profile.
Other [multi-statement
queries](https://cloud.google.com/bigquery/docs/multi-statement-queries) are
unsupported.

Leading whitespace and `--`, `#`, or `/* ... */` comments are recognized.
Malformed literals, block comments, or backtick identifiers fail before
translation. The scanner is not a substitute for grammar or semantic analysis.

<!-- section: translations -->
## Implemented Translations

| Input profile | Implemented transformation | What is not implied |
| --- | --- | --- |
| Generic admitted statement | Preserve strings and comments; convert backtick column, alias, and CTE identifiers to DuckDB double-quoted identifiers. | No function, operator, literal, coercion, null, collation, or evaluation-order translation. |
| Backtick relation after `FROM`, `JOIN`, `MERGE`, `INTO`, `UPDATE`, `USING`, or `TABLE`, including a same-level comma list | Map a three-part `project.dataset.table`, two-part `dataset.table`, or one-part default-dataset table to the encoded physical schema and quoted table. | Unquoted paths, decorators, wildcard tables, and every nested grammar position are not covered. |
| Spark `0.44.2` static overwrite | Parse the complete constant-false `MERGE`; translate `INSERT ROW` to DuckDB `INSERT BY NAME`; execute one atomic `MERGE INTO`. | No general BigQuery `MERGE` equivalence. |
| Spark `0.44.2` dynamic time-partition overwrite | Parse the complete connector script into a semantic operation; validate canonical source/destination schemas and partition metadata; delete touched partitions and insert source rows in one DuckDB transaction. | Script text is not translated, and arbitrary scripts are not admitted. |
| Supported DDL | Parse SQL into a semantic command and call `CatalogService`; the application coordinates physical storage and SQLite metadata. | DDL is never passed through as opaque DuckDB SQL. |

The static profile is tied to the exact
[`BigQueryClient.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java)
producer and the constant-false [BigQuery `MERGE`
contract](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement).
Its physical implementation uses [DuckDB `MERGE
INTO`](https://duckdb.org/docs/current/sql/statements/merge_into). A candidate
that resembles the profile but differs in tokens is rejected instead of falling
back to generic SQL.

The dynamic profile accepts `DATE_TRUNC` or `TIMESTAMP_TRUNC` over a canonical
top-level scalar partition field with `HOUR`, `DAY`, `MONTH`, or `YEAR`
granularity. Canonical metadata must identify a compatible `DATE`, `TIMESTAMP`,
or `DATETIME` field, and source fields must match destination type, mode, nested
names, and order. Range partition overwrite is unsupported.

<!-- section: ddl -->
## Semantic DDL Subset

The supported forms are intentionally smaller than the [GoogleSQL data
definition language](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language):

```sql
CREATE TABLE table_reference (
  column_name scalar_type [NOT NULL]
  [, ...]
)

DROP TABLE table_reference

ALTER TABLE table_reference ADD COLUMN column_name scalar_type [NOT NULL]

ALTER TABLE table_reference DROP COLUMN column_name

ALTER TABLE table_reference RENAME COLUMN old_name TO new_name

ALTER TABLE table_reference ALTER COLUMN column_name SET DATA TYPE scalar_type
```

`table_reference` may contain three parts, two parts using the request project,
or one part using the default project and dataset. Bare identifiers and
backtick-quoted identifiers are accepted. `COLUMN` is required in all four
supported `ALTER TABLE` forms.

DDL columns are top-level scalars. Accepted type names are
`BOOL`/`BOOLEAN`, `INT64`/`INTEGER`, `FLOAT64`/`FLOAT`, `NUMERIC`,
`BIGNUMERIC`, `STRING`, `BYTES`, `DATE`, `DATETIME`, `TIME`, `TIMESTAMP`, and
`JSON`. Decimal syntax is `NUMERIC(p[,s])` or `BIGNUMERIC(p[,s])`; shared Spark
precision limits and defaults apply. `GEOGRAPHY`, nested `STRUCT` declarations,
and repeated columns are unsupported in SQL DDL even though the storage port can
represent native structs and lists through REST-created schemas.

Every listed form executes through the catalog mutation boundary. With the
canonical SQLite repository, each `ALTER TABLE` change records durable intent,
applies one DuckDB transaction, and atomically publishes the canonical schema
with the terminal journal transition. Startup reconciles an interrupted change
from its recorded before/after schemas and physical fingerprints. An
incompatible `SET DATA TYPE` conversion fails without changing either schema.

Add and rename can use separately timed reverse-plan compensation when a state
repository has no canonical mutation journal. Drop and type change are rejected
in that composition. `CREATE TABLE` and `DROP TABLE` use the existing catalog
mutation ordering and are not covered by table-schema journal recovery.

The DDL tokenizer currently requires the supported command to end immediately;
trailing input, including a DDL semicolon, is rejected. This differs from the
generic single-statement scanner described above.

<!-- section: unsupported -->
## Current Unsupported Forms

The following list is explicit rather than exhaustive. Unknown syntax is not
implicitly supported.

| Area | Unsupported forms |
| --- | --- |
| DDL modifiers | `OR REPLACE`, `TEMP`/`TEMPORARY`, `IF [NOT] EXISTS`, and multiple actions |
| Table creation sources | `LIKE`, `COPY`, `CLONE`, `AS SELECT`, external tables, snapshots, and materialized views |
| Table properties | `PARTITION BY`, `CLUSTER BY`, `OPTIONS`, default expressions, constraints, collation, policy tags, and row access policies |
| Schema evolution | nested/repeated SQL declarations, table rename, column default/options, mode changes, and multiple actions |
| Query language | named/positional parameters, procedures, UDFs, views, dynamic SQL, transactions, variables, control flow, and general scripts |
| Relation syntax | unquoted project/dataset paths, table decorators, wildcard tables, connections, external sources, and remote functions |
| DML semantics | arbitrary BigQuery `MERGE` equivalence and connector dynamic range-partition overwrite |
| Functions and expressions | any GoogleSQL-only function or expression not already accepted with equivalent DuckDB behavior in a verified profile |

REST query-option fields that are known but unsupported are rejected at the
transport/application boundary rather than interpolated into SQL. DuckDB-only
syntax that happens to execute is outside the declared GoogleSQL contract.

<!-- section: failures -->
## Failure and Test Contract

Malformed supported syntax returns an invalid-input or invalid-query error.
Known missing syntax or semantics returns unsupported with a stable capability
identifier where one exists. Catalog conflicts, missing resources, and stale
canonical metadata retain their domain categories. Raw SQL and backend error
text do not belong in logs; diagnostics use statement class, model version,
byte count, token position where safe, and whole-query fingerprints.

Tests for any extension must include accepted syntax, neighboring rejected
syntax, quoted strings/comments, full token consumption, location/reference
analysis, canonical metadata validation, transaction rollback, and log
redaction. Connector profiles also require an exact producer version and drift
tests. General syntax must be implemented through a parser and semantic adapter,
not by broadening lexical replacement.
