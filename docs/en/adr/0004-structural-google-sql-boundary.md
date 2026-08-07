<!-- doc-id: adr-0004-structural-google-sql-boundary -->
<!-- lang: en -->

[English](0004-structural-google-sql-boundary.md) | [한국어](../../ko/adr/0004-structural-google-sql-boundary.md)

# ADR-0004: Require a Structural GoogleSQL Boundary

<!-- section: status -->
## Status

Accepted as a constraint; parser/semantic implementation is pending.

<!-- section: context -->
## Context

GoogleSQL uses backticks for identifiers in many syntactic positions and `MERGE`
has ordered clauses, cardinality constraints, and atomic effects. The contracts
are the [GoogleSQL lexical
structure](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical)
and [`MERGE` syntax](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement).
A regex that sees only text cannot distinguish table references from columns,
comments, strings, decorators, or scripts.

<!-- section: decision -->
## Decision

Treat the current backtick mapper as a narrow bootstrap implementation. New SQL
compatibility must use a parser/AST or an exact versioned connector-template
recognizer. Unknown SQL passes to a declared engine subset or fails explicitly;
it is never broadly rewritten by a permissive regex.

<!-- section: consequences -->
## Consequences

General GoogleSQL remains unsupported until a semantic adapter exists. Exact
connector template rules record version, fingerprint, authoritative source,
negative cases, and removal condition. SQL DDL must not mutate physical catalog
without synchronizing canonical metadata.

<!-- section: alternatives -->
## Alternatives

Growing a list of regex replacements was rejected because interactions are not
composable and failures can silently target the wrong table or column.
