<!-- doc-id: adr-0004-structural-google-sql-boundary -->
<!-- lang: en -->

[English](0004-structural-google-sql-boundary.md) | [한국어](../../ko/adr/0004-structural-google-sql-boundary.md)

# ADR-0004: Require a Structural GoogleSQL Boundary

<!-- section: status -->
## Status

Accepted and implemented.

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

Every public query and job SQL request enters the official GoogleSQL parser
exactly once. The adapter copies that parser tree into an immutable BQEMU AST,
binds relations and expression types against canonical metadata, and returns an
engine-neutral semantic statement. Engine adapters visit that statement to
produce private SQL and bind arguments.

The engine never receives user SQL or a foreign parser handle. There is no
keyword pre-classifier, version-specific template parser, or raw engine-SQL
fallback. A syntax, semantic, or lowering node outside the supported subset
fails before an engine side effect.

<!-- section: consequences -->
## Consequences

GoogleSQL support remains an explicit AST subset, but `SELECT`, DML, supported
scripts, and catalog DDL share one gateway and statement root. Catalog DDL uses
typed mutation plans and must synchronize canonical metadata with the physical
catalog. Adding a statement, expression, function, or type requires mapper,
semantic-binding, engine-lowering, and negative fail-closed tests.

<!-- section: alternatives -->
## Alternatives

Growing a list of regex replacements was rejected because interactions are not
composable and failures can silently target the wrong table or column.
