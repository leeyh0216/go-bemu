<!-- doc-id: adr-0003-public-api-edge -->
<!-- lang: en -->

[English](0003-public-api-edge.md) | [한국어](../../ko/adr/0003-public-api-edge.md)

# ADR-0003: Keep One Public API Edge

<!-- section: status -->
## Status

Accepted.

<!-- section: context -->
## Context

Public REST/gRPC callers, contract tests, and an optional console should observe
the same resources and errors. A private UI-only API would bypass compatibility
work and could report success while public callers fail. Public resource shapes are
defined by the [BigQuery REST
reference](https://cloud.google.com/bigquery/docs/reference/rest/v2).

<!-- section: decision -->
## Decision

All operational clients use `/bigquery/v2`. Emulator-only project lifecycle,
capability discovery, reset, and future seed/tracing operations use a clearly
namespaced `/emulator/v1` surface and must be disabled or protected when they can
change state. Console discovery may publish links but must not duplicate business
operations.

<!-- section: consequences -->
## Consequences

Public-edge tests benefit every client. Admin features cannot silently become
BigQuery extensions. Optional static UI serving is independent of API startup;
REST and gRPC must work when no UI assets exist.

<!-- section: alternatives -->
## Alternatives

A console-specific backend was rejected because it creates two contracts. Hiding
admin operations under BigQuery v2 paths was rejected because official clients
could accidentally discover non-BigQuery semantics.
