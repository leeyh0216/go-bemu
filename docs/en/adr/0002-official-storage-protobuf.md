<!-- doc-id: adr-0002-official-storage-protobuf -->
<!-- lang: en -->

[English](0002-official-storage-protobuf.md) | [한국어](../../ko/adr/0002-official-storage-protobuf.md)

# ADR-0002: Use Official Storage API Protobuf Types

<!-- section: status -->
## Status

Accepted.

<!-- section: context -->
## Context

Storage Read/Write compatibility depends on exact services, oneofs, wrapper
presence, field numbers, and streaming cardinality. Handwritten look-alike DTOs
can compile while producing incompatible wire bytes. The authoritative contract
is the official [BigQuery Storage v1 RPC
package](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1).

<!-- section: decision -->
## Decision

Register and implement the Google-generated `storagepb` server interfaces. Keep
protobuf values in the gRPC transport adapter and convert them to domain or
application inputs at that boundary. Golden tests must validate serialized
Arrow/Avro/Proto fields, not only Go object equality.

<!-- section: consequences -->
## Consequences

RPC method names and message evolution follow the upstream API package.
Registration and an application/protobuf slice can land before a production
adapter. Service health remains `NOT_SERVING` and runtime methods remain
`UNIMPLEMENTED` until a complete snapshot/encoder adapter is composed and tested
at the public edge. Official types do not provide BigQuery state semantics
automatically.

<!-- section: alternatives -->
## Alternatives

Copying `.proto` files or generated Go code was rejected because it creates a
second schema authority. A generic byte proxy was rejected because it cannot
safely enforce message-level invariants.
