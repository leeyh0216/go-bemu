<!-- doc-id: schema-evolution-cdc -->
<!-- lang: en -->

[English](schema-evolution-cdc.md) | [한국어](../ko/schema-evolution-cdc.md)

# Schema Evolution and Change Data Capture

<!-- section: schema-contract -->
## Schema Contract

BigQuery permits specific online schema changes; it does not define arbitrary
replacement of an existing schema. Adding a top-level or nested field requires
the new field to be `NULLABLE` or `REPEATED`, and existing field identity,
order, type, and mode must be preserved. The primary contract is [Managing table
schemas](https://cloud.google.com/bigquery/docs/managing-table-schemas), including
the [nested-field update procedure](https://cloud.google.com/bigquery/docs/managing-table-schemas#add_a_nested_column_to_a_record_column).

`go-bemu` verifies capability `CAP-SCHEMA-ADDITIVE-V1` at the public REST
boundary. It recursively accepts end-position `NULLABLE` or `REPEATED` fields at
the top level, inside records, and inside repeated records. It rejects removal,
rename, reorder, type change, mode change, and new `REQUIRED` fields. Existing
rows receive null for new nullable fields. This deliberately narrow capability
does not imply every BigQuery widening or relaxation is implemented.

<!-- section: rest-schema-updates -->
## REST Schema Updates

BigQuery recommends `tables.patch` for partial updates; `tables.update` replaces
the resource. Their official request semantics are
[`tables.patch`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/patch)
and [`tables.update`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/update).
An adapter must distinguish an omitted property from explicit JSON `null` and
must apply physical DDL before publishing canonical metadata, with compensation
or one transaction boundary on failure.

`CAP-REST-METADATA-PATCH-V1` verifies dataset/table PATCH and PUT for labels,
descriptions, expirations, and default expirations, including `If-Match` failure
as HTTP 412. `CAP-SCHEMA-ADDITIVE-V1` verifies the public REST boundary against
a real emulator process. The DuckDB adapter applies all physical additions in
an explicit transaction and tests existing-row null semantics.

SQLite persists canonical metadata across restarts while DuckDB stores the
physical table. Cross-store publication is not yet one durable atomic operation,
and direct DuckDB changes are never canonical BigQuery metadata changes. DDL
recovery, query-job `schemaUpdateOptions`, and Storage Write schema notification
remain separate gaps.

<!-- section: load-schema-updates -->
## Load and Query Job Evolution

Load and query jobs can request controlled schema updates with
`schemaUpdateOptions`, such as allowing field addition or field relaxation. The
wire field and write-disposition interaction are defined in
[`JobConfigurationLoad`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad)
and [`JobConfigurationQuery`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery).
Those paths require staging, validation against the destination's current
schema, atomic data plus metadata publication, and a durable job error. They are
not implemented merely by accepting the JSON option.

The Parquet load path supports `ALLOW_FIELD_ADDITION` and
`ALLOW_FIELD_RELAXATION` for an existing `WRITE_APPEND` destination. The update
may come from an explicit request schema or Parquet inference. Only end-position
NULLABLE/REPEATED additions and REQUIRED-to-NULLABLE relaxations are accepted,
including nested fields. A typed staging table is validated first, then the
physical schema update and row append commit in one DuckDB transaction. The
canonical SQLite schema is published after that commit; interruption recovery
between the two stores is still a gap. Query-job schema updates and the separate
`autodetect` flag remain unsupported.

<!-- section: write-schema-updates -->
## Storage Write Schema Changes

`AppendRows` supplies a writer schema on the first request for a connection and
may later receive an `updated_schema` in the response when the destination
changes. Google documents this as [schema update
detection](https://cloud.google.com/bigquery/docs/write-api#schema_update_detection),
and the canonical messages are in the
[`AppendRows` RPC](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.AppendRows).

The current public Partial Write service persists writer-schema fingerprints,
exact offsets, append receipts, and commit phases in SQLite and reconciles
incomplete operations at startup. It does not track a durable destination schema
version or emit `updated_schema`. Incompatible evolution and schema notification
therefore remain unsupported.

<!-- section: cdc-contract -->
## BigQuery CDC Contract

BigQuery CDC is a Storage Write ingestion mode, not SQL `MERGE` rewriting. The
table needs declared primary keys, and each row carries `_CHANGE_TYPE` as
`UPSERT` or `DELETE`; `_CHANGE_SEQUENCE_NUMBER` optionally orders competing
changes. BigQuery applies changes in the background, so mutation visibility and
`max_staleness` are part of the observable contract. The authoritative rules,
including pseudocolumn spelling, sequence-number format, ordering, and delete
payload requirements, are in [BigQuery change data
capture](https://cloud.google.com/bigquery/docs/change-data-capture).

The emulator supports a deliberately narrow CDC subset on the default stream:
declared primary-key metadata, paired CDC pseudocolumns in a ProtoRows schema,
ordered synchronous UPSERT/DELETE application, and a durable per-key sequence
ledger. Pseudocolumns are never projected into user-table reads. The local
diagnostic port exposes its latest applied time and key count, but is not an
`INFORMATION_SCHEMA` or `upsert_stream_apply_watermark` parity claim.

This remains **partial**, rather than a general CDC implementation. CDC is
rejected on PENDING streams and when ordinary rows are mixed on the same
connection. The documented one-to-four hexadecimal sequence shape is validated;
an equal-prefix pair with different section counts fails explicitly because the
public contract does not define its precedence. There is no asynchronous apply
queue, `max_staleness` model, production CDC metrics, or released-client recovery
and parallel-writer E2E evidence. Treating an `UPSERT` as an immediate DuckDB
`INSERT OR REPLACE` would hide ordering, duplicate, delete, and visibility bugs.

<!-- section: stream-ledger -->
## Required Stream and CDC Ledgers

A minimal correct design separates append acceptance from CDC application:

| Ledger | Required state |
| --- | --- |
| write stream | stream type/state, table, schema version/fingerprint, next offset, accepted payload digest, finalized row count |
| CDC mutation | primary-key digest, change type, parsed sequence tuple, append identity, receive time, apply state/error |
| table apply | watermark, last successful apply, outstanding mutation count, staleness policy |

`BatchCommitWriteStreams` must atomically publish pending-stream rows before CDC
application ordering is evaluated. The Write API batch contract is documented
in [batch loading with pending
streams](https://cloud.google.com/bigquery/docs/write-api-batch). These ledgers
belong behind ports; DuckDB tables are one adapter, not the state-machine API.

<!-- section: evolution-pipeline -->
## Modular Evolution Pipeline

Every schema/CDC behavior advances through:

```text
operation contract -> application transition -> port/adapter -> product test
```

The operation contract defines the public behavior. The application transition
owns validation and state changes. Ports keep storage-specific work behind an
adapter, and product tests exercise the public REST/gRPC boundary. Target-specific
cases, artifact locks, and released-runtime evidence belong to the
[integration test framework](../../tests/integration/docs/en/framework.md).

<!-- section: drift-report -->
## Drift Report

Every mismatch must carry these stable fields:

```text
contract_version=<manifest or schema version>
operation=<REST method, RPC, or SQL template>
shape=<JSON/protobuf/schema summary>
fingerprint=<deterministic digest>
fix_hint=<next actionable boundary>
```

Fingerprints cover canonical schemas or payload structure and may accompany raw
diagnostic context. Contract version and operation identify the affected public
boundary; shape and fingerprint localize drift; `fix_hint` names the product
layer or integration case that must change.

<!-- section: test-gates -->
## Promotion Test Gates

Verified schema tests cover top-level, nested, and repeated-record additions,
populated-table nulls, rejected destructive changes, transactional physical
failure, stale ETags, and public-process behavior. Durable cross-store recovery,
query-job schema updates, and Storage Write schema notification remain gaps.
Load-job addition and relaxation are covered at the domain, plan, DuckDB, and
REST boundaries. CDC coverage proves ordered UPSERT/DELETE, missing-key and
invalid-pseudocolumn rejection, reconnect/replay offsets, and a retained
per-key ledger. It still needs multiple streams, commit visibility, apply lag,
failure recovery, and released-client recovery/parallel-writer E2E. Promotion must still compare result types with [BigQuery data
types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types).
