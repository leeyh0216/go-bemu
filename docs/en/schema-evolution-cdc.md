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
as HTTP 412. `CAP-SCHEMA-ADDITIVE-V1` verifies raw REST and the official [Python
client `3.43.0`](https://pypi.org/project/google-cloud-bigquery/3.43.0/) against a
real emulator process. The DuckDB adapter applies all physical additions in an
explicit transaction and tests existing-row null semantics.

Canonical metadata is still process-local. A process crash cannot atomically
coordinate the in-memory catalog with the DuckDB file, and direct DuckDB changes
are never canonical BigQuery metadata changes. DDL conversions, load/query
`schemaUpdateOptions`, and Storage Write schema notification remain separate
gaps.

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

The current opt-in Parquet load slice validates casts against an existing table
and applies a write disposition atomically, but it rejects
`schemaUpdateOptions`, destination creation, and autodetect. Load-driven schema
evolution therefore remains unsupported.

<!-- section: write-schema-updates -->
## Storage Write Schema Changes

`AppendRows` supplies a writer schema on the first request for a connection and
may later receive an `updated_schema` in the response when the destination
changes. Google documents this as [schema update
detection](https://cloud.google.com/bigquery/docs/write-api#schema_update_detection),
and the canonical messages are in the
[`AppendRows` RPC](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.AppendRows).

The current public Partial Write service retains a writer-schema fingerprint for
ProtoRows appends and preserves exact PENDING-stream offsets during the live
process. It does not track a durable destination schema version or emit
`updated_schema`. Incompatible evolution, schema notification, and offset
recovery across restart therefore remain unsupported.

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

The emulator currently has no primary-key constraint metadata, CDC mutation
queue, apply watermark, background apply job, staleness model, or CDC metrics.
Therefore CDC is **unsupported**. Treating an `UPSERT` as an immediate DuckDB
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

<!-- section: flink-profile -->
## Flink Connector 1.2.0 Client Profile

The official GoogleCloudDataproc Flink connector `1.2.0` is a separate client
profile from [Spark connector `0.44.2`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2);
their task/checkpoint models and Storage RPC sequences are not interchangeable.
The planned profile must resolve
`com.google.cloud.flink:flink-1.17-connector-bigquery:1.2.0` from the [released
Maven directory](https://repo1.maven.org/maven2/com/google/cloud/flink/flink-1.17-connector-bigquery/1.2.0/),
record artifact URL, size, and SHA-256, and never clone or build upstream.
The exact version is proven by the tagged
[`pom.xml`](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/1.2.0/flink-1.17-connector-bigquery/pom.xml).

Profile operations must stay explicit: bounded source reads, at-least-once writes on
the default stream, checkpointed buffered-stream writes, schema mismatch, and
CDC UPSERT/DELETE. Connector code adds CDC pseudocolumns in
[`BigQueryCdcSchemaProvider.java`](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/1.2.0/flink-1.17-connector-bigquery/flink-connector-bigquery/src/main/java/com/google/cloud/flink/bigquery/sink/serializer/BigQueryCdcSchemaProvider.java)
and composes checkpointed writers in
[`BigQueryExactlyOnceSink.java`](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/1.2.0/flink-1.17-connector-bigquery/flink-connector-bigquery/src/main/java/com/google/cloud/flink/bigquery/sink/BigQueryExactlyOnceSink.java).
These source links describe client expectations, not emulator support. Public
Storage Read and the ProtoRows PENDING/default Write subset are Partial, but no
Flink `1.2.0` E2E has promoted an operation. Buffered/checkpointed Write, schema
notification, and CDC are explicit capability gaps.

<!-- section: evolution-pipeline -->
## Modular Evolution Pipeline

Every schema/CDC behavior advances through:

```text
protocol profile -> adapter -> capability -> golden -> E2E
```

The profile identifies client and protocol version. The adapter converts only a
known shape. The capability records supported, partial, or unsupported status.
The golden fixture includes the raw diagnostic context for positive and negative shapes.
E2E runs through the public REST/gRPC endpoint with the released client. A stage
cannot be skipped because a DuckDB unit test passed.

<!-- section: drift-report -->
## Drift Report

Every mismatch must carry these stable fields:

```text
version=<client/protocol version>
operation=<REST method, RPC, or SQL template>
shape=<JSON/protobuf/schema summary>
fingerprint=<deterministic digest>
fix_hint=<next actionable boundary>
```

Fingerprints cover canonical schemas or payload structure and may accompany raw
diagnostic context. Version and operation select the profile;
shape and fingerprint localize drift; `fix_hint` names the adapter, capability,
golden, or E2E step that must change.

<!-- section: test-gates -->
## Promotion Test Gates

Verified schema tests cover top-level, nested, and repeated-record additions,
populated-table nulls, rejected destructive changes, transactional physical
failure, stale ETags, and Python-client E2E. Restart reconciliation, DDL,
load/query schema-update paths, and Storage Write schema notification remain
gaps. CDC later requires out-of-order and
duplicate sequence values, UPSERT/DELETE, missing key, invalid pseudocolumn,
reconnect/replay offsets, multiple streams, commit visibility, apply lag, and
failure recovery. Promotion must still compare result types with [BigQuery data
types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types).
