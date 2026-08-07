<!-- doc-id: schema-evolution-cdc -->
<!-- lang: ko -->

[English](../en/schema-evolution-cdc.md) | [한국어](schema-evolution-cdc.md)

# Schema Evolution과 Change Data Capture

<!-- section: schema-contract -->
## Schema 계약

BigQuery는 특정 online schema 변경을 허용하지만 기존 schema의 임의 교체를
정의하지 않는다. 최상위 또는 nested field 추가 시 새 field는 `NULLABLE` 또는
`REPEATED`여야 하고 기존 field의 identity, 순서, type, mode를 보존해야 한다.
Primary contract는 [Table schema
관리](https://cloud.google.com/bigquery/docs/managing-table-schemas)와 그 안의
[nested field update 절차](https://cloud.google.com/bigquery/docs/managing-table-schemas#add_a_nested_column_to_a_record_column)다.

`go-bemu`는 public REST boundary에서 `CAP-SCHEMA-ADDITIVE-V1`을 verified로
검증한다. Top level, record 내부, repeated record 내부의 끝 위치에 `NULLABLE`
또는 `REPEATED` field를 재귀적으로 허용한다. Removal, rename, reorder, type
변경, mode 변경, 새 `REQUIRED` field는 거부한다. 기존 row의 새 nullable field는
null이 된다. 이 의도적으로 좁은 capability는 BigQuery가 지원하는 모든 widening
또는 relaxation이 구현되었다는 뜻이 아니다.

<!-- section: rest-schema-updates -->
## REST Schema Update

BigQuery는 partial update에 `tables.patch` 사용을 권장하며 `tables.update`는
resource를 교체한다. 공식 request 의미는
[`tables.patch`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/patch)와
[`tables.update`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/update)에
정의된다. Adapter는 omitted property와 명시적 JSON `null`을 구분해야 하고,
canonical metadata를 공개하기 전에 physical DDL을 적용해야 하며 실패 시 보상
또는 하나의 transaction 경계가 필요하다.

`CAP-REST-METADATA-PATCH-V1`은 label, description, expiration, default
expiration에 대한 dataset/table PATCH와 PUT을 verified로 검증하며 `If-Match`
실패는 HTTP 412다. `CAP-SCHEMA-ADDITIVE-V1`은 raw REST와 실제 emulator process를
대상으로 공식 [Python client
`3.43.0`](https://pypi.org/project/google-cloud-bigquery/3.43.0/)을 verified로
검증한다. DuckDB adapter는 모든 physical addition을 explicit transaction으로
적용하며 기존 row의 null semantics도 테스트한다.

Canonical metadata는 여전히 process-local이다. Process crash 시 in-memory
catalog와 DuckDB file을 atomic하게 조정할 수 없으며 DuckDB에서 직접 수행한
변경은 canonical BigQuery metadata 변경이 아니다. DDL conversion, load/query
`schemaUpdateOptions`, Storage Write schema notification은 별도 gap으로 남는다.

<!-- section: load-schema-updates -->
## Load와 Query Job Evolution

Load/query job은 field addition 또는 relaxation을 허용하는
`schemaUpdateOptions`로 제한된 schema update를 요청할 수 있다. Wire field와
write disposition 상호작용은
[`JobConfigurationLoad`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad)와
[`JobConfigurationQuery`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery)에
정의된다. 이 경로에는 staging, destination 현재 schema 기준 검증, data와
metadata의 atomic publish, durable job error가 필요하다. JSON option을
받아들이는 것만으로 구현된 것이 아니다.

현재 opt-in Parquet load 범위는 기존 table 기준으로 cast를 검증하고 write
disposition을 atomic하게 적용하지만 `schemaUpdateOptions`, destination create,
autodetect를 거부한다. 따라서 load-driven schema evolution은 unsupported로 남는다.

<!-- section: write-schema-updates -->
## Storage Write Schema 변경

`AppendRows`는 connection 첫 request에서 writer schema를 제공하고 destination이
변경되면 response에서 `updated_schema`를 받을 수 있다. Google은 이를 [schema
update detection](https://cloud.google.com/bigquery/docs/write-api#schema_update_detection)으로
문서화하며 canonical message는
[`AppendRows` RPC](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.AppendRows)에
정의된다.

현재 public Partial Write service는 ProtoRows append의 writer-schema fingerprint를
보관하고 live process에서 PENDING-stream exact offset을 유지한다. Durable destination
schema version을 추적하거나 `updated_schema`를 내보내지는 않는다. 따라서
incompatible evolution, schema notification, restart 후 offset recovery는
unsupported로 남는다.

<!-- section: cdc-contract -->
## BigQuery CDC 계약

BigQuery CDC는 Storage Write ingestion mode이며 SQL `MERGE` rewrite가 아니다.
Table에는 선언된 primary key가 필요하고 각 row는 `_CHANGE_TYPE`에 `UPSERT` 또는
`DELETE`를 실어 보낸다. `_CHANGE_SEQUENCE_NUMBER`는 경쟁 변경의 순서를 선택적으로
결정한다. BigQuery는 background에서 변경을 적용하므로 mutation visibility와
`max_staleness`도 관찰 가능한 계약이다. Pseudocolumn 철자, sequence number
format/order, delete payload 요구 사항을 포함한 권위 있는 규칙은 [BigQuery
change data capture](https://cloud.google.com/bigquery/docs/change-data-capture)에 있다.

현재 emulator에는 primary-key constraint metadata, CDC mutation queue, apply
watermark, background apply job, staleness model, CDC metric이 없다. 따라서 CDC는
**unsupported**다. `UPSERT`를 즉시 DuckDB `INSERT OR REPLACE`로 처리하면 ordering,
duplicate, delete, visibility bug를 숨긴다.

<!-- section: stream-ledger -->
## 필요한 Stream/CDC Ledger

최소한의 올바른 설계는 append acceptance와 CDC application을 분리한다.

| Ledger | 필요한 상태 |
| --- | --- |
| write stream | stream type/state, table, schema version/fingerprint, next offset, accepted payload digest, finalized row count |
| CDC mutation | primary-key digest, change type, parsed sequence tuple, append identity, receive time, apply state/error |
| table apply | watermark, last successful apply, outstanding mutation count, staleness policy |

CDC application ordering을 평가하기 전에 `BatchCommitWriteStreams`가 pending
stream row를 atomic하게 공개해야 한다. Write API batch 계약은 [pending stream을
사용한 batch load](https://cloud.google.com/bigquery/docs/write-api-batch)에 정의된다.
이 ledger는 port 뒤에 있어야 하며 DuckDB table은 하나의 adapter일 뿐 state
machine API가 아니다.

<!-- section: flink-profile -->
## Flink Connector 1.2.0 Client Profile

공식 GoogleCloudDataproc Flink connector `1.2.0`은 [Spark connector
`0.44.2`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2)와
분리된 client profile이다. 두 connector의 task/checkpoint model과 Storage RPC
sequence를 서로 바꿔 쓸 수 없다. 계획한 profile은 [released Maven
directory](https://repo1.maven.org/maven2/com/google/cloud/flink/flink-1.17-connector-bigquery/1.2.0/)에서
`com.google.cloud.flink:flink-1.17-connector-bigquery:1.2.0`을 resolve하고 artifact
URL, size, SHA-256을 기록해야 하며 upstream을 clone하거나 build하면 안 된다. 정확한
version은 tagged
[`pom.xml`](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/1.2.0/flink-1.17-connector-bigquery/pom.xml)로
증명한다.

Profile operation은 bounded source read, default stream at-least-once write,
checkpointed buffered-stream write, schema mismatch, CDC UPSERT/DELETE로 명시한다.
Connector code는
[`BigQueryCdcSchemaProvider.java`](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/1.2.0/flink-1.17-connector-bigquery/flink-connector-bigquery/src/main/java/com/google/cloud/flink/bigquery/sink/serializer/BigQueryCdcSchemaProvider.java)에서
CDC pseudocolumn을 추가하고
[`BigQueryExactlyOnceSink.java`](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/1.2.0/flink-1.17-connector-bigquery/flink-connector-bigquery/src/main/java/com/google/cloud/flink/bigquery/sink/BigQueryExactlyOnceSink.java)에서
checkpointed writer를 구성한다. 이 source link는 client expectation을 설명할 뿐
emulator support를 뜻하지 않는다. Public Storage Read와 ProtoRows PENDING/default
Write subset은 Partial이지만 Flink `1.2.0` E2E로 승격된 operation은 없다.
Buffered/checkpointed Write, schema notification, CDC는 명시적 capability gap이다.

<!-- section: evolution-pipeline -->
## 모듈식 Evolution Pipeline

모든 schema/CDC 동작은 다음 순서로 진행한다.

```text
protocol profile -> adapter -> capability -> golden -> E2E
```

Profile은 client와 protocol version을 식별한다. Adapter는 알려진 shape만
변환한다. Capability는 supported, partial, unsupported 상태를 기록한다.
Golden fixture는 정제되어야 하며 positive/negative shape를 모두 포함한다.
E2E는 released client로 공개 REST/gRPC endpoint를 통과한다. DuckDB unit test가
성공했다는 이유로 어느 단계도 생략할 수 없다.

<!-- section: drift-report -->
## Drift Report

모든 mismatch는 다음 안정된 field를 포함해야 한다.

```text
version=<client/protocol version>
operation=<REST method, RPC, or SQL template>
shape=<JSON/protobuf/schema summary>
fingerprint=<redacted deterministic digest>
fix_hint=<next actionable boundary>
```

Fingerprint는 canonical schema 또는 정제된 payload 구조를 대상으로 하며
credential이나 production row value를 포함하지 않는다. Version/operation은
profile을 선택하고 shape/fingerprint는 drift 위치를 좁히며 `fix_hint`는 바꿔야
할 adapter, capability, golden 또는 E2E 단계를 지목한다.

<!-- section: test-gates -->
## 승격 Test Gate

Verified schema test는 top-level, nested, repeated-record addition, populated
table null, destructive change 거부, transactional physical failure, stale ETag,
Python-client E2E를 다룬다. Restart reconciliation, DDL 및 load/query schema-update
path, Storage Write schema notification은 gap으로 남는다. 향후 CDC에는 out-of-order와 duplicate sequence value,
UPSERT/DELETE, missing key, invalid pseudocolumn, reconnect/replay offset,
multiple stream, commit visibility, apply lag, failure recovery가 필요하다.
승격 시에도 [BigQuery data
type](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)과 결과
type을 비교해야 한다.
