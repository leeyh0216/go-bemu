<!-- doc-id: compatibility -->
<!-- lang: ko -->

[English](../en/compatibility.md) | [한국어](compatibility.md)

# 호환성 계약

<!-- section: status-language -->
## 상태 용어

| 상태 | 의미 |
| --- | --- |
| Verified | 명시된 공개 또는 adapter 경계에서 구현하고 실행함 |
| Partial | 유용한 부분집합이 있으며 중요한 제한을 모두 명시함 |
| Registered | canonical service가 있지만 operation은 `UNIMPLEMENTED` 반환 |
| Planned | 설계/출처가 있으나 caller가 의존하면 안 됨 |
| Unsupported | 없거나 의도적으로 거부함 |

이 상태는 이 저장소를 설명하며 [BigQuery
service](https://cloud.google.com/bigquery/docs/introduction)와 동등하다는 뜻이 아니다.

<!-- section: rest-metadata -->
## REST Metadata

| Operation | 상태 | 계약 경계 |
| --- | --- | --- |
| health/readiness | Verified | process와 warehouse ping |
| emulator project lifecycle | Verified | emulator 전용 namespace |
| `projects.list` | Verified basic | emulator project와 opaque page token |
| dataset insert/get | Verified basic | location/label/default expiration 보존 |
| dataset list/delete | Verified basic | paging과 `deleteContents`, filter/all은 unsupported |
| dataset patch/update | Verified | metadata field와 ETag/HTTP 412 precondition |
| table insert/get/delete | Verified basic | standard table과 canonical schema metadata |
| table list | Verified basic | paging, view/storage statistics 없음 |
| table patch/update | Verified narrow | metadata, additive schema, ETag precondition |
| `tabledata.list` | Partial | scalar/nested/repeated `f/v` row, `startIndex`, 제한된 `maxResults`, resource-scoped opaque token, ETag precondition, 정확한 `totalRows`, `useInt64Timestamp`; selected field, ISO-8601 picosecond output, byte 기반 page trimming은 gap |
| `tabledata.insertAll` | Unsupported | route 없음 |

Request/response shape는 공식
[`datasets`](https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets)와
[`tables`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables)
resource와 비교한다. 알 수 없는 JSON field를 무시하는 것은 forward-tolerant
decode일 뿐 해당 field 구현이 아니다.

[`tabledata.list`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list)
adapter는 `tables.get`과 Storage Read가 사용하는 동일한 catalog TTL 확인 뒤, 하나의
DuckDB transaction에서 count와 ordinal page 선택을 수행한다. File-first
`tableData.maxPageRows` cap은 요청보다 적은 row를 반환할 수 있고
`tableData.operationTimeout`은 physical operation을 제한한다. BigQuery는 대략 10 MB
response 기준으로도 page를 자른다. Byte 기반 trimming, mutation-aware page 무효화,
`selectedFields`, `timestampOutputFormat`은 명시적 gap이다.
`formatOptions.useInt64Timestamp=true`는 고정 Python client가 요구하는 epoch
microsecond 문자열을 반환한다. 공식 [pagination criteria](https://cloud.google.com/bigquery/docs/paging-results#page_through_results_using_the_api)를 참고한다.

`CAP-REST-METADATA-PATCH-V1`과 `CAP-SCHEMA-ADDITIVE-V1`은 실제 process를
대상으로 공식 [Python client
`3.43.0`](https://pypi.org/project/google-cloud-bigquery/3.43.0/)으로도 실행된다.
Schema support는 nested/repeated record를 포함한 append-only
`NULLABLE`/`REPEATED`이며 DDL conversion, relaxation, job-driven evolution을
뜻하지 않는다.

<!-- section: jobs -->
## Query와 Job

| Operation | 상태 | 제한 |
| --- | --- | --- |
| `jobs.query` | Partial | Python 3.43.0 path verified, synchronous DuckDB-compatible SQL subset |
| query `jobs.insert` | Partial | Python 3.43.0 polling path verified, process-local asynchronous execution |
| `jobs.get` | Verified basic | `PENDING/RUNNING/DONE`, terminal error |
| `jobs.list` | Partial | location-aware identity와 opaque cursor token, process-local snapshot만 지원 |
| `jobs.getQueryResults` | Partial | location-aware lookup, `startIndex`, `maxResults`, job/result-bound opaque page token |
| explicit destination table | Partial | scalar exact-schema `WRITE_EMPTY`/`WRITE_APPEND`/`WRITE_TRUNCATE`, capability `query.destination.exact-schema-v1` |
| connector query metadata | Verified basic | `INTERACTIVE`/`BATCH` priority와 검증된 label을 fingerprint/round-trip하며 명시적 empty label map도 보존 |
| anonymous destination table | Partial | row-producing query job은 24시간 lazy expiration을 가진 hidden-dataset destination을 생성·공개, capability `query.destination.anonymous-v1` |
| `WRITE_TRUNCATE` schema replacement | Unsupported | exact-schema subset만 지원, gap `query.destination.truncate-schema-replacement-v1` |
| SQL DDL | Unsupported | physical/canonical catalog 변경이 하나의 application 계약을 공유할 때까지 `CREATE`/`ALTER`/`DROP`/`TRUNCATE`는 job/engine side effect 전에 실패, gap `query.ddl.catalog-sync-v1` |
| multi-statement query | Unsupported | literal/comment를 구분하는 scan은 선택적인 마지막 semicolon 하나만 허용하고 script를 job/engine side effect 전에 거부, gap `query.scripts.unsupported-v1`, 공식 [multi-statement query 계약](https://cloud.google.com/bigquery/docs/multi-statement-queries) 참고 |
| cancellation | Partial | runtime shutdown은 새 work를 거부하고 이미 수용한 sync/async work를 취소·drain한 뒤 Storage 또는 DuckDB를 닫음, 공개 [`jobs.cancel`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/cancel)과 cancellation state는 미지원 |
| Parquet load `jobs.insert` / `jobs.get` / `jobs.list` | Partial | opt-in, 기존 destination table, process-local state |
| copy/extract | Unsupported | configuration 거부 |
| durable job/result state | Unsupported | in-memory repository |
| bounded query result retention | Unsupported | 모든 result row가 Go memory에 남음, gap `query.results.unbounded-memory-v1` |
| complex query-result schema | Strict gap | ARRAY/STRUCT result는 mode/child를 평탄화하지 않고 metadata publication 전에 실패, gap `query.results.complex-schema-v1` |
| bounded async query execution | Partial | 파일 설정 `query.operationTimeout`으로 service-owned sync/async 실행을 제한하고 shutdown admission/cancel/wait를 구현, worker capacity와 정확한 request `timeoutMs`는 gap, capability `query.execution.bounded-v1` |
| same-ID query insert | Verified basic | atomic `(project, location, jobId)` uniqueness, 모든 재사용은 `409 duplicate`, fingerprint는 진단용으로 유지 |
| exact-request replay extension | Unsupported | 향후 opt-in 전용, gap `query.jobs.exact-replay-extension-v1` |
| query/load cross-type identity | Unsupported | 분리된 repository의 check/create race, gap `query.jobs.cross-repository-identity-v1` |
| synchronous request controls | Partial | 36바이트 ASCII `requestId`를 검증하고 음수가 아닌 `timeoutMs`를 수용함, 미완료 응답의 대기 제한·변경 쿼리 중복 제거·`jobTimeoutMs`는 gap `query.sync.request-controls-v1` |
| unsupported query option | Strict gap | parameter, `dryRun`, cache/billing control, `jobTimeoutMs`는 명시적으로 `400` 거부, gap `query.options.unsupported-v1` |
| omitted-location dataset inference | Partial | 구조적으로 참조한 table, cross-project `defaultDataset.projectId`, explicit destination dataset을 insert 전에 검증, capability `query.location.dataset-inference-v1` |
| terminal persistence recovery | Unsupported | terminal repository update 실패 시 `RUNNING` 잔류 가능, gap `query.terminal-persistence-v1` |

Canonical job state와 error field는 공식
[`Job`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job) resource를
기준으로 한다. Nested/repeated result cell과 type별 temporal value는 아직 완전한
[`TableRow`](https://cloud.google.com/bigquery/docs/reference/rest/v2/TableRow)
encoding이 아니다. Explicit destination은
[`JobConfigurationQuery`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery)를,
result token은
[`jobs.getQueryResults`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults)를
따른다. 공식
[`QueryRequest`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#QueryRequest)와
`JobConfigurationQuery`의 알려졌지만 미구현된 field는 REST 경계에서 presence를
보존해 실행 전에 실패한다. Zero value가 구현된 것처럼 조용히 수용되지 않는다.
BigQuery는 재사용한 모든 job ID를 `409 duplicate`로 거부하고 `jobs.get`
복구를 권장한다. BQEMU도 이를 기본 동작으로 따르며 configuration fingerprint는
안전한 drift 진단에만 사용한다. 공식
[retry guidance](https://cloud.google.com/bigquery/docs/reliability-intro#retry_failed_job_insertions)를
참고한다.

`destinationTable` 없는 row-producing query에서 BQEMU는
`JobRepository.CreateOrGet` 전에 destination을 생성하고
`configuration.query.destinationTable`로 반환한 뒤
`WRITE_EMPTY`/`CREATE_IF_NEEDED`로 result를 materialize한다. 이는 connector
`0.44.2`의
[`TempTableBuilder`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java#L1150-L1240)가
사용하는 계약이다. 생성 dataset은 `_`로 시작하고
[`all=true`](https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list)가
아니면 `datasets.list`에서 숨겨진다. Table은 connector 기본
[`MaterializationConfiguration`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/MaterializationConfiguration.java)과
BigQuery의 대략적인 [anonymous table
수명](https://cloud.google.com/bigquery/docs/cached-results#how_cached_results_are_stored)에
맞춰 publication 24시간 뒤 expiration을 노출한다. 정리는 `tables.get`,
`tables.list`, Storage Read resolve 시점에 lazy하게 수행하며 hidden dataset은 다음
result를 위해 유지한다. Cache-hit 재사용, background sweeper, restart-durable TTL
ledger는 아직 없다. Cleanup goroutine이나 `Close` ordering은 없고 각 request가
cleanup을 동기적으로 끝낸다. ID를 아는 hidden dataset은 일반 delete 규칙을 따른다.
Live table이 있으면 `deleteContents=true`가 필요하고 lazy expiration으로 비워진 뒤에는
일반 dataset delete가 성공한다.

Job insert 전에 structural analyzer는 지원하는 backtick table path, 같은 project의
cross-project `defaultDataset.projectId`, explicit destination dataset을 모두 해석한다. Location을 생략하면
공통 location을 사용하고 explicit/inferred cross-location mismatch는 repository나
engine side effect 전에 실패한다. 이는 BigQuery [location
규칙](https://cloud.google.com/bigquery/docs/locations#specify_locations)을 따른다.
현재 lexical adapter 밖의 unquoted relation path, connection, remote function,
dynamic SQL은 아직 inference
후보가 아니다. 지원 후보가 없을 때만 configured default를 사용한다.

<!-- section: sql -->
## SQL과 MERGE

| 동작 | 상태 | 제한 |
| --- | --- | --- |
| fully qualified table reference | Verified narrow case | backtick table token 변환 |
| `SELECT`/`INSERT` | Partial | DuckDB syntax와 function |
| `UPDATE`/`DELETE` | Partial | DuckDB statement 동작 |
| basic `MERGE` | Partial | DuckDB-compatible 형식 하나 테스트 |
| connector `0.44.2` static overwrite | Partial | source-derived token shape, atomic DuckDB `MERGE` |
| dynamic partition overwrite | Unsupported | script/array/partition 의미 없음 |
| parameter/script/view/UDF | Unsupported | semantic adapter 없음 |

[GoogleSQL lexical
계약](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical)은 syntax
위치에 따라 quoted identifier를 구분한다. 현재의 광범위한 backtick rewrite는
quoted column, comment, string을 안전하게 분류하지 못하므로 임의 backtick SQL을
지원하지 않는다. 일반 `MERGE`는 source cardinality와 atomic visibility를 포함한
[공식 DML
규칙](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)을
따라야 한다.

Static Partial adapter는
[`BigQueryClient.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java)가
조정하는 source-derived connector shape만 인식하고 identifier와 clause를 token으로
parse한 뒤 하나의 atomic [DuckDB `MERGE
INTO`](https://duckdb.org/docs/current/sql/statements/merge_into)를 실행한다. Dynamic
time/range partition overwrite와 일반 BigQuery `MERGE` parity는 gap으로 남는다.

<!-- section: types -->
## Type

| BigQuery type group | Physical table creation | REST query value | 전체 |
| --- | --- | --- | --- |
| BOOL/INT64/FLOAT64/STRING/BYTES | 기본 mapping | scalar encoding | Partial |
| NUMERIC | `DECIMAL(38,9)` | driver-dependent | Partial |
| BIGNUMERIC | text 보존 | engine type identity 손실 | Unsupported arithmetic |
| DATE/DATETIME/TIME/TIMESTAMP | engine mapping | temporal formatting 미완성 | Partial |
| JSON/GEOGRAPHY | JSON/text mapping | 불완전한 의미 | Partial/Unsupported |
| RECORD/REPEATED | STRUCT/LIST mapping | composite REST shape 비호환 | Partial |

호환성은 [BigQuery data
types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)를
기준으로 평가한다. REST, Arrow, Avro, direct Proto write, indirect load 전체를
end-to-end로 검증한 type은 아직 없다.

<!-- section: storage-read -->
## Storage Read

| RPC/동작 | 상태 |
| --- | --- |
| official service registration/reflection | Verified |
| read service health | enabled이고 draining 전에는 lifecycle-aware `SERVING` |
| public `CreateReadSession` / `ReadRows` | Partial, session마다 bounded DuckDB materialization 하나 |
| public `SplitReadStream` | Unsupported, `UNIMPLEMENTED` 반환 |
| Arrow/Avro schema와 row payload | Partial, bounded DuckDB row와 response byte에서 encoding |
| projection과 row restriction | Partial, top-level field와 제한된 expression subset, nested projection 미지원 |
| logical stream과 offset resume | Partial, live session 안에서 stable range와 stream-relative offset |
| historical snapshot과 compression | Unsupported |

Public capability는 Partial이다. 각 live session은 안정되고 bounded된 DuckDB
materialization 하나를 소유하고 설정 가능한 logical stream을 노출한다. Split RPC,
wire compression, historical `snapshot_time`, nested projection, restart 후 durable
session recovery는 gap으로 남는다.

목표 계약은 공식
[`BigQueryRead`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryRead)
service와 connector
[`ReadSessionCreator.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/ReadSessionCreator.java)다.

<!-- section: storage-write -->
## Storage Write

| RPC/동작 | 상태 |
| --- | --- |
| official service registration/reflection | Verified |
| write service health | enabled이고 draining 전에는 lifecycle-aware `SERVING` |
| PENDING create/get/append/finalize/commit | Partial, ProtoRows, exact offset, 숨김 DuckDB staging, finalized row count |
| default stream | Partial, 공식 alias와 connector `0.44.2` legacy alias, immediate append |
| multiple logical stream | Partial, 하나의 serialized DuckDB coordinator 위에서 weighted in-flight/staged-byte admission 적용 |
| atomic batch commit | 검증된 group의 destination insert와 staging/receipt 삭제를 하나의 transaction으로 처리해 Verified |
| ArrowRows, BUFFERED/explicit COMMITTED stream, `FlushRows` | Unsupported |

CDC, missing-value default expression, restart 후 durable staging/recovery,
distributed write concurrency는 unsupported다. PENDING row는 더 이상 decoded Go
object로 누적되지 않지만 stable staged-byte charge는 DuckDB의 정확한 물리 크기가
아니다. Serialized backend는 의도적인 embedded engine bound이며 BigQuery throughput
parity가 아니다.

목표 계약은 공식
[`BigQueryWrite`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite)
service와 connector
[`BigQueryDirectDataWriterHelper.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryDirectDataWriterHelper.java)다.

<!-- section: load-auth -->
## Load, Object Storage, Identity

| Capability | 상태 |
| --- | --- |
| filesystem object-store adapter | 명시적 local opt-in 뒤에서만 Verified |
| GCS/fake-GCS JSON adapter | Partial, bounded list/get/media와 URI glob expansion |
| 기존 table로의 Parquet load | Partial, explicit schema/cast validation |
| Avro/ORC/CSV/NDJSON load | terminal `notImplemented` job error와 함께 Unsupported |
| `WRITE_APPEND` / `WRITE_EMPTY` / `WRITE_TRUNCATE` | 하나의 DuckDB transaction에서 Verified |
| destination create, autodetect, `schemaUpdateOptions`, multipart/resumable download | Unsupported |
| REST/gRPC TLS | 설정 시 구현 |
| authentication disabled | 현재 mode |
| static token, ADC, OAuth, STS/WIF | Planned |
| IAM authorization | Unsupported |

Load 목표는
[`JobConfigurationLoad`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad)다.
Opt-in path는 bounded immutable object를 private temporary workspace에 download한 뒤
선택 disposition을 atomic하게 적용한다. Download는 destination transaction 밖에서
실행되고 load job과 idempotency record는 process-local이다.
Identity 주장은 [Google Cloud
인증](https://cloud.google.com/docs/authentication)에 따라 구분한다. 로컬 token
acquisition을 IAM parity로 설명하면 안 된다.

<!-- section: persistence-atomicity -->
## 영속성과 Atomicity

DuckDB file storage는 physical row를 보존할 수 있지만 catalog, job, read session,
write stream, load idempotency record는 process-local이다. Additive physical column은
하나의 DuckDB transaction을 사용하지만 in-memory catalog publish는 그 transaction과
crash-atomic하지 않다. Load disposition, default-stream append, 검증된 PENDING-stream
group commit은 각각 destination transaction을 사용한다. 이 atomicity는 live process
안에서만 성립하며 restart recovery와 durable replay는 unsupported다.

<!-- section: client-coverage -->
## Client Coverage

[Google Cloud SDK `566.0.0`](https://cloud.google.com/sdk/docs/release-notes#56600_2026-04-28)의
정확한 [`bq` CLI `2.1.31`](https://cloud.google.com/bigquery/docs/reference/bq-cli-reference)은
UI를 끈 독립 CI 계층에서 실행된다. Project 목록, dataset/table lifecycle,
nullable additive schema update, query polling, job/table 목록, cleanup,
not-found exit 계약을 검증한다. 공식 [Python client
`3.43.0`](https://pypi.org/project/google-cloud-bigquery/3.43.0/) E2E 여섯 개는
dataset administration, table metadata/schema administration, nested/repeated
decode를 포함한 `tabledata.list` pagination, synchronous
[`jobs.query`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query),
asynchronous [`jobs.insert`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert)에서
[`jobs.getQueryResults`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults)까지
검증한다. 해당 shape는 [`python-query-sync`](../../contract/golden/python-query-sync-3.43.0.json)와
[`python-query-async`](../../contract/golden/python-query-async-3.43.0.json),
[`python-tabledata-list`](../../contract/golden/python-tabledata-list-3.43.0.json)
golden에 고정한다. Load/copy/extract와 `insertAll`은 strict unsupported xfail 네 개로,
response-loss `requestId` replay는 별도 strict partial-contract xfail 하나로 남는다.
정확한 connector `0.44.2` matrix는 75개 중 20개를 verified로 기록한다. 여기에는
Arrow/Avro multi-stream table/query read, projection/filter pushdown, explicit
materialization, optimized count, exact PENDING append, default-stream append가
포함된다. 완전한 Spark compatibility를 주장하지 않으며 capability 승격에는
public-edge evidence와 negative 또는 boundary test가 필요하다.

[`bq-project-dataset-admin`](../../contract/golden/bq-project-dataset-admin-2.1.31.json),
[`bq-table-schema-admin`](../../contract/golden/bq-table-schema-admin-2.1.31.json),
[`bq-query-job`](../../contract/golden/bq-query-job-2.1.31.json),
[`bq-not-found-error`](../../contract/golden/bq-not-found-error-2.1.31.json) golden은
CLI wire stage를 고정한다. Load, copy, extract는 이 profile에서 Planned로 남아
있으므로 이슈 #13은 계속 열린 상태다.

<!-- section: removal-criteria -->
## Workaround 제거 기준

Compatibility workaround는 pinned upstream defect를 재현하고, 정확한 새 upstream
version에서 defect가 사라졌으며, golden wire trace가 일치하고, 해당 rule 없이
direct connector test가 통과한 뒤에만 제거할 수 있다. Workaround 일반화에는
regex 예제 하나가 아니라 protocol 또는 semantic source가 필요하다.
