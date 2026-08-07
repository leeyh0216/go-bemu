<!-- doc-id: architecture -->
<!-- lang: ko -->

[English](../en/architecture.md) | [한국어](architecture.md)

# 아키텍처

<!-- section: goals -->
## 목표와 비목표

이 서비스는 로컬 클라이언트에 필요한 관찰 가능한 계약을 재현하면서 실행,
메타데이터, object storage, identity, time, ID를 교체 가능하게 유지한다. 계약의
출처는 우연한 DuckDB 동작이 아니라 [BigQuery REST
API](https://cloud.google.com/bigquery/docs/reference/rest)와 [Storage
RPC](https://cloud.google.com/bigquery/docs/reference/storage/rpc)다. Dremel,
Colossus, slot, quota, billing, 지역 배치, 프로덕션 가용성 재현은 범위 밖이다.

<!-- section: dependency-rule -->
## 의존성 규칙

```text
transport/rest, transport/grpc  ->  application  ->  domain + ports
                                                  ^
adapters/duckdb, memory, objectstore, system  ----|
```

화살표는 소스 의존성을 뜻한다. Domain value는 Google JSON, protobuf, SQL
connection, framework type을 포함하지 않는다. Application service는 port를
조정하고 compensation/state transition을 소유한다. Transport는 공개 wire type을
변환한다. Adapter는 외부 side effect를 구현한다.

<!-- section: package-ownership -->
## Package 소유권

| Package | 소유 | 소유하면 안 되는 것 |
| --- | --- | --- |
| `internal/domain` | identity, canonical schema, job state, domain error | HTTP, protobuf, DuckDB |
| `internal/application` | use case, 순서, compensation | route, 생성 wire type, SQL syntax |
| `internal/ports` | inbound/outbound contract | 구체 client |
| `internal/adapters/duckdb` | physical name, type mapping, SQL execution | REST resource, job lifecycle |
| `internal/adapters/memory` | process-local repository | query 의미 |
| `internal/transport/*` | 공개 REST/gRPC 경계 | database import |
| `cmd/emulator` | composition과 lifecycle | business rule |

Compile-time assertion은 adapter가 port를 구현함을 증명한다. Application test도
DuckDB를 fake로 교체해야 한다. Assertion만으로 port가 동작상 교체 가능함을
증명하지 못한다.

<!-- section: control-data-planes -->
## Control Plane과 Data Plane

**BigQuery 계약:** REST resource는 metadata와 job을 만들고 Storage Read/Write
RPC는 대량 row data를 이동한다. Read session은 table snapshot을 stream으로
나누며 이는
[`CreateReadSession`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryRead.CreateReadSession)에
정의되어 있다. Write stream type은 visibility와 commit 동작을 결정하며 [Storage
Write API](https://cloud.google.com/bigquery/docs/write-api)에 설명되어 있다.

**현재 구현:** REST metadata/query와 opt-in Parquet load job이 public control
plane을 구성한다. Public Storage Read는 하나의 bounded DuckDB snapshot을
materialize하고 Arrow 또는 Avro로 encode하며 offset resume가 가능한 deterministic
logical range를 노출한다. Public Storage Write는 PENDING/default stream에서
ProtoRows를 수락하고 offset을 검증하며 PENDING stream을 finalize한 뒤 검증된 그룹을
serialized DuckDB transaction 하나로 commit한다. 두 data plane은 Partial이다. Read는
split/compression/historical snapshot, restart recovery, nested-field projection이 없고, Write는 CDC,
ArrowRows, BUFFERED/explicit COMMITTED stream, FlushRows, durable staging이 없다.

<!-- section: catalog-physical-model -->
## Catalog와 물리 모델

Canonical resource는 BigQuery project, dataset, table, field, partition,
clustering metadata를 보존한다. DuckDB adapter는 `project.dataset.table`을 hex로
인코딩한 physical schema와 quote된 table identifier로 매핑한다. DuckDB catalog와
SQL 동작은 [DuckDB CREATE
SCHEMA](https://duckdb.org/docs/stable/sql/statements/create_schema)와 [identifier
규칙](https://duckdb.org/docs/stable/sql/dialect/keywords_and_identifiers)에 정의되어
있다.

Metadata repository와 engine catalog는 현재 분리되어 있다. Create는 physical
DDL 이후 metadata를 저장하고 실패 시 보상한다. Delete는 physical 삭제 후
metadata를 삭제한다. 두 단계 사이에서 crash가 발생하면 drift가 생길 수 있다.
재시작 또는 atomic catalog를 주장하려면 durable metadata와 하나의 transaction
경계가 필요하다.

Anonymous query result는 project/location별 emulator-owned hidden dataset 하나와
job별 collision-resistant table identity를 사용한다. Metadata publication 시 24시간
기본값의 file-configured expiration을 붙인다. `CatalogService`는 한 process 안의
physical/metadata resource mutation을 직렬화하고 그 경계에서 expiration을 다시 확인하며
`tables.get`, `tables.list`, Storage Read resolve 시 physical storage를 먼저 drop한 뒤
metadata를 삭제한다. 이는 durable background expiration service를 주장하지 않으면서
BigQuery의 [anonymous result
storage](https://cloud.google.com/bigquery/docs/cached-results#how_cached_results_are_stored)를
모델링한다. `CatalogService`에는 cleanup goroutine과 `Close` phase가 없고 각 경계가
lazy cleanup을 동기적으로 완료한다. Hidden dataset을 ID로 직접 지정하면 일반
delete-content 검사를 사용하며 `datasets.list`에서는 `all=true`일 때만 보인다.

<!-- section: query-jobs -->
## Query Job 수명 주기

```text
PENDING -> RUNNING -> DONE(result)
                   -> DONE(errorResult)
```

BigQuery는 성공과 실패 job을 모두 `DONE`으로 보고하며 client는 공식 [JobStatus
resource](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobStatus)에
따라 `status.errorResult`를 검사한다. 현재 repository는 state와 materialized
result를 메모리에 저장한다. Query job identity는 `(project, location, jobId)`와
canonical configuration fingerprint다. 모든 재사용 ID는 동일 configuration 여부와
무관하게 `409 duplicate`이며 fingerprint는
SQL을 기록하지 않고 same/different configuration drift를 구분하는 진단 값이다. 이는
BigQuery의 공식 retry 동작을 따른다. 공식
[reliability guidance](https://cloud.google.com/bigquery/docs/reliability-intro#retry_failed_job_insertions)를
참고한다. `jobs.insert`는 분리된 background goroutine에서 실행하되 파일로 설정한
`query.operationTimeout` hard ceiling을 적용한다. 모든 query result row는 여전히 Go
memory에 남는다. Worker admission, durable terminal state, result retention, public
cancellation route는 `query.results.unbounded-memory-v1` gap으로 남는다. Cross-type query/load uniqueness와
terminal-update recovery는 `query.jobs.cross-repository-identity-v1`과
`query.terminal-persistence-v1` gap이다.
REST DTO는 알려진 미지원 query control의 presence를 보존하고
`query.options.unsupported-v1`로 실행 전에 거부한다. 진단에는 field 이름만 포함하며
parameter 값, label, SQL, row는 포함하지 않는다. 이 경계는 공식
[`QueryRequest`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#QueryRequest)와
[`JobConfigurationQuery`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery)
field 집합을 따른다.
REST table browsing은 별도 `TableDataReader` outbound port를 지난다. Application은
catalog mutation boundary에서 live canonical metadata와 lazy expiration을 확인하고
파일로 설정한 row 및 time limit을 적용한 뒤, DuckDB adapter에 하나의 transaction으로
정확한 count와 ordinal page를 요청한다. REST adapter만 schema-driven nested `f/v`
JSON 표현과 resource-scoped opaque token을 소유한다. 이는 DuckDB value를 transport
code에 노출하지 않으면서 공식
[`tabledata.list`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list)
경계를 따른다.
실행 상한은 공식
[`jobTimeoutMs`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfiguration.FIELDS.job_timeout_ms)와
같은 bounded-job 의도를 따르며, request 단위 `timeoutMs`의 정확한 동작은 별도
compatibility gap이다.
Connector 필수 `configuration.query.priority`와 `configuration.labels`는 scheduler
policy가 아닌 domain data다. Priority는 enum을 검증하고 label은 empty map을 포함해
검증하고 round-trip하며 둘 다 configuration fingerprint에 포함한다. 로그에는
priority, label 수, 정렬한 label-key fingerprint만 남기며 label 값은 남기지 않는다.

지원 query subset에서 job 생성 앞에는 명시적 `QueryAnalyzer` port가 있다.

```text
GoogleSQL request
  -> structural relation analysis
  -> source/default/destination dataset location validation
  -> generated anonymous destination (row-producing, destination omitted)
  -> JobRepository.CreateOrGet
  -> DuckDB materialization
  -> catalog publication
```

Application은 DuckDB parsing을 import하지 않는다. DuckDB adapter는 referenced table
identity와 `producesRows`만 반환하고 log에는 SQL length/digest, statement type,
model version, count만 남기며 SQL은 남기지 않는다. 이는 BigQuery [location
inference](https://cloud.google.com/bigquery/docs/locations#specify_locations)와
`JobConfigurationQuery`의 [generated destination
계약](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery)을
따른다. 정확한 connector consumer는 materialization dataset을 설정하지 않았을 때
완료 job의 `destinationTable`을 읽는 `0.44.2`
[`TempTableBuilder`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java#L1150-L1240)다.

<!-- section: transactions -->
## Transaction과 Visibility

Engine statement transaction이 자동으로 BigQuery operation transaction이 되는
것은 아니다. Metadata와 physical DDL은 여전히 별도 store에 걸쳐 있다. Explicit
query destination은 DuckDB staging table에 한 번 평가되고
`WRITE_EMPTY`, `WRITE_APPEND`, exact-schema `WRITE_TRUNCATE`를 같은 transaction에
적용한다. 기준은
[`JobConfigurationQuery`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery)다.
New-table metadata는 CTAS commit 이후 공개하며 publication 실패 시 physical drop으로
보상한다. Anonymous destination은 hidden dataset을 physical-first metadata publication으로
만든 뒤 같은 CTAS/compensation 경로를 사용한다. Cache reuse, durable TTL cleanup,
schema-replacing truncate는 명시적 gap이다. Parquet
load는 temporary staging table을 검증하고 destination disposition을 DuckDB
transaction 하나에서 적용한다. Storage Write는 명명한 모든 PENDING stream을 먼저
검증한 뒤 serialized coordinator의 DuckDB transaction 하나로 적용하며, 이는
[`BatchCommitWriteStreams`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.BatchCommitWriteStreams)로
정의된 atomic group 계약을 따른다. 이 atomic transaction은 process-local job 또는
stream ledger를 restart-durable하게 만들지 않으며 object download는 의도적으로
load commit 밖에 있다.

<!-- section: sql-boundary -->
## SQL Dialect 경계

Backtick reference 변환은 임시 adapter 책임이다. 현재 lexical scanner는 relation
위치와 quote된 column, string, comment를 구분하지만 script, table decorator,
function argument, 모든 unquoted path를 처리하는 완전한 parser는 아니다. 일반
호환성에는 구조적인 GoogleSQL parser/semantic adapter가 필요하다. 권위 있는 syntax는 [GoogleSQL lexical
structure](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical)와
[query syntax](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax)다.
알 수 없거나 지원하지 않는 형식은 근사 변환하지 말고 명시적으로 실패해야 한다.
Analyzer는 catalog-mutating DDL을 표시하고 application은
`query.ddl.catalog-sync-v1`에 따라 `CREATE`, `ALTER`, `DROP`, `TRUNCATE`를 job
생성이나 engine 실행 전에 거부한다. DDL 구현에는 DuckDB 직접 실행이 아니라 atomic
canonical catalog reconciliation port가 필요하다.

한 가지 Static Partial 예외는 의도적으로 structural하고 versioned되어 있다. Token
parser가
[`BigQueryClient.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java)의
source-derived connector `0.44.2` shape를 인식하고 constant-false [BigQuery
`MERGE` 계약](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)을
적용하며 하나의 atomic [DuckDB `MERGE
INTO`](https://duckdb.org/docs/current/sql/statements/merge_into)를 실행한다. Dynamic
time/range partition overwrite나 임의 `MERGE`로 일반화하지 않는다.

<!-- section: runtime-security -->
## Runtime, TLS, Identity

프로세스는 warehouse 하나, process-local catalog/job repository, system clock/ID
adapter, application service, public REST/gRPC listener, optional 별도 admin
listener를 구성한다. 하나의 certificate pair로 public listener와 enabled admin에서
TLS를 활성화할 수 있다. 현재 인증은 permissive다. 전송 보안과 identity는 별개이며
[Google Cloud
인증](https://cloud.google.com/docs/authentication)의 ADC 또는 IAM 동작을
구현하지 않는다.

<!-- section: observability -->
## Capability와 관측성

경계 log에는 operation, status, identifier, count, latency, digest가 포함된다.
Authorization, credential, token, raw SQL, row payload, protobuf JSON, HTTP body,
error text는 format, level, configuration mode와 관계없이 제외한다. Deprecated
`logging.unsafePayloads` input은 parse compatibility를 유지하지만 명시적인 no-op이다.
Opaque value는 observability adapter에서 shape, byte/item count, 전체 값 SHA-256으로만
log 경계를 통과한다. 이 fail-closed 경계는 [Cloud Logging audit
guidance](https://cloud.google.com/logging/docs/audit/best-practices)를 따르며 regex
redaction은 알 수 없는 protocol value가 안전하다는 증거로 취급하지 않는다.
Capability profile은 버전별
관찰값이지 feature negotiation이나 모든 flow 성공의 증거가 아니다.

<!-- section: replacement-roadmap -->
## 교체 로드맵

1. canonical metadata, job, read session, write/load ledger를 transactional system
   table에 영속화한다.
2. pinned static-overwrite shape를 일반화하지 않으면서 광범위한 regex SQL 변환을
   structural adapter로 교체한다.
3. 현재 byte/row/session bound를 약화하지 않고 Storage Read split/compression,
   historical snapshot support, nested projection, durable session recovery를 추가한다.
4. Storage Write ArrowRows, BUFFERED/explicit COMMITTED stream, FlushRows, default
   expression, CDC, durable pending recovery를 추가한다.
5. bounded staging을 유지하며 load port에 missing-table create, schema-update
   option, Parquet 이외 format, multipart/resumable transfer를 추가한다.
6. physical-first cleanup과 재시도 가능한 metadata를 유지하면서 anonymous-result
   ownership/expiration을 영속화하고 bounded background sweeper를 추가한다.

이 변경들은 dependency rule을 보존한다. DuckDB는 application API가 되지 않고
교체 가능한 상태로 남는다.
