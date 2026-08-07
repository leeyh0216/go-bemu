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
참고한다. `jobs.insert`는 여전히 제한 없는 background goroutine에서 실행되고 모든
query result row는 Go memory에 남는다. Worker admission, execution deadline,
durable terminal state, cancellation은 `query.execution.unbounded-v1`과
`query.results.unbounded-memory-v1` gap이다. Cross-type query/load uniqueness와
terminal-update recovery는 `query.jobs.cross-repository-identity-v1`과
`query.terminal-persistence-v1` gap이다.
REST DTO는 알려진 미지원 query control의 presence를 보존하고
`query.options.unsupported-v1`로 실행 전에 거부한다. 진단에는 field 이름만 포함하며
parameter 값, label, SQL, row는 포함하지 않는다. 이 경계는 공식
[`QueryRequest`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#QueryRequest)와
[`JobConfigurationQuery`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery)
field 집합을 따른다.
Connector 필수 `configuration.query.priority`와 `configuration.labels`는 scheduler
policy가 아닌 domain data다. Priority는 enum을 검증하고 label은 empty map을 포함해
검증하고 round-trip하며 둘 다 configuration fingerprint에 포함한다. 로그에는
priority, label 수, 정렬한 label-key fingerprint만 남기며 label 값은 남기지 않는다.

<!-- section: transactions -->
## Transaction과 Visibility

Engine statement transaction이 자동으로 BigQuery operation transaction이 되는
것은 아니다. Metadata와 physical DDL은 여전히 별도 store에 걸쳐 있다. Explicit
query destination은 DuckDB staging table에 한 번 평가되고
`WRITE_EMPTY`, `WRITE_APPEND`, exact-schema `WRITE_TRUNCATE`를 같은 transaction에
적용한다. 기준은
[`JobConfigurationQuery`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery)다.
New-table metadata는 CTAS commit 이후 공개하며 publication 실패 시 physical drop으로
보상한다. Anonymous destination과 schema-replacing truncate는 명시적 gap이다. Parquet
load는 temporary staging table을 검증하고 destination disposition을 DuckDB
transaction 하나에서 적용한다. Storage Write는 명명한 모든 PENDING stream을 먼저
검증한 뒤 serialized coordinator의 DuckDB transaction 하나로 적용하며, 이는
[`BatchCommitWriteStreams`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.BatchCommitWriteStreams)로
정의된 atomic group 계약을 따른다. 이 atomic transaction은 process-local job 또는
stream ledger를 restart-durable하게 만들지 않으며 object download는 의도적으로
load commit 밖에 있다.

<!-- section: sql-boundary -->
## SQL Dialect 경계

Backtick reference 변환은 임시 adapter 책임이다. Regex replacement는 table
identifier와 quote된 column, string, comment, script, table decorator, function
argument를 구분하지 못한다. 일반 호환성에는 구조적인 GoogleSQL parser/semantic
adapter가 필요하다. 권위 있는 syntax는 [GoogleSQL lexical
structure](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical)와
[query syntax](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax)다.
알 수 없거나 지원하지 않는 형식은 근사 변환하지 말고 명시적으로 실패해야 한다.

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
Authorization, credential, token, raw SQL, row payload는 명시적인 unsafe local-only
switch가 payload logging을 허용하지 않는 한 제외한다. Capability profile은 버전별
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

이 변경들은 dependency rule을 보존한다. DuckDB는 application API가 되지 않고
교체 가능한 상태로 남는다.
