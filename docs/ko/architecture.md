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

**현재 구현:** REST metadata/query가 첫 public vertical slice다. Storage Read에는
snapshot ownership, deterministic range, offset resume, bare Arrow/Avro payload
pass-through를 다루는 테스트된 application service와 protobuf adapter가 있다.
DuckDB snapshot/encoder adapter가 composition되지 않았으므로 public Read service는
`NOT_SERVING`이고 `UNIMPLEMENTED`를 반환한다. Storage Write는 registration-only다.
내부 protocol test는 data-plane 지원이 아니다.

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
result를 메모리에 저장한다. `jobs.insert`는 제한 없는 background goroutine에서
실행된다. Worker admission, durable terminal state, cancellation, location-aware
key, idempotent replay는 아직 설계 대상이다.

<!-- section: transactions -->
## Transaction과 Visibility

Engine statement transaction이 자동으로 BigQuery operation transaction이 되는
것은 아니다. Metadata와 physical DDL, load staging과 destination disposition,
multi-stream batch commit은 각각 application 소유 transaction port가 필요하다.
BigQuery는 pending stream 그룹을
[`BatchCommitWriteStreams`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.BatchCommitWriteStreams)로
atomic commit한다고 정의한다. DuckDB가 SQL transaction을 시작할 수 있다는
이유만으로 현재 코드가 이 동작을 주장하면 안 된다.

<!-- section: sql-boundary -->
## SQL Dialect 경계

Backtick reference 변환은 임시 adapter 책임이다. Regex replacement는 table
identifier와 quote된 column, string, comment, script, table decorator, function
argument를 구분하지 못한다. 일반 호환성에는 구조적인 GoogleSQL parser/semantic
adapter가 필요하다. 권위 있는 syntax는 [GoogleSQL lexical
structure](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical)와
[query syntax](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax)다.
알 수 없거나 지원하지 않는 형식은 근사 변환하지 말고 명시적으로 실패해야 한다.

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

1. inbound metadata/query/read/write port와 outbound query-engine, transaction,
   auth, stream-ledger port를 분리한다.
2. canonical metadata와 job을 transactional system table에 영속화한다.
3. regex SQL 변환을 구조적 adapter로 교체한다.
4. DuckDB read-snapshot/encoder adapter를 구현하고 테스트된 ranged-stream
   application service에 composition한 뒤 public endpoint에서 정확한 Arrow/Avro
   framing을 증명한다.
5. stream별 write ledger와 atomic pending-stream commit을 추가한다.
6. endpoint 설정 가능한 object-store port를 통해 staged load job을 추가한다.

이 변경들은 dependency rule을 보존한다. DuckDB는 application API가 되지 않고
교체 가능한 상태로 남는다.
