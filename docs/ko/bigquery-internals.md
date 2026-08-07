<!-- doc-id: bigquery-internals -->
<!-- lang: ko -->

[English](../en/bigquery-internals.md) | [한국어](bigquery-internals.md)

# BigQuery와 Spark Connector 내부 동작

<!-- section: mental-model -->
## 핵심 모델

Spark connector는 서로 다른 세 공개 경계를 통과한다.

1. table metadata, query/load job, polling, overwrite 조정용 BigQuery REST;
2. session 생성과 병렬 row stream용 BigQuery Storage Read gRPC;
3. direct append, stream finalize, pending-stream commit용 BigQuery Storage
   Write gRPC.

여기서 설명하는 정확한 client 동작은 [connector
`0.44.2`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2)를
기준으로 한다. BigQuery의 canonical service 경계는 [REST
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest)와 [Storage RPC
레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc)다.
`go-bemu`는 현재 REST metadata/query 범위만 구현한다. 아래 Storage 설명은 구현
요구사항이지 현재 지원 주장이지 않다.

<!-- section: read-planning -->
## 읽기 계획

Connector는 먼저 REST로 table 또는 query를 확인하고 선택 column, filter,
snapshot time, 요청 parallelism을 계산한 뒤 `CreateReadSession`을 전송한다. 정확한
builder는
[`ReadSessionCreator.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/ReadSessionCreator.java)에
있다. Server는 reference schema 하나와 이름 있는 stream 0개 이상을 반환한다.
Spark는 반환된 stream마다 input partition을 만든다. 요청 max parallelism은
상한이지 stream을 강제로 만들라는 명령이 아니다.

올바른 emulator는 모든 logical stream을 하나의 안정된 snapshot에 묶어야 한다.
각 range마다 순서 없는 query를 독립 재실행하면 안 된다. Projection과 row
restriction은 session snapshot에 속하고 `ReadRows` offset은 선택 stream에
상대적이다. 이 field와 의미는 공식
[`ReadSession`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readsession)과
[`ReadRowsRequest`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readrowsrequest)
message에 정의되어 있다.

<!-- section: read-wire -->
## Arrow와 Avro Read Wire Format

Arrow의 `serialized_schema`와 `serialized_record_batch`는 서로 다른 protobuf
field에 Arrow IPC message를 담는다. 임의의 전체 Arrow file이 아니다. Format
출처는 [Arrow IPC
specification](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc)이다.
Connector decode 경로는
[`ArrowReaderIterator.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/ArrowReaderIterator.java)에서
시작한다.

Avro는 JSON schema 하나와 연속된 binary row datum을 사용한다. Logical type과
null union은 [Apache Avro
specification](https://avro.apache.org/docs/1.11.4/specification/)을 따라야 하며
BigQuery format mapping은 [Storage API Avro schema
details](https://cloud.google.com/bigquery/docs/reference/storage#avro_schema_details)에
정의되어 있다.

어느 format이든 row count, schema, payload byte, empty result, multiple batch,
nested/repeated value, compression, offset resume가 일치해야 한다. Scalar fixture
하나를 decode했다는 사실만으로 wire compatibility를 증명할 수 없다.

<!-- section: direct-exact -->
## Direct Write: Pending Stream과 정확한 Offset

`writeMethod=direct` exactly-once mode에서는 Spark data partition마다 `PENDING`
stream을 만든다. Connector `0.44.2`는
[`BigQueryDirectDataWriterHelper.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryDirectDataWriterHelper.java)에서
이를 수행한다. `AppendRows`를 열고 writer schema를 제공하며 stream-relative
starting offset과 serialized Proto row를 전송하고 각 response offset을 검증한 뒤
stream을 finalize한다. 모든 task가 성공하면 driver가 stream 이름을 모아 commit한다.

공식 Write API는 정확한 offset 동작을 요구한다. 다음 offset은 수락하고 gap은
실패하며, 이미 수락한 offset replay는 duplicate로 인식하거나 payload가 다르면
거부해야 한다. `FinalizeWriteStream` 이후 append를 막고 row count를 확정한다.
`BatchCommitWriteStreams`는 pending stream을 atomic하게 visible 상태로 만든다.
Canonical RPC 계약은
[`BigQueryWrite`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite),
운영 순서는 [pending stream batch
load](https://cloud.google.com/bigquery/docs/write-api-batch)에 있다.

따라서 emulator에는 stream을 key로 하는 durable ledger가 필요하다. Schema
fingerprint, next offset, 수락 payload digest, final row count, state, staging
relation을 추적해야 한다. Process-global offset이나 임의 stream-map 조회는 동시
Spark task에서 올바르지 않다.

<!-- section: direct-at-least-once -->
## Direct Write: Default Stream과 At-least-once Mode

`writeAtLeastOnce=true`이면 connector `0.44.2`는 table의 `_default` stream을
사용하고 exact offset을 생략한다. Row는 finalize/batch commit 없이 visible해지지만
모호한 실패 후 retry하면 중복될 수 있다. Google은 [Storage Write streaming
semantics](https://cloud.google.com/bigquery/docs/write-api-streaming)에 이 차이를
정의한다.

로컬 테스트는 두 mode를 구분해야 한다. Default stream response offset을
제거했다는 사실만으로 at-least-once retry를 증명할 수 없다. Server side effect
이후 client가 response를 받기 전에 끊는 fault test가 필요하다.

<!-- section: overwrite-merge -->
## Direct Overwrite와 MERGE

Direct overwrite는 단순 append flag가 아니다. Connector는 temporary table에 쓴
뒤 destination row를 교체하는 `MERGE`를 제출하고 마지막에 정리할 수 있다.
Connector 조정 코드는
[`BigQueryClient.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java)에
있다.

BigQuery `MERGE`는 source/target match, 순서 있는 clause, atomic visibility를
결합한다. Constant-false predicate는 문서화된 replace 최적화지만 dynamic
partition overwrite는 expression, partition value, script, source-row
cardinality에도 의존한다. 권위 있는 규칙은 [GoogleSQL DML `MERGE`
레퍼런스](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)에
있다. Regex text substitution으로 일반 `MERGE`를 구현할 수 없다. Compatibility
rule은 정확한 connector template 하나를 인식하고 알 수 없는 SQL은 그대로
전달하거나 unsupported로 보고해야 한다.

<!-- section: indirect-write -->
## Indirect Write와 Load Job

`writeMethod=indirect`이면 executor가 GCS에 intermediate file을 쓰고 driver가
load configuration을 담은 `jobs.insert`를 제출하고 polling한 뒤 staging object를
정리한다. Connector 조정은
[`BigQueryWriteHelper.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/spark-bigquery-connector-common/src/main/java/com/google/cloud/spark/bigquery/write/BigQueryWriteHelper.java)에
있다.

올바른 emulator는 모든 source URI를 object-store port로 해석하고 immutable
input을 staging에 적재하며 schema/bad-record option을 검증한 뒤
`CREATE_IF_NEEDED`와 `WRITE_APPEND`, `WRITE_TRUNCATE`, `WRITE_EMPTY`를 하나의
destination transaction에서 적용해야 한다. BigQuery는 REST shape을
[`JobConfigurationLoad`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad),
format/type 동작을 [batch loading
문서](https://cloud.google.com/bigquery/docs/loading-data)에 정의한다. Parquet
file을 열었다는 사실은 BigQuery load 의미, job error, wildcard URI, atomic
visibility를 증명하지 않는다.

<!-- section: rest-jobs -->
## REST Job, Polling, Paging

`jobs.query`는 caller 관점에서 synchronous지만 job identity를 반환하고 result
polling이 필요할 수 있다. `jobs.insert`는 job을 먼저 저장한 뒤 asynchronous로
실행한다. 성공과 실패 모두 `DONE`이 되며 실패는 `errorResult`와 `errors`에
담긴다. Result page에는 안정된 opaque `pageToken`, total row count, schema,
BigQuery JSON cell shape가 필요하다. 공식 resource는
[`Job`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job)과
[`GetQueryResultsResponse`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults#response-body)다.

`startIndex` truncation만 구현한 것은 page-token 지원이 아니다. Result table
data가 DuckDB file에 있다는 이유만으로 in-memory job state가 영속화되지 않는다.

<!-- section: types -->
## Type 경계

Type은 BigQuery metadata, engine storage, REST JSON cell, Arrow/Avro/Proto wire
value라는 네 독립 mapping을 통과한다. Canonical type 정의와 범위는 [BigQuery
data types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)에
있다. NUMERIC/BIGNUMERIC precision, TIMESTAMP와 DATETIME, TIME microsecond, 특수
floating value, BYTES base64, JSON null과 SQL NULL, GEOGRAPHY transport, nested
STRUCT, repeated field, empty array, nullability가 중요하다.

DuckDB가 BIGNUMERIC이나 GEOGRAPHY를 text로 저장해도 canonical metadata는
BIGNUMERIC 또는 GEOGRAPHY로 남아야 한다. Query result encoding은 schema-aware
conversion을 사용해야 한다. List나 struct에 대한 `fmt.Sprint`는 BigQuery REST
row가 아니다.

<!-- section: authentication -->
## 인증과 인가

Service-account JSON, authorized-user ADC, external-account WIF는 token 획득
방식이 다르다. BigQuery REST/gRPC service는 최종적으로 Bearer token을 받는다.
ADC 검색 순서와 credential file type은 [Application Default
Credentials](https://cloud.google.com/docs/authentication/application-default-credentials),
WIF exchange는 [Workload Identity
Federation](https://cloud.google.com/iam/docs/workload-identity-federation)에 정의되어
있다.

로컬 OAuth/STS stub은 acquisition과 propagation을 테스트할 수 있다. Signature
trust, IAM role, permission inheritance, federation policy, token introspection,
production authorization을 에뮬레이션하지 않는다. TLS, authentication,
authorization은 서로 다른 capability 주장으로 유지해야 한다.

<!-- section: implementation-map -->
## 구현 매핑

| BigQuery/connector 단계 | 필요한 emulator 경계 | 현재 상태 |
| --- | --- | --- |
| REST metadata | catalog use case와 JSON transport | 기본 lifecycle, patch/update, paging, ETag verified |
| additive schema | schema validator와 warehouse transaction | top-level/nested/repeated-record addition verified |
| query job | job repository와 query-engine port | 공식 Python sync/async path verified, process-local 부분 구현 |
| CreateReadSession/ReadRows | snapshot/session ledger와 Arrow/Avro encoder | fake 기반 application/protobuf slice verified, DuckDB snapshot/encoder adapter가 없어 public runtime 미구현 |
| AppendRows/finalize/commit | stream별 ledger와 transaction coordinator | RPC 등록, 미구현 |
| indirect load | object store, staging, load disposition | 계획 |
| direct overwrite MERGE | 구조적인 connector-template adapter | 계획 |
| ADC/WIF | 선택적 token stub과 auth middleware | 계획 |

Capability 변경에는 공개 경계 테스트와 두 문서 언어의 호환성 갱신이 필요하다.
