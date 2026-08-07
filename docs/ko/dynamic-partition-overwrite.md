<!-- doc-id: dynamic-partition-overwrite -->
<!-- lang: ko -->

[English](../en/dynamic-partition-overwrite.md) | [한국어](dynamic-partition-overwrite.md)

# 동적 파티션 덮어쓰기

<!-- section: upstream-contract -->
## 업스트림 계약

지원 후보는 Spark connector `0.44.2`의
[`BigQueryUtil.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryUtil.java#L796-L870)가
생성하는 정확한 script다. 이 script는 `IGNORE NULLS`로 source의 고유 파티션
배열을 선언하고, `MERGE ... ON FALSE`로 source가 건드린 destination 파티션의
row를 삭제한 뒤 모든 source row를 삽입한다. 서비스 규칙은 [multi-statement
query](https://cloud.google.com/bigquery/docs/multi-statement-queries),
[`MERGE`](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement),
[DML transaction
semantics](https://cloud.google.com/bigquery/docs/data-manipulation-language#multi-statement_transactions)를
기준으로 한다.

이는 범용 script 변환기가 아니라 버전이 고정된 semantic adapter다. token,
alias, field list, relation, partition function 또는 trailing statement가 달라지면
model, capability 또는 gap, token index, expected shape, query digest, fix hint를
남기고 fail closed한다. SQL 원문은 log에 남기지 않는다.

<!-- section: execution-contract -->
## 현재 실행 계약

application은 context-aware resource-mutation gate 하나를 잡은 상태에서
destination과 source canonical table을 모두 조회한다. schema 검증과 DuckDB
transaction이 끝날 때까지 gate를 유지하므로 delete/recreate race를 차단한다.
취소된 waiter는 permit을 소비하지 않고 빠져나오며 이후 mutation이 같은 gate를
재사용할 수 있다.

transaction 시작 전에 선택된 모든 source field는 destination field의 canonical
BigQuery type, mode, nested name, nested order와 일치해야 한다. 문서화된 alias인
`BOOL`/`BOOLEAN`, `INTEGER`/`INT64`, `FLOAT`/`FLOAT64`, `STRUCT`/`RECORD`는 같은
type으로 정규화한다. 그 밖의 DuckDB implicit cast는 `invalidQuery`로 거부하고,
canonical resource가 없으면 `notFound`를 유지한다. Partition field는 connector의
truncation function 및 유효한 granularity와 함께 `DATE`, `TIMESTAMP`, `DATETIME`을
지원한다. Type 정의는 [BigQuery data
types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)를 따른다.

adapter는 delete와 insert를 하나의 명시적 DuckDB transaction에서 실행한다.
NULL source partition은 `IGNORE NULLS` 때문에 touched-partition 집합에서만
제외되며, 해당 source row 자체는 여전히 삽입된다. Log에는
begin/delete/insert/commit/rollback의 pre/post 경계, 정확한 transaction state,
affected-row count, duration, schema fingerprint, opaque resource fingerprint를
남긴다. Raw SQL, row, project, dataset, table, field 값은 남기지 않는다.

<!-- section: rest-contract -->
## REST Job 계약

승인된 operation은 일반 `jobs.insert` 및 `jobs.get` lifecycle을 통과한다. Query
statistic은 `statementType=SCRIPT`를 보고하며, 현재 제공 가능한 top-level 및
query-level affected-row 합계를 채운다. Error reason은 공식 [BigQuery error
table](https://cloud.google.com/bigquery/docs/error-messages)을 따른다. Schema/query
위반은 `invalidQuery`, resource 부재는 `notFound`, deadline은 `timeout`, 취소는
`stopped`, backend transaction 실패는 `jobBackendError`다.

<!-- section: stable-gaps -->
## 안정적으로 유지하는 Gap

BigQuery script는 child job과 script 전용 statistic을 노출한다. Child-job 열거,
`scriptStatistics`, statement별 `dmlStats`는 아직 구현하지 않았으며 wire 정의는
[`JobStatistics2`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobStatistics2)를
기준으로 남겨 둔다. 동적 range-partition overwrite도 등록된 unsupported gap이다.

공개 emulator endpoint를 대상으로 released Spark connector JAR의 direct-write 또는
indirect-write 동적 overwrite를 아직 증명하지 않았다. Unit test와 raw REST E2E는
semantic adapter, atomicity, NULL 동작, type, drift 거부, job reason을 증명하지만
connector 증거는 아니다. 따라서 released artifact를 내려받아 URL, version, size,
SHA-256을 기록하고 정제된 endpoint 증거를 남기기 전까지 connector profile,
golden fixture, compatibility matrix 및 direct/indirect E2E row는 gap으로 유지한다.

<!-- section: promotion-gates -->
## 승격 Gate

이후 connector release에는 `0.44.2` model을 느슨하게 바꾸지 말고 새 source-pinned
parser model을 추가한다. 승격에는 positive/negative token fixture,
destination/source schema drift, DATE/TIMESTAMP/DATETIME 및 NULL case, cancel과 lock
reuse, rollback 증거, opaque log assertion, raw REST E2E, released-JAR direct 및
indirect E2E가 필요하다. 이 조건을 모두 만족한 뒤에만 versioned profile, golden,
compatibility matrix를 gap에서 verified로 바꿀 수 있다.
