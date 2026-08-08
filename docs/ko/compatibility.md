<!-- doc-id: compatibility -->
<!-- lang: ko -->

[English](../en/compatibility.md) | [한국어](compatibility.md)

# 호환성 범위

REST 메서드와 경로, Storage RPC의 정확한 지원 범위는 엄격한 operation
매니페스트에서 생성합니다. 지원 상태와 검증 수준, 제한 사항, 관련 이슈, 수집 테스트는
[API 및 RPC 호환성](api-rpc-compatibility.md)에서 확인할 수 있습니다.

<!-- section: status-language -->
## 상태 용어

| 상태 | 의미 |
| --- | --- |
| 검증됨 (`Verified`) | 명시한 공개 API 또는 어댑터 경계에서 구현하고 실행했습니다. |
| 부분 지원 (`Partial`) | 일부 기능을 사용할 수 있으며 중요한 제한을 함께 설명합니다. |
| 등록됨 (`Registered`) | 공식 서비스는 등록했지만 작업은 `UNIMPLEMENTED`를 반환합니다. |
| 계획 (`Planned`) | 설계와 출처는 있지만 사용자가 아직 의존하면 안 됩니다. |
| 미지원 (`Unsupported`) | 기능이 없거나 요청을 의도적으로 거부합니다. |

이 상태는 이 저장소에서 확인한 범위만 설명합니다. [BigQuery
서비스](https://cloud.google.com/bigquery/docs/introduction) 전체와 동등하다는 뜻은
아닙니다.

<!-- section: rest-metadata -->
## REST 메타데이터

| 기능 | 상태 | 지원 범위 |
| --- | --- | --- |
| 활성 및 준비 상태 | 검증됨 | 프로세스와 웨어하우스 연결을 확인합니다. |
| 에뮬레이터 프로젝트 수명 주기 | 검증됨 | 에뮬레이터 전용 네임스페이스를 사용합니다. |
| `projects.list` | 기본 검증 | 에뮬레이터 프로젝트와 불투명한 페이지 토큰을 지원합니다. |
| 데이터 세트 생성 및 조회 | 기본 검증 | 위치, 라벨, 기본 만료 시간을 보존합니다. |
| 데이터 세트 목록 및 삭제 | 기본 검증 | 페이지 나누기와 `deleteContents`를 지원합니다. 필터와 `all`은 지원하지 않습니다. |
| 데이터 세트 부분 및 전체 수정 | 검증됨 | 메타데이터 필드, ETag, HTTP 412 사전 조건을 지원합니다. |
| 테이블 생성, 조회, 삭제 | 기본 검증 | 표준 테이블과 기준 스키마 메타데이터를 지원합니다. |
| 테이블 목록 | 기본 검증 | 페이지 나누기를 지원합니다. 뷰와 저장 통계는 제공하지 않습니다. |
| 테이블 부분 및 전체 수정 | 제한 검증 | 메타데이터, 스키마 필드 추가, ETag 사전 조건을 지원합니다. |
| `tabledata.list` | 부분 지원 | `f/v` 행과 페이지 단위 조회를 지원합니다. 세부 제한은 아래에 설명합니다. |
| `tabledata.insertAll` | 미지원 | 경로를 제공하지 않습니다. |

요청과 응답 구조는 공식
[`datasets`](https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets)와
[`tables`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables)
리소스를 기준으로 비교합니다. 알 수 없는 JSON 필드를 무시하고 해석할 수는 있지만,
해당 필드의 동작까지 구현했다는 의미는 아닙니다.

[`tabledata.list`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list)
어댑터는 `tables.get`과 Storage Read에서 사용하는 것과 같은 카탈로그 만료 검사를
수행합니다. 이후 DuckDB 트랜잭션 하나에서 전체 행 수를 계산하고 순서에 따라 페이지를
선택합니다. 파일 설정 `tableData.maxPageRows`가 요청값보다 작으면 요청보다 적은 행을
반환할 수 있습니다. `tableData.operationTimeout`에는 카탈로그 잠금 대기, 만료 시간
확인, DuckDB 작업이 모두 포함됩니다.

DuckDB는 설정한 행 수까지만 스트리밍하고 기준 자료형의 페이지를 순서대로 줄입니다.
백엔드 JSON에는 공개 `f/v` 행에 없는 열 이름도 포함되므로, 그 크기로 프로토콜 응답
크기를 판단하지 않습니다. REST는 승인한 조각을 스트리밍하면서 일반 페이지에는
`maxResponseBytes`, 단일 행에는 절대 상한인 `maxRowBytes`를 적용합니다. 로컬의
10,000,000바이트와 100,000,000바이트 규칙은 Cloud의 근사 [pagination
limit](https://cloud.google.com/bigquery/docs/paging-results#api-limits)을 재현합니다.

스칼라, 중첩, 반복 `f/v` 행을 지원합니다. 유한하지 않은 `FLOAT64` 표기,
`startIndex`, 행 수와 실제 바이트 수 제한, 리소스에 묶인 불투명 토큰, ETag 사전 조건,
`totalRows`, `useInt64Timestamp`도 지원합니다. 데이터 변경에 따른 페이지 무효화,
`selectedFields`, `timestampOutputFormat`은 지원하지 않습니다.

`formatOptions.useInt64Timestamp=true`이면 Unix epoch 마이크로초 문자열을
반환합니다. `maxResults=0`을 명시하면 정확한 `totalRows`와 빈 페이지 하나를 반환하며
이어받기 토큰은 만들지 않습니다. epoch 전후의 마이크로초 값은 모두 같은 UTC
표현을 사용합니다.

유한한 `FLOAT64` 셀은 JSON 숫자를 사용합니다. 그 밖의 값은 공식
[`StandardSqlDataType`](https://cloud.google.com/bigquery/docs/reference/rest/v2/StandardSqlDataType)
계약에 정의된 `NaN`, `Infinity`, `-Infinity` 표기를 사용합니다.

`CAP-REST-METADATA-PATCH-V1`과 `CAP-SCHEMA-ADDITIVE-V1`은 공개 전송 계층에서
검증합니다. 스키마 변경은 중첩 및 반복 레코드를 포함하여 `NULLABLE` 또는
`REPEATED` 필드를 끝에 추가하는 방식만 지원합니다. DDL 변환, 필수 여부 완화,
작업에서 요청하는 스키마 변경까지 지원하는 것은 아닙니다.

<!-- section: jobs -->
## 쿼리와 작업

### 작업 API

| 기능 | 상태 | 지원 범위 |
| --- | --- | --- |
| `jobs.query` | 부분 지원 | 통합 GoogleSQL AST 부분집합을 통해 동기 실행합니다. |
| 쿼리 `jobs.insert` | 부분 지원 | 비동기 상태 조회를 지원합니다. 실행은 현재 프로세스가 담당하며 구성, 상태, 오류, 시각, 통계는 SQLite에 저장합니다. |
| `jobs.get` | 기본 검증 | `PENDING`, `RUNNING`, `DONE`과 최종 오류를 지원합니다. |
| `jobs.list` | 부분 지원 | 위치를 포함해 SQLite에 저장한 작업 메타데이터와 불투명한 커서 토큰을 지원합니다. |
| `jobs.getQueryResults` | 부분 지원 | 위치 기반 조회, `startIndex`, `maxResults`, 작업과 결과에 묶인 불투명한 페이지 토큰을 지원합니다. |

### 대상 테이블과 쿼리 메타데이터

| 기능 | 상태 | 지원 범위 |
| --- | --- | --- |
| 명시적 대상 테이블 | 부분 지원 | 스칼라 결과와 같은 스키마에서 `WRITE_EMPTY`, `WRITE_APPEND`, `WRITE_TRUNCATE`를 지원합니다. ID는 `query.destination.exact-schema-v1`입니다. |
| 쿼리 메타데이터 | 기본 검증 | `INTERACTIVE` 및 `BATCH` 우선순위와 검증한 라벨을 해시와 왕복 결과에 반영합니다. 빈 라벨 맵도 보존합니다. |
| 익명 대상 테이블 | 부분 지원 | 행을 반환하는 쿼리는 24시간 뒤 만료되는 숨김 데이터 세트의 테이블을 만들고 공개합니다. ID는 `query.destination.anonymous-v1`입니다. |
| `WRITE_TRUNCATE` 스키마 교체 | 미지원 | 같은 스키마만 지원합니다. 미지원 ID는 `query.destination.truncate-schema-replacement-v1`입니다. |

### 실행 제어와 영속성

| 기능 | 상태 | 지원 범위 |
| --- | --- | --- |
| 의미 기반 SQL DDL | 부분 지원 | GoogleSQL AST 계획으로 `CREATE TABLE`, `DROP TABLE`, `TRUNCATE TABLE`, 최상위 `ADD`, `RENAME`, `DROP COLUMN`, `ALTER COLUMN SET DATA TYPE`을 실행합니다. 지원하지 않는 절은 변경 전에 `query.ddl.catalog-sync-v1`로 거부하며 SQLite와 엔진 사이의 중단 복구는 #26에 남아 있습니다. |
| 여러 문장으로 된 쿼리 | 부분 지원 | `DECLARE`, `SET`, 지원하는 쿼리 및 DML을 한 트랜잭션에서 실행합니다. 제어 흐름, 동적 SQL, 임시 루틴은 공식 [여러 문장 쿼리 계약](https://cloud.google.com/bigquery/docs/multi-statement-queries)의 미지원 범위입니다. |
| 취소 | 부분 지원 | 종료 과정에서는 새 작업을 거부하고 실행 중인 작업을 취소한 뒤 Storage와 DuckDB를 닫습니다. 공개 [`jobs.cancel`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/cancel)과 취소 상태는 지원하지 않습니다. |
| Parquet 로드 `jobs.insert`, `jobs.get`, `jobs.list` | 부분 지원 | 항상 사용할 수 있습니다. 구성, 상태, 오류, 시각, 통계는 SQLite에 저장합니다. |
| 복사와 추출 | 미지원 | 해당 설정을 거부합니다. |
| 작업과 결과의 영속 상태 | 부분 지원 | 쿼리와 로드 작업 메타데이터는 재시작 후 복구합니다. 쿼리 결과 행은 저장하지 않으며, 재시작한 비어 있지 않은 결과는 빈 성공 대신 명시적인 `backendError`를 반환합니다. |
| 쿼리 결과 보관 크기 제한 | 미지원 | 모든 결과 행을 Go 메모리에 보관합니다. 미지원 ID는 `query.results.unbounded-memory-v1`입니다. |
| 복합 쿼리 결과 스키마 | 부분 지원 | `ARRAY`와 `STRUCT` 스키마, 중첩 및 반복 `TableRow` 셀을 보존합니다. 자료형을 구분할 수 없는 10진수 하위 필드와 지원하지 않는 물리 스키마는 메타데이터 반영 전에 거부합니다. |
| 비동기 쿼리 실행 시간 제한 | 부분 지원 | `query.operationTimeout`으로 동기 및 비동기 실행을 제한합니다. 종료 시 승인 거부, 취소, 대기를 처리합니다. 작업자 용량과 요청의 정확한 `timeoutMs` 동작은 미지원이며 ID는 `query.execution.bounded-v1`입니다. |
| 같은 ID의 쿼리 등록 | 기본 검증 | `(project, location, jobId)`를 원자적으로 중복 검사합니다. 모든 재사용은 `409 duplicate`이며 해시는 진단에만 사용합니다. |
| 같은 요청 재실행 확장 기능 | 미지원 | 이후 별도 활성화 기능으로 계획합니다. 미지원 ID는 `query.jobs.exact-replay-extension-v1`입니다. |
| 쿼리와 로드 사이의 ID 중복 검사 | 미지원 | 분리된 저장소의 확인 및 생성 사이에 경쟁 상태가 있습니다. 미지원 ID는 `query.jobs.cross-repository-identity-v1`입니다. |
| 동기 요청 제어 | 부분 지원 | 36바이트 ASCII `requestId`와 음수가 아닌 `timeoutMs`를 받습니다. 미완료 응답 대기 제한, 변경 쿼리 중복 제거, `jobTimeoutMs`는 미지원이며 ID는 `query.sync.request-controls-v1`입니다. |
| 지원하지 않는 쿼리 옵션 | 엄격히 거부 | 매개변수, `dryRun`, 캐시 및 비용 제어, `jobTimeoutMs`는 `400`으로 거부합니다. 미지원 ID는 `query.options.unsupported-v1`입니다. |
| 위치를 생략한 데이터 세트 추론 | 부분 지원 | 구조적으로 참조한 테이블, 다른 프로젝트의 `defaultDataset.projectId`, 명시적 대상 데이터 세트를 실행 전에 검사합니다. ID는 `query.location.dataset-inference-v1`입니다. |
| 최종 상태 저장 실패 복구 | 부분 지원 | 시작할 때 중단된 `PENDING`과 `RUNNING` 작업을 최종 오류로 바꿉니다. 모든 교차 저장소 실패 지점의 복구는 `query.terminal-persistence-v1`에 남아 있습니다. |

작업 상태와 오류 필드는 공식 [`Job`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job)
리소스를 기준으로 합니다. 중첩 및 반복 결과 셀은 재귀적인
[`TableRow`](https://cloud.google.com/bigquery/docs/reference/rest/v2/TableRow)
형식으로 인코딩합니다. 자료형별 날짜 및 시간 값은 BigQuery의 전체 범위를 아직
지원하지 않습니다. 스칼라 쿼리와 `tabledata.list` 행에서 유한한
`FLOAT64`는 JSON 숫자로 반환합니다. 그 밖의 값은 공식
[`StandardSqlDataType`](https://cloud.google.com/bigquery/docs/reference/rest/v2/StandardSqlDataType)에
정의된 유한하지 않은 `FLOAT64` 표기를 사용합니다.

명시적 대상 테이블은
[`JobConfigurationQuery`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery)를
기준으로 합니다. 결과 토큰은
[`jobs.getQueryResults`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults)를
기준으로 합니다. 공식
[`QueryRequest`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#QueryRequest)와
`JobConfigurationQuery`에 정의되었지만 아직 지원하지 않는 필드는 REST 계층에서 입력
여부를 보존합니다. 해당 필드가 들어오면 실행 전에 실패하며, 기본값으로 간주해
넘기지 않습니다.

BigQuery는 이미 사용한 모든 작업 ID를 `409 duplicate`로 거부하고 `jobs.get`으로
복구하도록 안내합니다. BQEMU도 이 동작을 기본값으로 사용합니다. 설정 해시는 계약
불일치를 안전하게 진단할 때만 사용합니다. 자세한 내용은 공식 [재시도
안내](https://cloud.google.com/bigquery/docs/reliability-intro#retry_failed_job_insertions)를
참고합니다.

`destinationTable`이 없는 쿼리가 행을 반환하면 `JobRepository.CreateOrGet`을
호출하기 전에 대상 테이블을 만듭니다. 생성한 테이블을
`configuration.query.destinationTable`로 반환하고, `WRITE_EMPTY`와
`CREATE_IF_NEEDED`로 결과를 저장합니다.

생성한 데이터 세트의 이름은 `_`로 시작합니다. [`all=true`](https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list)를
지정하지 않으면 `datasets.list`에서 숨깁니다. 테이블은 BigQuery의 대략적인 [익명 테이블
수명](https://cloud.google.com/bigquery/docs/cached-results#how_cached_results_are_stored)에
맞춰 메타데이터를 반영한 지 24시간 뒤의 만료 시간을 제공합니다.

만료된 결과는 `tables.get`, `tables.list`, Storage Read에서 테이블을 찾을 때
동기적으로 정리합니다. 숨김 데이터 세트는 다음 결과를 위해 남겨 둡니다. 캐시 적중
결과 재사용, 백그라운드 정리 작업, 재시작 후 복구할 수 있는 만료 기록은 아직
없습니다. 별도 정리 고루틴이나 `Close` 순서도 없으며 각 요청이 정리를 끝냅니다.

ID를 알고 있는 숨김 데이터 세트에는 일반 삭제 규칙을 적용합니다. 사용 가능한
테이블이 있으면 `deleteContents=true`가 필요합니다. 조회 과정에서 만료된 테이블을
정리해 데이터 세트가 비면 일반 삭제가 성공합니다.

작업을 등록하기 전에 공식 GoogleSQL analyzer가 지원하는 quoted/unquoted table path,
다른 프로젝트를 지정한 `defaultDataset.projectId`, 명시적 대상 데이터 세트를
해석합니다. 위치를 생략하면 모든 데이터 세트에 공통된 위치를
사용합니다. 명시한 위치와 추론한 위치가 다르면 저장소나 엔진을 변경하기 전에
실패합니다. 이 동작은 BigQuery [location
규칙](https://cloud.google.com/bigquery/docs/locations#specify_locations)을 따릅니다.

연결, 원격 함수, table decorator, 동적 SQL에서는 위치를 추론하지 않습니다. 지원하는
relation이 없을 때만 설정한 기본 위치를 사용합니다.

<!-- section: sql -->
## SQL과 MERGE

| 동작 | 상태 | 제한 |
| --- | --- | --- |
| 기준 테이블 참조 | 검증 완료 | 공식 analyzer가 binding하며 엔진이 경로를 다시 추론하지 않습니다. |
| `SELECT`, `INSERT`, `UPDATE`, `DELETE` | 부분 지원 | 지원하는 AST node, operator, function, type만 실행합니다. |
| `MERGE` | 부분 지원 | 순서가 있는 matched/not-matched action, 첫 번째로 만족한 `WHEN`, 원본 행 대응 개수 오류, 항상 거짓인 교체를 지원하며 나머지는 실행 전에 안전하게 거부합니다. |
| 여러 명령문 script | 부분 지원 | `DECLARE`, `SET`, 지원하는 query/DML child를 한 트랜잭션에서 실행하며 제어 흐름과 임시 routine은 미지원입니다. |
| catalog DDL | 부분 지원 | create/drop/truncate와 문서에 적은 column mutation을 지원합니다. |
| 동적 파티션 덮어쓰기 | 부분 지원 | typed array script를 한 트랜잭션에서 실행하며 DATE/TIMESTAMP/DATETIME과 정수 범위 파티션 교체를 지원합니다. 추가 expression은 #8에 남아 있습니다. |
| parameter, view, UDF, procedure | 미지원 | 별도로 추적하며 raw SQL fallback은 없습니다. |

[GoogleSQL lexical
계약](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical)은 identifier,
comment, string, relation, expression을 구문 위치로 구분합니다. 하나의 공식
parse/analyze gateway가 이 구조를 불변 semantic statement로 옮깁니다. DuckDB visitor는
그 statement에서 어댑터 내부 SQL과 bind argument를 만들며 원문을 tokenize하거나
재시도하지 않습니다.

일반 `MERGE`는 [공식 DML
규칙](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)을
따릅니다. 구현한 부분집합은 clause 순서와 단일 transaction을 보존합니다. 대상
UPDATE 또는 DELETE 전에 여러 원본 행의 대응을 거부하고 동적 시간·정수 범위 교체에
필요한 typed array와 function 구조를 지원합니다. 지원하지 않는 expression과 action은
엔진 부작용 전에 거부합니다.

<!-- section: types -->
## 자료형

| BigQuery 자료형 | DuckDB 테이블 생성 | REST 쿼리 값 | 상태 |
| --- | --- | --- | --- |
| BOOL/INT64/FLOAT64/STRING/BYTES | 기본 자료형으로 변환 | 스칼라 값으로 인코딩 | 부분 지원 |
| NUMERIC | `DECIMAL(P,S)`, 기본값 `DECIMAL(38,9)` | 정확한 10진수 문자열 | 부분 지원 |
| BIGNUMERIC | `DECIMAL(P,S)`, 기본값 `DECIMAL(38,18)` | 정확한 10진수 문자열 | 정밀도 38까지 부분 지원 |
| DATE/DATETIME/TIME/TIMESTAMP | 엔진 자료형으로 변환 | 날짜 및 시간 형식 변환 미완성 | 부분 지원 |
| JSON | JSON으로 변환 | 의미를 완전하게 보존하지 못함 | 부분 지원 |
| GEOGRAPHY | 저장소를 변경하기 전에 거부 | 사용할 수 없음 | 미지원 |
| RECORD/REPEATED | 재귀적인 STRUCT 또는 LIST로 변환 | 스키마에 따라 중첩 및 반복 셀 인코딩 | 부분 지원 |

자료형 호환성은 [BigQuery data
types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)를
기준으로 평가합니다. 카탈로그와 REST 메타데이터는 두 10진수 자료형의 매개변수 생략
여부를 그대로 보존합니다. 실제 기본값은 저장 엔진이나 전송 형식으로 변환할 때만
적용합니다.

`roundingMode`도 생략 여부와 `ROUNDING_MODE_UNSPECIFIED`,
`ROUND_HALF_AWAY_FROM_ZERO`, `ROUND_HALF_EVEN`을 구분하여 메타데이터에
보존합니다. ProtoRows 쓰기는 정확한 10진수 연산으로 BigQuery의 기본 방식인
0에서 먼 쪽 반올림과 명시적인 짝수 쪽 반올림을 적용합니다. STRUCT와 REPEATED 내부의
값에도 같은 규칙을 적용합니다.

Parquet 로드는 대상 스키마에 자릿수를 줄이지 않고 넣을 수 있는 10진수 원본을
허용합니다. 원본의 자릿수를 줄여야 하면 대상을 변경하기 전에
`load.decimal-rounding.unsupported-v1`로 거부합니다. 쿼리 결과를 대상 스키마에
쓸 때는 유효 정밀도와 소수부 자릿수가 정확히 같아야 합니다. 자릿수를 줄여야 하면
`query.destination.decimal-rounding.unsupported-v1`, 그 밖의 스키마 차이는
`query.destination.exact-schema-v1`로 거부합니다. Parquet 제한은
[이슈 #5](https://github.com/leeyh0216/go-bemu/issues/5),
쿼리 반올림과 10진수 계보는 [이슈 #27](https://github.com/leeyh0216/go-bemu/issues/27)에서
관리합니다.

테이블의 `defaultRoundingMode`는 이후 추가하는 10진수 필드가 생략한 반올림 방식을
바꿉니다. [이슈 #21](https://github.com/leeyh0216/go-bemu/issues/21)과
[이슈 #26](https://github.com/leeyh0216/go-bemu/issues/26)에서 테이블 기본값과 복구를
구현하기 전까지 tables.insert, tables.patch, tables.update는 이 속성을 카탈로그나
엔진에 반영하기 전에 `schema.table-default-rounding-mode.unsupported-v1`로 거부합니다.
테이블 기본값을 생략하고 필드별 반올림 방식을 명시하는 동작은 지원합니다.

정밀도가 38보다 크면 현재 실행 환경이 표현할 수 없으므로 테이블, 로드 작업, 행
데이터를 변경하기 전에 요청을 거부합니다. `GEOGRAPHY`도 저장소를 변경하기 전에
거부합니다.

NUMERIC과 지원 범위 안의 BIGNUMERIC은 REST 테이블, 쿼리, `tabledata` 셀에서
동작합니다. Storage Read의 Arrow/Avro 스키마와 값도 지원합니다. 직접 ProtoRows
쓰기와 스칼라 Parquet 로드도 지원합니다. REST, Storage Read, Storage Write에서는
STRUCT 내부와 REPEATED 필드의 10진수 메타데이터를 재귀적으로 유지합니다.

매개변수를 생략한 BIGNUMERIC은 물리 및 전송 경계에서 정밀도 38과 소수부 자릿수
18을 적용합니다.

새 쿼리 결과에는 한 가지 제한이 있습니다. DuckDB의 `DECIMAL(P,S)`만으로는 NUMERIC
범위에 들어가는 값의 원래 자료형이 NUMERIC인지 BIGNUMERIC인지 알 수 없습니다. 지원하는
기존 대상 테이블이 있으면 카탈로그 스키마에서 스칼라, 중첩 STRUCT, REPEATED 자료형을
복원합니다. NUMERIC 범위를 벗어나는 물리 스키마는 BIGNUMERIC으로 명확하게 구분할 수
있습니다. 그 밖의 모호한 쿼리 결과는
#27에서 계보 메타데이터를 제공하기 전까지 메타데이터 반영 전에
`query.results.decimal-lineage-v1`로 거부합니다.

<!-- section: storage-read -->
## Storage Read

| RPC/동작 | 상태 |
| --- | --- |
| 공식 서비스 등록과 리플렉션 | 검증됨 |
| 읽기 서비스 상태 | 활성화되어 있고 연결 종료 전이면 수명 주기에 따라 `SERVING`을 반환합니다. |
| 공개 `CreateReadSession`, `ReadRows` | 부분 지원입니다. 세션마다 크기에 상한을 둔 DuckDB 결과 하나를 만듭니다. |
| 공개 `SplitReadStream` | 미지원이며 `UNIMPLEMENTED`를 반환합니다. |
| Arrow/Avro 스키마와 행 데이터 | 부분 지원입니다. 행과 응답 바이트 수에 상한을 두고 DuckDB 결과를 인코딩합니다. |
| 열 선택과 행 제한 | 재귀 STRUCT/REPEATED 선택은 카탈로그 순서를 유지합니다. 필터는 논리식, 비교, `IN`, `BETWEEN`, NULL 검사, `LIKE`, 스칼라 캐스트를 지원하지만 함수와 하위 쿼리는 지원하지 않습니다. |
| 논리 스트림과 오프셋 재개 | 실행 중인 세션에서 고정된 범위와 스트림 기준 오프셋을 지원하며 수명 주기 메타데이터는 SQLite에 저장합니다. |
| 과거 시점 스냅샷과 압축 | 지원하지 않습니다. |

공개 기능은 일부만 지원합니다. 실행 중인 세션마다 고정된 DuckDB 결과 하나를
소유하며, 설정한 수의 논리 스트림으로 나눕니다. 결과 크기에는 상한을 둡니다.

스트림 분할 RPC, 전송 압축, 과거 시점의 `snapshot_time`은 지원하지
않습니다. 재시작 후 만료되지 않은 이전 스트림은 `UNAVAILABLE`, 만료된 스트림은
`NOT_FOUND`를 반환하며 snapshot 행 데이터는 다시 만들지 않습니다. 목표 동작은 공식
[`BigQueryRead`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryRead)
서비스를 기준으로 합니다.

<!-- section: storage-write -->
## Storage Write

| RPC/동작 | 상태 |
| --- | --- |
| 공식 서비스 등록과 리플렉션 | 검증됨 |
| 쓰기 서비스 상태 | 활성화되어 있고 연결 종료 전이면 수명 주기에 따라 `SERVING`을 반환합니다. |
| `PENDING` 생성, 조회, 추가, 종료, 커밋 | 부분 지원입니다. `ProtoRows`, 정확한 오프셋, 숨김 DuckDB 준비 영역, 종료된 행 수를 지원합니다. |
| 기본 스트림 | 부분 지원입니다. 공식 리소스 이름으로 행을 즉시 추가합니다. |
| 여러 논리 스트림 | 부분 지원입니다. 직렬화된 DuckDB 조정자 하나에서 처리 중 요청과 준비된 바이트 수를 가중치로 제한합니다. |
| 원자적 일괄 커밋 | 검증한 그룹의 대상 테이블 삽입과 준비 데이터 및 확인 기록 삭제를 트랜잭션 하나에서 처리합니다. |
| `ArrowRows`, `BUFFERED`, 명시적 `COMMITTED`, `FlushRows` | 지원하지 않습니다. |

CDC, 누락된 값의 기본 표현식, 한쪽 저장소만 복원한 상태의 증명, 분산 쓰기 동시성은
지원하지 않습니다. SQLite는 스트림, 확인 기록, 커밋 그룹의 단계를 영속화하며 시작할
때 중단된 작업을 미확정 상태로 분류한 뒤 요청을 받습니다. `PENDING` 행을 디코딩된 Go
객체로 계속 쌓지는 않습니다. 그러나 준비 영역의 바이트 계산값은 DuckDB가 사용하는
실제 물리 크기가 아니라 일관된 논리적 크기입니다.

백엔드 쓰기를 직렬화하는 것은 내장형 엔진을 사용하면서 둔 의도적인 제한입니다.
BigQuery와 같은 처리량을 보장하지 않습니다. 목표 동작은 공식
[`BigQueryWrite`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite)
서비스를 기준으로 합니다.

<!-- section: load-auth -->
## 로드, 객체 저장소, 공개 접근

| 기능 | 상태 |
| --- | --- |
| 허용하는 로드 원본 URI | `gs://`만 허용합니다. 로컬 경로와 다른 scheme은 작업을 저장하기 전에 거부합니다. |
| fake GCS 서비스 | 기본 Compose 프로젝트의 필수 서비스입니다. 바이너리는 설정한 GCS 호환 JSON endpoint에 연결합니다. |
| GCS 및 fake GCS JSON 어댑터 | 목록, 조회, 미디어 요청의 크기에 상한을 둡니다. URI 글로브 확장은 부분 지원입니다. |
| 기존 테이블로 Parquet 로드 | 스칼라 필드만 부분 지원합니다. 명시한 스키마와 형 변환을 검사합니다. 중첩 또는 반복 필드는 객체를 읽기 전에 `load.parquet.nested-repeated.unsupported-v1`로 거부하며, 10진수 자릿수 축소는 대상을 변경하기 전에 `load.decimal-rounding.unsupported-v1`로 거부합니다. |
| Avro, ORC, CSV, NDJSON 로드 | 지원하지 않습니다. 작업은 최종 `notImplemented` 오류를 반환합니다. |
| `WRITE_APPEND`, `WRITE_EMPTY`, `WRITE_TRUNCATE` | DuckDB 트랜잭션 하나에서 실행하도록 검증했습니다. |
| 대상 생성, 자동 감지, `schemaUpdateOptions`, 멀티파트 및 재개 다운로드 | 지원하지 않습니다. |
| REST/gRPC TLS | 설정하면 사용할 수 있습니다. |
| BigQuery 호환 엔드포인트 인증 | 의도적으로 제공하지 않습니다. 인증 정보가 없거나 임의 값 또는 잘못된 형식이어도 무시합니다. |
| 폐기 가능한 service account, authorized user, WIF, direct token 파일 | 루프백 전용 개발 도구인 `bqemu-auth-fixture`로 생성할 수 있습니다. |
| OAuth/STS token 획득 | 생성한 로컬 인증 파일에 대해서만 제공합니다. Google 사용자 정보와 IAM 제어 영역은 재현하지 않습니다. |
| 진단용 관리 토큰 | 선택 사항이며 별도의 관리 수신기에만 적용합니다. |
| IAM 권한 확인 | 지원하지 않습니다. |

로드 동작은
[`JobConfigurationLoad`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad)를
기준으로 합니다. 로드 경로는 크기가 정해져 있고 변경할 수 없는 객체를
전용 임시 작업 공간에 내려받습니다. 이후 선택한 쓰기 방식을 원자적으로 적용합니다.
다운로드는 대상 테이블 트랜잭션 밖에서 실행합니다. 로드 작업 메타데이터와 멱등성
식별 정보는 SQLite에 저장하며 내려받은 객체와 임시 작업 공간은 저장하지 않습니다.

업로드하는 프로세스와 BQEMU는 GCS endpoint를 서로 독립적으로 설정합니다. 두 설정은
같은 객체 저장소 서비스를 가리켜야 합니다. Compose 설정은
[시작하기](getting-started.md)를 참고해 주십시오.

공개 API 경계는 `Authorization` 헤더와 메타데이터 값을 해석하거나 검증하지
않습니다. 클라이언트 인증 정보 요구 사항, TLS, 별도의 진단용 관리 토큰, IAM은 서로
다른 호환성 범위로 다룹니다. 생성 파일 형식, 수신 주소 계약, 엄격한 클라이언트
설정은 [로컬 클라이언트 인증 파일과 TLS](client-credentials-and-tls.md)에 설명되어
있습니다.

<!-- section: persistence-atomicity -->
## 영속성과 원자성

DuckDB 파일은 물리 테이블 행을 보존합니다. SQLite는 기준 카탈로그, 쿼리 및 로드
작업 메타데이터, Storage Read 수명 주기 메타데이터, Storage Write 원장을 보존합니다.
쿼리 결과 행과 Storage Read snapshot 바이트는 프로세스 메모리에만 있습니다.

교차 저장소 카탈로그 변경은 모든 실패 지점에서 아직 중단 안전성을 보장하지
않습니다. 로드의 쓰기 방식 적용, 기본 스트림 추가, 검증된 `PENDING` 스트림 그룹
커밋은 각각 대상 테이블 트랜잭션을 사용합니다. 시작할 때 중단된 작업과 쓰기 원장
단계를 조정하지만, 상태 파일 한쪽만 복원한 경우는 지원하지 않습니다.

<!-- section: client-coverage -->
## 통합 검증 범위

제품 호환성은 operation 매니페스트와 위 기능 표로 정의합니다. 외부 실행 파일의
정확한 버전, 변경되지 않는 아티팩트, scenario ID, 기대 호출, 생성 증거는
[통합 테스트 프레임워크](../../tests/integration/docs/ko/framework.md)와
[소비자 호환성](../../tests/integration/docs/ko/consumer-compatibility.md)에서 관리합니다.
통합 테스트는 공개 프로세스를 검증하지만 제품 실행 환경의 의존성은 아닙니다. 지원
상태를 높이려면 공개 프로세스 증거와 실패 또는 경계값 테스트가 모두 필요합니다.

<!-- section: removal-criteria -->
## 호환성 예외 처리 제거 기준

호환성 예외 처리는 원래 동작을 재현할 수 있어야 합니다. 해당 통합 사례가 더 이상
예외를 필요로 하지 않고, 규칙을 제거한 뒤 공개 operation 기록이 일치해야 제거할 수
있습니다.

예외 처리를 더 넓은 입력에 적용하려면 정규식 예제 하나만으로는 부족합니다. 프로토콜
정의나 구문의 의미를 설명하는 출처가 필요합니다.
