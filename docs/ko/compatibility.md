<!-- doc-id: compatibility -->
<!-- lang: ko -->

[English](../en/compatibility.md) | [한국어](compatibility.md)

# 호환성 범위

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
| 활성 및 준비 상태 | 검증됨 | 프로세스, SQLite 상태와 웨어하우스 연결을 확인합니다. |
| 에뮬레이터 프로젝트 수명 주기 | 검증됨 | 에뮬레이터 전용 네임스페이스를 사용합니다. |
| `projects.list` | 기본 검증 | 에뮬레이터 프로젝트와 불투명한 페이지 토큰을 지원합니다. |
| 데이터 세트 생성 및 조회 | 기본 검증 | 위치, 라벨, 기본 만료 시간을 보존합니다. |
| 데이터 세트 목록 및 삭제 | 기본 검증 | 페이지 나누기, `deleteContents`, 숨김 데이터 세트 `all`, `labels.<name>[:<value>]`를 공백으로 AND한 label filter를 지원합니다. |
| 데이터 세트 부분 및 전체 수정 | 검증됨 | 메타데이터 필드, ETag, HTTP 412 사전 조건을 지원합니다. |
| 테이블 생성, 조회, 삭제 | 기본 검증 | 표준 테이블과 기준 스키마 메타데이터를 지원합니다. `tables.get`은 `BASIC`과 최상위 `selectedFields`를 지원하며 저장 통계 view는 명시적으로 미지원입니다. |
| 테이블 목록 | 기본 검증 | 페이지 나누기를 지원합니다. 뷰와 저장 통계는 제공하지 않습니다. |
| 테이블 부분 및 전체 수정 | 제한 검증 | 메타데이터, 스키마 필드 추가, ETag 사전 조건을 지원합니다. |
| `tabledata.list` | 부분 지원 | `f/v` 행과 페이지 단위 조회를 지원합니다. 세부 제한은 아래에 설명합니다. |
| `tabledata.insertAll` | 부분 지원 | 스키마 기반 scalar/nullable/temporal/decimal/bytes/nested/repeated JSON 행, atomic 배치, 행 index별 `insertErrors`, 범위가 제한된 SQLite `insertId` 재시도 ledger와 공식 Python/pandas client 검증을 지원합니다. `skipInvalidRows`, `ignoreUnknownValues`, 템플릿 테이블은 아직 지원하지 않습니다. |

요청과 응답 구조는 공식
[`datasets`](https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets)와
[`tables`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables)
리소스를 기준으로 비교합니다. 알 수 없는 JSON 필드를 무시하고 해석할 수는 있지만,
해당 필드의 동작까지 구현했다는 의미는 아닙니다.

<!-- section: generated-integration-coverage -->
## 생성된 통합 Coverage

외부 consumer operation 목록은 literal integration-test annotation에서 생성한
[`contract/generated/integration-consumer-contract.json`](../../contract/generated/integration-consumer-contract.json)에 있습니다.
화면에서 바로 확인할 수 있는 operation 목록은 [생성된 통합 소비자
계약](generated/integration-consumer-contract.md)에 있습니다.
annotation을 추가한 뒤 `make integration-contract-check`를 실행하면 CI가 오래된
generated contract를 거부합니다. scenario 순서와 cardinality는 호환성 주장이 아니라
명시적 test assertion으로 유지합니다.

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

`formatOptions.useInt64Timestamp=true`이면 고정한 Python 클라이언트가 요구하는 Unix
epoch 마이크로초 문자열을 반환합니다. `maxResults=0`을 명시하면 정확한 `totalRows`와
빈 페이지 하나를 반환하며 이어받기 토큰은 만들지 않습니다. epoch 전후의 마이크로초
값은 모두 UTC 날짜 및 시간으로 디코딩합니다.

유한한 `FLOAT64` 셀은 JSON 숫자를 사용합니다. 그 밖의 값은 공식
[`StandardSqlDataType`](https://cloud.google.com/bigquery/docs/reference/rest/v2/StandardSqlDataType)
계약에 정의된 `NaN`, `Infinity`, `-Infinity` 표기를 사용합니다.

`CAP-REST-METADATA-PATCH-V1`과 `CAP-SCHEMA-ADDITIVE-V1`은 실제 프로세스를 대상으로
공식 [Python 클라이언트
`3.43.0`](https://pypi.org/project/google-cloud-bigquery/3.43.0/)으로도 확인했습니다.
스키마 변경은 중첩 및 반복 레코드를 포함하여 `NULLABLE` 또는 `REPEATED` 필드를
끝에 추가하는 방식만 지원합니다. 이 REST 수정 검증은 필수 여부 완화나 작업에서
요청하는 스키마 변경을 뜻하지 않습니다. 별도로 문서화한 의미 기반 DDL 범위가 넓어지는
것도 아닙니다.

<!-- section: jobs -->
## 쿼리와 작업

### 작업 API

| 기능 | 상태 | 지원 범위 |
| --- | --- | --- |
| `jobs.query` | 부분 지원 | Python 3.43.0으로 동기 실행을 확인했습니다. DuckDB 호환 SQL 일부만 지원합니다. |
| 쿼리 `jobs.insert` | 부분 지원 | Python 3.43.0으로 조회 흐름을 확인했습니다. 비동기 상태는 프로세스 메모리에 저장합니다. |
| `jobs.get` | 기본 검증 | `PENDING`, `RUNNING`, `DONE`과 최종 오류를 지원합니다. |
| `jobs.list` | 부분 지원 | 위치를 포함한 식별자와 불투명한 커서 토큰을 지원합니다. 현재 프로세스의 스냅샷만 조회합니다. |
| `jobs.getQueryResults` | 부분 지원 | 위치 기반 조회, `startIndex`, `maxResults`, 작업과 결과에 묶인 불투명한 페이지 토큰을 지원합니다. |

### 대상 테이블과 쿼리 메타데이터

| 기능 | 상태 | 지원 범위 |
| --- | --- | --- |
| 명시적 대상 테이블 | 부분 지원 | 스칼라 결과와 같은 스키마에서 `WRITE_EMPTY`, `WRITE_APPEND`, `WRITE_TRUNCATE`를 지원합니다. ID는 `query.destination.exact-schema-v1`입니다. |
| 커넥터 쿼리 메타데이터 | 기본 검증 | `INTERACTIVE` 및 `BATCH` 우선순위와 검증한 라벨을 해시와 왕복 결과에 반영합니다. 빈 라벨 맵도 보존합니다. |
| 쿼리/로드 작업 라벨 | 기본 검증 | BigQuery 라벨 키와 값 규칙을 검증하고 작업 구성 ID에 포함합니다. 작업 구성과 함께 영속화하며 `jobs.insert`, `jobs.get`, `jobs.list`에서 다시 반환합니다. |
| 익명 대상 테이블 | 부분 지원 | 행을 반환하는 쿼리는 24시간 뒤 만료되는 숨김 데이터 세트의 테이블을 만들고 공개합니다. ID는 `query.destination.anonymous-v1`입니다. |
| `WRITE_TRUNCATE` 스키마 교체 | 미지원 | 같은 스키마만 지원합니다. 미지원 ID는 `query.destination.truncate-schema-replacement-v1`입니다. |

### 실행 제어와 영속성

| 기능 | 상태 | 지원 범위 |
| --- | --- | --- |
| 의미 기반 SQL DDL | 부분 지원 | `CREATE TABLE`, `DROP TABLE`, 최상위 스칼라 `ADD COLUMN`, `RENAME COLUMN`을 지원합니다. 열을 파괴적으로 바꾸는 작업, `TRUNCATE`와 다른 절은 `query.ddl.catalog-sync-v1`에 따라 지원하지 않습니다. |
| 여러 문장으로 된 쿼리 | 특정 입력만 지원 | 문자열과 주석을 구분하여 일반 문장에는 마지막 세미콜론 하나만 허용합니다. 일반 스크립트는 실행 전에 거부하며 Spark 동적 시간 파티션 덮어쓰기 의미 어댑터만 예외입니다. 미지원 ID는 `query.scripts.unsupported-v1`이며 공식 [여러 문장 쿼리 계약](https://cloud.google.com/bigquery/docs/multi-statement-queries)을 참고합니다. |
| 취소 | 부분 지원 | 종료 과정에서는 새 작업을 거부하고 실행 중인 작업을 취소한 뒤 Storage와 DuckDB를 닫습니다. 공개 [`jobs.cancel`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/cancel)과 취소 상태는 지원하지 않습니다. |
| Parquet 로드 `jobs.insert`, `jobs.get`, `jobs.list` | 부분 지원 | 별도로 활성화해야 하며 `gs://`와 불변 multipart/resumable media source, 명시 스키마 대상 생성, `WRITE_EMPTY`/`WRITE_APPEND`/`WRITE_TRUNCATE`를 지원합니다. 상태는 프로세스 메모리에 저장합니다. |
| 복사와 추출 | 미지원 | 해당 설정을 거부합니다. |
| 작업과 결과의 영속 상태 | 미지원 | 메모리 저장소만 사용합니다. |
| 쿼리 결과 보관 크기 제한 | 미지원 | 모든 결과 행을 Go 메모리에 보관합니다. 미지원 ID는 `query.results.unbounded-memory-v1`입니다. |
| 복합 쿼리 결과 스키마 | 엄격히 거부 | `ARRAY`와 `STRUCT` 결과의 모드와 자식 필드를 평탄화하지 않습니다. 메타데이터 반영 전에 실패하며 ID는 `query.results.complex-schema-v1`입니다. |
| 비동기 쿼리 실행 시간 제한 | 부분 지원 | `query.operationTimeout`으로 동기 및 비동기 실행을 제한합니다. 종료 시 승인 거부, 취소, 대기를 처리합니다. 작업자 용량과 요청의 정확한 `timeoutMs` 동작은 미지원이며 ID는 `query.execution.bounded-v1`입니다. |
| 같은 ID의 쿼리 등록 | 기본 검증 | `(project, location, jobId)`를 원자적으로 중복 검사합니다. 모든 재사용은 `409 duplicate`이며 해시는 진단에만 사용합니다. |
| 같은 요청 재실행 확장 기능 | 미지원 | 이후 별도 활성화 기능으로 계획합니다. 미지원 ID는 `query.jobs.exact-replay-extension-v1`입니다. |
| 쿼리와 로드 사이의 ID 중복 검사 | 미지원 | 분리된 저장소의 확인 및 생성 사이에 경쟁 상태가 있습니다. 미지원 ID는 `query.jobs.cross-repository-identity-v1`입니다. |
| 동기 요청 제어 | 부분 지원 | 36바이트 ASCII `requestId`와 음수가 아닌 `timeoutMs`를 받습니다. 미완료 응답 대기 제한, 변경 쿼리 중복 제거, `jobTimeoutMs`는 미지원이며 ID는 `query.sync.request-controls-v1`입니다. |
| 쿼리 매개변수 | 부분 지원 | `NAMED`와 `POSITIONAL` scalar typed parameter(`BOOL`, `INT64`, `FLOAT64`, `NUMERIC`, `STRING`, `DATE`, `DATETIME`, `JSON`, `TIME`, `TIMESTAMP`)는 GoogleSQL AST 경계에서 검증한 뒤 DuckDB bind 인자로 전달합니다. ARRAY, STRUCT, GEOGRAPHY parameter는 아직 지원하지 않습니다. |
| 지원하지 않는 쿼리 옵션 | 엄격히 거부 | `dryRun`, 캐시 및 비용 제어, `jobTimeoutMs`는 `400`으로 거부합니다. 미지원 ID는 `query.options.unsupported-v1`입니다. |
| 위치를 생략한 데이터 세트 추론 | 부분 지원 | 구조적으로 참조한 테이블, 다른 프로젝트의 `defaultDataset.projectId`, 명시적 대상 데이터 세트를 실행 전에 검사합니다. ID는 `query.location.dataset-inference-v1`입니다. |
| 최종 상태 저장 실패 복구 | 미지원 | 저장소 갱신에 실패하면 `RUNNING` 상태가 남을 수 있습니다. 미지원 ID는 `query.terminal-persistence-v1`입니다. |

작업 상태와 오류 필드는 공식 [`Job`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job)
리소스를 기준으로 합니다. 중첩 및 반복 결과 셀과 자료형별 날짜 및 시간 값은 아직
완전한 [`TableRow`](https://cloud.google.com/bigquery/docs/reference/rest/v2/TableRow)
형식으로 인코딩하지 않습니다. 스칼라 쿼리와 `tabledata.list` 행에서 유한한
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
`CREATE_IF_NEEDED`로 결과를 저장합니다. 커넥터 `0.44.2`의
[`TempTableBuilder`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java#L1150-L1240)가
이 동작에 의존합니다.

생성한 데이터 세트의 이름은 `_`로 시작합니다. [`all=true`](https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list)를
지정하지 않으면 `datasets.list`에서 숨깁니다. 테이블은 커넥터의 기본
[`MaterializationConfiguration`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/MaterializationConfiguration.java)과
BigQuery의 대략적인 [익명 테이블
수명](https://cloud.google.com/bigquery/docs/cached-results#how_cached_results_are_stored)에
맞춰 메타데이터를 반영한 지 24시간 뒤의 만료 시간을 제공합니다.

만료된 결과는 `tables.get`, `tables.list`, Storage Read에서 테이블을 찾을 때
동기적으로 정리합니다. 숨김 데이터 세트는 다음 결과를 위해 남겨 둡니다. 캐시 적중
결과 재사용, 백그라운드 정리 작업, 재시작 후 복구할 수 있는 만료 기록은 아직
없습니다. 별도 정리 고루틴이나 `Close` 순서도 없으며 각 요청이 정리를 끝냅니다.

ID를 알고 있는 숨김 데이터 세트에는 일반 삭제 규칙을 적용합니다. 사용 가능한
테이블이 있으면 `deleteContents=true`가 필요합니다. 조회 과정에서 만료된 테이블을
정리해 데이터 세트가 비면 일반 삭제가 성공합니다.

작업을 등록하기 전에 구조 분석기가 백틱으로 감싼 지원 대상 테이블 경로를 해석합니다.
같은 프로젝트 안에서 다른 프로젝트를 지정한 `defaultDataset.projectId`와 명시적 대상
데이터 세트도 검사합니다. 위치를 생략하면 모든 데이터 세트에 공통된 위치를
사용합니다. 명시한 위치와 추론한 위치가 다르면 저장소나 엔진을 변경하기 전에
실패합니다. 이 동작은 BigQuery [location
규칙](https://cloud.google.com/bigquery/docs/locations#specify_locations)을 따릅니다.

현재 어휘 분석 어댑터가 처리하지 않는 따옴표 없는 테이블 경로, 연결, 원격 함수,
동적 SQL에서는 위치를 추론하지 않습니다. 추론할 수 있는 대상이 없을 때만 설정한
기본 위치를 사용합니다.

<!-- section: sql -->
## SQL과 MERGE

| 동작 | 상태 | 제한 |
| --- | --- | --- |
| 전체 경로 테이블 참조 | 제한 검증 | 백틱으로 감싼 테이블 토큰을 변환합니다. |
| `SELECT`, `INSERT` | 부분 지원 | DuckDB가 지원하는 구문과 함수만 실행합니다. |
| `UPDATE`, `DELETE` | 부분 지원 | DuckDB 문장의 동작을 따릅니다. |
| 기본 `MERGE` | 부분 지원 | DuckDB와 호환되는 형식 하나를 테스트했습니다. |
| 커넥터 `0.44.2` 정적 덮어쓰기 | 제한 검증 | 배포된 Spark의 임시 테이블 쓰기, 원자적 DuckDB `MERGE`, 작업 조회, 정리를 확인했습니다. |
| 커넥터 `0.44.2` 동적 시간 파티션 덮어쓰기 | 부분 지원 | 출처 버전을 고정한 의미 작업과 원시 REST E2E를 확인했습니다. 배포된 커넥터 JAR 검증은 아직 없습니다. |
| 동적 범위 파티션 덮어쓰기 | 미지원 | 범위 표현식 입력을 명시적으로 거부합니다. |
| 의미 기반 DDL | 부분 지원 | 생성, 삭제, 열 추가와 이름 변경의 정확한 일부만 지원합니다. 자세한 내용은 경계 안내서를 참고합니다. |
| ARRAY/STRUCT/GEOGRAPHY 매개변수, 스크립트, 뷰, UDF | 미지원 | scalar typed parameter는 지원하며, 나머지 형식은 구문의 의미를 변환하는 어댑터가 없습니다. |

[GoogleSQL lexical
계약](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical)은 구문에서
쓰인 위치에 따라 따옴표로 감싼 식별자를 구분합니다. 현재 어휘 분석기는 문자열과 주석을
보존하고 지원 대상으로 선언한 릴레이션 위치를 구분합니다. 하지만 완전한 파서는
아닙니다. 따라서 백틱이 들어간 임의의 SQL을 지원하지 않습니다. 일반 `MERGE`는 원본
행의 수와 원자적인 공개 시점을 포함한
[공식 DML
규칙](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)을
따라야 합니다.

정적 덮어쓰기 어댑터는
[`BigQueryClient.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryClient.java)가
생성하는 커넥터 `0.44.2`의 SQL 구조만 인식합니다. 식별자와 절을 토큰으로 해석한
뒤 원자적인 [DuckDB `MERGE
INTO`](https://duckdb.org/docs/current/sql/statements/merge_into) 하나를 실행합니다.

Spark `3.5.8` 프로세스 테스트에서는 `PENDING` 스트림 4개, 그룹 커밋 1회,
`MERGE` 작업 1회를 확인했습니다. 교체 결과의 공개 시점과 임시 테이블 정리도
확인했습니다. 동적 시간 어댑터는 별도의 출처 고정 스크립트를 의미 작업으로 파싱합니다.
기준 메타데이터를 확인한 뒤 DuckDB 트랜잭션 하나로 적용합니다. 동적 범위 덮어쓰기와
일반 BigQuery `MERGE`의 동작 일치 여부는 지원하지 않습니다. 정확한 변환과 DDL 절은
[GoogleSQL 경계 안내서](google-sql-boundary.md)에 정리했습니다.

<!-- section: types -->
## 자료형

| BigQuery 자료형 | DuckDB 테이블 생성 | REST 쿼리 값 | 상태 |
| --- | --- | --- | --- |
| BOOL/INT64/FLOAT64/STRING/BYTES | 기본 자료형으로 변환 | 스칼라 값으로 인코딩 | 부분 지원 |
| NUMERIC | 정밀도 38 이하의 기본 `DECIMAL`로 변환하며 생략한 매개변수는 `(38,9)`입니다. | 드라이버 동작에 따라 달라집니다. | 부분 지원 |
| BIGNUMERIC | 정밀도 38 이하의 기본 `DECIMAL`로 변환하며 생략한 매개변수는 `(38,18)`입니다. | BigQuery의 전체 범위는 제공하지 않습니다. | Spark 제한의 부분 지원 |
| DATE/DATETIME/TIME/TIMESTAMP | 엔진 자료형으로 변환 | 날짜 및 시간 형식 변환 미완성 | 부분 지원 |
| JSON | 기본 JSON으로 변환 | 의미를 완전하게 보존하지 못합니다. | 부분 지원 |
| GEOGRAPHY | 기준 스키마 검사에서 거부합니다. | 제공하지 않습니다. | 미지원 |
| RECORD/REPEATED | 기본 STRUCT 또는 LIST로 변환 | `tabledata`와 Storage 경로는 일부를 지원하며 복합 쿼리 결과 스키마는 거부합니다. | 부분 지원 |

자료형 호환성은 [BigQuery data
types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)를
기준으로 평가합니다. REST, Arrow, Avro, 직접 Proto 쓰기, 간접 로드를 모두 연결해
처음부터 끝까지 검증한 자료형은 아직 없습니다.

<!-- section: storage-read -->
## Storage Read

| RPC/동작 | 상태 |
| --- | --- |
| 공식 서비스 등록과 리플렉션 | 검증됨 |
| 읽기 서비스 상태 | 활성화되어 있고 연결 종료 전이면 수명 주기에 따라 `SERVING`을 반환합니다. |
| 공개 `CreateReadSession`, `ReadRows` | 부분 지원입니다. 세션마다 크기에 상한을 둔 DuckDB 결과 하나를 만듭니다. |
| 공개 `SplitReadStream` | 미지원이며 `UNIMPLEMENTED`를 반환합니다. |
| Arrow/Avro 스키마와 행 데이터 | 부분 지원입니다. 행과 응답 바이트 수에 상한을 두고 DuckDB 결과를 인코딩합니다. |
| 열 선택과 행 제한 | 요청 순서를 보존하는 재귀 STRUCT/REPEATED 필드 선택과 공식 GoogleSQL AST의 제한된 표현식(`AND`/`OR`/`NOT`, 비교, `IN`, `BETWEEN`, `IS NULL`, `CAST`, DATE/TIMESTAMP, `LOWER`, `STARTS_WITH`)을 지원합니다. 값은 bind parameter로 내립니다. 임의 함수, 반복 필드 predicate, 전체 GoogleSQL 의미는 지원하지 않습니다. |
| 논리 스트림과 오프셋 재개 | 실행 중인 세션에서 고정된 범위와 스트림 기준 오프셋을 지원합니다. |
| 과거 시점 스냅샷과 압축 | 지원하지 않습니다. |

공개 기능은 일부만 지원합니다. 실행 중인 세션마다 고정된 DuckDB 결과 하나를
소유하며, 설정한 수의 논리 스트림으로 나눕니다. 결과 크기에는 상한을 둡니다.

스트림 분할 RPC, 전송 압축, 과거 시점의 `snapshot_time`, 반복 필드 predicate, 재시작 후
세션 복구는 지원하지 않습니다. 목표 동작은 공식
[`BigQueryRead`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryRead)
서비스와 커넥터의
[`ReadSessionCreator.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/ReadSessionCreator.java)를
기준으로 합니다.

<!-- section: storage-write -->
## Storage Write

| RPC/동작 | 상태 |
| --- | --- |
| 공식 서비스 등록과 리플렉션 | 검증됨 |
| 쓰기 서비스 상태 | 활성화되어 있고 연결 종료 전이면 수명 주기에 따라 `SERVING`을 반환합니다. |
| `PENDING` 생성, 조회, 추가, 종료, 커밋 | 부분 지원입니다. `ProtoRows`, 정확한 오프셋, 숨김 DuckDB 준비 영역, 종료된 행 수를 지원합니다. |
| 기본 스트림 | 부분 지원입니다. 공식 별칭과 커넥터 `0.44.2`의 기존 별칭을 받고 행을 즉시 추가합니다. |
| 여러 논리 스트림 | 부분 지원입니다. 직렬화된 DuckDB 조정자 하나에서 처리 중 요청과 준비된 바이트 수를 가중치로 제한합니다. |
| 원자적 일괄 커밋 | 검증한 그룹의 대상 테이블 삽입과 준비 데이터 및 확인 기록 삭제를 트랜잭션 하나에서 처리합니다. |
| `ArrowRows`, `BUFFERED`, 명시적 `COMMITTED`, `FlushRows` | 지원하지 않습니다. |

CDC, 누락된 값의 기본 표현식, 재시작 후 준비 영역 복구, 분산 쓰기 동시성은 지원하지
않습니다. `PENDING` 행을 디코딩된 Go 객체로 계속 쌓지는 않습니다. 그러나 준비 영역의
바이트 계산값은 DuckDB가 사용하는 실제 물리 크기가 아니라 일관된 논리적 크기입니다.

백엔드 쓰기를 직렬화하는 것은 내장형 엔진을 사용하면서 둔 의도적인 제한입니다.
BigQuery와 같은 처리량을 보장하지 않습니다. 목표 동작은 공식
[`BigQueryWrite`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite)
서비스와 커넥터의
[`BigQueryDirectDataWriterHelper.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryDirectDataWriterHelper.java)를
기준으로 합니다.

<!-- section: load-auth -->
## 로드, 객체 저장소, 인증

| 기능 | 상태 |
| --- | --- |
| 로컬 `file://` load source | 지원하지 않습니다. 공개 load 요청은 `gs://`로 제한됩니다. |
| GCS 및 fake GCS JSON 어댑터 | 목록, 조회, 미디어 요청의 크기에 상한을 둡니다. URI 글로브 확장은 부분 지원입니다. |
| `gs://` 또는 media upload의 Parquet 로드 | 부분 지원입니다. 불변 multipart/resumable media source, 명시한 스키마와 형 변환, 명시 스키마 대상 생성을 지원합니다. |
| Avro, ORC, CSV, NDJSON 로드 | 지원하지 않습니다. 작업은 최종 `notImplemented` 오류를 반환합니다. |
| `WRITE_APPEND`, `WRITE_EMPTY`, `WRITE_TRUNCATE` | DuckDB 트랜잭션 하나에서 실행하도록 검증했습니다. |
| `schemaUpdateOptions` | 부분 지원: `WRITE_APPEND`의 `ALLOW_FIELD_ADDITION`만 지원합니다. 기존 필드를 보존하고 새 필드는 nullable 또는 repeated여야 합니다. 완화는 지원하지 않습니다. |
| 자동 감지, 멀티파트 및 재개 다운로드 | 지원하지 않습니다. |
| REST/gRPC TLS | 설정하면 사용할 수 있습니다. |
| BigQuery 호환 요청 인증 | 의도적으로 제공하지 않으며 인증 정보를 무시합니다. |
| 로컬 인증 JSON 생성 | 서비스 계정, 사용자 계정과 파일 기반 WIF 클라이언트용으로 구현했습니다. |
| 로컬 OAuth/STS 토큰 획득 | 루프백에서만 실행하는 별도 개발 명령으로 구현했습니다. |
| IAM 권한 확인 | 지원하지 않습니다. |

로드 동작은
[`JobConfigurationLoad`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad)를
기준으로 합니다. 별도로 활성화한 경로는 크기가 정해져 있고 변경할 수 없는 객체를
전용 임시 작업 공간에 내려받습니다. 이후 선택한 쓰기 방식을 원자적으로 적용합니다.
다운로드는 대상 테이블 트랜잭션 밖에서 실행합니다. 로드 작업과 중복 방지 기록은
프로세스 메모리에 저장합니다.

인증 관련 지원 범위는 [Google Cloud
인증](https://cloud.google.com/docs/authentication)에 따라 구분합니다. 로컬
클라이언트 도구는 [로컬 클라이언트 인증 파일과
TLS](client-credentials-and-tls.md)에 설명되어 있습니다. 이 도구는 BQEMU
엔드포인트를 보호하지 않으며 IAM과 같은 기능으로 설명하면 안 됩니다.

<!-- section: persistence-atomicity -->
## 영속성과 원자성

`/data/bqemu-state.sqlite`의 SQLite가 기준 메타데이터를 저장합니다.
`/data/bqemu.duckdb`의 DuckDB는 물리 객체와 행만 보존합니다. 쿼리·로드 작업, 읽기
세션, 쓰기 스트림 기록과 로드 중복 방지 기록은 아직 프로세스 안에만 있습니다.

필드를 추가하는 물리 DDL, 로드의 쓰기 방식 적용, 기본 스트림 추가, 검증된 `PENDING`
스트림 그룹 커밋은 DuckDB 트랜잭션을 사용합니다. SQLite 카탈로그 반영은 별도
트랜잭션입니다. 영속 변경 기록은 있지만, 모든 카탈로그 변경 단계를 기록하지는 않습니다.
시작할 때 미완료 의도를 복구하는 처리도 없습니다. 따라서 두 저장소를 함께 바꿀 때의
장애 원자성, 재시작 후 재실행과 실행 중인 인스턴스의 파일 하나만으로 만든 일관된 백업은
지원하지 않습니다. 자세한 내용은 [저장 엔진 어댑터
안내서](storage-engine-adapter.md)에 있습니다.

<!-- section: client-coverage -->
## 클라이언트 검증 범위

[Google Cloud SDK `566.0.0`](https://cloud.google.com/sdk/docs/release-notes#56600_2026-04-28)의
[`bq` CLI `2.1.31`](https://cloud.google.com/bigquery/docs/reference/bq-cli-reference)은
UI를 비활성화한 별도 CI 단계에서 실행합니다. 프로젝트 목록, 데이터 세트와 테이블의
수명 주기, `NULLABLE` 필드 추가, 쿼리 결과 조회를 확인합니다. 작업과 테이블 목록,
정리, 리소스를 찾지 못했을 때의 종료 상태도 검증합니다.

공식 [Python 클라이언트
`3.43.0`](https://pypi.org/project/google-cloud-bigquery/3.43.0/)을 사용하는 종단 간
테스트는 여섯 개입니다. 데이터 세트 관리와 테이블 메타데이터 및 스키마 관리를
검증합니다. 중첩 및 반복 행과 Unix epoch 전후의 `TIMESTAMP` 디코딩을 포함하여
`tabledata.list` 페이지 조회도 확인합니다. 동기
[`jobs.query`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query)와 비동기
[`jobs.insert`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert)부터
[`jobs.getQueryResults`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults)까지의
흐름도 검증합니다.

요청과 응답 구조는 [`python-query-sync`](../../contract/golden/python-query-sync-3.43.0.json)와
[`python-query-async`](../../contract/golden/python-query-async-3.43.0.json),
[`python-tabledata-list`](../../contract/golden/python-tabledata-list-3.43.0.json)
기준 파일에 고정합니다. 로드, 복사, 추출, `insertAll`은 반드시 실패해야 하는 미지원
테스트 네 개로 남겨 둡니다. 응답을 잃은 `requestId` 재실행은 부분 계약을 나타내는
별도 실패 예상 테스트 하나로 유지합니다.

커넥터 `0.44.2` 호환성 표에서는 75개 항목 중 21개를 검증됨으로 기록합니다.
Arrow와 Avro를 이용한 여러 스트림의 테이블 및 쿼리 읽기, 열 선택과 필터 전달, 명시적
결과 구체화, 최적화된 행 수 계산을 포함합니다. 정확한 `PENDING` 추가, 기본 스트림
추가, 파티션이 없는 직접 정적 덮어쓰기도 포함합니다.

이 결과가 Spark 전체와의 호환성을 뜻하지는 않습니다. 지원 상태를 높이려면 공개 API
경계에서 얻은 실행 근거와 실패 또는 경계값 테스트가 필요합니다.

[`bq-project-dataset-admin`](../../contract/golden/bq-project-dataset-admin-2.1.31.json),
[`bq-table-schema-admin`](../../contract/golden/bq-table-schema-admin-2.1.31.json),
[`bq-query-job`](../../contract/golden/bq-query-job-2.1.31.json),
[`bq-not-found-error`](../../contract/golden/bq-not-found-error-2.1.31.json) 기준 파일은
CLI 전송 단계를 기준 파일로 고정합니다. 로드, 복사, 추출은 이 기준 버전에서 계획
상태이므로 이슈 #13은 계속 열어 둡니다.

<!-- section: removal-criteria -->
## 호환성 예외 처리 제거 기준

호환성 예외 처리는 고정한 상위 버전의 결함을 재현할 수 있어야 합니다. 새 상위
버전에서 결함이 사라지고 기준 전송 기록이 일치해야 합니다. 해당 규칙을 제거한 뒤에도
직접 커넥터 테스트가 통과해야 제거할 수 있습니다.

예외 처리를 더 넓은 입력에 적용하려면 정규식 예제 하나만으로는 부족합니다. 프로토콜
정의나 구문의 의미를 설명하는 출처가 필요합니다.
