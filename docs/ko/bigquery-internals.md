<!-- doc-id: bigquery-internals -->
<!-- lang: ko -->

[English](../en/bigquery-internals.md) | [한국어](bigquery-internals.md)

# BigQuery 프로토콜 내부 동작

<!-- section: mental-model -->
## 핵심 모델

BigQuery 호환 호출자는 서로 다른 공개 경계 세 곳을 사용합니다.

1. BigQuery REST는 테이블 메타데이터, 쿼리·적재 작업, 상태 확인, 덮어쓰기 조정을
   담당합니다.
2. BigQuery Storage Read gRPC는 세션을 만들고 행 스트림을 병렬로 읽습니다.
3. BigQuery Storage Write gRPC는 직접 추가, 스트림 확정, 대기 스트림 커밋을
   담당합니다.

BigQuery 서비스의 기준 경계는 [REST
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest)와 [Storage RPC
레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc)입니다.

`go-bemu`는 REST 메타데이터와 쿼리 및 Parquet 적재 작업을 공개합니다. Storage
Read/Write 공개 범위는 부분 지원(`Partial`)입니다.
아래 설명은 현재 제한을 둔 실행 절차와 아직 남은 BigQuery 요구사항을 구분합니다.

<!-- section: read-planning -->
## 읽기 계획

Storage Read 호출자는 먼저 REST로 테이블이나 쿼리를 확인합니다. 선택할 열, 필터,
스냅샷 시각, 요청 병렬도를 계산한 뒤 `CreateReadSession`을 보냅니다.

서버는 참조 스키마 하나와 이름이 있는 스트림을 0개 이상 반환합니다. 읽기 작업은
반환된 스트림마다 나눌 수 있습니다. 요청한 최대 병렬도는 상한일 뿐, 해당 개수만큼
스트림을 반드시 만들라는 뜻은 아닙니다.

올바른 에뮬레이터는 모든 논리 스트림을 안정된 스냅샷 하나에 묶어야 합니다. 각
범위마다 정렬 기준이 없는 쿼리를 따로 실행해서는 안 됩니다.

열 선택과 행 제한 조건은 세션 스냅샷에 속합니다. `ReadRows` 오프셋은 선택한
스트림을 기준으로 계산합니다. 이 필드와 의미는 공식
[`ReadSession`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readsession)과
[`ReadRowsRequest`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readrowsrequest)
메시지에 정의되어 있습니다.

현재 공개 실행 환경은 활성 세션마다 크기 제한을 둔 DuckDB 결과 하나를
구체화합니다. 결과는 설정된 수의 고정된 논리 범위로 나눕니다. 각 스트림은 자체
오프셋에서 읽기를 재개할 수 있습니다.

재귀 열 선택은 카탈로그 필드 순서를 유지합니다. 행 조건은 논리식, 비교, `IN`,
`BETWEEN`, NULL 검사, `LIKE`, 스칼라 캐스트를 지원합니다. 함수와 하위 쿼리는
구체화 전에 거부합니다. `SplitReadStream`, 과거 `snapshot_time`, 압축, 재시작 복구는
지원하지 않습니다.

<!-- section: read-wire -->
## Arrow와 Avro 읽기 전송 형식

Arrow의 `serialized_schema`와 `serialized_record_batch`는 서로 다른 protobuf
필드에 Arrow IPC 메시지를 담습니다. 완전한 Arrow 파일을 임의로 넣는 형식이
아닙니다. 형식의 기준은 [Arrow IPC
명세](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc)입니다.

Avro는 JSON 스키마 하나와 연속된 이진 행 데이터를 사용합니다. 논리 유형과 null
유니온은 [Apache Avro
명세](https://avro.apache.org/docs/1.11.4/specification/)를 따라야 합니다.
BigQuery 형식 변환은 [Storage API Avro 스키마
설명](https://cloud.google.com/bigquery/docs/reference/storage#avro_schema_details)에
정의되어 있습니다.

어느 형식이든 행 수, 스키마, 데이터 바이트 수, 빈 결과, 여러 배치, 중첩·반복 값,
압축, 오프셋 재개가 일치해야 합니다. 스칼라 시험 데이터 하나를 디코딩한 결과만으로
전송 형식 호환성을 증명할 수는 없습니다.

현재 DuckDB 어댑터는 공개 `ReadRows` 처리 과정에서 크기 제한을 둔 Arrow 레코드 배치
메시지와 Avro 이진 행을 인코딩합니다. 압축과 완전한 중첩·반복 유형 변환은 아직
지원하지 않습니다. 따라서 전체 전송 형식 호환성이 아닌 부분 지원(`Partial`)
상태입니다.

<!-- section: direct-exact -->
## 직접 쓰기: 대기 스트림과 정확한 오프셋

정확한 오프셋을 사용하는 일괄 쓰기는 생산자 작업 단위마다 `PENDING` 스트림을
만듭니다. `AppendRows` 연결을 열고 작성자 스키마를 제공합니다. 스트림 기준
시작 오프셋과 직렬화된 Proto 행을 전송합니다. 각 응답 오프셋을 검증한 뒤 스트림을
확정합니다. 모든 작업이 성공하면 조정기가 스트림 이름을 모아 커밋합니다.

공식 Write API는 정확한 오프셋 처리를 요구합니다. 바로 다음 오프셋은 받습니다.
중간 오프셋을 건너뛴 요청은 실패해야 합니다. 이미 받은 오프셋을 다시 보내면 중복
요청으로 처리해야 합니다. 같은 오프셋의 데이터가 달라지면 거부해야 합니다.

`FinalizeWriteStream` 이후에는 행을 추가할 수 없습니다. 이 호출에서 최종 행 수를
확정합니다. `BatchCommitWriteStreams`는 대기 스트림을 원자적으로 공개합니다. 기준
RPC 계약은
[`BigQueryWrite`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite),
운영 순서는 [대기 스트림 일괄
적재](https://cloud.google.com/bigquery/docs/write-api-batch)에 있습니다.

현재 부분 지원 구현은 스트림을 키로 사용하는 SQLite 원장에 상태를 영속화합니다.
원장에는 스키마 지문값, 다음 오프셋, 수락한 데이터의 요약 해시, 최종 행 수, 작업
단계, 커밋 그룹이 들어갑니다.

공개 경계는 `ProtoRows` 추가, 정확한 오프셋, 스트림 확정, 검증된 `PENDING` 그룹의
원자적 커밋을 지원합니다. DuckDB 변경은 처리량에 상한을 둔 조정기 하나로
직렬화합니다. 시작할 때 준비 중이던 의도를 미확정 상태로 먼저 조정하며, 정확히 같은
요청을 재시도하면 한 쌍으로 관리하는 DuckDB 준비 상태에서 작업을 완료할 수 있습니다.
상태 파일 한쪽만 복원한 경우와 완전한 물리 증거 복구는 아직 지원하지 않습니다.

프로세스 전체에서 오프셋 하나를 공유하거나 스트림 맵을 임의로 조회하는 방식은 여러
생산자가 동시에 실행될 때 올바르지 않습니다.

<!-- section: direct-at-least-once -->
## 직접 쓰기: 기본 스트림과 한 번 이상 쓰기 모드

기본 스트림 쓰기는 테이블의 `_default` 스트림을 사용하며 정확한 오프셋을 보내지
않습니다. 행은 스트림 확정이나 일괄 커밋 없이 바로 보입니다. 결과가 불명확한 실패
뒤에 재시도하면 행이 중복될 수 있습니다. Google은 이 차이를 [Storage Write 스트리밍
의미](https://cloud.google.com/bigquery/docs/write-api-streaming)에 정의합니다.

로컬 테스트는 두 모드를 구분해야 합니다. 기본 스트림 응답에서 오프셋을 뺐다는
사실만으로 한 번 이상 쓰기의 재시도 동작을 증명할 수는 없습니다. 서버가 행을
저장한 뒤 클라이언트가 응답을 받기 전에 연결을 끊는 장애 시험이 필요합니다.

공개 부분 지원 구현은 공식 `/streams/_default` 이름을 받습니다. 오프셋 없이 행을
즉시 적용하므로 결과가 불명확한 응답을 재시도하면 행이 중복될 수 있습니다.
`ArrowRows`, `BUFFERED` 스트림, 명시적 `COMMITTED` 스트림, `FlushRows`는 지원하지
않습니다.

<!-- section: overwrite-merge -->
## 직접 덮어쓰기와 MERGE

직접 덮어쓰기는 단순한 행 추가 옵션이 아닙니다. 호출자는 임시 테이블에 먼저 쓰고,
대상 행을 교체하는 `MERGE`를 제출한 뒤 임시 테이블을 정리할 수 있습니다.

BigQuery `MERGE`는 원본과 대상의 일치 조건, 순서가 있는 절, 원자적인 공개 시점을
결합합니다. 항상 거짓인 조건식은 문서화된 교체 최적화입니다. 동적 파티션
덮어쓰기는 식, 파티션 값, 스크립트, 원본 행의 대응 개수에도 의존합니다. 기준 규칙은
[GoogleSQL DML `MERGE`
레퍼런스](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)에
있습니다. 정규식으로 SQL 문구만 치환해서는 일반 `MERGE`를 구현할 수 없습니다.
BQEMU는 모든 query와 동일한 공식 GoogleSQL gateway에서 statement를 parse/analyze하고,
원본과 대상 relation을 기준 metadata에 binding한 뒤 불변 semantic AST만 엔진
어댑터에 전달합니다. DuckDB visitor는 bind literal과 함께 원자적인 [DuckDB `MERGE
INTO`](https://duckdb.org/docs/current/sql/statements/merge_into)를 생성합니다. 항상
거짓인 교체, 순서형 action, DATE/TIMESTAMP/DATETIME 또는 정수 범위 동적 파티션
교체는 이 경로를 사용합니다. 가장 먼저 조건을 만족한 `WHEN` 절만 실행합니다. 같은
대상 행에 여러 원본 행이 대응하면서 UPDATE 또는 DELETE를 시도하면 같은 트랜잭션의
상관 사전조건이 본문 실행 전에 거부합니다. 지원하지 않는 expression, action,
script control flow의 남은 동작 일치는 #8에서 추적합니다.

<!-- section: indirect-write -->
## 간접 쓰기와 적재 작업

간접 쓰기는 GCS에 중간 파일을 놓고 적재 설정을 담은 `jobs.insert`를 제출한 뒤 작업
상태를 확인합니다. 작업이 끝나면 준비 객체를 정리합니다.

올바른 에뮬레이터는 모든 원본 URI를 객체 저장소 포트로 해석해야 합니다. 변경되지
않는 입력을 준비 영역에 적재하고 스키마와 잘못된 레코드 처리 옵션을 검증해야
합니다. `CREATE_IF_NEEDED`, `WRITE_APPEND`, `WRITE_TRUNCATE`, `WRITE_EMPTY`는 대상
트랜잭션 하나에서 적용해야 합니다.

BigQuery는 REST 요청 구조를
[`JobConfigurationLoad`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad),
형식과 유형 동작을 [일괄 적재
문서](https://cloud.google.com/bigquery/docs/loading-data)에 정의합니다. Parquet 파일을
열 수 있다는 사실만으로 BigQuery 적재 의미, 작업 오류, 와일드카드 URI, 원자적인
공개 시점을 검증할 수는 없습니다.

공개 로드 경로는 가짜 GCS 호환 JSON 어댑터를 사용합니다. 개수와
크기 제한을 적용하면서 `gs://` 목록·조회·미디어 요청을 해석합니다. 객체는 비공개
임시 작업 디렉터리에 내려받습니다.

기존 테이블을 기준으로 Parquet 열과 유형 변환을 검증합니다. 명시적 요청 스키마와
`CREATE_IF_NEEDED`가 있으면 같은 DuckDB 트랜잭션에서 물리 대상을 생성하고 행을
삽입한 뒤 커밋 후에만 카탈로그 메타데이터를 공개합니다. 메타데이터 공개가 실패하면
물리 대상을 보상 삭제합니다. `WRITE_APPEND`, `WRITE_EMPTY`, `WRITE_TRUNCATE`는
DuckDB 트랜잭션 하나에서 적용합니다. 로컬 경로와 GCS가 아닌 URI scheme은 작업을
저장하기 전에 거부합니다.

Parquet 스키마 추론, 자동 감지, `schemaUpdateOptions`, Avro/ORC/CSV/NDJSON,
멀티파트·재개 가능 다운로드는 지원하지 않습니다. 작업 메타데이터와 멱등성 식별 정보는 SQLite에
영속화하지만 내려받은 객체와 임시 준비 작업 공간은 영속화하지 않습니다.

<!-- section: rest-jobs -->
## REST 작업, 상태 확인, 페이지 조회

`jobs.query`는 호출자 관점에서 동기 방식이지만 작업 식별자를 반환합니다. 결과를
받기 위해 상태를 다시 확인해야 할 수도 있습니다. `jobs.insert`는 작업을 먼저 저장한
뒤 비동기로 실행합니다.

성공과 실패 상태는 모두 `DONE`입니다. 실패 정보는 `errorResult`와 `errors`에
담깁니다. 결과 페이지에는 안정된 불투명 `pageToken`, 전체 행 수, 스키마, BigQuery
JSON 셀 구조가 필요합니다. 공식 리소스는
[`Job`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job)과
[`GetQueryResultsResponse`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults#response-body)입니다.

`startIndex`에서 결과를 잘라 내는 기능만으로 페이지 토큰을 지원한다고 볼 수는
없습니다. 결과 테이블 데이터가 DuckDB 파일에 있어도 메모리의 작업 상태가 자동으로
영속화되지는 않습니다.

<!-- section: types -->
## 유형 경계

유형은 서로 독립적인 변환 네 단계를 거칩니다. BigQuery 메타데이터, 엔진 저장
유형, REST JSON 셀, Arrow/Avro/Proto 전송 값이 각각 하나의 단계입니다. 기준 유형의
정의와 범위는 [BigQuery 데이터
유형](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)에
있습니다.

`NUMERIC`/`BIGNUMERIC` 정밀도, `TIMESTAMP`와 `DATETIME`의 차이, `TIME`의 마이크로초
정밀도를 처리해야 합니다. 특수 부동소수점 값, `BYTES`의 base64 표현, JSON null과
SQL NULL의 차이도 처리해야 합니다. 중첩 `STRUCT`, 반복 필드, 빈 배열, null 허용
여부도 중요합니다.

현재 엔진 어댑터는 NUMERIC과 지원 범위 안의 BIGNUMERIC을 모두 `DECIMAL(P,S)`로
저장합니다. 기준 메타데이터는 두 자료형의 논리적 구분과 매개변수 생략 여부를
유지합니다. 정밀도는 현재 실행 환경이 지원하는 최대값인 38로 제한합니다.

`GEOGRAPHY`는 로컬에서 의미를 보존할 수 없으므로 저장소를 변경하기 전에
거부합니다. 쿼리 결과는 스키마에 따라 인코딩합니다. 목록이나 구조체에
`fmt.Sprint`를 적용한 값은 BigQuery REST 행 표현이 아닙니다.

<!-- section: authentication -->
## 인증과 인가

서비스 계정 JSON, 사용자 인증 ADC, 외부 계정 WIF는 토큰 획득 방식이 서로
다릅니다. BigQuery REST/gRPC 서비스는 최종적으로 Bearer 토큰을 받습니다. ADC 검색
순서와 인증 정보 파일 유형은 [Application Default
Credentials](https://cloud.google.com/docs/authentication/application-default-credentials),
WIF 교환은 [Workload Identity
Federation](https://cloud.google.com/iam/docs/workload-identity-federation)에 정의되어
있습니다.

BQEMU는 BigQuery 호환 엔드포인트의 요청을 인증하거나 인가하지 않습니다. REST와
gRPC는 인증 정보가 없는 요청을 허용하며 `Authorization` 값이 있어도 무시합니다.
클라이언트 토큰 획득, TLS, 별도의 진단용 관리 토큰, IAM은 서로 다른 지원 범위로
관리합니다. 공개 실행 환경은 서명 신뢰, IAM 역할, 권한 상속, 연합 정책, 토큰 검사,
운영 환경의 인가를 재현하지 않습니다.

<!-- section: implementation-map -->
## 구현 매핑

| BigQuery 단계 | 필요한 에뮬레이터 경계 | 현재 상태 |
| --- | --- | --- |
| REST 메타데이터 | 카탈로그 사용 사례와 JSON 전송 계층 | 기본 수명 주기, 부분·전체 갱신, 페이지 조회, ETag 검증 완료 |
| 스키마 필드 추가 | 스키마 검증기와 웨어하우스 트랜잭션 | 최상위·중첩·반복 레코드 필드 추가 검증 완료 |
| 쿼리 작업 | 작업 저장소, GoogleSQL gateway, statement 포트 | 공개 동기·비동기 절차 검증 완료, 결과 payload는 프로세스 내부에 유지 |
| `CreateReadSession`/`ReadRows` | 스냅샷·세션 원장과 Arrow/Avro 인코더 | 공개 API 부분 지원: 크기 제한이 있는 DuckDB 스냅샷, 재귀 필드 선택, 논리 스트림, 안정된 오프셋 지원. 분할, 압축, 과거 스냅샷, 복구는 미지원 |
| `AppendRows`/확정/커밋 | 영속 스트림별 원장과 트랜잭션 조정기 | 공개 API 부분 지원: `PENDING`·기본 `ProtoRows`, 오프셋, 확정, 원자적 커밋, 시작 시 상태 조정 지원. 고급 스트림 유형과 한쪽 저장소 복원 증명은 미지원 |
| 간접 적재 | 객체 저장소, 준비 영역, 적재 쓰기 방식 | 공개 API 부분 지원: 가짜 GCS JSON과 기존 테이블 또는 명시적 스키마가 있는 새 테이블 대상 Parquet 지원. 다른 형식, 스키마 추론, 스키마 변경, 다운로드 방식은 미지원 |
| 덮어쓰기 `MERGE` | 공식 analyzer, 불변 semantic AST, 엔진 visitor | 항상 거짓, 동적 시간·범위 파티션, 순서형 `WHEN`, 원본 행 대응 개수 동작 검증 완료. 추가 AST node는 #8에 남아 있음 |
| BigQuery 호환 요청 인증 | REST/gRPC 전송 동작 | 의도적으로 제공하지 않으며 인증 정보 값을 무시함 |
| ADC/WIF 획득 | 클라이언트 인증 정보 라이브러리 | 공개 BQEMU 실행 환경의 범위 밖 |

지원 범위를 바꾸려면 공개 경계 테스트를 추가해야 합니다. 한국어와 영어 호환성
문서도 함께 갱신해야 합니다.
