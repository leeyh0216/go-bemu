<!-- doc-id: dynamic-partition-overwrite -->
<!-- lang: ko -->

[English](../en/dynamic-partition-overwrite.md) | [한국어](dynamic-partition-overwrite.md)

# 동적 파티션 덮어쓰기

<!-- section: upstream-contract -->
## 커넥터 원본 계약

지원 후보는 Spark 커넥터 `0.44.2`의
[`BigQueryUtil.java`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryUtil.java#L796-L870)가
생성하는 정확한 스크립트입니다. 이 스크립트는 `IGNORE NULLS`를 사용해 원본의 고유
파티션 배열을 선언합니다. `MERGE ... ON FALSE`는 원본이 건드린 대상 파티션의 행을
삭제한 뒤 모든 원본 행을 삽입합니다.

서비스 규칙은 [여러 명령문
쿼리](https://cloud.google.com/bigquery/docs/multi-statement-queries),
[`MERGE`](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement),
[DML 트랜잭션
의미](https://cloud.google.com/bigquery/docs/data-manipulation-language#multi-statement_transactions)를
기준으로 합니다.

이 구현은 범용 스크립트 변환기가 아닙니다. 버전을 고정한 의미 분석
어댑터입니다. 토큰, 별칭, 필드 목록, 관계, 파티션 함수, 마지막 명령문이 예상과
다르면 안전하게 거부합니다.

거부 진단에는 모델, 지원 항목 또는 미지원 항목, 토큰 위치, 예상 구조, 쿼리 요약
해시, 해결 방법을 남깁니다. SQL 원문은 로그에 남기지 않습니다.

<!-- section: execution-contract -->
## 현재 실행 계약

애플리케이션은 컨텍스트 취소를 지원하는 리소스 변경 잠금 하나를 획득합니다. 잠금을
유지한 상태에서 대상 테이블과 원본 기준 테이블을 모두 조회합니다. 스키마 검증과
DuckDB 트랜잭션이 끝날 때까지 잠금을 해제하지 않습니다. 따라서 삭제와 재생성이
엇갈리는 경쟁 상태를 막습니다.

대기 중 취소된 요청은 잠금을 차지하지 않고 종료됩니다. 이후 변경 요청은 같은
잠금을 다시 사용할 수 있습니다.

트랜잭션을 시작하기 전에 선택한 모든 원본 필드를 검증합니다. 각 필드는 대상
필드의 기준 BigQuery 유형, 모드, 중첩 이름, 중첩 순서와 일치해야 합니다. 문서화된
별칭인
`BOOL`/`BOOLEAN`, `INTEGER`/`INT64`, `FLOAT`/`FLOAT64`, `STRUCT`/`RECORD`는 같은
유형으로 정규화합니다. 그 밖의 DuckDB 암시적 유형 변환은 `invalidQuery`로
거부합니다. 기준 리소스가 없으면 `notFound`를 유지합니다.

파티션 필드는 `DATE`, `TIMESTAMP`, `DATETIME`을 지원합니다. 커넥터의 자르기 함수와
유효한 단위가 함께 지정되어야 합니다. 유형 정의는 [BigQuery 데이터
유형](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)을 따릅니다.

어댑터는 삭제와 삽입을 명시적인 DuckDB 트랜잭션 하나에서 실행합니다. NULL 원본
파티션은 `IGNORE NULLS`에 따라 변경 대상 파티션 집합에서만 제외됩니다. 해당 원본
행 자체는 그대로 삽입됩니다.

로그에는 시작, 삭제, 삽입, 커밋, 롤백 전후의 경계를 기록합니다. 정확한 트랜잭션
상태, 변경한 행 수, 실행 시간, 스키마 지문값, 불투명 리소스 지문값도 기록합니다.
SQL 원문과 행, 프로젝트, 데이터 세트, 테이블, 필드 값은 기록하지 않습니다.

<!-- section: rest-contract -->
## REST 작업 계약

승인된 작업은 일반 `jobs.insert`와 `jobs.get` 수명 주기를 거칩니다. 쿼리 통계는
`statementType=SCRIPT`를 보고합니다. 현재 제공할 수 있는 최상위 및 쿼리 수준의 변경
행 수 합계도 채웁니다.

오류 사유는 공식 [BigQuery 오류
표](https://cloud.google.com/bigquery/docs/error-messages)를 따릅니다. 스키마나
쿼리 위반은 `invalidQuery`입니다. 리소스가 없으면 `notFound`입니다. 제한 시간 초과는
`timeout`, 취소는 `stopped`, 저장소 트랜잭션 실패는 `jobBackendError`입니다.

<!-- section: stable-gaps -->
## 명시적으로 유지하는 미지원 항목

BigQuery 스크립트는 하위 작업과 스크립트 전용 통계를 제공합니다. 하위 작업 목록,
`scriptStatistics`, 명령문별 `dmlStats`는 아직 구현하지 않았습니다. 전송 형식은
[`JobStatistics2`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobStatistics2)를
기준으로 유지합니다. 동적 범위 파티션 덮어쓰기도 등록된 미지원 항목입니다.

공개 에뮬레이터 API를 대상으로 출시된 Spark 커넥터 JAR의 직접 쓰기나 간접 쓰기
동적 덮어쓰기를 아직 검증하지 않았습니다. 단위 테스트와 REST 원본 요청 E2E는 의미
분석 어댑터, 원자성, NULL 동작, 유형, 구조 불일치 거부, 작업 오류 사유를 검증합니다.
다만 이 결과는 커넥터 자체의 검증 자료가 아닙니다.

출시된 파일을 내려받아 URL, 버전, 크기, SHA-256을 기록해야 합니다. 민감한 값을
제거한 공개 API 검증 자료도 남겨야 합니다. 이 작업을 마치기 전까지 커넥터 프로필,
기준 결과, 호환성 표, 직접·간접 쓰기 E2E 항목은 미지원으로 유지합니다.

<!-- section: promotion-gates -->
## 승격 조건

이후 커넥터 버전을 지원할 때 `0.44.2` 모델의 조건을 느슨하게 바꾸면 안 됩니다.
원본 버전을 고정한 새 파서 모델을 추가해야 합니다.

승격하려면 성공·실패 토큰 시험 데이터와 대상·원본 스키마 불일치 시험이 필요합니다.
DATE/TIMESTAMP/DATETIME과 NULL 사례도 검증해야 합니다. 취소와 잠금 재사용, 롤백,
불투명 로그 검증, REST 원본 요청 E2E, 출시 JAR의 직접·간접 쓰기 E2E 자료도
필요합니다.

이 조건을 모두 만족한 뒤에만 버전별 프로필, 기준 결과, 호환성 표를 미지원에서 검증
완료로 바꿀 수 있습니다.
