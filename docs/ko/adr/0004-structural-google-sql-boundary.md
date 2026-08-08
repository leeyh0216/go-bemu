<!-- doc-id: adr-0004-structural-google-sql-boundary -->
<!-- lang: ko -->

[English](../../en/adr/0004-structural-google-sql-boundary.md) | [한국어](0004-structural-google-sql-boundary.md)

# ADR-0004: GoogleSQL을 구문 구조에 따라 처리합니다

<!-- section: status -->
## 상태

제약 사항으로 승인했습니다. 파서 및 의미 분석은 아직 구현하지 않았습니다.

<!-- section: context -->
## 배경

GoogleSQL은 여러 구문 위치의 식별자에 백틱을 사용합니다. `MERGE`에는 순서가
정해진 절, 일치 행 수 제약, 원자적으로 적용해야 하는 변경이 있습니다. 공식
계약은 [GoogleSQL 어휘
구조](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical)와
[`MERGE` 구문](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)입니다.
텍스트만 검사하는 정규식은 테이블 참조, 열, 주석, 문자열, 데코레이터,
스크립트를 구분할 수 없습니다.

<!-- section: decision -->
## 결정

현재의 백틱 변환기는 초기 구현에 필요한 좁은 범위에서만 사용합니다. 새로운 SQL
호환 기능은 파서와 AST를 사용해야 합니다. 또는 버전이 정확히 정해진 커넥터
템플릿 인식기를 사용할 수 있습니다. 알 수 없는 SQL은 지원 범위로 명시한 엔진
SQL에 그대로 전달하거나 명확한 오류를 반환합니다. 이를 조건이 느슨한 정규식으로
폭넓게 변환하지 않습니다.

<!-- section: consequences -->
## 결과

일반 GoogleSQL 쿼리는 명시한 부분집합만 지원합니다. 카탈로그 DDL은 GoogleSQL AST
어댑터와 타입 있는 의미 명령으로 처리합니다. 커넥터 템플릿 규칙을 추가할 때는
정확한 버전, 지문, 공식 근거, 실패 사례, 제거 조건을 기록합니다. SQL DDL은 BigQuery
기준 메타데이터를 함께 반영해야 하며 물리 카탈로그만 변경해서는 안 됩니다.

<!-- section: alternatives -->
## 대안

정규식 치환 목록을 늘리는 방식으로는 구문 사이의 상호작용을 처리할 수 없습니다.
잘못된 테이블이나 열을 조용히 변경할 수도 있으므로 이 방식은 채택하지
않았습니다.
