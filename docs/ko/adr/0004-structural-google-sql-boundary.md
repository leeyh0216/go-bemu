<!-- doc-id: adr-0004-structural-google-sql-boundary -->
<!-- lang: ko -->

[English](../../en/adr/0004-structural-google-sql-boundary.md) | [한국어](0004-structural-google-sql-boundary.md)

# ADR-0004: GoogleSQL을 구문 구조에 따라 처리합니다

<!-- section: status -->
## 상태

승인했고 구현했습니다.

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

모든 공개 query와 job SQL 요청은 공식 GoogleSQL parser에 정확히 한 번 들어갑니다.
어댑터는 parser tree를 불변 BQEMU AST로 복사하고 relation과 expression type을 기준
메타데이터에 결합한 뒤 엔진 중립 semantic statement를 반환합니다. 엔진 어댑터는 이
statement를 방문하여 비공개 SQL과 bind argument를 만듭니다.

엔진은 사용자 SQL이나 외부 parser handle을 받지 않습니다. keyword 선분류기, 버전별
template parser, raw engine SQL fallback도 두지 않습니다. 지원 범위 밖의 문법, 의미,
lowering node는 엔진 부작용 전에 실패합니다.

<!-- section: consequences -->
## 결과

GoogleSQL은 명시한 AST 부분집합만 지원하지만 `SELECT`, DML, 지원하는 script,
catalog DDL은 하나의 gateway와 statement root를 공유합니다. Catalog DDL은 타입 있는
mutation plan을 사용하며 물리 catalog와 기준 metadata를 함께 반영해야 합니다.
statement, expression, function, type을 추가할 때는 mapper, semantic binding, engine
lowering, 실행 전에 안전하게 거부하는 음성 테스트를 함께 추가해야 합니다.

<!-- section: alternatives -->
## 대안

정규식 치환 목록을 늘리는 방식으로는 구문 사이의 상호작용을 처리할 수 없습니다.
잘못된 테이블이나 열을 조용히 변경할 수도 있으므로 이 방식은 채택하지
않았습니다.
