<!-- doc-id: adr-0001-duckdb-warehouse-port -->
<!-- lang: ko -->

[English](../../en/adr/0001-duckdb-behind-warehouse-port.md) | [한국어](0001-duckdb-behind-warehouse-port.md)

# ADR-0001: DuckDB는 웨어하우스 포트를 통해 사용합니다

<!-- section: status -->
## 상태

초기 종단 간 기능 구현에 적용하기로 승인했습니다.

<!-- section: context -->
## 배경

에뮬레이터에는 로컬 SQL 실행과 테이블 저장소가 필요합니다. 데이터베이스 자체를
새로 구현하지 않고 이 기능을 제공해야 합니다. DuckDB는 [DuckDB SQL
소개](https://duckdb.org/docs/stable/sql/introduction)에 설명된 내장형 SQL,
중첩 및 리스트 타입, 트랜잭션, 파일 영속성을 제공합니다. 그러나 DuckDB의 SQL
문법, 카탈로그, 타입, 트랜잭션 동작은 BigQuery 계약이 아닙니다.

<!-- section: decision -->
## 결정

도메인 및 애플리케이션 코드는 웨어하우스 및 쿼리 엔진용 포트에 의존합니다.
DuckDB 드라이버, 물리 스키마 명명 규칙, 인용 규칙, 타입 대응, SQL 실행은 외부
연동 어댑터에 둡니다. BigQuery 기준 메타데이터는 엔진 타입과 독립적으로
유지합니다. 어댑터가 포트를 구현하는지는 컴파일 시점에 확인합니다. 테스트용
구현을 사용한 애플리케이션 테스트로 이 경계를 검증합니다.

<!-- section: consequences -->
## 결과

공개 REST/gRPC DTO나 작업 상태를 바꾸지 않고 DuckDB를 교체할 수 있습니다.
다만 기준 메타데이터와 저장 엔진 내부 DDL 사이에는 현재 보상 작업이 필요합니다.
프로세스 오류가 발생하면 두 상태가 서로 달라질 수 있습니다. 재시작 후에도
원자성이 보장되는 카탈로그를 제공하려면 영속 시스템 테이블과 트랜잭션 포트가
필요합니다.

<!-- section: alternatives -->
## 대안

애플리케이션 서비스에서 DuckDB를 직접 호출하면 BigQuery 수명 주기의 의미가 특정
엔진에 결합됩니다. 따라서 이 방식은 채택하지 않았습니다. 데이터베이스를 직접
구현하는 방식도 호환성 목표와 무관하므로 채택하지 않았습니다.
