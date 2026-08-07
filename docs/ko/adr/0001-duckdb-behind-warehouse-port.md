<!-- doc-id: adr-0001-duckdb-warehouse-port -->
<!-- lang: ko -->

[English](../../en/adr/0001-duckdb-behind-warehouse-port.md) | [한국어](0001-duckdb-behind-warehouse-port.md)

# ADR-0001: DuckDB를 Warehouse Port 뒤에 유지한다

<!-- section: status -->
## 상태

초기 vertical slice에 대해 승인됨.

<!-- section: context -->
## 배경

Emulator는 database를 직접 구현하지 않으면서 로컬 SQL 실행과 table storage가
필요하다. DuckDB는 [DuckDB SQL
소개](https://duckdb.org/docs/stable/sql/introduction)에 정의된 embedded SQL,
nested/list type, transaction, file persistence를 제공한다. DuckDB dialect,
catalog, type, transaction 동작은 BigQuery 계약이 아니다.

<!-- section: decision -->
## 결정

Domain/application code는 warehouse/query-engine port에 의존한다. DuckDB driver,
physical schema naming, quoting, type mapping, SQL execution은 outbound adapter에
둔다. Canonical BigQuery metadata는 engine type과 독립적으로 유지한다. Adapter
assertion과 fake를 사용하는 application test가 경계를 검사한다.

<!-- section: consequences -->
## 결과

공개 REST/gRPC DTO나 job state를 바꾸지 않고 DuckDB를 교체할 수 있다. 그러나
metadata와 engine DDL은 현재 compensation이 필요하며 process failure 때 drift할
수 있다. Restart-atomic catalog를 주장하려면 durable system table과 transaction
port가 필요하다.

<!-- section: alternatives -->
## 대안

Application service에 DuckDB call을 직접 넣는 방식은 BigQuery lifecycle 의미를
한 engine에 결합하므로 거부했다. Database 직접 구현은 호환성 목표와 무관하므로
거부했다.
