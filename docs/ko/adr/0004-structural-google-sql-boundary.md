<!-- doc-id: adr-0004-structural-google-sql-boundary -->
<!-- lang: ko -->

[English](../../en/adr/0004-structural-google-sql-boundary.md) | [한국어](0004-structural-google-sql-boundary.md)

# ADR-0004: 구조적인 GoogleSQL 경계를 요구한다

<!-- section: status -->
## 상태

제약으로 승인됨. Parser/semantic 구현은 대기 중이다.

<!-- section: context -->
## 배경

GoogleSQL은 여러 syntax 위치의 identifier에 backtick을 사용하고 `MERGE`는 순서
있는 clause, cardinality 제약, atomic effect를 가진다. 계약은 [GoogleSQL
lexical structure](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical)와
[`MERGE` syntax](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)다.
Text만 보는 regex는 table reference와 column, comment, string, decorator,
script를 구분할 수 없다.

<!-- section: decision -->
## 결정

현재 backtick mapper를 좁은 bootstrap 구현으로 취급한다. 새 SQL 호환성은
parser/AST 또는 정확한 versioned connector-template recognizer를 사용해야 한다.
알 수 없는 SQL은 선언된 engine subset으로 전달하거나 명시적으로 실패하며
permissive regex로 광범위하게 변환하지 않는다.

<!-- section: consequences -->
## 결과

Semantic adapter가 생기기 전까지 일반 GoogleSQL은 unsupported다. Exact connector
template rule은 version, fingerprint, authoritative source, negative case, removal
condition을 기록한다. SQL DDL은 canonical metadata를 동기화하지 않고 physical
catalog만 변경하면 안 된다.

<!-- section: alternatives -->
## 대안

Regex replacement 목록을 늘리는 방식은 상호작용을 조합할 수 없고 잘못된 table
또는 column을 조용히 대상으로 삼을 수 있어 거부했다.
