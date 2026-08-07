<!-- doc-id: docs-index -->
<!-- lang: ko -->

[English](../en/index.md) | [한국어](index.md)

# 문서 색인

<!-- section: guides -->
## 안내서

- [아키텍처](architecture.md): 의존성 규칙, runtime composition, 영속성 경계,
  교체 지점.
- [BigQuery와 connector 내부 동작](bigquery-internals.md): REST job, Storage
  Read/Write, indirect load, MERGE, type, 인증 흐름.
- [호환성](compatibility.md): 구현, 부분 지원, 등록, 계획, 미지원 동작.
- [Schema evolution과 CDC](schema-evolution-cdc.md): additive schema 규칙,
  Storage Write schema 변경, CDC 순서 규칙, 명시적인 현재 한계.
- [동적 파티션 덮어쓰기](dynamic-partition-overwrite.md): 고정된 Spark script
  의미, atomic execution, type 검증, 승격 gap.
- [Maintainer 안내서](maintainer-guide.md): clone부터 실행까지의 학습 경로,
  version 추가, drift 진단, release runbook.
- [설정과 운영](operations.md): precedence, container hardening,
  health/shutdown, test timeout, diagnostics endpoint 설계.
- [아키텍처 결정](adr/): 구현을 제약하는 결정.

<!-- section: reading-contract -->
## 문서를 읽는 방법

**BigQuery 계약**으로 시작하는 설명은 [공식 BigQuery
문서](https://cloud.google.com/bigquery/docs)의 서비스 계약을 뜻한다.
**현재 구현**으로 시작하는 설명은 이 저장소를 뜻한다. RPC 등록이나 DuckDB SQL
성공이 BigQuery 의미 동등성을 증명하지 않으므로 둘을 의도적으로 구분한다.

<!-- section: version-policy -->
## 버전과 출처 정책

Connector 의존 설명은 정확한 [Spark BigQuery connector `0.44.2`
tag](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2)를
사용한다. 이전 emulator 비교에는 정확한 [goccy BigQuery emulator `v0.8.1`
tag](https://github.com/goccy/bigquery-emulator/tree/v0.8.1)를 사용하며 이 project의
일부로 clone하거나 build하지 않는다. Wire 계약은 [BigQuery Storage RPC
레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc), 엔진 설명은
[DuckDB 문서](https://duckdb.org/docs/stable/)를 사용한다. 버전에 묶인 주장에
변경 가능한 upstream branch 링크를 허용하지 않는다.

<!-- section: maintenance -->
## 유지보수 계약

`docs/en`의 모든 파일은 `docs/ko`에 같은 상대 경로로 존재한다. 두 파일은 같은
`doc-id`, 순서가 같은 `section` marker, 같은 primary-source URL을 가진다.
`go test ./...`가 이 계약을 검사한다. [기여 안내](../../CONTRIBUTING.ko.md)를
참고한다.
