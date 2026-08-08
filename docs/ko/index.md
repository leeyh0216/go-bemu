<!-- doc-id: docs-index -->
<!-- lang: ko -->

[English](../en/index.md) | [한국어](index.md)

# 문서 색인

<!-- section: guides -->
## 안내서

- [아키텍처](architecture.md): 의존성 규칙, 실행 환경 구성, 영속성 경계와 구현체
  교체 지점을 설명합니다.
- [BigQuery와 커넥터 내부 동작](bigquery-internals.md): REST 작업, Storage
  Read/Write, 간접 로드, `MERGE`, 자료형과 인증 흐름을 설명합니다.
- [호환성](compatibility.md): 기능별 구현 상태와 제한 사항을 정리합니다.
- [로컬 클라이언트 인증 파일과 TLS](client-credentials-and-tls.md): 폐기할 수 있는
  인증 파일, 루프백 token 교환, TLS 신뢰, 클라이언트별 설정을 설명합니다.
- [스키마 변경과 CDC](schema-evolution-cdc.md): 필드 추가 규칙, Storage Write 스키마
  변경, CDC 처리 순서와 현재 제한 사항을 설명합니다.
- [동적 파티션 덮어쓰기](dynamic-partition-overwrite.md): 특정 Spark 스크립트의 의미,
  원자적 실행, 자료형 검사와 현재 미지원 항목을 설명합니다.
- [유지보수 안내서](maintainer-guide.md): 저장소 복제부터 실행까지의 학습 과정,
  버전 추가, 호환성 차이 진단과 릴리스 절차를 설명합니다.
- [설정과 운영](operations.md): 설정 우선순위, 컨테이너 보안, 상태 확인과 종료,
  테스트 제한 시간, 진단용 API를 설명합니다.
- [아키텍처 결정](adr/): 구현에서 지켜야 하는 설계 결정을 기록합니다.

<!-- section: reading-contract -->
## 문서를 읽는 방법

**BigQuery 계약**으로 시작하는 설명은 [공식 BigQuery
문서](https://cloud.google.com/bigquery/docs)에 정의된 서비스 동작을 뜻합니다.
**현재 구현**으로 시작하는 설명은 이 저장소에서 지원하는 동작을 뜻합니다. RPC를
등록했거나 DuckDB에서 SQL이 실행되었다는 사실만으로 BigQuery와 같은 의미를
보장하지는 않습니다. 두 범위를 구분해서 읽어야 합니다.

<!-- section: version-policy -->
## 버전과 출처 정책

커넥터 동작에 관한 설명은 특정 버전의 [Spark BigQuery 커넥터 `0.44.2`
태그](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2)를
기준으로 작성합니다. 기존 에뮬레이터와 비교할 때는 [goccy BigQuery 에뮬레이터
`v0.8.1` 태그](https://github.com/goccy/bigquery-emulator/tree/v0.8.1)를 사용합니다.
해당 코드를 이 프로젝트에 포함하거나 직접 빌드하지는 않습니다.

전송 형식은 [BigQuery Storage RPC
레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc)를 기준으로
설명합니다. 엔진 동작은 [DuckDB 문서](https://duckdb.org/docs/stable/)를 기준으로
설명합니다. 특정 버전에 관한 설명에는 변경될 수 있는 상위 저장소의 브랜치 링크를
사용하지 않습니다.

<!-- section: maintenance -->
## 유지보수 계약

`docs/en`의 모든 파일은 `docs/ko`의 같은 상대 경로에도 존재합니다. 두 언어의 문서는
같은 `doc-id`, 같은 순서의 `section` 표식, 같은 주요 출처 URL을 사용합니다.
`go test ./...`에서 이 조건을 검사합니다. 자세한 내용은 [기여
안내](../../CONTRIBUTING.ko.md)를 참고합니다.
