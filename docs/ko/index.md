<!-- doc-id: docs-index -->
<!-- lang: ko -->

[English](../en/index.md) | [한국어](index.md)

# 사용자 문서

이 문서는 애플리케이션, CLI, 커넥터에서 BQEMU를 사용하는 분을 위한 안내입니다.
BigQuery 리소스는 공개 [BigQuery REST API
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest)를 기준으로 합니다.

<!-- section: start -->
## 먼저 읽을 문서

- [시작하기](getting-started.md): Compose 실행, 리소스 생성, 첫 쿼리, fake GCS 추가,
  개발 컨테이너 연결 방법을 설명합니다.
- [클라이언트 인증 파일과 TLS](client-credentials-and-tls.md): 로컬 service account,
  authorized user, WIF, direct token, 인증서, truststore 자료를 생성하는 방법을
  설명합니다.

<!-- section: clients -->
## 클라이언트 사용 안내

- [Python BigQuery 클라이언트](clients/python-bigquery.md)
- [`bq` CLI](clients/bq-cli.md)
- [PySpark와 Scala Spark](clients/spark-bigquery-connector.md)

사용자 가이드는 호스트, Compose 네트워크, 개발 컨테이너별 접속 주소를 설명합니다.
버전을 고정한 검증 근거는 통합 테스트 매니페스트에서 별도로 관리합니다.

<!-- section: compatibility -->
## 호환성 자료

- [호환성](compatibility.md): 지원 상태의 의미와 자동 생성 API/RPC 표의 연결
  문서입니다.
- [API 및 RPC 호환성](api-rpc-compatibility.md): 메서드, 접속 경로, 적용 조건,
  테스트, 이슈를 자동으로 정리한 표입니다.
- [소비자 호환성](consumer-compatibility.md): 클라이언트 버전, 실행 아티팩트,
  scenario selector를 자동으로 정리한 문서입니다.

<!-- section: maintainers -->
## 유지보수 문서

아키텍처, 어댑터 계약, 구현 설명, 운영 절차, 버전 추가 방법은 별도의 [유지보수 문서
색인](maintainers/index.md)에 정리되어 있습니다.
