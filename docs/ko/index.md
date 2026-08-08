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
## 클라이언트별 안내

- [Python BigQuery 클라이언트 3.43.0](clients/python-bigquery.md)
- [`bq` CLI 2.1.31](clients/bq-cli.md)
- [Spark BigQuery 커넥터 0.44.2를 사용하는 PySpark와 Scala Spark
  3.5.8](clients/spark-bigquery-connector.md). 검토한 [커넥터 소스
  리비전](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/719817782a214b8ca72be520870013a3e0253d92)을
  기준으로 합니다.

각 문서에는 시험한 scenario ID, 공개 operation ID, 요청 순서, 요청과 응답 형식이
정리되어 있습니다.

<!-- section: compatibility -->
## 호환성 자료

- [호환성](compatibility.md): 지원 상태의 의미와 자동 생성 API/RPC 표의 연결
  문서입니다.
- [API 및 RPC 호환성](api-rpc-compatibility.md): 메서드, 접속 경로, 적용 조건,
  테스트, 이슈를 자동으로 정리한 표입니다.
- [소비자 호환성](../../tests/integration/docs/ko/consumer-compatibility.md): 통합 테스트 클라이언트 버전, 실행 아티팩트,
  scenario selector를 자동으로 정리한 문서입니다.

<!-- section: maintainers -->
## 유지보수 문서

아키텍처, 어댑터 계약, 구현 설명, 운영 절차, 버전 추가 방법은 별도의 [유지보수 문서
색인](maintainers/index.md)에 정리되어 있습니다.
