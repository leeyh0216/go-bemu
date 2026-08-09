<!-- doc-id: docs-index -->
<!-- lang: ko -->

[English](../en/index.md) | [한국어](index.md)

# go-bemu 문서

이 문서는 BQEMU를 실행하고 사용하는 사람을 위한 경로입니다. 어디에 접속하는지,
어떤 리소스를 먼저 만드는지, 기능을 사용할 수 있는지를 설명합니다. 공개 리소스
형태는 [BigQuery REST API 레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest)를
따릅니다.

<!-- section: start -->
## 시작

- [시작하기](getting-started.md): Compose 실행, 올바른 접속 주소 선택, 프로젝트/데이터세트/
  테이블 생성, 첫 쿼리를 설명합니다.
- [설정](configuration.md): 서비스 설정과 준비 상태가 되기 전 여러 프로젝트와 데이터세트를
  생성하는 방법을 설명합니다.

<!-- section: use -->
## 서비스 사용

- [로컬 인증 파일과 TLS](client-credentials-and-tls.md): 필요한 호출자에만 사용하는 선택형
  로컬 TLS와 인증 파일
- [호환성](compatibility.md): 지금 사용할 수 있는 기능, 제한 기능, 미지원 기능을 짧게
  정리한 문서
- [API 및 RPC 레퍼런스](api-rpc-compatibility.md): 자동 생성한 정확한 공개 메서드 인벤토리.
  특정 필드나 RPC에 의존할 때 확인합니다.

<!-- section: maintain -->
## 서비스 유지보수

구현, 통합 테스트 증거, 아키텍처, CI, 릴리스, 기여 절차는 [유지보수 문서](maintainers/index.md)에
분리되어 있습니다. 로컬 서비스를 사용하는 데 먼저 읽을 문서는 아닙니다.
