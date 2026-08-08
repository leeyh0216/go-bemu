<!-- doc-id: adr-0003-public-api-edge -->
<!-- lang: ko -->

[English](../../en/adr/0003-public-api-edge.md) | [한국어](0003-public-api-edge.md)

# ADR-0003: 공개 API 경계를 하나로 유지합니다

<!-- section: status -->
## 상태

승인했습니다.

<!-- section: context -->
## 배경

SDK, `bq`, Spark, 계약 테스트, 선택적 관리 화면은 같은 리소스와 오류를 확인해야
합니다. 관리 화면 전용 비공개 API는 호환성 검증을 우회합니다. 공개
클라이언트에서는 실패했는데 관리 화면에서는 성공으로 표시할 수도 있습니다.
공개 리소스 형식은 [BigQuery REST
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest/v2)에 정의되어 있습니다.

<!-- section: decision -->
## 결정

기능을 호출하는 모든 클라이언트는 `/bigquery/v2`를 사용합니다. 에뮬레이터 전용
프로젝트 수명 주기, 지원 기능 조회, 초기화, 앞으로 추가할 초기 데이터 등록 및
추적 기능은 명확히 구분된 `/emulator/v1` 네임스페이스를 사용합니다. 상태를
변경할 수 있는 기능은 비활성화하거나 접근을 제한해야 합니다. 관리 화면은 조회
기능을 위한 링크를 제공할 수 있지만 업무 동작을 별도로 구현하면 안 됩니다.

<!-- section: consequences -->
## 결과

공개 API 경계 테스트의 결과는 모든 클라이언트에 적용됩니다. 관리 기능을 BigQuery
확장처럼 조용히 추가할 수 없습니다. 선택적인 정적 관리 화면 제공은 API 시작
과정과 독립적이어야 합니다. 관리 화면의 정적 파일이 없어도 REST와 gRPC는
동작해야 합니다.

<!-- section: alternatives -->
## 대안

관리 화면 전용 백엔드는 두 개의 계약을 만들기 때문에 채택하지 않았습니다. 관리
기능을 BigQuery v2 경로 아래에 숨기면 공식 클라이언트가 BigQuery에 없는 동작을
우연히 발견할 수 있습니다. 따라서 이 방식도 채택하지 않았습니다.
