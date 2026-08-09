<!-- doc-id: maintainers/index -->
<!-- lang: ko -->

[English](../../en/maintainers/index.md) | [한국어](index.md)

# 유지보수 문서

이 색인은 BQEMU를 구현하고 검토하며 운영하고 배포하는 분을 위한 문서입니다. 공개
프로토콜에 관한 판단은 [BigQuery REST API
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest)에서 시작합니다.

<!-- section: architecture -->
## 아키텍처와 계약

- [애플리케이션 경계](application-boundaries.md): handler 소유권과 의존 방향을 설명합니다.
- [아키텍처](../architecture.md): 패키지 경계, 의존성 방향, 실행 환경 구성, 영속성
  소유권을 설명합니다.
- [BigQuery와 커넥터 내부 동작](../bigquery-internals.md): 프로토콜 흐름, 커넥터
  동작, 자료형, 변환 경계를 설명합니다.
- [아키텍처 결정](../adr/): 구현에서 지켜야 하는 결정을 기록합니다.

<!-- section: implementation -->
## 구현 안내

- [SQL 회귀 케이스](sql-regression.md): 데이터 기반 픽스처, 자료형이 있는 기대값,
  집중 실행과 필수 CI 동작을 설명합니다.
- [안정 릴리스](release.md): canonical SemVer와 main 전용 게시를 설명합니다.
- [저장 엔진 어댑터 구현 안내서](../engine-adapter-guide.md): 기능 선언, 계획 계약,
  실행 환경 구성, 적합성 검사를 설명합니다.
- [스키마 변경과 CDC](../schema-evolution-cdc.md): 스키마 변경과 쓰기 경로 설계를
  설명합니다.
- [설정과 운영](../operations.md): 설정 우선순위, 상태 확인, 종료, 진단 정보,
  컨테이너 운영 방법을 설명합니다.

<!-- section: workflow -->
## 저장소 작업 절차

- [유지보수 안내서](../maintainer-guide.md): 로컬 실행 준비, 소비자 버전 추가,
  호환성 문제 진단, 배포 절차를 설명합니다.
- [기여 안내](../../../CONTRIBUTING.ko.md): 이슈 단위 변경, 검증, 커밋, 검토 절차를
  설명합니다.
- [호환성 매니페스트](../compatibility.md): 코드, 테스트, 자동 생성 문서가 함께
  유지해야 하는 공개 계약입니다.

완성되지 않은 기능은 호환성 매니페스트와 연결된 이슈에서 관리합니다. 사용자 기능
안내로 표시하지 않습니다.
