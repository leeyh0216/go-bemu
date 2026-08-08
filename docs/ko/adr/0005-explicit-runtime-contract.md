<!-- doc-id: adr-0005-explicit-runtime-contract -->
<!-- lang: ko -->

[English](../../en/adr/0005-explicit-runtime-contract.md) | [한국어](0005-explicit-runtime-contract.md)

# ADR-0005: 실행 환경과 진단 기능의 계약을 명시합니다

<!-- section: status -->
## 상태

설정, 진단 기능 및 리스너 구성, 컨테이너 프로필, 종료 시간을 제한한 서버 종료
절차에 적용하기로 승인했습니다. 준비 상태를 해제한 뒤 진행 중인 요청이 끝날
때까지 기다리는 절차와 작업 현황 집계는 제안 단계입니다.

<!-- section: context -->
## 배경

숨겨진 기본값, 테스트 곳곳에 흩어진 대기 코드, 공개된 진단 경로, 종료 기한이
없는 정상 종료 절차는 호환성 오류를 재현하기 어렵게 합니다. 민감한 값이 노출될
위험도 있습니다. BigQuery 호환 REST와 Storage RPC 리스너는 외부에 공개하는
프로토콜 접점입니다. 반면 에뮬레이터 진단 기능은 프로젝트가 소유하는 관리
기능입니다.
서비스 목록은 [Storage RPC
레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc)를 기준으로
합니다. 컨테이너 격리 설정은 [Compose 서비스
레퍼런스](https://docs.docker.com/reference/compose-file/services/)를 기준으로 합니다.

<!-- section: decision -->
## 결정

1. 설정은 `compiled defaults < YAML file < mapped environment < repeated --set`
   순서로 병합합니다. `--config`는 `BQEMU_CONFIG` 파일 선택값보다 우선합니다.
2. 진단 기능은 기본적으로 꺼진 별도 리스너에서 제공합니다. BigQuery REST
   네임스페이스를 공유하지 않습니다. 루프백이 아닌 주소에 바인딩하려면
   토큰과 서버 TLS가 필요합니다.
3. 시작, 요청, 최종 상태 확인, 종료 테스트에는 이름이 있고 설정으로 조정할 수
   있는 제한 시간을 사용합니다. 진단 정보는 민감한 내용을 제거하고 크기를
   제한합니다.
4. 릴리스 컨테이너는 루트 권한이 아닌 사용자로 실행합니다. 운영 프로필에는 읽기
   전용 루트 파일 시스템, 쓰기 가능한 데이터 볼륨, 용량이 제한된 임시 저장소,
   상태 점검과 준비 상태 점검을 설정합니다. 제한 시간이 지나면 강제로 종료하는
   하나의 정상 종료 기한을 사용합니다.
5. 어떤 모드에서도 엔드포인트나 로그에 인증 정보, 토큰, 개인 키, SQL 원문, 행
   데이터, HTTP 본문, Protobuf JSON, 오류 원문을 노출하지 않습니다. 내용을
   공개하지 않는 값은 구조, 개수, 길이와 SHA-256 해시만 요약해 기록합니다. 일부만
   가려서는 안전을 보장할 수 없으므로 기존 `unsafePayloads` 입력은 사용 중단
   예정이며 아무 동작도 하지 않도록 유지합니다. [Cloud Logging 감사 로그
   권장사항](https://cloud.google.com/logging/docs/audit/best-practices)을 따릅니다.

<!-- section: consequences -->
## 결과

설정 로더는 버전을 엄격하게 확인합니다. 범용 설정 덮어쓰기는 타입을 확인하며,
유효성 검사와 최종 설정 모델을 지원합니다. 설정 원본과 최종 설정의 지문도
계산합니다. 접근과 응답 크기를 제한한 진단 기능을 구현했습니다. 보안을 강화한
Compose 프로필과 파일 기반 구성도 제공합니다.

서버는 설정된 HTTP/gRPC 제한과 공통 TLS 설정을 적용합니다. 관리용 리스너는
활성화한 경우에만 시작합니다. 공통 종료 기한이 지나면 gRPC 서버를 강제로
중지합니다. 리소스 종료 단계에서는 `QueryService`의 신규 요청 수락, 취소, 진행
중인 요청 완료를 관리합니다. 이 단계는 Storage Read, Storage Write, DuckDB
종료보다 먼저 실행합니다.

`logging.unsafePayloads`를 설정한 기존 구성도 계속 해석하며 같은 최종 모델을
만듭니다. 값이 `true`이면 사용 중단 안내 이벤트만 남깁니다. 데이터 내용을
노출하지 않는 로그 정책은 바뀌지 않습니다.

준비 상태를 해제한 뒤 진행 중인 요청이 끝날 때까지 기다리는 기능, 미완료 작업
보고, 두 번째 종료 신호 전용 처리 경로, 단계별로 나눈 테스트 제한 시간은 아직
완성되지 않았습니다. 설정 오류에는
`model_version/operation/shape/fingerprint/fix_hint`를 사용합니다. 프로토콜
차이에는 `version/operation/shape/fingerprint/fix_hint`를 사용합니다.

<!-- section: alternatives -->
## 대안

공개 BigQuery 리스너에 진단 처리기를 두면 에뮬레이터 관리 API와 호환성 경계가
섞입니다. 따라서 이 방식은 채택하지 않았습니다. 코드에 고정된 제한 시간을 여러
곳에 두는 방식도 채택하지 않았습니다. CI 환경에 맞춰 조정하려면 코드를 바꿔야
하고, 오류가 발생했을 때 실제로 적용한 설정을 하나로 보여줄 수 없기 때문입니다.

비밀 정보를 자동으로 불러오는 방식도 채택하지 않았습니다. 저장소에 포함된
`.envrc`는 `.envrc.example`의 비밀 정보가 없는 기본값과 Git에서 제외한
`.envrc.local`의 선택적 장비별 설정만 불러옵니다.
