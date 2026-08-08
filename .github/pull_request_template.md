## 관련 이슈

- 주 이슈: refs #
- 의존 이슈 또는 커밋:

## 변경 범위

- [ ] 이 풀 리퀘스트는 주 이슈 하나의 응집된 범위만 포함합니다.
- [ ] 기능 변경과 관련 없는 리팩터링을 섞지 않았습니다.
- [ ] 공유 파일은 의존 커밋을 기준으로 rebase한 뒤 수정했습니다.

변경한 공개 동작과 내부 경계를 설명해 주세요.

## 공개 계약

- operation ID 또는 capability ID:
- REST method/path, gRPC RPC, SQL 규칙 또는 wire 형식:
- 지원 입력과 명시적으로 지원하지 않는 입력:
- 공식 출처와 고정 버전:

## 아키텍처

- [ ] domain/application은 transport, storage engine, client SDK를 직접 참조하지 않습니다.
- [ ] 외부 엔진과 서비스는 작은 port와 adapter 뒤에 있습니다.
- [ ] 모듈 내부 concrete type이나 내부 저장 형식을 다른 모듈에 노출하지 않습니다.
- [ ] 새 의존성은 typed constructor를 통해 composition root에서 주입합니다.

## 검증

- [ ] 관련 단위·애플리케이션·transport 테스트를 추가하거나 갱신했습니다.
- [ ] 필요한 공개 프로세스 소비자 테스트를 실행했습니다.
- [ ] `make ci-static`이 통과합니다.
- [ ] `make ci-test-all`이 통과합니다.
- [ ] race 검사가 필요한 기능군의 CI target이 통과합니다.

실행한 명령과 결과를 적어 주세요.

## 생성물과 문서

- [ ] 생성된 매니페스트·문서·검증 자료를 원본과 같은 커밋에 포함했습니다.
- [ ] 사용자 문서는 현재 구현된 동작만 설명합니다.
- [ ] 관리자 문서는 영어와 한국어를 함께 갱신했습니다.
- [ ] 아직 구현하지 않은 동작은 한국어 제목의 열린 이슈에 연결했습니다.

## 보안과 운영

- [ ] 로그, 오류, fixture, CI artifact에 토큰·SQL·행 값·리소스 이름을 노출하지 않습니다.
- [ ] timeout, cancellation, shutdown, retry 경계를 검토했습니다.
- [ ] GitHub Actions는 허용된 공식 또는 Verified Creator와 전체 커밋 SHA만 사용합니다.

## 완료 판단

- [ ] 모든 완료 조건을 충족했습니다. 병합 후 `closes #N`으로 종료할 수 있습니다.
- [ ] 일부 조건이 남았습니다. 이슈를 열린 상태로 유지하고 차단 조건을 갱신했습니다.
