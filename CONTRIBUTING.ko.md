<!-- doc-id: contributing -->
<!-- lang: ko -->

[English](CONTRIBUTING.md) | [한국어](CONTRIBUTING.ko.md)

# go-bemu 기여 안내

<!-- section: scope -->
## 범위를 먼저 정의하기

BigQuery 동작을 구현하기 전에 재현할 대상을 먼저 정합니다. 공개 REST 메서드,
gRPC 호출이나 메시지, SQL 규칙, 전송 형식 중 무엇을 구현하는지 명확히 합니다.
관련 공식 명세의 링크는 구현 및 문서와 가까운 곳에 둡니다. [BigQuery REST
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest), [Storage RPC
레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc),
[GoogleSQL 레퍼런스](https://cloud.google.com/bigquery/docs/reference/standard-sql)에서
시작합니다.

<!-- section: architecture -->
## 아키텍처 규칙

- `internal/domain`과 `internal/application`은 HTTP, gRPC, Google DTO, DuckDB,
  객체 저장소 SDK에 의존하지 않도록 유지합니다.
- 외부 시스템은 포트 인터페이스를 통해 연결합니다. 어댑터가 포트를 구현하는지는
  컴파일 시점에 확인합니다.
- 트랜잭션, 오프셋, 멱등성, 가시성의 불변 조건은 해당 조건이 제한하는 상태 전이와
  가까운 곳에 둡니다.
- 지원하지 않는 동작은 명확한 오류로 거부합니다. 다른 BigQuery 타입이나
  수명 주기로 조용히 변환하지 않습니다.
- 실행 설정을 추가할 때는 버전이 지정된 YAML 모델, 기본값, 유효성 검사, 타입을
  확인하는 `--set` 경로, 예제를 함께 수정합니다.
  `defaults < file < environment < --set` 우선순위를 유지합니다. 숨은 플래그나
  테스트에만 통하는 특수값을 추가하지 않습니다.
- 비밀 정보는 마운트한 파일의 경로로 참조합니다. 최종 설정, 로그, 시험 데이터,
  예제에 비밀 정보의 실제 내용을 넣지 않습니다.

<!-- section: provenance -->
## 출처 규칙

- 1차 출처를 사용합니다. 커넥터에 의존하는 동작은 변경될 수 있는 브랜치가 아니라
  정확한 [`0.44.2`
  태그](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2)를
  연결합니다.
- 이전 에뮬레이터와 비교할 때는 정확한 [goccy BigQuery 에뮬레이터 `v0.8.1`
  태그](https://github.com/goccy/bigquery-emulator/tree/v0.8.1)를 연결합니다. 소스 코드를
  복사하거나 빌드 의존성으로 추가하지 않습니다.
- 정답으로 사용하는 시험 데이터와 호환성 설명에는 근거가 된 외부 저장소의
  정확한 태그나 커밋을 연결합니다.
- 계약 내용은 직접 풀어 씁니다. 인용이 꼭 필요하면 짧게 쓰고 출처를
  밝힙니다.
- 특정 버전에 관한 설명에는 GitHub `master`나 `main` 소스 링크를 사용하지
  않습니다.
- 모든 호환성 보완 사항에는 제거 조건을 적습니다.

<!-- section: bilingual-docs -->
## 이중 언어 문서

유지보수 담당자를 위한 Markdown을 수정할 때는 같은 풀 리퀘스트에서 두 언어
파일을 모두 갱신해야 합니다.

한국어 문서는 [한국어 문서 작성 기준](docs/korean-writing-style.md)에 따라
작성합니다.

- `README.md`와 `README.ko.md`
- `CONTRIBUTING.md`와 `CONTRIBUTING.ko.md`
- `docs/en/**`와 `docs/ko/**`의 동일한 상대 경로

`doc-id`와 순서가 있는 `section` 표식을 동일하게 유지합니다. 링크 문구를
번역하더라도 1차 출처 URL은 같아야 합니다. 모든 페이지에서 영어와 한국어를
전환하는 링크를 유지합니다.

<!-- section: tests -->
## 테스트

```bash
make ci-static
make ci-test-all
```

도메인 상태 테스트, 외부 연동 어댑터의 테스트용 구현을 사용하는 애플리케이션
테스트, 공개 REST/gRPC 계약 테스트, 잘못된 입력 및 경계값 사례를 추가합니다.
DuckDB가 SQL을 받아들였다는 사실만으로는 BigQuery 동작을 재현했다고 볼 수
없습니다.

개발 중에는 관련 패키지나 소비자 테스트를 먼저 실행합니다. 커밋하기 전에는
저장소의 필수 검증을 모두 실행합니다. 생성된 매니페스트와 문서, 검증 자료는
원본과 같은 커밋에 포함해야 합니다. CI는 오래된 생성 파일을 거부합니다.

<!-- section: evolution-pipeline -->
## 호환성 확장 절차

동작은 다음 순서로 추가합니다.

```text
protocol profile -> adapter -> capability -> golden -> E2E
```

프로필은 공개 클라이언트와 버전, 관찰한 전송 계약을 고정합니다. 어댑터에는 꼭
필요한 변환만 둡니다. 지원 기능 선언에는 정확한 지원 수준을 적습니다. 민감한
정보를 제거한 검증 기준 데이터에는 데이터 구조를 기록합니다. E2E 테스트는 배포된
클라이언트가 공개 API까지 정상적으로 요청을 전달하는지 검증합니다. 차이
보고서에는
`version`, `operation`, `shape`, `fingerprint`, `fix_hint`가 반드시 포함되어야
합니다. 이 절차가 [BigQuery API
계약](https://cloud.google.com/bigquery/docs/reference)과의 비교를 대신하지는 않습니다.

<!-- section: issue-workflow -->
## 이슈 단위 작업 절차

기능 구현과 리팩터링은 한국어 제목을 사용하는 열린 이슈 하나에서 시작합니다.
이슈에는 구체적인 범위와 완료 조건, 제외 범위, 의존성을 적습니다. 이슈 하나는
브랜치와 작업 트리 하나만 사용합니다. 예를 들어 #32는
`issue/32-contribution-process`를 사용합니다. 테스트를 통과시키기 위해 서로
관계없는 이슈의 변경을 한 커밋에 섞지 않습니다.

1. 파일을 수정하기 전에 이슈와 완료 조건을 확인합니다.
2. 마지막으로 검증된 기준 커밋에서 이슈 전용 브랜치와 작업 트리를 만듭니다.
3. 공유 파일을 수정하기 전에 소유 범위를 알립니다. 의존 작업의 미커밋 코드를
   복사하지 않고, 해당 커밋이 만들어진 뒤 rebase합니다.
4. 응집된 변경 하나와 테스트, 생성 파일, 관리자 문서를 함께 구현합니다.
5. 관련 범위 검증을 먼저 실행한 뒤 저장소의 필수 검증을 실행합니다.
6. stage한 diff를 정확히 검토합니다. 변경 파일이 남아 있는 작업 트리에서는
   `git add .`이나 `git add -A`를 사용하지 않습니다. 이슈가 소유한 파일이나
   patch hunk만 stage합니다.
7. 커밋 메시지에 이슈 번호를 넣고 바로 push합니다. 이슈에는 커밋과 CI 실행을
   연결합니다.
8. 커밋이 대상 브랜치에 반영되고 필수 `validation-complete` 작업이 성공한 뒤에만
   이슈를 닫습니다.

완료 조건이 하나라도 남아 있으면 `refs #N`을 사용합니다. 커밋 하나로 이슈가
완료될 때만 `closes #N`을 사용합니다. 구현 중 추가 작업이 발견되면 열린 이슈의
범위를 갱신하거나 별도 한국어 이슈를 만듭니다. 임시 진행 상황이나 계획 중인
기능을 사용자 문서에 기록하지 않습니다.

병렬 에이전트도 같은 소유 규칙을 따릅니다. 에이전트는 수정한 파일과 검증 결과를
정확히 보고하고, 다른 이슈의 변경을 커밋하지 않습니다. 다른 이슈가 해당 커밋을
기준으로 rebase하기 전에는 공유 파일 수정을 중단합니다.

<!-- section: change-description -->
## 변경 설명

지원하는 커넥터 및 클라이언트 버전, 지원 기능 ID, 공식 근거, 확인한 차이,
선택한 경계, 오류 동작, 남은 제약을 적습니다. 모든 완료 조건을 실제로
충족하기 전에는 `refs #N`을 사용합니다. 풀 리퀘스트에는 주 이슈 하나만 지정하고,
의존 커밋이 있다면 범위를 섞지 말고 그 관계를 설명합니다.
