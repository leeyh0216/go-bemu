<!-- doc-id: integration-framework -->
<!-- lang: ko -->

[English](../en/framework.md) | [한국어](framework.md)

# 통합 테스트 프레임워크

이 프레임워크는 버전을 고정한 공개 프로세스를 [BigQuery
API](https://cloud.google.com/bigquery/docs/reference/rest)에 대해 실행합니다. 제품
패키지는 이 프레임워크를 가져오거나 클라이언트 버전으로 동작을 선택하지 않습니다.

<!-- section: manifests -->
## 매니페스트와 증거

`tests/integration/contract/consumers.yaml`에는 실행 환경 프로필, 실행 어댑터, 호환성
프로필, scenario와 scenario set을 정의합니다. 각 릴리스는
`tests/integration/contract/cases/*.yaml`에서 이 ID를 선택하고 모든 버전과 아티팩트
해시를 고정합니다. `testEvidence`는 scenario operation ID와 통합 테스트의 operation
marker를 연결합니다. 컴파일러는 알 수 없는 필드, 중복 ID, 누락된 marker, 잘못된
해시, 순서 순환, 실행 환경과 어댑터 불일치를 거부합니다.

완전히 펼친 입력은 다음 명령으로 생성하고 검사합니다.

```bash
make integration-contract-generate
make integration-contract-check
make consumer-runner-test
```

<!-- section: executions -->
## 실행 단위

CI는 `tests/integration/contract/consumers.normalized.json`만 읽습니다. 한 사례에는 공개
API 계약과 간접 Parquet 로드 계약처럼 여러 execution이 있을 수 있습니다. 각
execution은 자료형이 고정된 실행 어댑터 하나를 선택합니다. 실행기는 프로세스를
시작하기 전에 도구의 정확한 버전과 아티팩트 SHA-256을 확인하고, 관찰한 operation
호출 횟수와 순서를 비교한 뒤 JSON 증거, 구조화된 차이, JUnit 결과를 생성합니다.

`required` 사례는 게시 조건입니다. `preview`와 `nightly` 사례는 해당 workflow lane을
선택했을 때만 실행합니다. workflow YAML에 버전을 복사하지 말고 생성 행렬을
사용합니다.

```bash
go run ./tests/integration/cmd/integrationctl matrix \
  --root . --family spark --lane required --execution public
```

<!-- section: extending -->
## 사례 추가 또는 변경

기존 실행 환경, 호출 방법, 통신 계약을 재사용하는 릴리스는 사례 YAML 하나만
추가합니다. 이 계약 중 하나가 바뀔 때만 `consumers.yaml`을 변경합니다. 버전 범위로
어댑터를 추측하지 않습니다. Python, CLI, Spark별 설정은 각 통합 실행 어댑터와
안내 문서 안에서만 관리합니다.

현재 정확한 버전, 변경되지 않는 아티팩트, execution과 scenario ID는 [소비자
호환성](consumer-compatibility.md)에서 자동 생성합니다.
