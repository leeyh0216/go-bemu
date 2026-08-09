<!-- doc-id: integration-framework -->
<!-- lang: ko -->

[English](../en/framework.md) | [한국어](framework.md)

# 통합 테스트 프레임워크

이 프레임워크는 버전을 고정한 공개 프로세스를 [BigQuery
API](https://cloud.google.com/bigquery/docs/reference/rest)에 대해 실행합니다. 제품
패키지는 이 프레임워크를 가져오거나 클라이언트 버전으로 동작을 선택하지 않습니다.

<!-- section: manifests -->
## 매니페스트와 증거

수기로 관리하는 입력은 세 가지입니다. 그 외는 생성하거나 실행 중에 관찰합니다.

1. `tests/integration/<family>`의 테스트 하나가 호출자에게 보이는 동작 하나를
   증명하고 literal operation annotation을 가집니다.
2. `tests/integration/contract/consumers.yaml`은 runner 소유권, selector, scenario
   grouping, operation 순서/cardinality expectation을 선언합니다.
3. `tests/integration/contract/cases/` 아래 파일 하나가 릴리스의 runtime, provenance,
   변경되지 않는 artifact를 고정합니다.

Python 계열 테스트에는 테스트 함수 바로 위에 공개 operation마다 marker 하나를
붙입니다.

```python
@pytest.mark.operation("bigquery.tables.get")
@pytest.mark.operation("grpc.bigquery-read.create-read-session")
def test_reads_one_table(...):
    ...
```

명령행 runner는 같은 의미의 `@operation("...")` decorator를 사용합니다. marker에는
literal ID 하나만 넣습니다. 컴파일러는 없는 ID, scenario evidence가 선택하지 않은
marker, marker가 없는 선택 테스트를 거부합니다.

`consumers.yaml`에는 실행 환경 프로필, 실행 어댑터, 호환성 프로필, scenario와
scenario set을 정의합니다. 각 릴리스는 `tests/integration/contract/cases/*.yaml`에서
이 ID를 선택하고 모든 버전과 artifact 해시를 고정합니다. 컴파일러는 알 수 없는
필드, 중복 ID, 잘못된 해시, 순서 순환, 실행 환경과 어댑터 불일치를 거부합니다.

완전히 펼친 입력은 다음 명령으로 생성하고 검사합니다.

```bash
make integration-contract-generate
make integration-contract-check
make consumer-runner-test
```

`integration-contract-generate`는 정규 실행 입력과 EN/KO 호환성 페이지를 만듭니다.
생성물은 직접 수정하지 않습니다.

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

<!-- section: add-behavior -->
## 동작을 추가하는 순서

1. 외부 프로세스 테스트와 literal operation annotation을 먼저 추가합니다.
2. `consumers.yaml`에 scenario selector를 추가하거나 범위를 좁힙니다. selector는
   자료형이 있는 runner가 실행할 test file 또는 command entrypoint를 가리킵니다.
3. runner가 비교해야 할 순서와 cardinality만 기록합니다. marker는 operation이
   관련 있다는 사실만 말하며, 몇 번 호출되고 무엇 뒤에 와야 하는지는 expectation을
   통해서만 알 수 있습니다.
4. 현재 매니페스트 schema가 요구하는 동안에는 annotation한 test function을
   `testEvidence`에 넣습니다. compiler는 source annotation과의 불일치를 거부합니다.
5. 새로운 고정 executable/runtime/provenance 조합에만 release case YAML을 추가합니다.
   wire contract가 같다면 runner adapter와 scenario set을 재사용합니다.
6. `make integration-contract-generate`를 실행해 정규 matrix를 검토하고,
   `make integration-contract-check`와 runner unit test를 실행합니다.

load 전용 흐름은 현재 `load:` adapter가 선택하고 구조화한 runtime evidence로 operation
순서를 증명합니다. Python test function annotation으로 직접 표현하지 못하므로,
annotation-derived scenario projection이 이 특수 경로를 바꾸기 전까지 순서/cardinality를
명시적으로 유지합니다.

<!-- section: extending -->
## 사례 추가 또는 변경

기존 실행 환경, 호출 방법, 통신 계약을 재사용하는 릴리스는 사례 YAML 하나만
추가합니다. 이 계약 중 하나가 바뀔 때만 `consumers.yaml`을 변경합니다. 버전 범위로
어댑터를 추측하지 않습니다. Python, CLI, Spark별 설정은 각 통합 실행 어댑터와
안내 문서 안에서만 관리합니다.

현재 정확한 버전, 변경되지 않는 아티팩트, execution과 scenario ID는 [소비자
호환성](consumer-compatibility.md)에서 자동 생성합니다.

<!-- section: generated-output -->
## 생성물과 CI

compiler는 검토 가능한 projection만 다음과 같이 만듭니다.

- `tests/integration/contract/consumers.normalized.json`: runner와 workflow matrix가
  읽는 완전히 펼친 입력
- `tests/integration/docs/en/consumer-compatibility.md`와 한국어 짝: 릴리스/runtime/
  provenance를 표시하는 문서

두 번째 coverage JSON을 만들거나 이 projection을 직접 수정하지 않습니다. runner는
실행 중에만 evidence, diff, JUnit을 만듭니다. CI는 Job Summary에 결과를 표시하고,
`test-report-*`에 JUnit과 `index.html`을 보관하며 raw diagnostic은 실패한 job에서만
올립니다.

<!-- section: failures -->
## 실패 안내

| 실패 | 원본에서 고칠 위치 |
| --- | --- |
| 알 수 없는 operation annotation | 기존 공개 operation ID를 쓰거나 `contract/operations.yaml`을 통해 추가합니다. |
| Test evidence에 marker가 없음 | 선택한 테스트 함수에 literal marker를 붙입니다. |
| 생성물이 오래됨 | `make integration-contract-generate`를 실행하고 projection을 검토·커밋합니다. |
| Runner가 예상 밖 operation을 보고 | 제품 동작/테스트를 고치거나 근거 있는 scenario expectation을 추가합니다. 관찰 event를 버리지 않습니다. |
| Artifact/version 불일치 | workflow YAML이 아니라 versioned case YAML과 변경되지 않는 provenance를 갱신합니다. |
