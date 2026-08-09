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

<!-- section: add-behavior -->
## 동작을 추가하는 순서

1. 외부 프로세스 테스트와 literal operation annotation을 먼저 추가합니다.
2. `consumers.yaml`에 scenario selector를 추가하거나 범위를 좁힙니다. selector는
   자료형이 있는 runner가 실행할 test file 또는 command entrypoint를 가리킵니다.
3. runner가 비교해야 할 순서와 cardinality만 기록합니다. marker는 operation이
   관련 있다는 사실만 말하며, 몇 번 호출되고 무엇 뒤에 와야 하는지는 expectation을
   통해서만 알 수 있습니다.
4. scenario에 `operationIds`나 `testEvidence`를 작성하지 않습니다.
   `trafficSource: {kind: annotations}`이면 compiler가 선택한 annotation에서 둘 다
   파생합니다. `runner-evidence`는 `load:` selector만 사용할 수 있고 reason이 필요하며,
   operation은 명시한 ordering expectation에서 파생합니다.
5. 새로운 고정 executable/runtime/provenance 조합에만 release case YAML을 추가합니다.
   wire contract가 같다면 runner adapter와 scenario set을 재사용합니다.
6. `make integration-contract-generate`를 실행해 생성된 claim과 정규 matrix를 검토하고,
   `make integration-contract-check`와 runner unit test를 실행합니다.

load 전용 흐름은 `load:` adapter가 선택하고 구조화한 runtime evidence로 operation 순서를
증명합니다. 선택 test function으로 직접 표현하지 못하는 예외는 `trafficSource`에
명시하며, 다른 scenario kind에는 사용할 수 없습니다.

dataframe media upload는 `runner-evidence` 예외가 아니라 선택된 Python test입니다. CI는
`tests/integration/python/dataframe-requirements.lock`을 설치하며, 로컬에서 같은 환경을
준비할 때는 `make python-dataframe-setup`을 사용합니다. 이 test는 fake-GCS runtime을
소유하고 외부 client는 BQEMU 공개 endpoint로만 연결하며 multipart와 resumable Parquet
upload를 모두 확인합니다. dataframe helper를 검증할 때는 호출 전에 같은 endpoint-aware
client로 destination table을 만들어야 합니다. 누락된 table을 생성하는 과정에서 helper가
기본 endpoint client를 새로 만들 수 있기 때문입니다.
계약은 공개 client의 생성과, 미리 만든 destination에 대한 helper append/replace를
검증합니다.

REST setup 또는 assertion query에는 phase-aware harness helper를 사용합니다. helper가
request log에 `setup` 또는 `assertion`을 기록하고 runner는 그 응답을 호출자 wire
claim에서 제외합니다. 새 harness helper를 추가하기 전에 이 mechanism을 확장합니다.
comparator를 통과시키기 위해 harness 전용 operation을 `contract_case(...)`에 넣지 마세요.

<!-- section: extending -->
## 사례 추가 또는 변경

기존 실행 환경, 호출 방법, 통신 계약을 재사용하는 릴리스는 사례 YAML 하나만
추가합니다. 이 계약 중 하나가 바뀔 때만 `consumers.yaml`을 변경합니다. 버전 범위로
어댑터를 추측하지 않습니다. Python, CLI, Spark별 설정은 각 통합 실행 어댑터와
안내 문서 안에서만 관리합니다.

현재 정확한 버전, 변경되지 않는 아티팩트, execution과 scenario ID는 [소비자
호환성](consumer-compatibility.md)에서 자동 생성합니다.
