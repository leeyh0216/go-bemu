<!-- doc-id: maintainer-guide -->
<!-- lang: ko -->

[English](../en/maintainer-guide.md) | [한국어](maintainer-guide.md)

# 유지보수 담당자 안내서

<!-- section: bootstrap -->
## 저장소 복제부터 서비스 실행까지

Go 1.26 이상과 DuckDB용 C/C++ 컴파일러가 필요합니다. Docker와 `direnv`는
선택 사항입니다. 외부 에뮬레이터 저장소를 복제할 필요는 없습니다.

```bash
direnv allow
mkdir -p data "$BQEMU_TEMP_DIRECTORY"
make check
make run
```

저장소에 포함된 `.envrc`에는 인증 정보가 없습니다. 이 파일은
`.envrc.example`을 불러온 뒤, Git에서 제외한 선택적 `.envrc.local`을 불러옵니다.
예제 설정은 `configs/bqemu.yaml`, 호스트의 데이터베이스 및 임시 저장 경로,
테스트 자원 한도를 지정합니다.

`.envrc.local`에는 개발 장비에서만 사용하는 비운영 설정만 넣습니다. 토큰,
개인 키, 인증 정보 JSON, 운영 환경 엔드포인트는 저장소에 포함되는 어느 파일에도
넣지 않습니다. `direnv`를 사용하지 않는다면 같은 값을 환경 변수로 내보내거나
`--config`와 반복 `--set`을 직접 전달합니다.

정확한 설정 병합 및 유효성 검사 규칙은 [설정과 운영](operations.md)에 있습니다.
`GET /healthz`를 확인한 뒤 최상위 README에 설명된 에뮬레이터 프로젝트를
생성합니다. REST 응답 형식은 [BigQuery REST
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest)를 기준으로 합니다.

<!-- section: learning-path -->
## 학습 경로

1. [아키텍처](architecture.md)를 읽습니다. 특히 의존성 방향과 트랜잭션 경계를
   확인합니다.
2. 서비스를 실행하고 README에 있는 프로젝트, 데이터 세트, 테이블, 쿼리 요청 중
   하나를 실행합니다.
3. `go test ./internal/domain -run Schema`처럼 변경 사항과 가장 가까운 테스트를
   실행합니다.
4. 요청이 `internal/transport`에서 애플리케이션 및 포트를 거쳐 어댑터에 도달하는
   과정을 추적합니다.
5. Storage 또는 커넥터에 의존하는 계약을 바꾸기 전에 [BigQuery와 커넥터 내부
   동작](bigquery-internals.md)을 읽습니다.
6. [호환성](compatibility.md)에서 기존 지원 기능을 선택하거나 새로 선언합니다.
7. 저장 엔진 포트를 변경한다면 [저장 엔진 어댑터 구현
   안내서](engine-adapter-guide.md)의 계획 및 조립 계약을 확인합니다.

기준 커넥터는 정확히 [Spark BigQuery 커넥터
`0.44.2`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2)입니다.

<!-- section: first-change -->
## 첫 변경 절차

공개 동작 하나와 실패 사례 하나로 시작합니다. 도메인 불변 조건, 테스트용 포트를
사용한 애플리케이션 테스트, 어댑터 동작, 공개 REST/gRPC 계약 테스트, 호환성 표
항목, 두 언어 문서를 추가하거나 갱신합니다. 다음 명령을 실행합니다.

```bash
gofmt -w ./cmd ./internal docs/documentation_test.go
go test ./...
go vet ./...
```

검토를 요청하기 전에 엔진 SQL만 검증하는 테스트가 없는지 확인합니다. 지원하지
않는 필드를 구현한 것처럼 조용히 무시해서도 안 됩니다. 오류 메시지에는 관련
지원 기능과 해결 방법이 있어야 합니다.

<!-- section: new-version -->
## 프로토콜 또는 클라이언트 버전 추가

소비자 릴리스는 `contract/cases/*.yaml`에 선언합니다. 사례에는 사용할
`runtimeProfile`, `runnerAdapter`, `compatibilityProfile`, `scenarioSet`을 지정합니다.
정확한 버전과 변경되지 않는 산출물 URI 및 SHA-256도 함께 기록합니다. 기존 실행
환경, 호출 방법, 통신 계약을 그대로 사용하는 릴리스라면 사례 YAML 파일 하나만
추가합니다. 버전 범위로 어댑터를 추측하지 않습니다.

실행 환경의 형태, 호출 방법, 통신 계약, 시나리오 묶음이 새로 생긴 경우에만
`contract/consumers.yaml`을 변경합니다. operation ID와 scenario ID는 서로 다른
식별자입니다. 테스트 annotation에는 operation ID만 기록하고, 사례 YAML에서 실행할
시나리오를 선택합니다. 커밋하기 전에 다음 명령을 실행합니다.

```bash
make contract-generate
make ci-static
go run ./cmd/contractctl matrix --root . --family spark --lane required
```

CI는 행마다 실행 환경을 분리합니다. 로컬에 설치된 클라이언트를 사용할 때는
`BQEMU_CONSUMER_CASE=case-id make bq-test`처럼 해당 사례를 명시합니다. 실행 환경 하나를
여러 사례가 공유하게 되는 경우에는 계열만 지정한 로컬 명령이 실행을 중단합니다.

컴파일러는 알 수 없는 필드와 참조, 중복 ID, 잘못된 해시, 한 시나리오 묶음 안에서
겹치는 동작 ID, 순서 제약의 순환, 함께 사용할 수 없는 실행 환경과 어댑터 조합을
거부합니다. 각 시나리오의 선택자에는 실행 어댑터가 수행할 테스트를 기록합니다.
시나리오와 실행 준비 동작에 선언하지 않은 호출이 발생하면 검증에 실패합니다.

CI와 실행 어댑터는 `contract/consumers.normalized.json`만 읽습니다. 호출 횟수는 사례
전체를 기준으로 합산합니다. 호출 순서는 개별 서버 실행 안에서만 비교합니다. 증거의
실행 식별자는 로그 상대 경로의 해시이므로 로컬 경로를 노출하지 않습니다. `execution`
산출물은 해시 또는 해시 잠금 파일을 확인한 뒤 실행에 사용합니다. `tool-provenance`
산출물은 릴리스 출처만 나타내며 CI가 설치한 실행 파일과 같다고 간주하지 않습니다.
산출물의 `usage`는 `python-wheel`, `spark-connector-dsv1-jar`처럼 어댑터가 엄격하게 검사하는
용도입니다. 컴파일러는 어댑터가 요구한 각 용도의 산출물이 정확히 하나인지 확인합니다.
모든 필수 사례는 인증과 TLS 행렬에도 함께 추가됩니다. 실행 어댑터가 인증 실행
유형을 선택하며, 같은 실행 환경 버전과 산출물 용도로 설치 및 동일성을 검증합니다.

bq 사례는 처리 방식이 다릅니다. `cloud-sdk-release-provenance`는
`tool-provenance` 역할의 OCI 해시이며 실행 어댑터가 직접 실행하지 않습니다. CI는
`setup-gcloud`로 사례에 선언한 Cloud SDK 버전을 설치합니다. 어댑터는 구조화된
`gcloud version` 출력에서 Cloud SDK와 bq 구성 요소의 정확한 버전을 확인하고,
`bq version` 값도 별도로 확인합니다. 증거에도 이 경계를 기록하며 OCI 이미지가
설치된 실행 파일이라고 표현하지 않습니다.

`required` 사례가 실패하면 이미지가 배포되지 않습니다. `preview`와 `nightly` 사례는
수동 또는 주기 실행 워크플로에서 선택하며 릴리스 조건에 포함하지 않습니다. 두 실행
구분에 사례가 하나도 없어도 정상입니다. Storage 동작은 [공식 RPC
레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc)와 비교합니다.

<!-- section: diagnose-drift -->
## 호환성 차이 진단

1. 공개 엔드포인트에서 문제를 재현하고 처리 단계와 식별자를 수집합니다.
2. `version`, `operation`, `shape`, `fingerprint`, `fix_hint`와 함께 요청, 응답, SQL,
   행 데이터, 백엔드 오류의 원본 맥락을 출력합니다.
3. 요청과 응답을 고정된 프로필 및 공식 계약과 비교합니다.
4. 차이가 전송 계층, 애플리케이션 불변 조건, 외부 연동 어댑터 중 어디에서
   발생했는지 확인합니다.
5. 수정하기 전에 실패 사례를 담은 검증 기준 데이터를 추가합니다. 코드는 필요한
   범위만 변경합니다.
6. 관련 범위 테스트, 전체 테스트, `vet`, 배포된 클라이언트를 사용하는 E2E
   테스트를 실행합니다.

스키마 지문은 같은 입력에 항상 같은 값을 내는 해시일 뿐, 스키마의 기준 정보가
아닙니다. 타입의 기준은 [BigQuery 데이터
타입](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)입니다.

<!-- section: release -->
## 릴리스 및 문서 점검 절차

`make check`를 실행합니다. Docker 동작을 바꿨다면 컨테이너도 빌드합니다.
호환성 변경 내용을 검토하고, 외부에 공개하는 모든 설명에 어디까지 테스트했는지
명시되어 있는지 확인합니다.

README, CONTRIBUTING, 모든 `docs/en/**` 파일에는 표식과 출처 URL이 같은 한국어
대응 문서가 있어야 합니다. 모든 이슈 본문에도 범위, 완료 조건, 제외 범위,
출처가 같은 `## English`와 `## 한국어` 구역이 있어야 합니다. 완료 조건을 실제로
검증하기 전에는 이슈를 닫지 않습니다.
