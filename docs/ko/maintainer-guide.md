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

`protocol profile -> adapter -> capability -> golden -> E2E` 순서를 사용합니다.
정확한 산출물 및 태그, REST/RPC 호출 순서, 필드 존재 여부 규칙, 전송 형식,
스키마 대응, 재시도 및 오프셋 의미, 제거 조건을 기록합니다. 이전 프로필을 직접
수정하거나 변경될 수 있는 브랜치를 연결하지 않습니다. Storage 동작은
[공식 RPC 레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc)와
비교합니다.

<!-- section: diagnose-drift -->
## 호환성 차이 진단

1. 공개 엔드포인트에서 문제를 재현하고 처리 단계와 식별자를 수집합니다.
2. 인증 정보, SQL 원문, 행 데이터 없이 `version`, `operation`, `shape`,
   `fingerprint`, `fix_hint`를 출력합니다.
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

통합 operation coverage는 실행 가능한 integration source 옆의 literal
`# bqemu:operation <id> scenario=<scenario>` annotation에 둡니다. 하나를
추가한 뒤 `make integration-contract-check`를 실행하면 정규 consumer manifest를
다시 만들고 drift를 거부합니다. runner-only/load scenario는 비어 있지 않은 이유를
명시한 reviewed exception일 때만 허용합니다.

모든 CI 테스트 job은 suite 결과, 통과·실패·건너뜀 수, 실행 시간, 읽기 쉬운 report
artifact 이름을 GitHub Job Summary에 하나로 남깁니다. JUnit을 사용하는 경로에서는
XML을 기계용 입력으로만 사용하고 JUnit HTML report를 올립니다. Go, 정적 검사, CLI,
Compose, matrix 경로도 같은 방식의 payload-safe suite HTML을 만듭니다. 간단한 report는
7일 동안 보관합니다. Compose, service, Spark, 원시 진단 자료는 실패한 job에서만
`-diagnostics-<run-id>`로 끝나는 artifact 이름으로 보관하며, CI의 첫 화면으로 쓰지
않습니다.

`make check`를 실행합니다. Docker 동작을 바꿨다면 컨테이너도 빌드합니다.
호환성 변경 내용을 검토하고, 외부에 공개하는 모든 설명에 어디까지 테스트했는지
명시되어 있는지 확인합니다.

README, CONTRIBUTING, 모든 `docs/en/**` 파일에는 표식과 출처 URL이 같은 한국어
대응 문서가 있어야 합니다. 모든 이슈 본문에도 범위, 완료 조건, 제외 범위,
출처가 같은 `## English`와 `## 한국어` 구역이 있어야 합니다. 완료 조건을 실제로
검증하기 전에는 이슈를 닫지 않습니다.
