# 공개 Operation 계약

이 디렉터리는 BQEMU가 외부에 제공하는 REST와 gRPC 표면을 기계적으로 검사하는
계약을 소유합니다. 사용자 기능 안내가 아닙니다. 사용자는
[현재 지원 범위](../docs/ko/compatibility.md)부터 읽고, 정확한 메서드와 RPC는 생성된
[API/RPC 표](../docs/ko/api-rpc-compatibility.md)에서 확인합니다.

## 먼저 볼 파일

| 파일 | 역할 | 직접 수정 |
| --- | --- | --- |
| `operations.yaml` | 모든 공개 operation의 수기 단일 원본 | 예 |
| `operations.normalized.json` | 결정적으로 정규화한 매니페스트 | 아니요. 생성물 |
| `operation_manifest.go` | 엄격한 스키마와 검증 규칙 | 계약 모델을 바꿀 때 |
| `operation_annotations.go` | YAML 테스트 ID와 Go 테스트의 literal operation ID 연결 | 드물게 |
| `operation_generate.go` | route spec과 EN/KO API 표 생성 | 생성 표현을 바꿀 때 |
| `../internal/contractspec/operations_gen.go` | 실행 환경이 쓰는 route/RPC spec | 아니요. 생성물 |

변경을 검토하기 전에 `make contract-check`를 실행합니다. 이 명령은 오래된 생성물,
알 수 없는 operation ID, 빠진 테스트 근거, 잘못된 조건, REST/gRPC descriptor
불일치를 거부합니다.

## Operation을 추가하거나 바꾸는 절차

1. **프로토콜 출처에서 시작합니다.** 아직 없다면 `sources`에 HTTPS 공식 출처를
   추가합니다. 클라이언트 구현이나 내부 route만 보고 공개 메서드를 추정하지 않습니다.
2. **공개 경계를 구현하고 시험합니다.** REST는 등록된 method/path, gRPC는 등록된
   service/method가 먼저 있어야 합니다.
3. **`operations.yaml`을 수정합니다.** 안정적인 dotted ID, 하나의 protocol shape,
   허용된 component, EN/KO 설명, 지원/검증 등급, 출처, 제한, Go 테스트 ID를 모두
   작성합니다.
4. **선언한 모든 Go 테스트를 annotation합니다.** 컴파일러가 정적으로 읽을 수 있도록
   테스트 함수 안에서 literal ID를 사용합니다.

   ```go
   import "github.com/leeyh0216/go-bemu/internal/contracttest"

   func TestExample(t *testing.T) {
       contracttest.Operation(t, "bigquery.example.get")
       // 공개 route 또는 RPC를 여기서 시험합니다.
   }
   ```

   YAML의 테스트 ID는 `go:_test.go를_제외한_경로:TestExample` 형식입니다. 선언만 하고
   annotation하지 않거나, 없는 ID를 annotation하면 `make contract-check`가 실패합니다.
5. **생성하고 검토합니다.** `make contract-generate`를 실행한 뒤 정규 JSON, 생성된
   Go route spec, EN/KO API/RPC 표를 함께 검토합니다. 생성물을 직접 수정하지 않습니다.
6. **집중 동작 테스트와 `make contract-check`를 실행합니다.** 같은 매니페스트에서
   route 등록, Discovery, gRPC descriptor, annotation을 함께 검사합니다.

외부 호환성 suite는 `tests/integration`에 둡니다. 이는 종단간 동작의 근거가 될 수
있지만, 이 매니페스트가 검사하는 제품 Go 테스트 근거를 대체하지는 않습니다.

## 필수 분류

`support`는 upstream API 전체가 아니라 BQEMU operation의 상태를 뜻합니다.

| 값 | 의미 |
| --- | --- |
| `implemented` | 문서에 적은 operation 동작이 제공되고 공개 경계 근거가 있습니다. |
| `partial` | 호출할 수 있지만 `supportedInput`과 `limitations` 범위 안에서만 동작합니다. |
| `registered` | route 또는 RPC descriptor는 등록되었지만 동작은 구현되지 않았습니다. |
| `unsupported` | 지원한다고 노출하지 않는 operation이며 실행 테스트 근거가 없습니다. |

`verification`은 필요한 가장 강한 근거입니다. `transport`는 공개 REST/gRPC 경계,
`application`은 애플리케이션 경로, `unit`은 지역 계약을 뜻합니다. `none`은
unsupported entry에만 허용됩니다. 표를 좋게 보이게 하려고 등급을 올리지 말고, 먼저
빠진 공개 경계 테스트를 추가합니다.

제한은 반드시 명시합니다.

- `none`: 적은 입력 범위 외에 알려진 제한이 없습니다.
- `by-design`: 의도한 emulator 경계이며 승인된 scope를 적습니다.
- `tracked`: 빠진 동작이며 하나 이상의 GitHub 이슈가 있습니다.
- `mixed`: 의도적 경계와 추적 중인 부족한 동작이 함께 있습니다.

현재 조건은 `admin.enabled`, `ui.enabled`, `storage.read.enabled`,
`storage.write.enabled`만 허용합니다. 조건에는 EN/KO 설명과 operation의 `tests`에도
들어 있는 테스트 근거가 필요합니다.

## Drift를 막는 규칙

- production transport는 compiler package `contract`가 아니라 생성된
  `internal/contractspec`만 import합니다.
- REST listener/method/path 하나와 gRPC service/method 하나는 각각 operation 하나에만
  연결합니다.
- operation ID와 test annotation은 literal이고 안정적인 식별자여야 합니다. 동적으로
  만들지 않습니다.
- `operations.normalized.json`, `operations_gen.go`, EN/KO API 표는
  `make contract-generate`로만 바꿉니다.
- 사용자 설명은 짧게 유지합니다. 구현 세부 내용은 이슈, ADR, 유지보수 문서에 둡니다.

## 자주 쓰는 명령

```bash
make contract-check       # 원본, annotation, descriptor, 생성물을 검사
make contract-generate    # 결정적인 생성물 갱신
go test ./contract        # 스키마와 compiler 동작만 검사
go test ./internal/transport/rest ./internal/transport/grpc
```

전체 설계는 [개발 절차](../docs/ko/maintainers/development-workflow.md),
[유지보수 문서 허브](../docs/ko/maintainers/index.md),
[CI 리포트 정책](../docs/ko/maintainers/ci-reporting.md)을 참고하세요.
