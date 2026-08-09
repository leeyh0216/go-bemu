<!-- doc-id: operations -->
<!-- lang: ko -->

[English](../en/operations.md) | [한국어](operations.md)

# 설정과 운영 절차

<!-- section: configuration -->
## 설정 규칙

설정 로더는 다음 순서로 값을 덮어씁니다. 오른쪽에 있는 설정의 우선순위가 더
높습니다.

```text
컴파일 기본값 < YAML 파일 < 매핑된 환경 변수 < 반복 --set path=value
```

`BQEMU_CONFIG`로 선택 설정 파일을 지정할 수 있습니다. `--config`를 함께 사용하면
이 값을 덮어씁니다. 모든 스칼라 최종 설정 항목은 `config.bqemu.dev/v1alpha1` 모델에
정의되어 있으며, 자료형을 검사하는 `--set`으로 변경할 수 있습니다. 자주 사용하는
항목에는 `BQEMU_*` 환경 변수도 제공합니다. 구조화된 카탈로그 부트스트랩 선언은
YAML 파일이 소유합니다. Docker용 전체 예시는
[`configs/bqemu.yaml`](../../configs/bqemu.yaml)에 있습니다.

| 설정 계층 | 지정 방법 | 규칙 |
| --- | --- | --- |
| 컴파일 기본값 | 없음 | 메모리에 완전하고 유효한 모델을 만듭니다. |
| 파일 | `BQEMU_CONFIG` 또는 `--config` | 최대 1 MiB인 YAML 문서 하나를 읽습니다. |
| 환경 변수 | 문서화된 `BQEMU_*` 변수 | 비어 있지 않은 스칼라 값만 덮어씁니다. |
| CLI | 반복 가능한 `--set path=value` | 스칼라 최종 항목의 자료형을 검사한 뒤 덮어씁니다. |

구성 진입점에서는 기본값과 HTTP, gRPC, TLS 제한을 읽습니다. 데이터베이스와 임시
디렉터리 경로, 종료 제한 시간, 로그, 관리 API, UI, 두 Storage 서비스, 로드 기능의
설정도 이곳에서 사용합니다.

### Storage Read 스냅샷 제한

`storage.read.maxSnapshotBytes`는 각 결과를 구체화하기 전에 예상 크기를 예약합니다.
결과를 만든 뒤에는 어댑터가 실제로 유지하는 바이트 수로 정산합니다. 실행 중인 세션과
처리 중인 예약의 합은 `maxTotalSnapshotBytes`를 넘을 수 없습니다.

### 쿼리 제한 시간

`query.operationTimeout`의 기본값은 `2m`이며 환경 변수는
`BQEMU_QUERY_OPERATION_TIMEOUT`입니다. 서버가 소유한 동기 쿼리와 비동기 쿼리의
전체 실행 시간에 이 상한을 적용합니다.

`query.materialization`은 서버가 생성하는 결과 테이블의 대상과 수명을 관리합니다.
`projectId`와 `datasetId`는 둘 다 비우거나, `bootstrap`에서 조정한 공개 데이터 세트
(또는 영속 카탈로그에 이미 존재하는 데이터 세트)를 함께 지정해야 합니다. 둘 다
비우면 내부 숨김 데이터 세트를 사용합니다. `expiration`의 기본값은 `24h`이며
`BQEMU_QUERY_MATERIALIZATION_EXPIRATION`에 매핑됩니다. 선택 대상은
`BQEMU_QUERY_MATERIALIZATION_PROJECT_ID`와
`BQEMU_QUERY_MATERIALIZATION_DATASET_ID`로 덮어쓸 수 있습니다. 프로세스는 공개
listener를 열기 전과 작업을 저장하기 전에 대상을 확인하며, 대상 위치는 모든 원본과
명시한 작업 위치에 일치해야 합니다. 요청의 `destinationTable`이 항상 우선하며 이
경우 생성 결과 만료 시간을 적용하지 않습니다. 기간 설정은 양수여야 하며 모든
스칼라 항목을 `--set`으로 변경할 수 있습니다.
관련 프로토콜은 공식 [`jobTimeoutMs`](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfiguration.FIELDS.job_timeout_ms)와
[anonymous cached-result lifetime](https://cloud.google.com/bigquery/docs/cached-results#how_cached_results_are_stored)을
기준으로 합니다.

`query.compensationTimeout`의 기본값은 `30s`이며 환경 변수는
`BQEMU_QUERY_COMPENSATION_TIMEOUT`입니다. 메타데이터 반영에 실패한 뒤 실행하는
DuckDB 정리 작업에는 이 제한 시간을 별도로 적용합니다. 요청이 취소된 경우와 정리
작업의 제한 시간은 분리하지만, 정리 작업을 무기한 실행하지는 않습니다.

### 테이블 데이터 조회 제한

`tableData.operationTimeout`의 기본값은 `30s`이며 환경 변수는
`BQEMU_TABLE_DATA_OPERATION_TIMEOUT`입니다. 제한 시간은 전역 카탈로그 변경 잠금을
기다리기 전에 시작합니다. 잠금 대기, 현재 메타데이터와 만료 시간 확인,
[`tabledata.list`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list)를
위한 DuckDB 행 수 계산과 페이지 조회 트랜잭션을 모두 포함합니다.

`tableData.maxPageRows`의 기본값은 `10000`이며 환경 변수는
`BQEMU_TABLE_DATA_MAX_PAGE_ROWS`입니다. 호출자가 더 많은 행을 요청해도 한 응답에
이 값까지만 담습니다. 설정값은 1 이상, BigQuery의 응답 제한인 100,000행 이하여야
합니다.

`tableData.maxResponseBytes`의 기본값은 `10000000`이며 환경 변수는
`BQEMU_TABLE_DATA_MAX_RESPONSE_BYTES`입니다. 직렬화한 JSON 페이지의 실제 바이트
수를 제한합니다. 빈 메타데이터 응답도 담을 수 있도록 최소값은 1,024바이트입니다.

DuckDB 어댑터는 행 수를 제한한 쿼리 결과를 스트리밍하면서 기준 자료형의 값을
순서대로 줄입니다. 백엔드 JSON에는 공개 `f/v` 행에 없는 스키마 이름도 들어가므로,
그 크기로 공개 API의 응답 제한을 판단하지 않습니다. 압축하지 않은 실제 응답 크기는
REST 계층에서 검사합니다. 이어받기 토큰은 아직 보내지 않은 첫 번째 행을 가리킵니다.

`tableData.maxRowBytes`의 기본값은 `100000000`이며 환경 변수는
`BQEMU_TABLE_DATA_MAX_ROW_BYTES`입니다. BigQuery가 문서화한 단일 행 예외의 절대
상한으로 사용합니다. Cloud의 [10 MB page와 100 MB single-row 제한](https://cloud.google.com/bigquery/docs/paging-results#api-limits)은
내부 표현을 기준으로 한 근사치입니다. 로컬 계산은 같은 입력에 항상 같은 결과가
나오도록 별도로 정의합니다.

이 네 설정은 모두 파일을 기준으로 하며, 자료형을 검사하는 `--set`으로 덮어쓸 수
있습니다. 승인한 전송 조각은 전체 페이지를 한 번 더 복사하지 않고 스트리밍합니다.
로그에는 원본 행과 함께 행 수, 기준 바이트 수, 프레임 단위 누적 해시, HTTP 경계에서
실제로 쓴 바이트의 해시를 기록합니다. 메모리 스냅샷은 인코딩된 행의
바이트 수를 계산하고, 임시 파일은 각 행 앞의 8바이트 프레임 길이까지 계산합니다.

### Storage Write 메모리와 대기 제한

`storage.write.maxInFlightBytes*`는 직렬화된 DuckDB 조정자를 기다리는 디코딩된 요청의
크기를 제한합니다. `maxStagedBytes*`는 숨김 DuckDB `PENDING` 테이블에 보관한 행의
논리적 크기를 제한합니다. 설정한 추가 요청 크기는 스트림별 제한 이하여야 하며,
스트림별 제한은 전체 제한 이하여야 합니다. 준비된 바이트 수는 이식 가능한 논리적
크기이며 DuckDB 파일이나 페이지의 실제 크기는 아닙니다.

`storage.write.queueWaitTimeout`의 기본값은 `5s`입니다. 바이트 수 검사를 통과한 뒤
직렬화 조정자에 들어갈 때까지의 대기 시간을 제한합니다.
`storage.write.operationTimeout`의 기본값은 `30s`입니다. 승인된 작업이 직렬화
대기열에 머문 시간과 백엔드 실행 시간을 함께 제한합니다. 타이머는 대기열 진입이
승인된 뒤에 시작하므로, 대기열 포화는 별도의 `RESOURCE_EXHAUSTED` 오류가 됩니다.

두 설정은 각각 `BQEMU_STORAGE_WRITE_QUEUE_WAIT_TIMEOUT`과
`BQEMU_STORAGE_WRITE_OPERATION_TIMEOUT` 환경 변수에 대응합니다. 대기열 포화는
재시도할 수 있는 gRPC `RESOURCE_EXHAUSTED`로 보고합니다. 서버 작업의 제한 시간이
끝나면 `DEADLINE_EXCEEDED`로 보고합니다. 호출자가 더 이른 제한 시간을 설정했다면
호출자의 값이 우선합니다.

`PENDING` 추가 요청이 응답 전에 준비 영역에 저장되었다면, 스트림을 종료하기 전에
같은 오프셋, 스키마, 전송 데이터 확인값으로 다시 보내야 합니다. 같은 요청을 다시
보내도 결과는 한 번만 반영됩니다.

### Storage Write 요청 크기

기본 스트림은 공식 at-least-once 동작의 모호성을 유지합니다. 공식 제한은 전체 요청에
적용됩니다. 호환성 설정 `maxAppendRequestBytes`는 직렬화한 행 데이터의 크기를
제한합니다. 처리 중 메모리 제한은 전체 `AppendRowsRequest`를 기준으로 계산합니다.

시작할 때 `server.grpc.maxReceiveMessageBytes`가 설정된 전송 데이터와 파일에서 읽은
`maxAppendEnvelopeBytes`를 함께 수용할 수 있는지 검사합니다.
`maxAppendEnvelopeBytes`의 기본값은 64 KiB이고 환경 변수는
`BQEMU_STORAGE_WRITE_MAX_APPEND_ENVELOPE_BYTES`입니다. 이 규칙은 공식
[`AppendRows` 요청 및 재시도 계약](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.AppendRows)을
따릅니다.

`maxConcurrentAppendRequests`의 기본값은 `16`이며 환경 변수는
`BQEMU_STORAGE_WRITE_MAX_CONCURRENT_APPEND_REQUESTS`입니다. gRPC `Recv` 전에 허가를
획득하므로, 양방향 스트림에서 Protobuf 디코딩, 복제, 해시에 동시에 사용하는
메모리를 가중치 기반 조정자의 용량 검사 전부터 제한합니다.

### 로드 설정

`load.gcsEndpoint`에는 HTTP(S) 절대 URL을 지정해야 합니다. 객체 목록 조회와
다운로드에는 설정된 제한을 적용합니다. 로드 원본 URI는 `gs://`만 허용하며 로컬
경로와 다른 scheme은 작업을 저장하기 전에 거부합니다. multipart와 resumable media
upload는 먼저 설정한 GCS 호환 서비스에 불변 객체로 저장한 뒤 같은 `gs://` Parquet load
경로를 사용합니다.

실행 중에 프로토콜 기준 버전을 협상하는 기능은 아직 구성하지 않았습니다. 설정값이
유효하더라도 부분 지원 기능의 범위가 넓어지는 것은 아닙니다.

### 카탈로그 부트스트랩

`bootstrap.projects`에는 공개 리스너가 열리기 전에 반드시 존재해야 하는 프로젝트와
데이터셋을 선언합니다. 중첩된 리소스 식별자를 서로 독립적인 환경 변수나 `--set`
스칼라 값으로 안전하게 표현할 수 없으므로 이 목록은 설정 파일에서만 지정합니다.

```yaml
defaults:
  projectId: local-project
  location: US
bootstrap:
  projects:
    - id: local-project
      friendlyName: Local project
      datasets:
        - id: analytics
          location: US
          labels:
            environment: local
    - id: secondary-project
      datasets:
        - id: staging
```

데이터셋 위치를 생략하면 `defaults.location`을 상속합니다. 선언 안에서 프로젝트 ID와
각 프로젝트의 데이터셋 ID는 중복될 수 없습니다. 데이터셋 설명, 라벨, 기본 테이블
만료 시간과 파티션 만료 시간은 기준 상태에 보존됩니다.

시작할 때는 먼저 선언한 모든 리소스를 기준 상태와 비교합니다. 정확히 일치하면 아무
변경도 하지 않으므로 같은 파일로 다시 시작해도 메타데이터나 ETag가 바뀌지 않습니다.
메타데이터가 다르면 리스너를 열기 전에 시작에 실패하고, 누락된 리소스는 선언 순서대로
생성합니다. `bootstrap.projects`를 생략하거나 비워 두면 이전 동작과 같이
`defaults.projectId` 하나만 생성합니다. 해당 프로젝트를 목록에도 선언했다면 선언한
메타데이터를 기준으로 사용하면서 기본 요청 컨텍스트 역할은 유지합니다.

SQLite 상태 파일과 엔진 데이터베이스는 같은 실행 세대의 파일을 함께 보관해야 합니다.
엔진 변경과 기준 상태 커밋 사이에서 프로세스가 종료된 경우의 복구는 [교차 저장소 복구
작업](https://github.com/leeyh0216/go-bemu/issues/26)에서 계속 관리합니다.

### 공개 접근

BigQuery 호환 REST와 gRPC 수신기는 요청을 인증하거나 인가하지 않습니다.
`Authorization` 값이 없거나, 임의 값이거나, 형식이 잘못되었거나, 중복되었거나,
만료된 형태여도 무시합니다. 공개 요청 인증을 위한 `auth.*` 설정과 `BQEMU_AUTH_*`
환경 변수 계약은 없습니다. 알 수 없는 YAML 필드와 `--set` 경로는 기존과 같이 엄격한
설정 검증에서 거부합니다.

TLS는 별도의 전송 보안 설정입니다. 클라이언트 라이브러리가 요청 전에 인증 정보를
요구할 수 있지만 토큰 획득은 이 실행 환경의 책임이 아닙니다. `admin.tokenFile`은
진단용 관리 수신기만 보호하며 BigQuery 호환 엔드포인트에는 적용하지 않습니다.

### HTTP 요청 본문 제한

HTTP API는 `identity`와 `gzip`으로 인코딩한 요청 본문을 허용합니다.
`server.http.maxCompressedRequestBytes`는 gzip을 풀기 전에 전송 구간에서 읽는
바이트 수를 제한합니다. `server.http.maxUncompressedRequestBytes`는 압축을 푼 뒤의
바이트 수를 별도로 제한합니다. 두 설정의 기본값은 2 MiB입니다.

각 설정은 `BQEMU_HTTP_MAX_COMPRESSED_REQUEST_BYTES`와
`BQEMU_HTTP_MAX_UNCOMPRESSED_REQUEST_BYTES` 환경 변수에 대응합니다. 자료형을
검사하는 `--set`으로도 값을 바꿀 수 있습니다. 스트림을 직접 읽으면서 제한을
적용하므로 `ContentLength`를 알 수 없는 청크 요청도 검사합니다.

지원하지 않는 인코딩은 `415`를 반환합니다. 형식이 잘못되었거나 인코딩을 여러 개
지정하면 `400`을 반환합니다. 압축 전후의 바이트 제한을 넘으면 `413`을 반환합니다.
경계 로그에는 크기를 제한한 압축 및 디코딩 요청 본문, 헤더, 인코딩, 허용 또는 거부
결과, 바이트 수, SHA-256 해시, 상태, 사유를 남깁니다.

구현은 Go의
[`Request` 본문 계약](https://pkg.go.dev/net/http#Request),
[`MaxBytesReader`](https://pkg.go.dev/net/http#MaxBytesReader),
[`gzip.NewReader`](https://pkg.go.dev/compress/gzip#NewReader), 공식
[`tables.insert` 메서드](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/insert)를
기준으로 합니다.

### 설정 검증과 민감정보

알 수 없는 YAML 필드, 여러 YAML 문서, 의미가 모호한 숫자형 기간, 알 수 없는 덮어쓰기
경로, 유효하지 않은 필드 조합은 리스너를 시작하기 전에 거부합니다. 오류에는
`stage`, `operation`, `model_version`, `field`, `shape`, `fingerprint`, `fix_hint`를
포함합니다. 여기서 `shape`는 값의 원문이 아닌 설정 구조를 뜻합니다.

`--print-effective-config`는 병합한 모델을 검사한 뒤 출력합니다. 원본 파일과 최종
모델의 SHA-256 해시를 함께 제공하므로 설정 불일치를 재현할 수 있습니다. 스키마는
[YAML 1.2.2 명세](https://yaml.org/spec/1.2.2/)를 따릅니다.

실행 설정 YAML이나 환경 변수에 민감정보 원문을 넣으면 안 됩니다. TLS 키와 원격 관리
토큰은 마운트한 파일의 경로로 참조합니다. 민감정보
파일은 배포 환경에 맞는 권한으로 읽기 전용 마운트해야 합니다.

최종 설정에는 참조 경로가 포함될 수 있지만 파일 내용은 읽거나 출력하지 않습니다.
출력 결과는 운영 메타데이터로 취급합니다. TLS는 전송 구간만 보호하며 [Google Cloud
인증](https://cloud.google.com/docs/authentication)을 구현하지 않습니다.

<!-- section: logging-safety -->
## 원본 진단 로그

실행 로그는 에뮬레이터 호출을 재현하는 데 필요한 요청과 실패 맥락을 원본으로
보존합니다. `logging.unsafePayloads` 같은 선택 스위치는 없으며 원본 진단 정보 기록이
기본 동작입니다.

| 경계 | 기록하는 맥락 |
| --- | --- |
| REST | 메서드와 경로, 쿼리와 헤더의 이름 및 값, 크기를 제한한 요청 본문, 상태, 처리 시간, 처리기 오류 |
| Storage gRPC | RPC 이름, 메타데이터, Protobuf 메시지, 직렬화한 행과 스키마 바이트, 스트림 이벤트, 상태, 오류 |
| SQL과 저장 엔진 | 제출한 SQL 또는 조건식, 작업의 행과 스키마, 생성한 문장 맥락, 백엔드 원본 오류 |
| 상태 변경 | 구성 요소, 작업, 변경 전후 상태, 요청과 결과 원본 맥락, 성공 여부, 처리 시간 |

크기와 개수 제한은 자원 제어이며 정보 가림 정책이 아닙니다. 상관관계를 찾기 위한
구조화 해시를 원본 값과 함께 기록할 수 있습니다. 로그에 헤더, 인증 정보, SQL, 행
데이터가 포함될 수 있으므로 접근, 보관, 외부 전송 정책은 실행 환경에서 관리해야
합니다.

<!-- section: local-run -->
## 로컬 실행 절차

```bash
direnv allow
mkdir -p data "$BQEMU_TEMP_DIRECTORY"
make check
make run
curl --fail http://localhost:9050/healthz
curl --fail http://localhost:9050/readyz
```

Direnv는 선택 사항이며 민감정보를 자동으로 불러오지 않습니다. 저장소의 `.envrc`는
`.envrc.example`을 불러온 뒤, 파일이 있으면 Git에서 제외한 `.envrc.local`도
불러옵니다. 예제는 `configs/bqemu.yaml`을 선택합니다. 컨테이너용 데이터베이스와 임시
디렉터리 경로는 호스트용으로 바꾸고, Go, Python, Docker 테스트에는 제한 시간을
설정합니다. 컴퓨터마다 다른 개발용 설정만 `.envrc.local`에 넣습니다.

리스너를 시작하지 않고 설정 병합 결과를 확인하려면 다음 명령을 실행합니다.

```bash
go run ./cmd/emulator --print-effective-config
go run ./cmd/emulator --set logging.level=debug --print-effective-config
```

활성 상태는 프로세스가 응답한다는 뜻입니다. 준비 상태는 웨어하우스 연결 확인도
성공했다는 뜻입니다. gRPC는 표준 Health 서비스를 제공합니다.

활성화한 Storage Read/Write 서비스는 `SERVING`을 보고합니다. 비활성화한 서비스는
`NOT_SERVING`을 보고합니다. 전송 계층의 연결을 종료하기 전에 모든 gRPC Health
항목을 `NOT_SERVING`으로 바꿉니다. 공식 Storage 서비스 목록은 [Storage RPC
레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc)에 있습니다.

<!-- section: container -->
## 컨테이너 실행 규칙

이미지는 루트 권한이 없는 전용 `bqemu` 사용자로 실행합니다. `/data`는 쓰기 가능한
볼륨으로 선언하고 `/readyz`로 준비 상태를 확인합니다. 기본 Compose 설정은 저장소
기준의 설정 파일 경로를 빌드 인자로 전달합니다. 이미지는 이 파일을 권한 `0440`으로
고정된 실행 경로에 복사합니다.

```yaml
build:
  context: .
  args:
    BQEMU_CONFIG_SOURCE: configs/bqemu.yaml
environment:
  BQEMU_CONFIG: /etc/bqemu/bqemu.yaml
volumes:
  - bqemu-data:/data
tmpfs:
  - /tmp/bqemu:uid=65532,gid=65532,mode=0700
```

민감정보가 없는 다른 파일을 사용하려면 [Docker build
context](https://docs.docker.com/build/building/context/) 안의 경로를
`BQEMU_CONFIG_SOURCE=configs/<profile>.yaml`로 지정한 뒤 이미지를 다시 빌드합니다.
기본 방식은 Docker Desktop의 호스트 공유 제한을 포함하여 실행 중 호스트 파일을
바인드하는 문제를 피합니다. 필요하면 읽을 수 있는 파일을
`/etc/bqemu/bqemu.yaml:ro`에 명시적으로 바인드할 수 있습니다. 자세한 동작은 [bind
mount 문서](https://docs.docker.com/engine/storage/bind-mounts/)를 따릅니다.

데이터베이스는 컨테이너 UID `65532`가 소유한 이름 있는 볼륨에만 영속화합니다. 이미지에
포함하는 설정에는 경로와 민감하지 않은 값만 넣습니다. TLS와 토큰 파일은 별도로 읽기
전용 마운트하며 이미지 계층에 내용을 복사하지 않습니다.

저장소의 Compose 프로필은 루트 파일 시스템을 읽기 전용으로 설정하고
`no-new-privileges`를 사용합니다. 전용 `/tmp/bqemu` tmpfs와 준비 상태 검사도
설정합니다. 애플리케이션 종료 제한 10초보다 긴 15초의 종료 유예 시간을 둡니다.

모든 Linux 기능 권한 제거와 명시적인 CPU 및 메모리 제한은 배포 환경에서 추가해야
합니다. Docker 설정은 [read-only root
filesystem](https://docs.docker.com/reference/cli/docker/container/run/#read-only)과
[Compose 서비스 설정](https://docs.docker.com/reference/compose-file/services/)을
기준으로 합니다.

<!-- section: shutdown -->
## 상태 확인과 정상 종료

SIGINT, SIGTERM 또는 리스너 오류가 발생하면 먼저 모든 gRPC Health 항목을
`NOT_SERVING`으로 바꿉니다. `runtime.serverDrainTimeout`의 기본값은 `5s`입니다.
하나의 컨텍스트로 공개 및 관리용 HTTP 종료와 gRPC `GracefulStop` 시간을 제한합니다.
시간이 끝나면 `grpc.Stop`으로 강제 종료합니다.

`runtime.storageCloseTimeout`의 기본값은 `4s`이며 공유 저장소를 닫는 전체 시간에
적용합니다. 먼저 새 쿼리를 거부합니다. 이미 승인한 동기 및 비동기 쿼리를 취소하고
종료를 기다린 뒤, Read 스냅샷 정리, Write 고아 데이터 정리, 조정자 종료를 차례로
제한합니다.

제한 시간 안에 쿼리가 DuckDB 사용권을 반환하지 않으면 Storage와 DuckDB를 닫지
않습니다. 실행 중인 쿼리와 자원 종료가 경합하는 상황을 피하기 위한 동작입니다.
남은 자원은 프로세스가 종료될 때 회수합니다.

`runtime.shutdownTimeout`의 기본값은 `10s`입니다. 시작 과정이나 조기 반환 경로에서
지연된 Storage 정리를 제한하는 보조 수단입니다. 현재 HTTP 준비 상태는 연결 종료를
시작하기 전에 `false`로 바뀌지 않습니다. 처리 중인 작업 수도 보고하지 않으며, 두
번째 종료 신호만을 위한 즉시 종료 경로도 없습니다.

갑작스럽게 종료하면 프로세스 메모리에 있던 쿼리 결과 행과 Storage Read 스냅샷 바이트는
잃을 수 있습니다. 카탈로그, 작업, 로드 중복 방지, Storage Write 수명 주기 메타데이터는
설정한 SQLite 상태 저장소에 남아 다음 시작에서 다시 조정합니다.

테스트에서는 새 쿼리 거부, 실행 중인 동기 및 비동기 쿼리 취소, 제한 시간 안의 쿼리
종료, 쿼리 종료 후 Storage를 닫는 순서를 확인합니다. 유휴 상태 종료, 처리 중인 REST
요청과 gRPC 스트림, 제한 시간 만료, 두 번째 신호에 따른 강제 종료, 마운트한 볼륨을
사용한 재시작은 추가 검증이 필요합니다.

Storage Write의 공개 시점은 공식
[`BatchCommitWriteStreams` 계약](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite.BatchCommitWriteStreams)이며
종료 과정에서 커밋이 성공한 것으로 바뀌면 안 됩니다.

<!-- section: timeouts -->
## 테스트 제한 시간 설정

테스트에서는 용도를 알 수 없는 `sleep`을 사용하지 않습니다. 각 설정 이름에 시간
단위와 적용 범위를 표시합니다.

| 설정 | 형식과 기본값 | 적용 범위 |
| --- | --- | --- |
| `BQEMU_GO_TEST_TIMEOUT` | Go 기간, `10m` | `make test`, 경쟁 상태 테스트, CI 패키지 전체 |
| `BQEMU_STORAGE_READ_TEST_TIMEOUT` | Go 기간, `5s` | Storage Read 애플리케이션 테스트 컨텍스트 하나 |
| `BQEMU_STORAGE_WRITE_TEST_TIMEOUT` | Go 기간, `5s` | Storage Write 애플리케이션, 어댑터, 공개 gRPC 테스트 컨텍스트 |
| `BQEMU_REST_TEST_TIMEOUT` | Go 기간, `5s` | REST 요청, gzip 경계, 페이지 나누기, 덮어쓰기 테스트 컨텍스트 |
| `BQEMU_DOCKER_START_TIMEOUT_SECONDS` | 양의 초, `120` | `docker compose --wait` 시작 과정 |

외부 프로세스 제한 시간은 [통합 테스트
프레임워크](../../tests/integration/docs/ko/framework.md)에서 관리합니다. 이후 공통
단계별 제한은 다음과 같이 분리할 계획입니다.

| 설정 | 목적 | 시간 만료 시 진단 정보 |
| --- | --- | --- |
| `BQEMU_TEST_STARTUP_TIMEOUT` | 프로세스와 컨테이너의 준비 대기 | 포트, 상태 확인 본문, 크기를 제한한 마지막 원본 로그 |
| `BQEMU_TEST_REQUEST_TIMEOUT` | REST 또는 RPC 작업 하나 | 작업, 지원 기능, 요청 해시 |
| `BQEMU_TEST_EVENTUALLY_TIMEOUT` | 조회와 최종 상태 대기 | 마지막 상태와 상태 전환 기록 |
| `BQEMU_TEST_SHUTDOWN_TIMEOUT` | 정상 종료 | 처리 중인 REST, RPC, 작업 수 |

기본값은 하나의 테스트 설정 모듈에서 관리하고 실패 결과에 출력해야 합니다. CI는 환경
변수로 제한 시간을 늘릴 수 있습니다. 개별 테스트는 명시적인 테스트 자료나 옵션으로
제한 시간을 줄일 수 있습니다.

시간이 만료되면 `version`, `operation`, `shape`, `fingerprint`, `fix_hint`를
보고합니다. 여기서 `shape`는 요청이나 응답의 구조를 뜻합니다. 위 네 개의 단계별
설정은 설계만 정해졌으며 아직 실제 테스트에 연결하지 않았습니다.

<!-- section: diagnostics -->
## 진단용 관리 API 설계

버전이 있는 설정 모델에는 `admin.enabled`, `admin.address`, `admin.tokenFile`,
`admin.readHeaderTimeout`, `admin.maxStackBytes`가 있습니다. 관리 API는 기본적으로
비활성화되며 주소는 `127.0.0.1:9051`입니다. 활성화했을 때만 별도 리스너를 시작하며
BigQuery REST 네임스페이스와 공유하지 않습니다.

루프백이 아닌 주소에 바인드하려면 토큰 파일과 서버 TLS를 모두 설정해야 합니다.
관리용 리스너는 공개 서버와 같은 TLS 인증 정보를 사용합니다.

| 메서드와 경로 | 응답 내용 |
| --- | --- |
| `GET /healthz` | 관리 API 활성 상태와 `admin.bqemu.dev/v1alpha1` |
| `GET /bqemu/v1/admin/diagnostics/runtime` | 실행 시간, Go와 빌드 및 프로세스 정보, 고루틴, 힙, GC 스냅샷 |
| `GET /bqemu/v1/admin/diagnostics/goroutines` | SHA-256 및 잘림 헤더가 있는 크기 제한 텍스트 스택 |

`tokenFile`을 설정하면 모든 경로에서 Bearer 토큰을 요구합니다. 토큰 비교에는 입력값에
따라 실행 시간이 달라지지 않는 방식을 사용합니다. 앞뒤 공백을 제거한 토큰은 최소
16바이트여야 하며 파일은 최대 64 KiB입니다.

고루틴 출력의 기본 최대 크기는 4 MiB이며 요청 시 진단 로그에도 복사합니다. 원격
접근과 로그 저장소는 그에 맞게 보호해야 합니다.

향후 `GET /bqemu/v1/admin/config`는 모델 버전, 원본과 최종 설정의 해시, 최종 설정
모델을 반환할 수 있습니다.

이 설정 API, 지원 기능과 작업 수, 최근 계약 불일치 요약은 아직 계획 단계입니다.
관리 API는 IAM을 대체하지 않습니다.
