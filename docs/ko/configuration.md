<!-- doc-id: configuration -->
<!-- lang: ko -->

[English](../en/configuration.md) | [한국어](configuration.md)

# 설정

BQEMU는 시작할 때 하나의 버전 있는 YAML 파일을 읽습니다. 설정을 바꾼 뒤에는
프로세스나 Compose 서비스를 재시작합니다. 공개 리소스 계약은 [BigQuery REST API
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest)를 따릅니다.

<!-- section: precedence -->
## 우선순위와 override

설정은 다음 순서로 적용합니다.

```text
compiled defaults < YAML file < mapped BQEMU_* environment variables < --set path=value
```

`BQEMU_CONFIG`는 YAML 파일을 고르고 `--config`는 그 선택을 바꿉니다. `--set`을
반복하면 하나의 scalar 필드를 바꿀 수 있습니다.

```bash
go run ./cmd/emulator --config configs/bqemu.yaml \
  --set server.http.publicUrl=http://bqemu:9050 \
  --set load.gcsEndpoint=http://fake-gcs:4443
```

구조화된 `bootstrap.projects`는 의도적으로 환경 변수나 `--set`으로 조립하지 않고
YAML에서 관리합니다.

<!-- section: bootstrap-resources -->
## 시작 리소스

프로젝트는 BigQuery 데이터세트가 아니라 에뮬레이터 리소스입니다. 서비스가 준비되는
즉시 있어야 하는 모든 프로젝트와 데이터세트를 선언합니다.

```yaml
defaults:
  projectId: local-project
  location: US

bootstrap:
  projects:
    - id: local-project
      friendlyName: Local development
      datasets:
        - id: analytics
          location: US
          labels:
            environment: local
    - id: integration-project
      datasets:
        - id: fixtures
          location: US
```

reconciler는 공개 준비 상태 전에 실행됩니다. 같은 선언으로 다시 시작해도 안전하며,
리소스는 저장된 identity와 metadata를 유지합니다.

<!-- section: topology -->
## 접속 주소 구성

| 설정 | 기본값 | 용도 |
| --- | --- | --- |
| `server.http.address` | `:9050` | HTTP listener bind address |
| `server.http.publicUrl` | `http://localhost:9050` | 서비스가 공개하는 REST base URL |
| `server.grpc.address` | `:9060` | Storage gRPC listener bind address |
| `server.tls.certFile`, `server.tls.keyFile` | 비어 있음 | REST와 gRPC TLS를 함께 활성화 |
| `admin.enabled`, `admin.address` | `false`, `127.0.0.1:9051` | 선택형 로컬 diagnostics listener |

Compose에서는 listener 주소는 그대로 두고 `server.http.publicUrl`을 호출 프로세스가
실제로 도달할 수 있는 주소로 설정합니다. 기본 `compose.yaml`은 `BQEMU_PUBLIC_URL`로
이 값을 전달합니다. 호스트, 같은 Compose 서비스, 개발 컨테이너 주소는
[시작하기](getting-started.md#올바른-접속-주소-사용)를 참고하세요.

<!-- section: runtime-settings -->
## 실행 설정

| 그룹 | 주요 설정 | 효과 |
| --- | --- | --- |
| 기본값 | `defaults.projectId`, `defaults.location` | 요청이 생략한 기본 프로젝트와 location |
| 엔진 데이터 | `database.adapter`, `database.dsn`, `database.tempDirectory` | 엔진과 물리 데이터/임시 경로 선택 |
| BQEMU 상태 | `state.dsn` | canonical catalog, job, stream metadata를 저장하는 SQLite 파일. 엔진 데이터와 함께 보관합니다. |
| 종료 | `runtime.shutdownTimeout`, `runtime.serverDrainTimeout`, `runtime.storageCloseTimeout` | 제한된 drain과 storage close 순서 |
| 쿼리 결과 | `query.operationTimeout`, `query.compensationTimeout`, `query.materialization.*` | 쿼리 실행 한도, cleanup budget, 선택형 생성 결과 데이터세트 |
| 테이블 페이지 | `tableData.operationTimeout`, `tableData.maxPageRows`, `tableData.maxResponseBytes`, `tableData.maxRowBytes` | `tabledata.list` admission과 응답 크기 제한 |
| 로그 | `logging.level`, `logging.format` | 구조화된 프로세스 로그 수준과 형식 |
| UI | `ui.enabled`, `ui.directory` | 선택형 정적 UI 제공 |

`query.materialization.projectId`와 `datasetId`를 함께 설정하면 그 bootstrap 데이터세트가
존재하고 쿼리 location과 같아야 합니다. 둘 다 비워 두면 내부 결과 데이터세트를 사용합니다.

<!-- section: storage-limits -->
## Storage Read와 Write 한도

모든 Storage 설정은 시작할 때 읽습니다. 로컬 테스트에서 더 작고 제한된 서비스를
필요로 하지 않는다면 기본값을 유지합니다.

| 영역 | 설정 |
| --- | --- |
| Read availability와 concurrency | `storage.read.enabled`, `maxStreams`, `defaultStreamCount`, `maxSessions` |
| Read response와 snapshot budget | `rowsPerResponse`, `maxResponseBytes`, `maxSchemaBytes`, `maxRowBytes`, `maxSnapshotBytes`, `maxTotalSnapshotBytes`, `maxSnapshotRows`, `spillThresholdBytes`, `tempFilePattern` |
| Write availability와 concurrency | `storage.write.enabled`, `maxStreams`, `maxConcurrentAppendRequests`, `queueCapacity`, `queueWaitTimeout`, `operationTimeout` |
| Write request와 staging budget | `maxAppendRequestBytes`, `maxAppendEnvelopeBytes`, `maxInFlightBytes`, `maxInFlightBytesPerStream`, `maxStagedBytes`, `maxStagedBytesPerStream`, `orphanTtl`, `cleanupInterval` |

두 서비스의 `protocolModelVersion`은 프로토콜 모델 호환 설정이므로 일반 사용에서는
바꾸지 않습니다.

<!-- section: load-jobs -->
## Load Job

모든 load job은 설정한 GCS 호환 JSON endpoint를 사용합니다. 로컬 파일 원본 모드는
없습니다.

| 설정 | 기본값 | 효과 |
| --- | --- | --- |
| `load.gcsEndpoint` | `http://127.0.0.1:4443` | BQEMU가 쓰는 object API endpoint. Compose에서는 `http://fake-gcs:4443`로 override합니다. |
| `load.operationTimeout` | `2m` | 제한된 전체 load 작업 시간 |
| `load.maxObjects` | `1000` | resolve한 최대 원본 객체 수 |
| `load.maxObjectBytes` | `1GiB` | 다운로드하는 객체 하나의 최대 크기 |
| `load.maxTotalBytes` | `4GiB` | 합친 원본 byte의 최대값 |
| `load.maxMetadataBytes` | `8MiB` | object metadata 응답 최대 크기 |
| `load.maxListedObjects` | `10000` | URI pattern 확장 중 검사하는 최대 객체 수 |

업로더와 BQEMU는 서로 다른 네트워크 위치에 있으므로 각각 도달 가능한 주소를
설정합니다. 지원 원본과 형식은 `gs://`와 Parquet이며 [현재 지원 범위](compatibility.md)를
확인하세요.

<!-- section: full-reference -->
## 전체 기본 설정 파일

[`configs/bqemu.yaml`](../../configs/bqemu.yaml)은 모든 scalar leaf, 기본값, Compose
이미지가 쓰는 resource budget을 담은 실행 가능한 전체 레퍼런스입니다. 바꾼 파일은
시작 전에 검증합니다.

```bash
go run ./cmd/emulator --config path/to/bqemu.yaml --print-effective-config
```

이 명령은 listener를 시작하지 않고 환경 변수와 `--set` override를 적용한 최종 설정을
출력합니다.
