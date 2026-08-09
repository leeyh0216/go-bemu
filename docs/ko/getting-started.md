<!-- doc-id: getting-started -->
<!-- lang: ko -->

[English](../en/getting-started.md) | [한국어](getting-started.md)

# 시작하기

이 문서는 BQEMU를 시작하고 최소 카탈로그 리소스를 만든 뒤 첫 쿼리를 실행하는 방법을
설명합니다. 공개 리소스 형태는 [BigQuery REST API 레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest)를
따릅니다.

<!-- section: run -->
## BQEMU 시작

저장소 루트에서 실행합니다.

```bash
docker compose up --build -d --wait
curl --fail http://localhost:9050/readyz
```

기본 Compose 프로젝트는 필수 fake GCS 서비스도 함께 시작합니다. `bqemu-data` 볼륨에는
로컬 메타데이터와 엔진 데이터가 저장됩니다. 재시작에 유지하거나, 깨끗한 상태가 필요하면
`docker compose down --volumes`로 삭제합니다.

<!-- section: endpoints -->
## 올바른 접속 주소 사용

| 호출 프로세스 | REST | Storage gRPC |
| --- | --- | --- |
| Compose를 실행한 호스트 | `http://localhost:9050` | `localhost:9060` |
| 같은 Compose의 다른 서비스 | `http://bqemu:9050` | `bqemu:9060` |
| 호스트에서 BQEMU를 실행하는 개발 컨테이너 | `http://host.docker.internal:9050` | `host.docker.internal:9060` |

REST만 사용하는 호출자는 REST 주소만 필요합니다. Storage 호출자는 두 주소를 모두
사용합니다. Linux 개발 컨테이너에서 호스트의 BQEMU에 접속한다면 일반적인
`host.docker.internal:host-gateway` 호스트 매핑을 추가합니다.

<!-- section: resources -->
## 리소스 생성

기본 설정은 이미 `local-project`와 `analytics` 데이터세트를 생성합니다. 별도 테스트
프로젝트, 데이터세트, 테이블을 만들려면 다음을 실행합니다.

```bash
curl --fail -X POST http://localhost:9050/bqemu/v1/projects \
  -H 'Content-Type: application/json' \
  -d '{"projectId":"test-project","friendlyName":"Local tests"}'

curl --fail -X POST \
  http://localhost:9050/bigquery/v2/projects/test-project/datasets \
  -H 'Content-Type: application/json' \
  -d '{"datasetReference":{"projectId":"test-project","datasetId":"analytics"},"location":"US"}'

curl --fail -X POST \
  http://localhost:9050/bigquery/v2/projects/test-project/datasets/analytics/tables \
  -H 'Content-Type: application/json' \
  -d '{"tableReference":{"projectId":"test-project","datasetId":"events"},"schema":{"fields":[{"name":"id","type":"INTEGER","mode":"REQUIRED"},{"name":"label","type":"STRING"}]}}'
```

계속 유지할 시작 리소스는 설정 파일의 `bootstrap.projects`에 선언합니다. 서비스는
준비 상태가 되기 전에 이를 맞춥니다. [설정](configuration.md#bootstrap-resources)을
참고하세요.

<!-- section: query -->
## 쿼리 실행

```bash
curl --fail -X POST \
  http://localhost:9050/bigquery/v2/projects/test-project/queries \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT 1 AS answer","useLegacySql":false,"location":"US"}'
```

응답은 BigQuery `QueryResponse` 형태입니다. 문서에 명시한 범위를 넘어선 쿼리 기능에
의존하기 전에는 [호환성](compatibility.md)을 확인합니다.

<!-- section: gcs-load -->
## Fake GCS를 통한 Parquet Load

Load job은 `gs://` 원본 URI만 받습니다. BQEMU는 `load.gcsEndpoint`를 사용하며,
업로더는 같은 fake GCS 서비스의 별도 주소를 사용합니다.

| 프로세스 | 기본 Compose 설정의 주소 또는 설정 |
| --- | --- |
| BQEMU load worker | `load.gcsEndpoint: http://fake-gcs:4443` |
| 호스트 업로더 | `http://localhost:4443` |
| 같은 Compose의 업로더 | `http://fake-gcs:4443` |
| 호스트 Compose에 접속하는 개발 컨테이너 업로더 | `http://host.docker.internal:4443` |

Parquet만 지원하는 load 형식입니다. 필수 설정과 한도는 [설정](configuration.md#load-jobs)에
정리되어 있습니다.

<!-- section: tls -->
## 선택형 TLS와 인증 파일

호출 프로세스가 필요로 할 때만 로컬 인증서와 인증 파일을 생성합니다.

```bash
go run ./cmd/bqemu-auth-fixture generate --output .bqemu-auth
docker compose -f compose.yaml -f compose.tls.yaml up --build -d --wait
```

호출자가 사용하는 주소와 일치하는 DNS 이름을 사용합니다. 예를 들어 같은 Compose의
서비스는 `--tls-dns-name bqemu`로 생성합니다. 파일 계약은 [로컬 인증 파일과 TLS](client-credentials-and-tls.md)를
참고하세요.

<!-- section: stop -->
## 중지

```bash
docker compose down
```

다음 실행을 이전 로컬 상태 없이 시작하려면 `--volumes`를 추가합니다.
