<!-- doc-id: getting-started -->
<!-- lang: ko -->

[English](../en/getting-started.md) | [한국어](getting-started.md)

# 시작하기

이 문서는 BQEMU를 실행하고 로컬 클라이언트에 필요한 리소스를 만든 뒤 쿼리 하나를
실행하는 과정을 설명합니다. 특정 필드나 메서드를 사용하기 전에는
[호환성](compatibility.md)을 확인해 주십시오. 요청 리소스는 [BigQuery REST API
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest)를 기준으로 합니다.

<!-- section: run -->
## BQEMU 실행하기

저장소 루트에서 다음 명령을 실행합니다.

```bash
docker compose up --build -d --wait
curl --fail http://localhost:9050/readyz
```

Compose는 카탈로그와 테이블 데이터를 `bqemu-data` 볼륨에 보관합니다. 재시작 후에도
데이터를 사용하려면 사용자가 이 볼륨을 유지해야 합니다.

<!-- section: endpoints -->
## 접속 주소 선택하기

| 클라이언트 위치 | REST | Storage gRPC |
| --- | --- | --- |
| Compose를 실행한 호스트 | `http://localhost:9050` | `localhost:9060` |
| 같은 Compose의 다른 서비스 | `http://bqemu:9050` | `bqemu:9060` |
| BQEMU를 호스트에서 실행하는 개발 컨테이너 | `http://host.docker.internal:9050` | `host.docker.internal:9060` |

REST 호출자는 REST 주소만 사용합니다. Storage Read와 Storage Write 호출자는 gRPC
주소도 사용합니다.

<!-- section: resources -->
## 프로젝트, 데이터 세트, 테이블 만들기

BQEMU 프로젝트는 에뮬레이터 전용 리소스입니다. BigQuery v2 메서드를 호출하기 전에
먼저 프로젝트를 만듭니다.

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
  -d '{"tableReference":{"projectId":"test-project","datasetId":"analytics","tableId":"events"},"schema":{"fields":[{"name":"id","type":"INTEGER","mode":"REQUIRED"},{"name":"label","type":"STRING"}]}}'
```

위 요청은 `bqemu.projects.create`, `bigquery.datasets.insert`,
`bigquery.tables.insert`를 호출합니다.

<!-- section: query -->
## 첫 쿼리 실행하기

```bash
curl --fail -X POST \
  http://localhost:9050/bigquery/v2/projects/test-project/queries \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT 1 AS answer","useLegacySql":false,"location":"US"}'
```

이 요청은 `bigquery.jobs.query`를 호출합니다. 응답은 job 참조, 스키마, 완료 여부,
인코딩된 행이 포함된 BigQuery `QueryResponse` 형식입니다.

<!-- section: integrations -->
## 호출 프로세스 설정하기

호출 프로세스가 실행되는 위치에 맞춰 위 표에서 REST와 gRPC 주소를 선택합니다. 버전
고정 설정 예제, TLS 설정, operation ID와 확인한 요청 순서는 [통합 테스트
안내](../../tests/integration/docs/ko/index.md)에서 관리합니다.

<!-- section: external-gcs -->
## Parquet 로드 사용하기

BQEMU의 Parquet 로드 작업에는 GCS 호환 JSON endpoint가 필요합니다. 기본 Compose
프로젝트는 fake GCS 서비스를 실행하고 BQEMU가 이 서비스를 사용하도록 설정합니다.

```bash
docker compose up --build -d --wait
curl --fail http://localhost:4443/storage/v1/b
```

두 주소 설정은 서로 다른 호출자가 사용합니다.

| 호출자 | 설정 | Compose 값 |
| --- | --- | --- |
| BQEMU 로드 작업자 | `BQEMU_LOAD_GCS_ENDPOINT` / `load.gcsEndpoint` | `http://fake-gcs:4443` |
| Parquet 객체를 업로드하는 프로세스 | 업로더별 주소 설정 | `http://localhost:4443` |

데이터를 만드는 프로세스가 임시 Parquet 객체를 fake GCS에 업로드합니다. BQEMU는
필요할 때 일치하는 객체 목록을 조회하고 객체 내용을 내려받은 뒤 로드를 반영합니다.
직접 Storage Write는 GCS를 사용하지 않습니다. 검증된 업로더 설정은 [통합 테스트
안내](../../tests/integration/docs/ko/index.md)에서 관리합니다.

Parquet만 지원하는 load 형식입니다. 호출자는 BigQuery media-upload endpoint도 사용할 수
있습니다. BQEMU는 완료된 media를 설정한 fake GCS 서비스에 저장한 뒤 같은 `gs://` Parquet
load 경로를 제출합니다. media 세션은 프로세스 로컬이므로 재시작 뒤에는 유효하지 않습니다.

<!-- section: tls -->
## TLS와 인증 파일 사용하기

로컬 CA, REST/gRPC 서버 인증서, Java truststore와 클라이언트 인증 파일을 생성합니다.

```bash
go run ./cmd/bqemu-auth-fixture generate --output .bqemu-auth
mkdir -p data
export BQEMU_HOST_UID="$(id -u)"
export BQEMU_HOST_GID="$(id -g)"
docker compose -f compose.yaml -f compose.tls.yaml up --build -d --wait
```

생성되는 service account, authorized user, WIF, direct token 파일은 로컬 테스트용
자료입니다. 클라이언트가 token을 교환하거나 생성한 CA를 신뢰해야 한다면 [클라이언트
인증 파일과 TLS](client-credentials-and-tls.md)의 절차를 따라 주십시오.

<!-- section: devcontainer -->
## 개발 컨테이너에서 연결하기

BQEMU를 호스트에서 실행한다면 `host.docker.internal`을 사용합니다. Linux에서는
`host.docker.internal:host-gateway` 호스트 매핑을 추가합니다. BQEMU가 같은
Compose의 다른 서비스라면 서비스 이름인 `bqemu`를 사용합니다.

인증 파일은 이를 사용할 컨테이너 안에서 생성합니다. 그래야 `wif.json`에 기록된
subject token의 절대 경로가 유효합니다. 같은 Compose 서비스에 TLS로 연결한다면
`--tls-dns-name bqemu`로 인증서를 만들고 `.bqemu-auth`를 읽기 전용으로 연결합니다.

<!-- section: stop -->
## BQEMU 종료하기

```bash
docker compose down
```

보관한 테스트 데이터도 함께 삭제하려면 다음 명령을 실행합니다.

```bash
docker compose down --volumes
```
