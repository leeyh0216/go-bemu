<!-- doc-id: readme -->
<!-- lang: ko -->

[English](README.md) | [한국어](README.ko.md)

# go-bemu

`go-bemu`는 애플리케이션과 커넥터 테스트를 위한 실험용 로컬 BigQuery 에뮬레이터입니다.
BigQuery v2 REST API와 BigQuery Storage Read/Write gRPC 서비스를 제공합니다.
DuckDB는 물리 데이터를 저장하고 실행하며, BigQuery 모델과 호환성 동작은 BQEMU가
관리합니다.

운영 환경용 데이터베이스, IAM 구현, 완전한 BigQuery 대체품은 아닙니다. 아래에 적힌
지원 경로부터 사용하시고, 호환성 계약에 없는 BigQuery 기능은 지원하지 않는 것으로
보셔야 합니다.

호환성 계약은 [BigQuery REST v2
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest), [Storage RPC
레퍼런스](https://cloud.google.com/bigquery/docs/reference/storage/rpc), 고정 버전인
[Spark BigQuery 커넥터 `0.44.2`
소스](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2)를
기준으로 합니다.

<!-- section: status -->
## 사용할 수 있는 기능

공개 API 기준으로 테스트한 기능은 다음과 같습니다.

- BigQuery v2 REST를 통한 프로젝트, 데이터 세트, 테이블의 생성과 관리
- 동기 `jobs.query`, 프로세스 메모리 안의 `jobs.get` 및 `getQueryResults` 작업 조회,
  DuckDB와 호환되는 제한된 SQL 실행
- 최상위 및 중첩 필드 추가와 반복 레코드 내부 필드 추가
- 쿼리 API에서 기준 메타데이터와 함께 처리하는 `CREATE TABLE`, `DROP TABLE`,
  `ALTER TABLE ADD COLUMN`, `ALTER TABLE RENAME COLUMN`
- Arrow 또는 Avro 행, 스트림 오프셋, 열 선택 검사, 지원되는 행 제한을 사용하는
  Storage Read 세션
- 기본 스트림 및 `PENDING` 스트림의 Storage Write `ProtoRows`
- 저장소 계약으로 검증한 Spark 커넥터 `0.44.2` 읽기 경로와 파티션 없는 직접 정적
  덮어쓰기
- 명시적으로 설정한 fake GCS 또는 `file://` 어댑터에서 읽는 선택적 Parquet 로드 작업
- REST와 Storage gRPC의 선택적 TLS

주요 제한은 다음과 같습니다.

- GoogleSQL 전체를 구현하지 않았습니다. `ALTER COLUMN SET DATA TYPE`, `DROP
  COLUMN`, `TRUNCATE`, 일반 `MERGE`, 스크립트와 여러 표현식은 아직 사용할 수
  없습니다.
- 복사 및 추출 작업, Parquet 이외의 로드, CDC, Arrow Storage Write 행, 여러
  Storage Read/Write RPC는 아직 지원하지 않습니다. `tabledata.insertAll`은
  typed JSON 행과 재시도 `insertId`를 포함한 atomic profile을 지원하지만, partial 행과
  템플릿 테이블은 아직 지원하지 않습니다.
- IAM, OAuth 인가, 할당량, 비용 청구, 리전 배치, Google 제어 영역 동작은
  에뮬레이션하지 않습니다.
- 프로젝트, 데이터 세트, 테이블, 스키마 메타데이터는 BQEMU 전용 SQLite에 저장하고,
  물리 테이블과 행은 DuckDB에 저장합니다. 쿼리와 로드 작업 이력은 아직 프로세스
  메모리에만 있으며, 두 저장소에 걸친 비정상 종료 복구도 완성되지 않았습니다.

정확하고 테스트 가능한 범위는 [호환성 계약](docs/ko/compatibility.md)에서 확인하실 수
있습니다. 구성 요소의 책임은 [아키텍처](docs/ko/architecture.md)에 설명되어 있습니다.

<!-- section: quick-start -->
## Docker Compose로 시작하기

Docker Compose를 사용하면 가장 간단하게 로컬 에뮬레이터를 실행할 수 있습니다. 이
저장소에서 이미지를 빌드하고, 공개 API 포트를 열며, 이름 있는 `/data` 볼륨을 만들고,
`/readyz`가 준비될 때까지 기다립니다.

```bash
git clone https://github.com/leeyh0216/go-bemu.git
cd go-bemu
docker compose up --build --wait
curl --fail --silent --show-error http://localhost:9050/readyz
```

호스트에서 사용하는 주소는 다음과 같습니다.

| 기능 | 주소 | 용도 |
| --- | --- | --- |
| BigQuery REST와 상태 확인 | `http://localhost:9050` | BigQuery v2 API, `/healthz`, `/readyz` |
| BigQuery Storage gRPC | `localhost:9060` | Storage Read 및 Storage Write 클라이언트 |
| 관리 API | `127.0.0.1:9051` | 기본 비활성화, 루프백 주소만 사용 |

`/healthz`는 프로세스가 실행 중인지만 확인합니다. `/readyz`는 필요한 런타임 의존성이
준비되었는지 확인합니다. 애플리케이션 시작 검사와 통합 테스트에서는 `/readyz`를
사용해 주십시오.

이름 있는 데이터 볼륨을 유지한 채 서비스를 중지하려면 다음 명령을 실행합니다.

```bash
docker compose down
```

볼륨까지 지우고 비어 있는 에뮬레이터로 다시 시작하려면 다음 명령을 실행합니다.

```bash
docker compose down --volumes
```

이 저장소에서는 `make docker-up`, `make docker-logs`, `make docker-down`으로 같은
작업을 수행하실 수 있습니다.

<!-- section: image -->
## 공개 GHCR 이미지 사용하기

컨테이너 패키지 이름은 `ghcr.io/leeyh0216/go-bemu`입니다. 발행 워크플로는 `amd64`와
`arm64` Linux 이미지를 만들며, 태그는 다음과 같습니다.

| 원본 | 발행 태그 |
| --- | --- |
| `main`에 푸시 | `edge`, `sha-<전체-커밋-sha>` |
| SemVer Git 태그 `vX.Y.Z` | `X.Y.Z`, `X.Y`, `latest`, `sha-<전체-커밋-sha>`와 `X > 0`일 때 `X` |

`edge`는 `main`을 따라갑니다. `latest`는 `main`이 아니라 가장 최근의 SemVer 릴리스를
따릅니다. 로컬에서 직접 확인할 때는 릴리스 태그를 사용하고, 공유 CI나 오래 유지하는
테스트 환경에서는 digest까지 확정해 주십시오.

GHCR 패키지는 소유자가 GitHub 패키지 설정에서 공개로 바꾸기 전까지 처음 발행할 때
비공개입니다. 비공개 패키지를 받으려면 `read:packages` 권한이 있는 classic personal
access token으로 먼저 로그인해야 합니다. 소유자가 패키지를 공개한 경우에만 로그인 없이
받을 수 있습니다.

```bash
export GHCR_TOKEN=... # 비공개 패키지에는 read:packages 권한의 classic token을 사용합니다.
printf '%s' "$GHCR_TOKEN" | docker login ghcr.io --username <github-user> --password-stdin

export BQEMU_IMAGE=ghcr.io/leeyh0216/go-bemu:0.1.0
docker pull "$BQEMU_IMAGE"

# 승인한 이미지 목록 또는 `docker inspect`가 알려 주는 digest를 사용합니다.
export BQEMU_IMAGE=ghcr.io/leeyh0216/go-bemu@sha256:<digest>
docker compose up --no-build --wait bqemu
```

저장소의 Compose 파일은 `BQEMU_IMAGE` 값을 읽습니다. digest까지 지정하면 기본 로컬
이미지 `go-bemu:dev`를 빌드하지 않고 공개 이미지를 사용합니다. 이때
`--no-build`가 필요합니다. 자동화에는 변경될 수 있는 `edge`나 `latest` 태그를 사용하지
마십시오.

<!-- section: compose -->
## 다른 Compose 서비스에서 사용하기

같은 Compose 프로젝트의 서비스는 `localhost`가 아니라 Docker DNS를 사용합니다.
REST는 `http://bqemu:9050`, Storage gRPC는 `bqemu:9060`으로 연결합니다. 이 저장소의
`compose.yaml`을 포함하는 Compose 파일에 다음 애플리케이션 서비스를 추가하거나,
기존 프로젝트에 같은 설정을 적용해 주십시오.

```yaml
services:
  app:
    build: .
    depends_on:
      bqemu:
        condition: service_healthy
    environment:
      BIGQUERY_REST_ENDPOINT: http://bqemu:9050
      BIGQUERY_STORAGE_GRPC_ENDPOINT: bqemu:9060
```

기본 `bqemu` 서비스는 `bqemu-data` 이름의 볼륨을 `/data`에 연결합니다. 호스트에서
직접 볼 수 있는 디렉터리를 사용하려면 Compose 오버라이드 파일에 다음 내용을
추가합니다.

```yaml
services:
  bqemu:
    volumes:
      - ./bqemu-data:/data
```

개별 데이터베이스 파일만 마운트하지 마십시오. SQLite 보조 파일과 DuckDB 데이터를
함께 보관할 수 있도록 `/data` 디렉터리 전체를 마운트해야 합니다.
`/data/bqemu-state.sqlite`에는 BQEMU 메타데이터를, `/data/bqemu.duckdb`에는 물리
행을 저장합니다. 백업과 복원도 이 디렉터리 전체를 한 단위로 처리해 주십시오.

<!-- section: dev-container -->
## Dev Container에서 사용하기

이 저장소는 Dev Container 정의를 요구하지 않습니다. 다만 사용하는 프로젝트의 작업
공간과 BQEMU를 같은 Compose 네트워크에서 실행할 수 있습니다. 사용하는 프로젝트에
`.devcontainer/compose.yaml` 파일을 만들고 다음 내용을 넣습니다.

```yaml
services:
  workspace:
    image: mcr.microsoft.com/devcontainers/go:1-1.26-bookworm
    volumes:
      - ..:/workspaces/app:cached
    working_dir: /workspaces/app
    command: sleep infinity

  bqemu:
    environment:
      BQEMU_PUBLIC_URL: http://bqemu:9050
```

같은 디렉터리에 `.devcontainer/devcontainer.json` 파일을 만듭니다. 목록의 첫 Compose
파일은 `bqemu` 서비스를 정의한 `compose.yaml`입니다. BQEMU를 내려받은 위치에 맞게
상대 경로를 조정하거나, 해당 서비스를 프로젝트 Compose 파일에 복사해 주십시오.

```json
{
  "name": "app-with-bqemu",
  "dockerComposeFile": ["../../go-bemu/compose.yaml", "compose.yaml"],
  "service": "workspace",
  "workspaceFolder": "/workspaces/app",
  "shutdownAction": "stopCompose"
}
```

Dev Container 안의 클라이언트에는 `http://bqemu:9050`과 `bqemu:9060`을 설정합니다.
호스트에서는 `http://localhost:9050`과 `localhost:9060`을 사용합니다. 통합 테스트는
`http://bqemu:9050/readyz`가 준비된 뒤 시작해 주십시오.

<!-- section: bootstrap -->
## 프로젝트, 데이터 세트, 테이블 만들기

BQEMU 프로젝트는 로컬 에뮬레이터 리소스입니다. BigQuery v2 데이터 세트와 테이블
API를 호출하기 전에 먼저 만들어야 합니다.

```bash
export BQEMU_REST_ENDPOINT=http://localhost:9050
export BQEMU_PROJECT=demo-project

curl --fail --silent --show-error -X POST "$BQEMU_REST_ENDPOINT/bqemu/v1/projects" \
  -H 'Content-Type: application/json' \
  -d '{"projectId":"demo-project"}'

curl --fail --silent --show-error -X POST \
  "$BQEMU_REST_ENDPOINT/bigquery/v2/projects/$BQEMU_PROJECT/datasets" \
  -H 'Content-Type: application/json' \
  -d '{"datasetReference":{"datasetId":"analytics"},"location":"US"}'

curl --fail --silent --show-error -X POST \
  "$BQEMU_REST_ENDPOINT/bigquery/v2/projects/$BQEMU_PROJECT/datasets/analytics/tables" \
  -H 'Content-Type: application/json' \
  -d '{
    "tableReference":{"tableId":"events"},
    "schema":{"fields":[
      {"name":"event_id","type":"INT64","mode":"REQUIRED"},
      {"name":"name","type":"STRING","mode":"NULLABLE"}
    ]}
  }'
```

제한된 범위의 쿼리는 일반 BigQuery v2 리소스 경로로 제출합니다.

```bash
curl --fail --silent --show-error -X POST \
  "$BQEMU_REST_ENDPOINT/bigquery/v2/projects/$BQEMU_PROJECT/queries" \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT event_id, name FROM `demo-project.analytics.events`","useLegacySql":false}'
```

<!-- section: clients -->
## 클라이언트 설정

BQEMU는 Bearer 토큰을 인가하지 않습니다. 다만 일부 Google 클라이언트 라이브러리는
요청을 보내기 전에 인증 객체를 요구합니다. 클라이언트에 맞는 로컬 파일 또는 익명
인증 객체를 사용하시면 되며, 서버는 토큰 값을 검사하지 않습니다.

### Python BigQuery 클라이언트

공식 Python 클라이언트는 익명 인증 객체와 명시적인 REST 주소를 받을 수 있습니다.

```python
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

client = bigquery.Client(
    project="demo-project",
    credentials=AnonymousCredentials(),
    client_options=ClientOptions(api_endpoint="http://localhost:9050"),
)
table = client.get_table("demo-project.analytics.events")
```

### bq CLI

`bq` CLI는 요청 전에 자체 옵션을 검사합니다. 비어 있지 않은 로컬 토큰을 주고 현재
gcloud 설정을 사용하지 않도록 지정합니다.

```bash
bq --api=http://localhost:9050 \
  --project_id=demo-project \
  --use_gcloud_config=false \
  --oauth_access_token=local-bqemu-token \
  ls
```

### Spark BigQuery 커넥터

HTTP와 Storage gRPC 옵션은 별도로 설정해야 합니다. 같은 Compose 네트워크의
컨테이너에서는 `bqemu`를 호스트 이름으로 사용합니다.

```python
df = (
    spark.read.format("bigquery")
    .option("table", "demo-project.analytics.events")
    .option("parentProject", "demo-project")
    .option("billingProject", "demo-project")
    .option("project", "demo-project")
    .option("bigQueryHttpEndpoint", "http://bqemu:9050")
    .option("bigQueryStorageGrpcEndpoint", "bqemu:9060")
    .option("gcpAccessToken", "local-bqemu-token")
    .load()
)
```

호스트에서 실행하는 Spark 프로세스에는 `http://localhost:9050`과 `localhost:9060`을
사용합니다. 커넥터 지원 범위는 버전에 따라 다르며, 현재 저장소에서 검증한 버전은
`0.44.2`입니다.

<!-- section: credentials -->
## 로컬 인증 파일

에뮬레이터에는 인증이나 IAM 하위 시스템을 의도적으로 두지 않습니다. `Authorization`
헤더 없이 요청을 받아들이며 Google 액세스 토큰을 발급하거나 검사하지 않습니다. 인증
파일은 클라이언트 쪽의 형식 검사와 토큰 획득 흐름을 통과하기 위한 용도만 있습니다.

완전한 로컬 인증 파일 디렉터리를 만들고, 다른 터미널에서 발급기를 실행합니다.

```bash
go run ./cmd/bqemu-auth-fixture generate --output .bqemu-auth
go run ./cmd/bqemu-auth-fixture serve \
  --manifest .bqemu-auth/manifest.json \
  --listen 127.0.0.1:9052
```

GHCR 이미지만 사용하는 경우에는 이미지에 포함된 정적 바이너리를 호스트로 꺼내서
실행합니다. 호스트에서 실행해야 WIF subject token의 파일 경로와 루프백 전용 TLS 발급
주소를 호스트 클라이언트가 그대로 사용할 수 있습니다.

```bash
fixture_container="$(docker create "$BQEMU_IMAGE")"
docker cp "$fixture_container:/usr/local/bin/bqemu-auth-fixture" ./bqemu-auth-fixture
docker rm "$fixture_container"
chmod 0755 ./bqemu-auth-fixture

./bqemu-auth-fixture generate --output .bqemu-auth
./bqemu-auth-fixture serve \
  --manifest .bqemu-auth/manifest.json \
  --listen 127.0.0.1:9052
```

생성 명령은 `manifest.json`, `ca.pem`, `server.pem`, `server-key.pem`,
`service-account.json`, `authorized-user.json`, `wif.json`, `subject-token.txt`를
만듭니다. 발급기는 `/healthz`를 제공하며, 서비스 계정과 승인된 사용자 토큰 교환은
<https://localhost:9052/oauth/token>에서, WIF 토큰 교환은
<https://localhost:9052/sts/token>에서 처리합니다.

클라이언트에서 사용할 인증 파일 하나를 선택합니다.

```bash
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.bqemu-auth/service-account.json"
# 또는 authorized-user.json
# 또는 wif.json
export REQUESTS_CA_BUNDLE="$PWD/.bqemu-auth/ca.pem"
export SSL_CERT_FILE="$PWD/.bqemu-auth/ca.pem"
```

Java 기반 클라이언트는 `ca.pem`을 Java 신뢰 저장소에 추가하고
`-Djavax.net.ssl.trustStore=/path/to/truststore`를 설정해야 합니다. 이 발급기는
로컬 클라이언트 테스트를 위한 도구일 뿐입니다. BQEMU 엔드포인트를 보호하지 않으며,
발급한 토큰은 다른 서비스의 인가 정보로 사용할 수 없습니다. 로컬 에뮬레이터 설정,
Compose 파일, Dev Container 마운트에는 실제 Google 인증 정보를 넣지 마십시오.

<!-- section: tls -->
## TLS

TLS는 REST와 gRPC를 모두 보호합니다. 클라이언트가 사용하는 호스트 이름을 인증서의
SAN에 포함해 로컬 인증서를 만듭니다.

```bash
mkdir -p certs
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 30 \
  -keyout certs/server-key.pem -out certs/server.pem \
  -subj '/CN=localhost' \
  -addext 'subjectAltName=DNS:localhost,DNS:bqemu,IP:127.0.0.1'
```

로컬 프로세스에서는 다음과 같이 설정합니다.

```bash
export BQEMU_TLS_CERT_FILE="$PWD/certs/server.pem"
export BQEMU_TLS_KEY_FILE="$PWD/certs/server-key.pem"
export BQEMU_PUBLIC_URL=https://localhost:9050
make run
```

생성한 로컬 인증 파일에서 같은 인증서와 키를 사용할 수도 있습니다.

```bash
export BQEMU_TLS_CERT_FILE="$PWD/.bqemu-auth/server.pem"
export BQEMU_TLS_KEY_FILE="$PWD/.bqemu-auth/server-key.pem"
export BQEMU_PUBLIC_URL=https://localhost:9050
make run
```

Compose에서는 오버라이드 파일에 읽기 전용 인증서 마운트와 같은 환경 변수를 추가합니다.

```yaml
services:
  bqemu:
    environment:
      BQEMU_TLS_CERT_FILE: /run/bqemu-tls/server.pem
      BQEMU_TLS_KEY_FILE: /run/bqemu-tls/server-key.pem
      BQEMU_PUBLIC_URL: https://localhost:9050
    volumes:
      - ./certs:/run/bqemu-tls:ro
```

클라이언트는 인증서를 발급한 CA를 신뢰해야 하며, SAN에 들어 있는 호스트 이름으로
접속해야 합니다. TLS를 켜도 토큰 검사나 IAM 동작이 추가되지는 않습니다.

<!-- section: configuration -->
## 설정과 영속성

저장소의 [설정 파일](configs/bqemu.yaml)이 모든 설정의 기준입니다. `--config`,
`BQEMU_CONFIG`, 지원하는 `BQEMU_*` 환경 변수로 다른 값을 지정할 수 있습니다. Compose
환경에서는 discovery 문서를 받는 클라이언트가 볼 수 있는 주소로 `BQEMU_PUBLIC_URL`을
설정해 주십시오.

기본 Compose 구성은 `/data`를 `bqemu-data` 볼륨으로 연결합니다. 컨테이너를 다시
만들어도 로컬 상태를 유지하려면 이 볼륨을 보존해야 합니다. BQEMU SQLite 상태 저장소가
활성화되면 이 디렉터리에는 기준 메타데이터와 DuckDB 데이터가 함께 들어가므로, 둘 중
하나만 따로 보관하지 마십시오.

<!-- section: troubleshooting -->
## 문제 해결

| 증상 | 확인할 내용 |
| --- | --- |
| 클라이언트가 연결하지 못함 | `curl http://localhost:9050/readyz`가 성공하는지, 공개 HTTP 포트를 다른 프로세스가 쓰고 있지 않은지 확인해 주십시오. |
| 컨테이너 클라이언트가 `localhost`를 호출함 | Compose 네트워크 안에서는 `http://bqemu:9050`과 `bqemu:9060`을 사용해 주십시오. |
| REST는 되지만 Spark Storage 호출이 실패함 | 별도 설정인 `bigQueryStorageGrpcEndpoint` 값과 `9060` 포트를 확인해 주십시오. gRPC 주소에는 `http://`를 붙이지 않습니다. |
| TLS 연결이 실패함 | 로컬 CA를 신뢰하고 인증서 SAN에 포함된 호스트 이름을 사용해 주십시오. 자체 서명 인증서는 클라이언트 신뢰 저장소를 별도로 설정해야 합니다. |
| 데이터가 예상보다 빨리 사라짐 | `/data` 마운트를 확인해 주십시오. `docker compose down --volumes`는 이름 있는 볼륨을 의도적으로 삭제합니다. |
| SQL이 `invalidQuery` 또는 미지원 오류를 반환함 | 제한된 호환성 계약과 비교해 주십시오. 전체 GoogleSQL을 구현한 서비스가 아닙니다. |
| Google 클라이언트가 실제 OAuth를 시도함 | 이 저장소의 로컬 인증 파일 또는 해당 클라이언트의 익명 인증 모드를 사용해 주십시오. 로컬 테스트를 운영 인증 정보에 연결하지 마십시오. |

<!-- section: documentation -->
## 문서

- [문서 색인](docs/ko/index.md)
- [호환성 계약](docs/ko/compatibility.md)
- [아키텍처](docs/ko/architecture.md)
- [설정과 운영](docs/ko/operations.md)
- [스키마 변경과 CDC](docs/ko/schema-evolution-cdc.md)
- [유지보수 안내](docs/ko/maintainer-guide.md)
- [기여 안내](CONTRIBUTING.ko.md)

<!-- section: development -->
## 소스에서 빌드하기

로컬 개발에는 Go 1.26 이상과 DuckDB Go 드라이버를 빌드하는 데 필요한 C/C++ 도구
모음이 필요합니다.

```bash
make setup
make check
make run
```

Go 테스트는 `make test`로 실행합니다. Python, `bq`, Spark 계약은 각각 고정된
클라이언트 준비 조건이 있으므로 실행하기 전에 유지보수 안내를 확인해 주십시오.

<!-- section: non-goals -->
## 지원하지 않는 사용 목적

`go-bemu`를 운영 데이터 처리, 성능 예측, 인가 테스트, 할당량 또는 비용 청구 테스트,
GoogleSQL 동등성 증명에 사용하지 마십시오. 로컬 테스트 성공은 문서화한 에뮬레이터
계약과 클라이언트 버전에서만 의미가 있습니다.
