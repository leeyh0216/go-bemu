<!-- doc-id: client-credentials-and-tls -->
<!-- lang: ko -->

[English](../en/client-credentials-and-tls.md) | [한국어](client-credentials-and-tls.md)

# 로컬 클라이언트 인증 파일과 TLS

<!-- section: boundary -->
## 생성 파일의 역할

BQEMU는 BigQuery 호환 REST와 gRPC 요청을 보낸 사용자를 인증하거나 인가하지
않습니다. 여기서 생성하는 인증 파일은 형식을 엄격하게 검사하는 Google
클라이언트의 사전 검사만 통과하기 위한 자료입니다. BQEMU는 발급된 access token을
검사하지 않으며 IAM, OAuth 동의, Google 사용자 정보를 재현하지 않습니다.

`admin.tokenFile`의 용도는 다릅니다. 이 파일은 선택형 BQEMU 관리 수신기만
보호합니다.

제공 도구는 [Application Default
Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)와
[Workload Identity
Federation](https://cloud.google.com/iam/docs/workload-identity-federation)의 로컬
시험 절차를 제공합니다. 폐기할 수 있는 로컬 테스트에서만 사용해야 하며, 실제
사용자를 증명하는 시스템으로 사용하면 안 됩니다.

<!-- section: generate -->
## TLS와 인증 파일 생성

Go 1.26 이상과 `PATH`에서 실행할 수 있는 JDK `keytool`이 필요합니다. 다른 실행
파일을 사용하려면 `--keytool`을 지정합니다. 생성 도구는 출력 디렉터리를 만들기
전에 이 의존성을 확인합니다.

저장소 루트에서 다음 명령 하나를 실행합니다.

```bash
go run ./cmd/bqemu-auth-fixture generate --output .bqemu-auth
```

이 명령은 다음 파일을 생성합니다.

| 파일 | 클라이언트 용도 | Unix 권한 |
| --- | --- | --- |
| `manifest.json` | 절대 파일 경로, 발급 서버 주소, 프록시 주소, trust store 암호 | `0600` |
| `ca.pem` | Python, `bq`, curl, BQEMU 상태 확인용 CA | `0644` |
| `server.pem` | BQEMU와 로컬 발급 서버의 TLS 인증서 | `0644` |
| `server-key.pem` | TLS 개인 키 | `0600` |
| `service-account.json` | 로컬 JWT bearer 교환 | `0600` |
| `authorized-user.json` | 로컬 OAuth refresh 교환 | `0600` |
| `wif.json` | 파일 기반 external account STS 교환 | `0600` |
| `subject-token.txt` | `wif.json`에서 참조하는 subject token | `0600` |
| `access-token.txt` | 교환 없이 직접 전달하는 token | `0600` |
| `truststore.p12` | Java와 Spark용 PKCS12 trust store | `0600` |

출력 디렉터리 권한은 `0700`입니다. `wif.json`에는 `subject-token.txt`의 절대
경로가 기록되므로 최종 사용할 위치에서 파일을 생성해야 합니다. 기존 파일과 심볼릭
링크는 덮어쓰지 않습니다. 폐기 가능한 파일 묶음 전체를 교체할 때만 `--force`를
지정합니다.

`--force`를 지정하면 먼저 같은 상위 경로에 전체 파일 묶음을 생성합니다. 각 파일과
디렉터리를 동기화하고 내용을 검증한 뒤 기존 디렉터리와 원자적으로 교환합니다. 교환
전에 실패하거나 프로세스가 종료되면 기존 파일 묶음은 바뀌지 않습니다. 다음 생성
명령은 중단된 실행이 남긴 표시된 임시 디렉터리를 정리합니다. 원자 교체는 Linux 또는
macOS와 원자적 디렉터리 교환을 지원하는 파일 시스템에서만 사용할 수 있습니다.
지원하지 않는 환경에서는 기존 파일을 변경하지 않고 명령이 실패합니다.
생성 중에는 같은 상위 경로의 `.bqemu-auth.lock`을 잠급니다. 따라서 다른 프로세스가
사용 중인 임시 디렉터리를 정리할 수 없습니다. Linux와 macOS에서는 인증 정보가 없는
이 잠금 파일이 명령 종료 후에도 남을 수 있습니다. 생성 프로세스가 실행 중이 아닐
때만 삭제해야 합니다.

Token 교환에는 `https://localhost:9052` 주소를 기본으로 사용합니다. 로컬 CONNECT
프록시의 기본 주소는 `http://127.0.0.1:9053`입니다. 포트가 사용 중이라면 두 주소를
함께 변경합니다.

```bash
go run ./cmd/bqemu-auth-fixture generate --output .bqemu-auth --base-url https://localhost:19052 --proxy-url http://127.0.0.1:19053
```

생성한 매니페스트에는 두 수신 주소가 모두 기록됩니다. `serve`는 이 주소를 기본값으로
읽으므로 위 예시에서 `--listen`이나 `--proxy-listen`을 다시 지정할 필요가 없습니다.
수신 주소를 명시적으로 재정의하려면 루프백 호스트와 매니페스트에 기록된 포트를
사용해야 합니다. 아래 예시는 기본 포트를 기준으로 설명합니다.

클라이언트가 컨테이너 서비스 이름으로 BQEMU에 접속한다면 `--tls-dns-name`을
반복해서 지정할 수 있습니다.

```bash
go run ./cmd/bqemu-auth-fixture generate --output .bqemu-auth --tls-dns-name bqemu
```

인증서에는 로컬 프록시에 필요한 Google OAuth 호스트 이름 두 개도 포함됩니다.
아래에 설명한 테스트 프로세스에서만 `ca.pem`을 신뢰해야 합니다. 운영체제 전체나
브라우저의 trust store에 이 CA를 추가하면 안 됩니다.

배포한 BQEMU 이미지에는 생성 도구가 들어 있지만 JDK는 들어 있지 않습니다.
`keytool`이 설치된 호스트나 개발 컨테이너로 도구를 꺼낸 뒤 실행합니다.

```bash
fixture_container="$(docker create "$BQEMU_IMAGE")"
docker cp "$fixture_container:/usr/local/bin/bqemu-auth-fixture" ./bqemu-auth-fixture
docker rm "$fixture_container"
chmod 0755 ./bqemu-auth-fixture
./bqemu-auth-fixture generate --output .bqemu-auth
```

<!-- section: issuer -->
## Token 교환 실행

JSON 인증 파일을 사용하기 전에 발급 서버를 실행합니다.

```bash
go run ./cmd/bqemu-auth-fixture serve --manifest .bqemu-auth/manifest.json
```

이 명령은 `generate`가 `manifest.json`에 기록한 발급 서버와 프록시 주소에서
수신합니다.

같은 프로세스가 두 개의 루프백 수신기를 시작합니다.

| 수신기 또는 엔드포인트 | 용도 |
| --- | --- |
| `https://localhost:9052/oauth/token` | OAuth refresh와 JWT bearer grant |
| `/token`과 `/o/oauth2/token` | 엄격한 클라이언트를 위한 같은 grant의 별칭 |
| `https://localhost:9052/sts/token` | [RFC 8693](https://www.rfc-editor.org/rfc/rfc8693.html) token 교환 |
| `https://localhost:9052/introspect` | WIF token 확인 |
| `https://localhost:9052/healthz` | 발급 서버 준비 상태 |
| `http://127.0.0.1:9053` | 고정된 Google OAuth token 주소용 CONNECT 프록시 |

일부 공식 클라이언트는 authorized user 파일의 `token_uri`를 무시하거나 고정된
Google OAuth audience를 사용합니다. 프록시는 `oauth2.googleapis.com:443`과
`accounts.google.com:443`에 대한 CONNECT 요청만 허용하며, TLS 연결을 로컬 발급
서버로 전달합니다. 다른 인터넷 주소로 요청을 전달할 수 없습니다.

JSON 인증 파일을 사용하는 Python과 `bq` 프로세스에 다음 환경 변수를 지정합니다.

```bash
export AUTH_DIR="$PWD/.bqemu-auth"
export REQUESTS_CA_BUNDLE="$AUTH_DIR/ca.pem"
export SSL_CERT_FILE="$AUTH_DIR/ca.pem"
export HTTPS_PROXY=http://127.0.0.1:9053
export https_proxy="$HTTPS_PROXY"
export NO_PROXY=localhost,127.0.0.1,::1
export no_proxy="$NO_PROXY"
```

발급 서버는 token의 SHA-256 값과 만료 시각만 메모리에 보관하고 유효 시간이 한
시간인 token을 발급합니다. 프로세스를 종료하면 모든 상태가 사라집니다. 진단 로그는
assertion, 인증 정보, subject token, access token을 포함한 요청 및 응답 헤더와 본문을
기록합니다. 요청 본문 기록 크기는 발급 서버의 64 KiB token 요청 제한을 따릅니다.

<!-- section: clients -->
## 인증 방식 선택

네 가지 방식을 서로 독립적으로 사용할 수 있습니다.

| 방식 | 파일 또는 옵션 | 교환 절차 |
| --- | --- | --- |
| Service account | `service-account.json` | 서명한 JWT bearer grant |
| Authorized user | `authorized-user.json` | Refresh token grant |
| WIF external account | `wif.json` | 파일 subject token과 STS |
| Direct access token | `access-token.txt` | 없음 |

BQEMU는 임의의 bearer 값을 허용하므로 direct token 방식의 설정이 가장
간단합니다. 실제 클라이언트의 인증 파일 해석 동작을 시험해야 할 때는 JSON 파일
세 가지 중 하나를 사용합니다. REST와 Storage gRPC 주소는 인증 방식과 별도로
지정해야 합니다.

<!-- section: python -->
## Python 3.43.0

공식 [google-cloud-bigquery Python
클라이언트](https://cloud.google.com/python/docs/reference/bigquery/latest)를
설치해서 사용합니다. 생성한 JSON 파일을 scope 없이 읽고 프로젝트와 엔드포인트를
명시합니다. 이렇게 설정하면 WIF 인증 정보가 프로젝트를 자동으로 찾기 위해 외부
API를 호출하지 않습니다.

```python
from google.auth import load_credentials_from_file
from google.cloud import bigquery

credentials, _ = load_credentials_from_file(
    ".bqemu-auth/wif.json",
)
client = bigquery.Client(
    project="test-project",
    credentials=credentials,
    client_options={"api_endpoint": "https://localhost:9050"},
)
print([item.dataset_id for item in client.list_datasets()])
client.close()
```

`wif.json` 대신 `service-account.json` 또는 `authorized-user.json`을 지정하면
다른 교환 절차를 사용할 수 있습니다. Direct token은 다음과 같이 전달합니다.

```python
from pathlib import Path
from google.cloud import bigquery
from google.oauth2.credentials import Credentials

token = Path(".bqemu-auth/access-token.txt").read_text().strip()
client = bigquery.Client(
    project="test-project",
    credentials=Credentials(token=token),
    client_options={"api_endpoint": "https://localhost:9050"},
)
print([item.dataset_id for item in client.list_datasets()])
client.close()
```

Python 프로세스에는 앞 절의 `REQUESTS_CA_BUNDLE`, 프록시, `NO_PROXY` 설정이
필요합니다.

<!-- section: bq -->
## bq CLI 2.1.31

공식 [`bq` CLI](https://cloud.google.com/bigquery/docs/reference/bq-cli-reference)를
별도의 Cloud SDK 설정 디렉터리와 함께 사용합니다. 로컬 테스트 인증 정보가 평소
사용하는 gcloud 설정을 바꾸지 않도록 분리할 수 있습니다.

```bash
export CLOUDSDK_CONFIG="$(mktemp -d)"
export CLOUDSDK_CORE_DISABLE_PROMPTS=1
export CLOUDSDK_COMPONENT_MANAGER_DISABLE_UPDATE_CHECK=true
export CLOUDSDK_CORE_CUSTOM_CA_CERTS_FILE="$AUTH_DIR/ca.pem"
export CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE="$AUTH_DIR/service-account.json"

bq --api=https://localhost:9050 --project_id=test-project --ca_certificates_file="$AUTH_DIR/ca.pem" --format=json ls
```

`CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE`를 `authorized-user.json`이나
`wif.json`으로 바꾸면 다른 인증 파일을 사용할 수 있습니다. 발급 서버를 실행하지
않고 token 파일을 직접 전달할 수도 있습니다.

```bash
bq --api=https://localhost:9050 --project_id=test-project --ca_certificates_file="$AUTH_DIR/ca.pem" --oauth_access_token="$(tr -d '\r\n' < "$AUTH_DIR/access-token.txt")" --format=json ls
```

테스트가 끝나면 임시 `CLOUDSDK_CONFIG` 디렉터리를 삭제합니다.

<!-- section: spark -->
## PySpark와 Scala Spark

지원 계약은 Spark `3.5.8`과 [Spark BigQuery 커넥터
`0.44.2`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.44.2)입니다.
생성 도구가 PKCS12 trust store도 함께 만듭니다. PySpark나 `spark-shell`을
시작하기 전에 JVM trust store와 루프백 프록시를 지정합니다.

```bash
export JAVA_TOOL_OPTIONS="-Djavax.net.ssl.trustStore=$AUTH_DIR/truststore.p12 -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=PKCS12 -Dhttps.proxyHost=127.0.0.1 -Dhttps.proxyPort=9053 -Dhttp.nonProxyHosts=localhost|127.*"
export SPARK_LOCAL_IP=127.0.0.1
```

PySpark에서 JSON 인증 파일로 테이블을 읽는 예시는 다음과 같습니다.

```python
df = (
    spark.read.format("bigquery")
    .option("parentProject", "test-project")
    .option("billingProject", "test-project")
    .option("project", "test-project")
    .option("bigQueryHttpEndpoint", "https://localhost:9050")
    .option("bigQueryStorageGrpcEndpoint", "localhost:9060")
    .option("credentialsFile", "/absolute/path/.bqemu-auth/service-account.json")
    .load("test-project.analytics.events")
)
df.show()
```

`credentialsFile`에 authorized user 또는 WIF 파일 경로를 지정할 수도 있습니다.
Direct token을 사용하려면 `credentialsFile`을 제거하고 `access-token.txt`의
앞뒤 공백을 제거한 값을 `gcpAccessToken`에 지정합니다.

Scala의 `DataFrameReader`에도 같은 옵션을 적용합니다. 예를 들어
`spark.read.format("bigquery").option("credentialsFile", path).load(table)`로 JSON
인증 파일을 사용할 수 있습니다. 메타데이터 요청은 HTTPS를 사용하고 테이블 읽기는
Storage gRPC를 사용하므로 두 엔드포인트 옵션을 모두 유지해야 합니다.

커넥터가 내부 의존성으로 가져오는 Java BigQuery SDK 버전은 구현 세부사항입니다.
`google-cloud-bigquery 2.60.0`을 별도의 호환성 기준이나 테스트 대상으로 사용하지
않습니다.

<!-- section: bqemu-tls -->
## BQEMU에서 TLS 사용

생성한 인증서를 BQEMU REST와 Storage gRPC에 함께 적용합니다.

```bash
export BQEMU_TLS_CERT_FILE="$AUTH_DIR/server.pem"
export BQEMU_TLS_KEY_FILE="$AUTH_DIR/server-key.pem"
export BQEMU_PUBLIC_URL=https://localhost:9050
go run ./cmd/emulator
```

클라이언트는 `ca.pem`을 신뢰하고 인증서 SAN에 있는 이름으로 접속해야 합니다.
TLS는 전송 구간만 보호합니다. BQEMU가 `Authorization` 헤더를 검사하도록 만들지는
않습니다.

<!-- section: compose -->
## Docker Compose

먼저 호스트에서 파일을 생성합니다. TLS 덮어쓰기 설정은 컨테이너를 호스트 UID와
GID로 실행합니다. 따라서 `0700` 디렉터리와 `0600` 키 권한을 완화하지 않고 읽을 수
있습니다. 이미지 사용자 소유의 named volume 대신 호스트의 `data` 디렉터리도
연결합니다.

```bash
command -v keytool
go run ./cmd/bqemu-auth-fixture generate --output .bqemu-auth
mkdir -p data
export BQEMU_HOST_UID="$(id -u)"
export BQEMU_HOST_GID="$(id -g)"
docker compose -f compose.yaml -f compose.tls.yaml up -d --build --wait
```

덮어쓰기 설정은 `.bqemu-auth`를 `/run/bqemu-auth`에 읽기 전용으로 연결합니다.
외부 REST 접속에는 `https://localhost:9050` 주소를 사용하고 Storage gRPC에는
`localhost:9060` 주소를 사용합니다. 호스트에서 실행하는 클라이언트에는 token 발급
서버도 호스트에서 실행합니다.

읽기 전용 bind mount의 동작은 [Docker
문서](https://docs.docker.com/engine/storage/bind-mounts/#use-a-read-only-bind-mount)에
설명되어 있습니다. 다음 명령으로 서비스를 종료합니다.

```bash
docker compose -f compose.yaml -f compose.tls.yaml down --remove-orphans
```

<!-- section: devcontainer -->
## 개발 컨테이너

클라이언트가 개발 컨테이너에서 실행된다면 파일도 같은 컨테이너에서 생성합니다.
그래야 `wif.json`에 기록한 `subject-token.txt`의 절대 경로가 유효하고, 클라이언트와
루프백 발급 서버가 같은 네트워크 공간에 있게 됩니다.

BQEMU가 `bqemu`라는 Compose 서비스로 함께 실행된다면 해당 DNS 이름을 인증서에
추가합니다.

```bash
go run ./cmd/bqemu-auth-fixture generate --output .bqemu-auth --tls-dns-name bqemu
go run ./cmd/bqemu-auth-fixture serve --manifest .bqemu-auth/manifest.json
```

커넥터 REST 엔드포인트에는 `https://bqemu:9050` 주소를 사용하고 Storage gRPC에는
`bqemu:9060` 주소를 사용합니다. `NO_PROXY`에도 `bqemu`를 추가합니다. Token 교환에는
매니페스트에 기록된 주소를 사용합니다. 기본값은 `localhost:9052`입니다. 개발
컨테이너에는 Go 1.26 이상과 `keytool`이
필요합니다. 다른 파일 시스템 경로에서 생성한 파일을 컨테이너 안으로 옮기면
안 됩니다.

<!-- section: verification -->
## 지원 클라이언트 검증

저장소의 계약 테스트는 선택한 Python과 Spark 의존성을 설치하고, 정규화된 소비자
사례가 선언한 실행 산출물의 SHA-256 값을 확인합니다. TLS를 적용한 BQEMU와 발급
서버를 시작한 뒤 모든 필수 사례를 실제 클라이언트 프로세스에서 실행합니다.

```bash
make auth-client-setup
make auth-client-test
```

기본 명령은 `contract/consumers.normalized.json`의 모든 필수 사례를 실행합니다. CI는
같은 진입점을 사용하면서 `BQEMU_AUTH_CASE`에 정규화된 사례 ID를 지정합니다. 따라서
실행기를 바꾸지 않고 실패한 실행 환경과 어댑터를 구분할 수 있습니다.
`BQEMU_AUTH_JUNIT`을 지정하면 case 이름, 실행 시간, 오류 자료형, 원본 오류 문구를
포함한 JUnit XML 파일을 생성합니다. `BQEMU_AUTH_DIAGNOSTICS`를 지정하면 상태, 출력
크기, 상관관계 확인용 SHA-256 값과 보관한 자식 및 백그라운드 프로세스 원문을
NDJSON으로 기록합니다.

`PATH`에 있는 `bq` 실행 파일은 선택한 사례가 선언한 버전이어야 합니다. 필수 Python
클라이언트, CLI, Spark, 커넥터, 실행 환경의 정확한 버전은 [소비자
호환성](consumer-compatibility.md) 문서에서 자동 생성합니다.

진단 정보에는 작업 이름, 종료 상태, 출력 크기, 상관관계 확인용 SHA-256 값과
클라이언트 및 서버 원문 출력이 포함됩니다. 따라서 로컬 산출물에 인증 정보나 token이
포함될 수 있으므로 생성한 fixture 디렉터리와 같은 수준으로 접근과 보관을 관리해야
합니다.

<!-- section: cleanup -->
## 교체와 삭제

테스트가 끝나면 발급 서버를 종료하고 생성한 파일을 삭제합니다.

```bash
rm -rf .bqemu-auth
rm -f .bqemu-auth.lock
```

다시 생성하면 키와 token이 모두 바뀝니다. JSON 엔드포인트를 개별적으로 수정하지
말고 전체를 다시 생성해야 합니다. Service account JWT audience, WIF subject token
경로, 인증서 이름, 매니페스트가 서로 일치해야 합니다.
