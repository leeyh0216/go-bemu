<!-- doc-id: client-credentials-and-tls -->
<!-- lang: ko -->

[English](../en/client-credentials-and-tls.md) | [한국어](client-credentials-and-tls.md)

# 로컬 클라이언트 인증 파일과 TLS

<!-- section: boundary -->
## 책임 범위

BQEMU는 BigQuery 호환 REST와 gRPC 요청을 인증하거나 인가하지 않습니다.
`Authorization` 값이 없는 요청도 허용하며, 클라이언트가 보낸 인증 정보도 무시합니다.
`admin.tokenFile`은 이와 다릅니다. 이 설정은 선택형 BQEMU 관리 수신기만 보호합니다.

일부 Google 클라이언트 라이브러리는 에뮬레이터를 호출하기 전에도 형식이 올바른 인증
파일을 요구하며 OAuth 또는 STS 교환을 수행합니다. 이 저장소는 이러한 클라이언트를
위해 로컬 인증 파일 생성기와 루프백 전용 토큰 발급 서버를 제공합니다. [Application
Default
Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)와
[Workload Identity
Federation](https://cloud.google.com/iam/docs/workload-identity-federation)의 클라이언트
절차를 로컬에서 실행하기 위한 도구입니다. BQEMU에 접근 제어를 추가하지 않으며 IAM,
OAuth 동의, Google 사용자 식별 정보와 운영용 토큰 검증을 재현하지 않습니다.

<!-- section: generate -->
## 파일 생성

저장소 루트에서 다음 명령을 실행합니다.

```bash
go run ./cmd/bqemu-auth-fixture generate --output .bqemu-auth
```

발행한 이미지에도 정적으로 연결한 도구가 들어 있습니다. GHCR 이미지만 사용하는 경우
다음과 같이 바이너리를 호스트로 꺼낼 수 있습니다.

```bash
fixture_container="$(docker create "$BQEMU_IMAGE")"
docker cp "$fixture_container:/usr/local/bin/bqemu-auth-fixture" ./bqemu-auth-fixture
docker rm "$fixture_container"
chmod 0755 ./bqemu-auth-fixture
```

호스트 클라이언트가 생성한 파일을 사용한다면 파일 생성과 발급 서버 실행도 호스트에서
수행해야 합니다. 그래야 `wif.json`에 기록한 subject token의 절대 경로와 발급 서버의
루프백 전용 네트워크 경계가 유지됩니다.

기본 발급 서버 주소는 <https://localhost:9052>입니다. 다른 루프백 포트를 사용하려면
`--base-url`을 지정합니다. `--force`를 지정하지 않으면 기존 파일을 덮어쓰지 않습니다.

명령은 매니페스트 경로만 출력하고 다음 파일을 만듭니다.

| 파일 | 용도 | Unix 권한 |
| --- | --- | --- |
| `manifest.json` | 파일 경로와 로컬 엔드포인트 주소 | `0600` |
| `ca.pem` | 로컬 CA 인증서 | `0644` |
| `server.pem` | `localhost`, `127.0.0.1`, `::1`용 서버 인증서 | `0644` |
| `server-key.pem` | 서버 개인 키 | `0600` |
| `service-account.json` | 서비스 계정용 클라이언트 인증 파일 | `0600` |
| `authorized-user.json` | 사용자 계정 ADC 인증 파일 | `0600` |
| `wif.json` | 파일에서 subject token을 읽는 외부 계정 인증 파일 | `0600` |
| `subject-token.txt` | `wif.json`에서 참조하는 subject token | `0600` |

출력 디렉터리 권한은 `0700`입니다. 생성한 파일은 폐기 가능한 로컬 테스트 자료입니다.
소스 관리에 추가하거나 운영 환경에서 다시 사용하면 안 됩니다.

<!-- section: issuer -->
## 로컬 토큰 발급 서버 실행

별도 터미널에서 HTTPS 발급 서버를 시작합니다.

```bash
go run ./cmd/bqemu-auth-fixture serve \
  --manifest .bqemu-auth/manifest.json \
  --listen 127.0.0.1:9052
```

발급 서버에는 루프백 주소만 지정할 수 있습니다. 다음 엔드포인트를 제공합니다.

| 엔드포인트 | 지원 절차 |
| --- | --- |
| `POST /oauth/token` | OAuth refresh token과 JWT bearer grant |
| `POST /sts/token` | `wif.json`용 RFC 8693 token exchange |
| `GET /healthz` | 발급 서버 상태 확인 |

요청 본문, 헤더, 인증 정보, 서명한 assertion, subject token과 발급한 access token은
로그에 남기지 않습니다. Form 본문과 헤더 크기, 제한 시간, 토큰 수명과 메모리 사용량에
상한을 적용합니다. Token exchange 응답은 [RFC
8693](https://www.rfc-editor.org/rfc/rfc8693.html)을 따릅니다.

<!-- section: clients -->
## 클라이언트 인증 파일 선택

생성한 파일 중 하나를 Application Default Credentials로 지정합니다.

```bash
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.bqemu-auth/service-account.json"
# 또는 authorized-user.json
# 또는 wif.json
```

세 파일은 모두 로컬 발급 서버에서 토큰을 받도록 구성되어 있습니다. 클라이언트가
BQEMU를 호출하도록 설정하는 REST 또는 Storage gRPC 엔드포인트와는 별도입니다.

Python HTTP 클라이언트에서는 일반적으로 다음과 같이 CA를 지정합니다.

```bash
export REQUESTS_CA_BUNDLE="$PWD/.bqemu-auth/ca.pem"
export SSL_CERT_FILE="$PWD/.bqemu-auth/ca.pem"
```

Java와 Spark에서는 테스트용 trust store를 만들고 JVM에 지정합니다.

```bash
keytool -importcert -noprompt \
  -alias bqemu-local-ca \
  -file .bqemu-auth/ca.pem \
  -keystore .bqemu-auth/truststore.p12 \
  -storetype PKCS12 \
  -storepass changeit

export BQEMU_JAVA_TLS_OPTS="-Djavax.net.ssl.trustStore=$PWD/.bqemu-auth/truststore.p12 -Djavax.net.ssl.trustStorePassword=changeit"
```

Java 또는 Spark 프로세스의 JVM 옵션 전달 방법을 사용해 `BQEMU_JAVA_TLS_OPTS`를
적용합니다. 생성한 서버 인증서는 문서에 적힌 세 루프백 이름에서만 유효합니다.

<!-- section: bqemu-tls -->
## BQEMU에 TLS 적용

생성한 인증서를 BQEMU REST와 gRPC의 TLS 종료에도 사용할 수 있습니다.

```bash
export BQEMU_TLS_CERT_FILE="$PWD/.bqemu-auth/server.pem"
export BQEMU_TLS_KEY_FILE="$PWD/.bqemu-auth/server-key.pem"
go run ./cmd/emulator
```

TLS는 전송 중인 데이터를 보호합니다. BQEMU가 생성된 access token을 검증하도록 만들지는
않습니다. 클라이언트는 `ca.pem`을 신뢰해야 하며 `localhost`, `127.0.0.1`, `::1` 중
하나로 접속해야 합니다.

<!-- section: cleanup -->
## 정리

테스트가 끝나면 발급 서버를 종료하고 생성한 디렉터리 전체를 삭제합니다.

```bash
rm -rf .bqemu-auth
```

발급 서버 주소를 바꿀 때는 파일을 다시 생성해야 합니다. JSON의 엔드포인트만 직접
바꾸면 서명한 assertion의 audience와 매니페스트가 일치하지 않으므로 지원하지 않습니다.
