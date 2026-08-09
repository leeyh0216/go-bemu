<!-- doc-id: clients/python-bigquery -->
<!-- lang: ko -->

[English](../../en/clients/python-bigquery.md) | [한국어](python-bigquery.md)

# Python BigQuery Client

클라이언트를 만들 때 명시적으로 REST endpoint를 설정합니다. API는 공식 [Python BigQuery
레퍼런스](https://cloud.google.com/python/docs/reference/bigquery/latest)를 따릅니다.

<!-- section: endpoint -->
## 필수 Client Option

```python
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

client = bigquery.Client(
    project="test-project",
    credentials=AnonymousCredentials(),
    client_options=ClientOptions(api_endpoint="http://localhost:9050"),
)
```

같은 Compose 서비스에서는 `api_endpoint`를 `http://bqemu:9050`으로, 호스트 Compose에
접속하는 개발 컨테이너에서는 `http://host.docker.internal:9050`으로 설정합니다. 프로젝트
ID만 설정해도 요청 주소는 바뀌지 않습니다.

<!-- section: tls -->
## TLS

HTTPS endpoint URL은 `https://...:9050` 로 설정하고 생성한 CA를 신뢰합니다.

```bash
export REQUESTS_CA_BUNDLE="$PWD/.bqemu-auth/ca.pem"
export SSL_CERT_FILE="$REQUESTS_CA_BUNDLE"
```

credential file과 TLS fixture 설정은 [로컬 인증 파일과 TLS](../../../../../docs/ko/client-credentials-and-tls.md)를
참고하세요.

<!-- section: query -->
## 최소 쿼리

```python
job = client.query("SELECT 1 AS answer", location="US")
rows = list(job.result())
assert rows[0]["answer"] == 1
client.close()
```

먼저 [시작하기](../../../../../docs/ko/getting-started.md)로 에뮬레이터 프로젝트를 만듭니다.

<!-- section: load -->
## Parquet Load

URI load는 `gs://` Parquet 입력을 사용합니다. 업로더와 BQEMU가 같은 fake GCS 서비스에
도달하는 주소를 설정합니다. [시작하기](../../../../../docs/ko/getting-started.md#fake-gcs를-통한-parquet-load)를
참고하세요. 다른 load 형식과 로컬 경로는 지원하지 않습니다.

<!-- section: validation -->
## 검증

이 설정은 `google-cloud-bigquery` `3.43.0`에서 검증했습니다. 정확한 scenario ID와 요청
trace는 클라이언트 설정이 아니라 통합 테스트 증거입니다.
