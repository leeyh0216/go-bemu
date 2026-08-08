<!-- doc-id: clients/python-bigquery -->
<!-- lang: ko -->

[English](../../en/clients/python-bigquery.md) | [한국어](python-bigquery.md)

# Python BigQuery 클라이언트

Python `3.13`에서 `google-cloud-bigquery` `3.43.0`을 사용합니다. 클라이언트 API는
공식 [Python BigQuery 레퍼런스](https://cloud.google.com/python/docs/reference/bigquery/latest)를
참고해 주십시오.

<!-- section: endpoint -->
## REST 접속 주소

Python 클라이언트는 REST 수신기를 사용합니다. 프로젝트 ID만 지정하면 요청 주소가
바뀌지 않으므로 클라이언트를 만들 때 주소를 함께 전달해야 합니다.

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

같은 Compose의 다른 서비스에서는 `http://bqemu:9050`을 사용합니다. BQEMU를
호스트에서 실행하고 클라이언트를 개발 컨테이너에서 실행한다면
`http://host.docker.internal:9050`을 사용합니다.

<!-- section: credentials -->
## 인증 정보와 TLS

BQEMU는 공개 BigQuery 요청을 인증하지 않지만 Python 라이브러리는 credential 객체를
요구합니다. TLS를 사용하지 않는 로컬 주소에는 `AnonymousCredentials`를 사용할 수
있습니다.

인증 파일 해석이나 HTTPS 연결을 시험하려면 이 저장소의 생성 도구로 파일을 만들고
로컬 발급 서버를 실행합니다. `service-account.json`, `authorized-user.json`,
`wif.json`, `access-token.txt` 사용법은 [클라이언트 인증 파일과
TLS](../client-credentials-and-tls.md)를 참고해 주십시오.

```bash
export REQUESTS_CA_BUNDLE="$PWD/.bqemu-auth/ca.pem"
export SSL_CERT_FILE="$REQUESTS_CA_BUNDLE"
```

위 변수를 설정한 뒤 `localhost`의 `9050` 포트에 HTTPS로 접속합니다. JSON 파일로
token을 교환한다면 인증 파일 문서에 설명된 로컬 발급 서버와 프록시도 실행해야
합니다.

<!-- section: example -->
## 최소 쿼리 예제

```python
job = client.query(
    "SELECT 1 AS answer",
    location="US",
)
rows = list(job.result())
assert rows[0]["answer"] == 1
client.close()
```

먼저 [시작하기](../getting-started.md)의 절차로 에뮬레이터 프로젝트를 만들어 주십시오.

<!-- section: operations -->
## API 호출 순서

정규화된 소비자 사례 ID는 `google-cloud-bigquery-python-3.43.0`입니다. 다음
scenario ID를 실행합니다.

| Scenario ID | 클라이언트 동작과 operation 순서 |
| --- | --- |
| `python-metadata` | 데이터 세트 메서드는 `bigquery.datasets.insert`, `bigquery.datasets.list`, `bigquery.datasets.get`, `bigquery.datasets.patch`, `bigquery.datasets.delete`를 호출합니다. 테이블 메서드는 `bigquery.tables.insert`, `bigquery.tables.list`, `bigquery.tables.get`, `bigquery.tables.patch`, `bigquery.tables.delete` 중 해당 operation을 호출합니다. |
| `python-query` | `client.query()`는 `bigquery.jobs.insert`를 전송하고 `bigquery.jobs.getQueryResults`로 결과 페이지를 읽습니다. `get_job()`은 `bigquery.jobs.get`, `query_and_wait()`는 `bigquery.jobs.query`, `list_jobs()`는 `bigquery.jobs.list`를 호출합니다. |
| `python-tabledata` | `list_rows()`는 `bigquery.tabledata.list`를 호출하고 `pageToken`이 있으면 다음 페이지를 읽습니다. |
| `python-indirect-load` | `load_table_from_uri()`는 `bigquery.jobs.insert`를 전송하고, 로드 작업이 끝날 때까지 `bigquery.jobs.get`으로 상태를 확인합니다. |

매니페스트의 operation ID는 시험한 동작에서 허용하는 호출을 나타냅니다. 상태 확인과
페이지 나누기 여부에 따라 같은 요청을 여러 번 보낼 수 있습니다.

<!-- section: shapes -->
## 요청과 응답 형식

| Operation | 요청 | 클라이언트가 사용하는 응답 |
| --- | --- | --- |
| `bigquery.datasets.insert` | Dataset 리소스를 포함한 `POST /bigquery/v2/projects/{projectId}/datasets` | Dataset 리소스 |
| `bigquery.tables.insert` | Table 리소스를 포함한 `POST /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables` | Table 리소스 |
| `bigquery.jobs.insert` | `configuration.query` 또는 `configuration.load`를 포함한 `POST /bigquery/v2/projects/{projectId}/jobs` | Job 리소스와 `jobReference` |
| `bigquery.jobs.get` | 경로와 쿼리 매개변수에 포함한 job ID와 location | `status.state`와 오류를 포함한 Job 리소스 |
| `bigquery.jobs.getQueryResults` | Job ID, location, 페이지 매개변수 | 스키마, `jobComplete`, 행, 다음 페이지 token |
| `bigquery.tabledata.list` | 테이블 경로와 페이지 매개변수 | BigQuery `f`/`v` 행 인코딩과 다음 페이지 token |

필드별 처리 범위와 지원 수준은 [호환성](../compatibility.md)에 정리되어 있습니다.
클라이언트 버전과 scenario selector는 [소비자 호환성](../consumer-compatibility.md)에서
자동으로 생성합니다.

<!-- section: related -->
## 관련 작업

이 문서에 없는 동작은 [열린 이슈](https://github.com/leeyh0216/go-bemu/issues)와
호환성 문서에서 관리합니다.
