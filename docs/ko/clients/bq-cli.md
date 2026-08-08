<!-- doc-id: clients/bq-cli -->
<!-- lang: ko -->

[English](../../en/clients/bq-cli.md) | [한국어](bq-cli.md)

# bq CLI

Google Cloud SDK `566.0.0`에 포함된 `bq` `2.1.31`을 사용합니다. 명령 문법은 공식
[`bq` CLI 레퍼런스](https://cloud.google.com/bigquery/docs/reference/bq-cli-reference)를
참고해 주십시오.

<!-- section: endpoint -->
## REST 접속 주소

모든 명령에 `--api`로 BQEMU REST 주소를 전달합니다. 로컬 테스트 설정은 사용자가
평소 사용하는 Google Cloud 설정과 분리합니다.

```bash
export CLOUDSDK_CONFIG="$(mktemp -d)"
export CLOUDSDK_CORE_DISABLE_PROMPTS=1

bq \
  --api=http://localhost:9050 \
  --project_id=test-project \
  --use_gcloud_config=false \
  --oauth_access_token=local-test-token \
  --format=json \
  ls --projects
```

같은 Compose의 다른 서비스에서는 `http://bqemu:9050`을 사용합니다. BQEMU를
호스트에서 실행하고 클라이언트를 개발 컨테이너에서 실행한다면
`http://host.docker.internal:9050`을 사용합니다.

<!-- section: credentials -->
## 인증 정보와 TLS

BQEMU는 공개 bearer token을 검사하지 않으므로 위 예제의 직접 token만으로 요청을
보낼 수 있습니다. 인증 파일 해석을 시험하려면 이 저장소의 생성 도구로 파일을 만들고
로컬 발급 서버를 실행한 뒤 생성된 JSON 파일 하나를 지정합니다.

```bash
export AUTH_DIR="$PWD/.bqemu-auth"
export CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE="$AUTH_DIR/service-account.json"
export CLOUDSDK_CORE_CUSTOM_CA_CERTS_FILE="$AUTH_DIR/ca.pem"
export REQUESTS_CA_BUNDLE="$AUTH_DIR/ca.pem"
```

그다음 `localhost`의 `9050` 포트에 HTTPS로 접속하고
`--ca_certificates_file="$AUTH_DIR/ca.pem"`을 추가합니다. Authorized user와 WIF
파일에는 [클라이언트 인증 파일과 TLS](../client-credentials-and-tls.md)에 설명된 발급
서버와 제한형 프록시 설정도 필요합니다. 직접 access token은 발급 서버를 사용하지
않습니다.

<!-- section: example -->
## 최소 명령 예제

에뮬레이터 프로젝트를 만들었다고 가정합니다.

```bash
bq --api=http://localhost:9050 --project_id=test-project \
  --use_gcloud_config=false --oauth_access_token=local-test-token \
  mk --dataset --location=US test-project:analytics

bq --api=http://localhost:9050 --project_id=test-project \
  --use_gcloud_config=false --oauth_access_token=local-test-token \
  mk --table test-project:analytics.events id:INTEGER,label:STRING

bq --api=http://localhost:9050 --project_id=test-project \
  --use_gcloud_config=false --oauth_access_token=local-test-token \
  --format=json query --use_legacy_sql=false 'SELECT 1 AS answer'
```

프로젝트 생성 명령은 [시작하기](../getting-started.md)에 있습니다.

<!-- section: operations -->
## API 호출 순서

정규화된 소비자 사례 ID는 `bq-cli-2.1.31`입니다.

| Scenario ID | CLI 동작과 operation 순서 |
| --- | --- |
| `bq-metadata` | CLI가 `bqemu.discovery.get`을 읽습니다. `ls --projects`는 `bigquery.projects.list`를 호출합니다. 데이터 세트 명령은 `bigquery.datasets.insert`, `bigquery.datasets.get`, `bigquery.datasets.patch`, `bigquery.datasets.delete`를 호출하고, 테이블 명령은 `bigquery.tables.insert`, `bigquery.tables.get`, `bigquery.tables.patch`, `bigquery.tables.list`, `bigquery.tables.delete` 중 해당 operation을 호출합니다. |
| `bq-query` | `bq query`는 `bigquery.jobs.insert`를 전송한 뒤 `bigquery.jobs.getQueryResults`로 결과를 읽습니다. Job을 조회하는 명령은 `bigquery.jobs.get`을 호출하고 `bq ls --jobs`는 `bigquery.jobs.list`를 호출합니다. |
| `bq-indirect-load` | `bq load`는 로드 설정을 포함한 `bigquery.jobs.insert`를 전송하고 작업이 끝날 때까지 `bigquery.jobs.get`으로 상태를 확인합니다. |

새 CLI 프로세스는 Discovery 문서를 다시 요청할 수 있습니다. 상태 확인과 페이지
나누기가 필요하면 해당 GET operation을 여러 번 호출합니다.

<!-- section: shapes -->
## 요청과 응답 형식

| CLI 명령 | 공개 요청 | `bq`가 사용하는 응답 |
| --- | --- | --- |
| 첫 명령 | `GET /$discovery/rest` | BigQuery v2 Discovery 문서 |
| `mk --dataset` | `bigquery.datasets.insert`에 Dataset 리소스 전달 | Dataset 리소스 |
| `mk --table` | `bigquery.tables.insert`에 Table 리소스와 스키마 전달 | Table 리소스 |
| `query` | `bigquery.jobs.insert`에 `configuration.query`를 포함한 Job 전달 | Job 참조와 쿼리 결과 페이지 |
| `load` | Parquet 형식과 `configuration.load.sourceUris`를 포함한 Job 전달 | Job 리소스와 이후 상태 응답 |
| `ls --jobs` | `bigquery.jobs.list`에 프로젝트, location, 페이지 매개변수 전달 | Job 요약과 다음 페이지 token |

CLI는 BigQuery JSON 리소스를 명령별 출력으로 변환합니다. 테스트에서 기계가 읽을 수
있는 결과가 필요하다면 `--format=json`을 사용합니다.

필드별 처리 범위와 지원 수준은 [호환성](../compatibility.md)에 정리되어 있습니다.
CLI와 Cloud SDK 버전은 [소비자 호환성](../../../tests/integration/docs/ko/consumer-compatibility.md)에서 자동으로
생성합니다.

<!-- section: related -->
## 관련 작업

이 문서에 없는 동작은 [열린 이슈](https://github.com/leeyh0216/go-bemu/issues)와
호환성 문서에서 관리합니다.
