<!-- doc-id: clients/bq-cli -->
<!-- lang: ko -->

[English](../../en/clients/bq-cli.md) | [한국어](bq-cli.md)

# bq CLI

CLI가 BQEMU REST endpoint를 사용하도록 설정합니다. 명령 문법은 공식 [`bq` CLI
레퍼런스](https://cloud.google.com/bigquery/docs/reference/bq-cli-reference)를 따릅니다.

<!-- section: endpoint -->
## 필수 override

각 명령에 다음 설정을 전달하거나 로컬 shell function으로 감쌉니다.

```bash
bq \
  --api=http://localhost:9050 \
  --project_id=test-project \
  --use_gcloud_config=false \
  --oauth_access_token=local-test-token \
  <command>
```

| Override | 필요한 이유 |
| --- | --- |
| `--api` | BigQuery REST 요청을 BQEMU로 보냅니다. |
| `--project_id` | 명령이 프로젝트를 생략했을 때 사용하는 에뮬레이터 프로젝트를 고릅니다. |
| `--use_gcloud_config=false` | 로컬 Cloud SDK profile이 프로젝트나 endpoint 선택을 바꾸지 않게 합니다. |
| `--oauth_access_token` | CLI의 로컬 credential 요구를 만족시킵니다. BQEMU는 공개 요청을 authorize하지 않습니다. |

같은 Compose 서비스에서는 `http://bqemu:9050`, 호스트 Compose에 접속하는 개발
컨테이너에서는 `http://host.docker.internal:9050`을 사용합니다. 다른 컨테이너에서는
`localhost`를 사용하면 안 됩니다.

<!-- section: tls -->
## TLS

HTTPS endpoint URL은 `https://...:9050` 로 지정하고 생성한 CA를 추가합니다.

```bash
export AUTH_DIR="$PWD/.bqemu-auth"
export CLOUDSDK_CORE_CUSTOM_CA_CERTS_FILE="$AUTH_DIR/ca.pem"

bq --api=https://localhost:9050 --ca_certificates_file="$AUTH_DIR/ca.pem" \
  --project_id=test-project --use_gcloud_config=false \
  --oauth_access_token=local-test-token <command>
```

로컬 TLS 파일은 [로컬 인증 파일과 TLS](../../../../../docs/ko/client-credentials-and-tls.md)에서 생성합니다.

<!-- section: commands -->
## 자주 쓰는 명령

먼저 [시작하기](../../../../../docs/ko/getting-started.md)로 에뮬레이터 프로젝트를 만듭니다.

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

로컬 테스트가 안정적인 기계 판독 응답을 확인해야 하면 `--format=json`을 사용합니다.

<!-- section: load -->
## Parquet Load

`load`는 `gs://` Parquet 입력을 받습니다. 업로더와 BQEMU가 같은 fake GCS 서비스에
도달하는 주소를 각각 설정합니다. [시작하기](../../../../../docs/ko/getting-started.md#fake-gcs를-통한-parquet-load)를
참고하세요. 다른 load 형식과 로컬 경로는 지원하지 않습니다.

<!-- section: validation -->
## 검증

이 설정은 Google Cloud SDK `566.0.0`에 포함된 `bq` `2.1.31`에서 검증했습니다. 정확한
operation trace, scenario ID, artifact provenance는 CLI 설정 요구가 아니라 테스트
프레임워크 증거입니다.
