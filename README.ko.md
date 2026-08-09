<!-- doc-id: readme -->
<!-- lang: ko -->

[English](README.md) | [한국어](README.ko.md)

# go-bemu

`go-bemu`는 애플리케이션과 프로토콜 테스트를 위한 로컬 BigQuery 호환 서비스입니다.
BigQuery REST와 Storage gRPC 주소를 제공하고 로컬 카탈로그 상태를 보존하며, Parquet
load job에는 외부 GCS 호환 서비스를 사용합니다. 공개 리소스 형태는 [BigQuery REST API
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest)를 따릅니다.

<!-- section: start -->
## 시작하기

```bash
docker compose up --build -d --wait
curl --fail http://localhost:9050/readyz
```

기본 Compose 프로젝트는 BQEMU와 필수 fake GCS 서비스를 함께 시작합니다. 상태는
`bqemu-data` 볼륨에 남습니다. 로컬 테스트 상태가 더 이상 필요 없으면
`docker compose down --volumes`로 삭제합니다.

<!-- section: connect -->
## 접속하기

| 호출 프로세스 위치 | REST 주소 | Storage gRPC 주소 |
| --- | --- | --- |
| Compose를 실행한 호스트 | `http://localhost:9050` | `localhost:9060` |
| 같은 Compose의 다른 서비스 | `http://bqemu:9050` | `bqemu:9060` |
| 호스트에서 BQEMU를 실행하는 개발 컨테이너 | `http://host.docker.internal:9050` | `host.docker.internal:9060` |

호출 프로세스에 맞는 주소를 설정합니다. BQEMU가 같은 컨테이너에서 실행되지 않는다면
컨테이너 안에서 `localhost`를 사용하면 안 됩니다.

<!-- section: docs -->
## 문서

- [시작하기](docs/ko/getting-started.md): 리소스 생성, 첫 쿼리, 필수 fake GCS 사용법
- [설정](docs/ko/configuration.md): 주소 구성, 시작 프로젝트와 데이터세트, 상태 보존,
  TLS, 실행 한도
- [현재 지원 범위](docs/ko/compatibility.md): 지금 사용할 수 있는 기능, 제한 기능,
  미지원 기능을 한 페이지로 정리
- [API 및 RPC 레퍼런스](docs/ko/api-rpc-compatibility.md): 자동 생성한 정확한 메서드와
  endpoint 인벤토리
- [유지보수 문서](docs/ko/maintainers/index.md): 구현, CI, 릴리스, 기여 문서
