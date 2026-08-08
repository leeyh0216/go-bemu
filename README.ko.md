<!-- doc-id: readme -->
<!-- lang: ko -->

[English](README.md) | [한국어](README.ko.md)

# go-bemu

`go-bemu`는 애플리케이션과 커넥터를 시험할 때 사용하는 로컬 BigQuery 호환
서비스입니다. BQEMU를 테스트 프로세스와 함께 실행하고 에뮬레이터 프로젝트를 만든
뒤, 클라이언트가 로컬 REST 또는 Storage gRPC 주소를 사용하도록 설정합니다.

API와 RPC의 정확한 지원 범위는 [호환성](docs/ko/compatibility.md)에 정리되어
있습니다. BigQuery 요청과 응답 리소스는 공개 [BigQuery API
레퍼런스](https://cloud.google.com/bigquery/docs/reference/rest)를 기준으로 합니다.

<!-- section: quick-start -->
## Docker Compose로 실행하기

```bash
docker compose up --build -d --wait

curl --fail -X POST http://localhost:9050/bqemu/v1/projects \
  -H 'Content-Type: application/json' \
  -d '{"projectId":"test-project"}'
```

기본 브랜치에서 게시한 이미지를 사용하려면 로컬 빌드 대신 다음 명령을 실행합니다.

```bash
export BQEMU_IMAGE=ghcr.io/leeyh0216/go-bemu:edge
docker compose pull bqemu
docker compose up --no-build -d --wait
```

Compose 프로젝트는 `bqemu-data` 볼륨에 데이터를 보관합니다. 테스트 데이터를 더
이상 사용하지 않는다면 `docker compose down --volumes`로 볼륨까지 삭제합니다.

<!-- section: endpoints -->
## 접속 주소

| 서비스 | 기본 주소 |
| --- | --- |
| BigQuery REST와 상태 확인 | `http://localhost:9050` |
| BigQuery Storage gRPC | `localhost:9060` |

컨테이너에서 실행하는 클라이언트는 Compose 실행 위치에 따라 BQEMU 서비스 이름이나
`host.docker.internal`을 사용합니다. TLS를 적용해도 포트는 같으며 REST에는 HTTPS를
사용합니다.

<!-- section: next-steps -->
## 다음 문서

- [시작하기](docs/ko/getting-started.md): 데이터 세트와 테이블을 만들고 첫 쿼리를
  실행하는 방법, TLS 설정, 개발 컨테이너 연결 방법을 설명합니다.
- [검토된 통합 예제](tests/integration/docs/ko/index.md): CI에서 검증하는 버전 고정
  프로세스와 실행 환경 설정입니다.
- [클라이언트 인증 파일과 TLS](docs/ko/client-credentials-and-tls.md)
- [호환성](docs/ko/compatibility.md): operation ID별 API와 RPC 지원 범위입니다.
- [문서 색인](docs/ko/index.md)
