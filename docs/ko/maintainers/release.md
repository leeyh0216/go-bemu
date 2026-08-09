<!-- doc-id: maintainers/release -->
<!-- lang: ko -->

[English](../../en/maintainers/release.md) | [한국어](release.md)

# 안정 릴리스

<!-- section: version -->

`release/version.json`은 제품 버전의 유일한 입력입니다. 접두사 없는 안정 `MAJOR.MINOR.PATCH`만 허용하며 prerelease, build, snapshot, 역행 버전은 거부합니다.

릴리스 workflow는 [GitHub 릴리스
문서](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository)와 에뮬레이터의 [BigQuery REST 출처](https://cloud.google.com/bigquery/docs/reference/rest)를 따릅니다.

```sh
make version-bump BUMP=patch
make version-set VERSION=1.2.3
```

<!-- section: main-release -->

동일한 작업은 **prepare-release-pr** workflow에서도 할 수 있습니다. PR이 `main`에 병합되면 CI가 descriptor와 필수 job을 검증하고 같은 merge SHA에 `v<version>` 태그를 만든 뒤 GHCR `<version>`, `<major>.<minor>`, `latest`, `sha-<sha>` 태그(0.x 밖에서는 `<major>`도)를 게시하고 GitHub Release를 만듭니다. feature, develop, PR 이벤트는 게시하지 않습니다.
