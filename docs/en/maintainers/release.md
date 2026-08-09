<!-- doc-id: maintainers/release -->
<!-- lang: en -->

[English](release.md) | [한국어](../../ko/maintainers/release.md)

# Stable releases

<!-- section: version -->

`release/version.json` is the only product-version input. It must contain a stable, prefix-free `MAJOR.MINOR.PATCH` value; prerelease, build, snapshot, and regressing versions are rejected.

The release workflow follows [GitHub's release
documentation](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository) and the emulator's [BigQuery REST source](https://cloud.google.com/bigquery/docs/reference/rest).

```sh
make version-bump BUMP=patch
make version-set VERSION=1.2.3
```

<!-- section: main-release -->

The same action is available through **prepare-release-pr**. After a PR reaches `main`, CI verifies the descriptor and required jobs, tags that exact merge SHA as `v<version>`, publishes GHCR `<version>`, `<major>.<minor>`, `latest`, `sha-<sha>` (and `<major>` outside `0.x`), then creates the GitHub Release. Feature, develop, and PR events never publish.
