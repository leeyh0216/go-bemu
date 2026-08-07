#!/bin/sh
set -eu

# Toolchain sources:
# - Go installation: https://go.dev/doc/install
# - DuckDB Go/CGO: https://duckdb.org/docs/stable/clients/go
# - Docker Engine: https://docs.docker.com/engine/install/

require() {
  tool=$1
  fix_hint=$2
  if ! command -v "$tool" >/dev/null 2>&1; then
    printf 'stage=doctor operation=toolchain-check model_version=dev shape=executable tool=%s status=missing fix_hint=%s\n' "$tool" "$fix_hint" >&2
    return 1
  fi
  printf 'stage=doctor operation=toolchain-check model_version=dev shape=executable tool=%s status=ok path=%s\n' "$tool" "$(command -v "$tool")"
}

failed=0
require go install-go-version-from-go-mod || failed=1
require cc install-a-c-toolchain-for-duckdb-cgo || failed=1
require curl install-curl-for-container-readiness || failed=1

require_docker=${BQEMU_DOCTOR_REQUIRE_DOCKER:-false}
case "$require_docker" in
  true|false) ;;
  *)
    printf 'stage=doctor operation=input-validation model_version=dev shape=boolean field=BQEMU_DOCTOR_REQUIRE_DOCKER status=invalid fix_hint=use-true-or-false\n' >&2
    exit 1
    ;;
esac
if [ "$require_docker" = true ]; then
  require docker install-and-start-docker || failed=1
fi

config_path=${BQEMU_CONFIG:-configs/bqemu.yaml}
if [ ! -r "$config_path" ]; then
  printf 'stage=doctor operation=config-check model_version=config.bqemu.dev/v1alpha1 shape=yaml-file path=%s status=missing fix_hint=set-BQEMU_CONFIG\n' "$config_path" >&2
  failed=1
fi

if [ "$require_docker" = true ] && command -v docker >/dev/null 2>&1; then
  if ! docker info >/dev/null 2>&1; then
    printf '%s\n' 'stage=doctor operation=docker-daemon model_version=dev shape=socket status=unavailable fix_hint=start-docker' >&2
    failed=1
  elif ! docker compose version >/dev/null 2>&1; then
    printf '%s\n' 'stage=doctor operation=compose-plugin model_version=dev shape=command status=missing fix_hint=install-docker-compose-v2' >&2
    failed=1
  fi
fi

if [ "$failed" -ne 0 ]; then
  exit 1
fi
printf 'stage=doctor operation=preflight model_version=%s shape=toolchain status=ok go_version=%s\n' \
  "$(awk '$1 == "go" { print $2 }' go.mod)" "$(go env GOVERSION)"
