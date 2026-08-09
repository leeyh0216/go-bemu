.DEFAULT_GOAL := help

BINARY ?= bin/go-bemu
BQEMU_CONFIG ?= configs/bqemu.yaml
BQEMU_LOCAL_DATA_DIR ?= $(CURDIR)/data
BQEMU_STATE_DSN ?= $(BQEMU_LOCAL_DATA_DIR)/bqemu-state.sqlite
BQEMU_DATABASE_DSN ?= $(BQEMU_LOCAL_DATA_DIR)/bqemu.duckdb
BQEMU_TEMP_DIRECTORY ?= $(if $(TMPDIR),$(TMPDIR),/tmp)/bqemu
BQEMU_GO_TEST_TIMEOUT ?= 10m
BQEMU_PYTEST_TIMEOUT_SECONDS ?= 300
BQEMU_BQCLI_BIN ?= bq
BQEMU_BQCLI_TIMEOUT_SECONDS ?= 300
BQEMU_BQCLI_VERSION ?= 2.1.31
BQEMU_DOCKER_START_TIMEOUT_SECONDS ?= 120
BQEMU_SPARK_TEST_TIMEOUT_SECONDS ?= 600
BQEMU_SPARK_RPC_TIMEOUT_SECONDS ?= 30
BQEMU_ARTIFACT_TIMEOUT_SECONDS ?= 180
BQEMU_DSV2_OVERLAY_BUILD_TIMEOUT_SECONDS ?= 120
BQEMU_SPARK_VENV ?= $(CURDIR)/.artifacts/spark/venv
BQEMU_SPARK_PYTHON ?= $(BQEMU_SPARK_VENV)/bin/python
GO_TEST_FLAGS ?=
GO_SOURCE_DIRS ?= ./cmd ./contract ./docs ./internal
IMAGE ?= go-bemu:dev
PYTHON ?= .venv/bin/python
PYTHON3 ?= python3

.PHONY: help doctor docker-doctor setup python-setup build run format format-check test test-race python-test bq-test dsv2-overlay spark-contract vet check config-check github-actions-policy integration-contract-check ci-static ci-test-all ci-test-core ci-test-adapters ci-test-storage-read ci-test-storage-write ci-test-transport ci-test-composition docker-build docker-up docker-down docker-logs clean

help:
	@printf '%s\n' \
	  'make doctor       Validate Go, CGO, Docker, and configuration prerequisites' \
	  'make setup        Run doctor and download Go modules' \
	  'make python-setup Create a Python 3.13 venv from the hash lock' \
	  'make build        Build bin/go-bemu with CGO enabled' \
	  'make run          Run locally with repository data and temp directories' \
	  'make check        Run formatting, bounded race tests, and vet' \
	  'make python-test  Run the official Python client real-process contract' \
	  'make bq-test      Run the exact-version official bq CLI contract' \
	  'make dsv2-overlay Build the version-locked one-class DSv2 overlay' \
	  'make spark-contract Install exact Spark locks and run the released connector contract' \
	  'make ci-static     Validate formatting, vet, configuration, and GitHub Actions policy' \
	  'make ci-test-*     Run the same functional Go test groups used by CI' \
	  'make docker-build Build the standalone non-root image' \
	  'make docker-up    Start the read-only Compose service and wait for readiness' \
	  'make docker-down  Stop the Compose service and remove transient resources'

doctor:
	BQEMU_CONFIG="$(BQEMU_CONFIG)" BQEMU_DOCTOR_REQUIRE_DOCKER=false ./scripts/doctor.sh

docker-doctor:
	BQEMU_CONFIG="$(BQEMU_CONFIG)" BQEMU_DOCTOR_REQUIRE_DOCKER=true ./scripts/doctor.sh

setup: doctor
	mkdir -p "$(BQEMU_LOCAL_DATA_DIR)" "$(BQEMU_TEMP_DIRECTORY)"
	go mod download

python-setup:
	uv venv --python 3.13 .venv
	uv pip sync --python "$(PYTHON)" --require-hashes tests/python/requirements.lock

build:
	mkdir -p $(dir $(BINARY))
	CGO_ENABLED=1 go build -trimpath -o "$(BINARY)" ./cmd/emulator

run:
	mkdir -p "$(BQEMU_LOCAL_DATA_DIR)" "$(BQEMU_TEMP_DIRECTORY)"
	BQEMU_STATE_DSN="$(BQEMU_STATE_DSN)" \
	BQEMU_DATABASE_DSN="$(BQEMU_DATABASE_DSN)" \
	BQEMU_TEMP_DIRECTORY="$(BQEMU_TEMP_DIRECTORY)" \
	CGO_ENABLED=1 go run ./cmd/emulator --config "$(BQEMU_CONFIG)"

format:
	gofmt -w $(GO_SOURCE_DIRS)

format-check:
	@unformatted="$$(gofmt -l $(GO_SOURCE_DIRS))"; \
	  if test -n "$$unformatted"; then \
	    files="$$(printf '%s\n' "$$unformatted" | paste -sd, -)"; \
	    printf '%s\n' "stage=format operation=gofmt shape=go-source status=failed files=$$files fix_hint=make-format" >&2; \
	    exit 1; \
	  fi

test:
	CGO_ENABLED=1 go test -timeout "$(BQEMU_GO_TEST_TIMEOUT)" $(GO_TEST_FLAGS) ./...

test-race:
	CGO_ENABLED=1 go test -race -timeout "$(BQEMU_GO_TEST_TIMEOUT)" $(GO_TEST_FLAGS) ./...

python-test:
	BQEMU_PYTEST_TIMEOUT_SECONDS="$(BQEMU_PYTEST_TIMEOUT_SECONDS)" \
	"$(PYTHON)" -m pytest -c tests/python/pytest.ini tests/python

bq-test:
	@command -v "$(BQEMU_BQCLI_BIN)" >/dev/null 2>&1 || \
	  (printf '%s\n' 'stage=setup operation=find-bq shape=missing-binary fix_hint=install-Google-Cloud-SDK-566.0.0' >&2; exit 1)
	BQEMU_BQCLI_VERSION="$(BQEMU_BQCLI_VERSION)" \
	BQEMU_BQCLI_TIMEOUT_SECONDS="$(BQEMU_BQCLI_TIMEOUT_SECONDS)" \
	BQEMU_BQCLI_ARTIFACT_DIR="$(CURDIR)/.artifacts/bqcli" \
	"$(PYTHON3)" tests/bqcli/run_contract.py

dsv2-overlay:
	BQEMU_ARTIFACT_TIMEOUT_SECONDS="$(BQEMU_ARTIFACT_TIMEOUT_SECONDS)" \
	"$(PYTHON3)" scripts/fetch_spark_artifacts.py
	BQEMU_DSV2_OVERLAY_BUILD_TIMEOUT_SECONDS="$(BQEMU_DSV2_OVERLAY_BUILD_TIMEOUT_SECONDS)" \
	PYTHONDONTWRITEBYTECODE=1 \
	"$(PYTHON3)" tools/dsv2-overlay/build.py

spark-contract:
	mkdir -p "$(CURDIR)/.artifacts/spark/diagnostics"
	@if test -x "$(BQEMU_SPARK_PYTHON)"; then \
	  "$(BQEMU_SPARK_PYTHON)" -c 'import sys; assert sys.version_info[:2] == (3, 11), "Spark contract requires Python 3.11"'; \
	else \
	  uv venv --python 3.11 "$(BQEMU_SPARK_VENV)"; \
	fi
	uv pip sync --python "$(BQEMU_SPARK_PYTHON)" --require-hashes tests/spark/requirements.lock
	BQEMU_ARTIFACT_TIMEOUT_SECONDS="$(BQEMU_ARTIFACT_TIMEOUT_SECONDS)" \
	"$(BQEMU_SPARK_PYTHON)" scripts/fetch_spark_artifacts.py
	BQEMU_DSV2_OVERLAY_BUILD_TIMEOUT_SECONDS="$(BQEMU_DSV2_OVERLAY_BUILD_TIMEOUT_SECONDS)" \
	PYTHONDONTWRITEBYTECODE=1 \
	"$(BQEMU_SPARK_PYTHON)" tools/dsv2-overlay/build.py
	BQEMU_SPARK_TEST_TIMEOUT_SECONDS="$(BQEMU_SPARK_TEST_TIMEOUT_SECONDS)" \
	BQEMU_SPARK_RPC_TIMEOUT_SECONDS="$(BQEMU_SPARK_RPC_TIMEOUT_SECONDS)" \
	BQEMU_ARTIFACT_TIMEOUT_SECONDS="$(BQEMU_ARTIFACT_TIMEOUT_SECONDS)" \
	BQEMU_DSV2_OVERLAY_BUILD_TIMEOUT_SECONDS="$(BQEMU_DSV2_OVERLAY_BUILD_TIMEOUT_SECONDS)" \
	PYTHONPYCACHEPREFIX="$(CURDIR)/.artifacts/spark/pycache" \
	"$(BQEMU_SPARK_PYTHON)" -m pytest -c tests/spark/pytest.ini tests/spark \
	  --basetemp="$(CURDIR)/.artifacts/spark/pytest" \
	  --junitxml="$(CURDIR)/.artifacts/spark/diagnostics/junit.xml"

vet:
	CGO_ENABLED=1 go vet ./...

github-actions-policy:
	CGO_ENABLED=1 go test ./internal/cipolicy

integration-contract-check:
	CGO_ENABLED=1 go run ./cmd/integration-contract
	git diff --exit-code -- contract/generated docs/en/generated docs/ko/generated

ci-static: github-actions-policy format-check vet config-check integration-contract-check

ci-test-all:
	CGO_ENABLED=1 go test -timeout "$(BQEMU_GO_TEST_TIMEOUT)" $(GO_TEST_FLAGS) ./...

ci-test-core:
	CGO_ENABLED=1 go test -race -timeout "$(BQEMU_GO_TEST_TIMEOUT)" $(GO_TEST_FLAGS) \
		./internal/admin \
		./internal/application \
		./internal/auth/... \
		./internal/domain \
		./internal/loadjob/... \
		./internal/observability \
		./internal/ports \
		./internal/tabledata

ci-test-adapters:
	CGO_ENABLED=1 go test -race -timeout "$(BQEMU_GO_TEST_TIMEOUT)" $(GO_TEST_FLAGS) ./internal/adapters/...

ci-test-storage-read:
	CGO_ENABLED=1 go test -race -timeout "$(BQEMU_GO_TEST_TIMEOUT)" $(GO_TEST_FLAGS) ./internal/storageread/...

ci-test-storage-write:
	CGO_ENABLED=1 go test -race -timeout "$(BQEMU_GO_TEST_TIMEOUT)" $(GO_TEST_FLAGS) ./internal/storagewrite/...

ci-test-transport:
	CGO_ENABLED=1 go test -race -timeout "$(BQEMU_GO_TEST_TIMEOUT)" $(GO_TEST_FLAGS) ./internal/transport/...

ci-test-composition:
	CGO_ENABLED=1 go test -race -timeout "$(BQEMU_GO_TEST_TIMEOUT)" $(GO_TEST_FLAGS) \
		./cmd/emulator \
		./contract \
		./docs \
		./internal/config

config-check:
	CGO_ENABLED=1 go run ./cmd/emulator --config "$(BQEMU_CONFIG)" --print-effective-config >/dev/null

check: format-check test-race vet config-check

docker-build: docker-doctor
	docker build --tag "$(IMAGE)" .

docker-up: docker-doctor
	docker compose up -d --build --wait --wait-timeout "$(BQEMU_DOCKER_START_TIMEOUT_SECONDS)" bqemu

docker-down:
	docker compose down --remove-orphans

docker-logs:
	docker compose logs --no-color bqemu

clean:
	rm -rf bin coverage.out
