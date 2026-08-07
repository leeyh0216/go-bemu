.DEFAULT_GOAL := help

BINARY ?= bin/go-bemu
BQEMU_CONFIG ?= configs/bqemu.yaml
BQEMU_LOCAL_DATA_DIR ?= $(CURDIR)/data
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
BQEMU_SPARK_VENV ?= $(CURDIR)/.artifacts/spark/venv
BQEMU_SPARK_PYTHON ?= $(BQEMU_SPARK_VENV)/bin/python
GO_TEST_FLAGS ?=
IMAGE ?= go-bemu:dev
PYTHON ?= .venv/bin/python
PYTHON3 ?= python3

.PHONY: help doctor docker-doctor setup python-setup build run format format-check test test-race python-test bq-test spark-contract vet check config-check docker-build docker-up docker-down docker-logs clean

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
	  'make spark-contract Install exact Spark locks and run the released connector contract' \
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
	BQEMU_DATABASE_DSN="$(BQEMU_DATABASE_DSN)" \
	BQEMU_TEMP_DIRECTORY="$(BQEMU_TEMP_DIRECTORY)" \
	CGO_ENABLED=1 go run ./cmd/emulator --config "$(BQEMU_CONFIG)"

format:
	gofmt -w ./cmd ./contract ./docs ./internal

format-check:
	@test -z "$$(gofmt -l ./cmd ./contract ./docs ./internal)" || \
	  (printf '%s\n' 'stage=format operation=gofmt shape=go-source status=failed fix_hint=make-format' >&2; exit 1)

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
	BQEMU_SPARK_TEST_TIMEOUT_SECONDS="$(BQEMU_SPARK_TEST_TIMEOUT_SECONDS)" \
	BQEMU_SPARK_RPC_TIMEOUT_SECONDS="$(BQEMU_SPARK_RPC_TIMEOUT_SECONDS)" \
	BQEMU_ARTIFACT_TIMEOUT_SECONDS="$(BQEMU_ARTIFACT_TIMEOUT_SECONDS)" \
	PYTHONPYCACHEPREFIX="$(CURDIR)/.artifacts/spark/pycache" \
	"$(BQEMU_SPARK_PYTHON)" -m pytest -c tests/spark/pytest.ini tests/spark \
	  --basetemp="$(CURDIR)/.artifacts/spark/pytest" \
	  --junitxml="$(CURDIR)/.artifacts/spark/diagnostics/junit.xml"

vet:
	CGO_ENABLED=1 go vet ./...

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
