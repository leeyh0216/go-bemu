.DEFAULT_GOAL := help

BINARY ?= bin/go-bemu
BQEMU_CONFIG ?= configs/bqemu.yaml
BQEMU_LOCAL_DATA_DIR ?= $(CURDIR)/data
BQEMU_DATABASE_DSN ?= $(BQEMU_LOCAL_DATA_DIR)/bqemu.duckdb
BQEMU_TEMP_DIRECTORY ?= $(if $(TMPDIR),$(TMPDIR),/tmp)/bqemu
BQEMU_GO_TEST_TIMEOUT ?= 10m
BQEMU_PYTEST_TIMEOUT_SECONDS ?= 300
BQEMU_DOCKER_START_TIMEOUT_SECONDS ?= 120
GO_TEST_FLAGS ?=
IMAGE ?= go-bemu:dev
PYTHON ?= .venv/bin/python

.PHONY: help doctor docker-doctor setup python-setup build run format format-check test test-race python-test vet check config-check docker-build docker-up docker-down docker-logs clean

help:
	@printf '%s\n' \
	  'make doctor       Validate Go, CGO, Docker, and configuration prerequisites' \
	  'make setup        Run doctor and download Go modules' \
	  'make python-setup Create a Python 3.13 venv from the hash lock' \
	  'make build        Build bin/go-bemu with CGO enabled' \
	  'make run          Run locally with repository data and temp directories' \
	  'make check        Run formatting, bounded race tests, and vet' \
	  'make python-test  Run the official Python client real-process contract' \
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
