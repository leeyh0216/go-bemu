"""Real-endpoint fixtures for google-cloud-bigquery 3.43.0.

Official client source used for this contract:
https://pypi.org/project/google-cloud-bigquery/3.43.0/
"""

from __future__ import annotations

from dataclasses import dataclass, replace
import json
import os
from pathlib import Path
import socket
import subprocess
import time
from typing import Iterator
import urllib.error
import urllib.request
import uuid

from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery
import pytest

from tests.integration.load.runtime import (
    EmulatorRuntime,
    FakeGCSRuntime,
    Settings,
    ensure_fake_gcs_image,
    read_locked_json,
    run_process,
    validate_infrastructure_lock,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
OPERATION_MANIFEST_PATH = REPOSITORY_ROOT / "contract" / "operations.normalized.json"


@dataclass(frozen=True)
class MediaRuntime:
    endpoint: str
    gcs_endpoint: str
    project_id: str
    dataset_id: str
    table_id: str
    location: str
    client: bigquery.Client
    timeout: float


def _test_timeout() -> float:
    raw = os.getenv("BQEMU_PYTEST_TIMEOUT_SECONDS", "90")
    try:
        value = float(raw)
    except ValueError as error:
        raise pytest.UsageError(
            "BQEMU_PYTEST_TIMEOUT_SECONDS must be a positive number of seconds"
        ) from error
    if value <= 0:
        raise pytest.UsageError(
            "BQEMU_PYTEST_TIMEOUT_SECONDS must be a positive number of seconds"
        )
    return value


def pytest_configure(config: pytest.Config) -> None:
    environment_override = "BQEMU_PYTEST_TIMEOUT_SECONDS" in os.environ
    if hasattr(config.option, "timeout") and (
        environment_override or config.option.timeout in (None, 0)
    ):
        config.option.timeout = _test_timeout()


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    with OPERATION_MANIFEST_PATH.open("r", encoding="utf-8") as stream:
        manifest = json.load(stream)
    operation_ids = {operation["id"] for operation in manifest["operations"]}
    for item in items:
        for marker in item.iter_markers("operation"):
            if (
                len(marker.args) != 1
                or marker.kwargs
                or not isinstance(marker.args[0], str)
            ):
                raise pytest.UsageError(
                    f"{item.nodeid} operation marker must contain one operation ID"
                )
            if marker.args[0] not in operation_ids:
                raise pytest.UsageError(
                    f"{item.nodeid} references unknown operation {marker.args[0]}"
                )


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _json_request(url: str, method: str, timeout: float, payload: dict | None = None) -> dict | None:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=body, method=method)
    if body is not None:
        request.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        encoded = response.read()
    return None if not encoded else json.loads(encoded)


def _phase_json_request(
    endpoint: str,
    method: str,
    path: str,
    timeout: float,
    payload: dict | None = None,
    *,
    phase: str,
) -> dict | None:
    if phase not in {"setup", "assertion"}:
        raise ValueError("integration request phase is invalid")
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(endpoint + path, data=body, method=method)
    if body is not None:
        request.add_header("Content-Type", "application/json")
    request.add_header("X-BQEMU-Integration-Phase", phase)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        encoded = response.read()
    return None if not encoded else json.loads(encoded)


def _create_media_dataset_and_table(
    endpoint: str,
    project_id: str,
    dataset_id: str,
    table_id: str,
    location: str,
    timeout: float,
) -> None:
    try:
        _phase_json_request(
            endpoint,
            "POST",
            "/bqemu/v1/projects",
            timeout,
            {"projectId": project_id, "friendlyName": "Dataframe media contract"},
            phase="setup",
        )
    except urllib.error.HTTPError as error:
        if error.code != 409:
            raise
    _phase_json_request(
        endpoint,
        "POST",
        f"/bigquery/v2/projects/{project_id}/datasets",
        timeout,
        {
            "datasetReference": {
                "projectId": project_id,
                "datasetId": dataset_id,
            },
            "location": location,
        },
        phase="setup",
    )
    _phase_json_request(
        endpoint,
        "POST",
        f"/bigquery/v2/projects/{project_id}/datasets/{dataset_id}/tables",
        timeout,
        {
            "tableReference": {
                "projectId": project_id,
                "datasetId": dataset_id,
                "tableId": table_id,
            },
            "schema": {
                "fields": [
                    {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
                    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "active", "type": "BOOLEAN", "mode": "NULLABLE"},
                    {"name": "payload", "type": "STRING", "mode": "NULLABLE"},
                ]
            },
        },
        phase="setup",
    )


@pytest.fixture(scope="session")
def test_timeout() -> float:
    return _test_timeout()


@pytest.fixture(scope="session")
def bqemu_endpoint(tmp_path_factory: pytest.TempPathFactory, test_timeout: float) -> Iterator[str]:
    configured = os.getenv("BQEMU_TEST_ENDPOINT")
    if configured:
        yield configured.rstrip("/")
        return

    work = tmp_path_factory.mktemp("bqemu-server")
    binary = work / "go-bemu"
    subprocess.run(
        ["go", "build", "-trimpath", "-o", str(binary), "./cmd/emulator"],
        cwd=REPOSITORY_ROOT,
        check=True,
        timeout=test_timeout,
    )
    http_port, grpc_port = _free_port(), _free_port()
    endpoint = f"http://127.0.0.1:{http_port}"
    environment = os.environ.copy()
    environment.update(
        {
            "BQEMU_HTTP_ADDRESS": f"127.0.0.1:{http_port}",
            "BQEMU_GRPC_ADDRESS": f"127.0.0.1:{grpc_port}",
            "BQEMU_PUBLIC_URL": endpoint,
            "BQEMU_DATABASE_DSN": str(work / "contracts.duckdb"),
            "BQEMU_UI_ENABLED": "false",
        }
    )
    log_path = work / "server.log"
    with log_path.open("wb") as log:
        process = subprocess.Popen(
            [str(binary)], cwd=REPOSITORY_ROOT, env=environment, stdout=log, stderr=subprocess.STDOUT
        )
    deadline = time.monotonic() + test_timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            break
        try:
            _json_request(endpoint + "/readyz", "GET", min(1.0, test_timeout))
            last_error = None
            break
        except (OSError, urllib.error.URLError) as error:
            last_error = error
            time.sleep(0.05)
    else:
        last_error = TimeoutError(f"readiness exceeded {test_timeout}s")
    if last_error is not None or process.poll() is not None:
        process.terminate()
        try:
            process.wait(timeout=min(5.0, test_timeout))
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=min(5.0, test_timeout))
        diagnostics = log_path.read_text(errors="replace")[-8000:]
        pytest.fail(f"BQEMU failed readiness: {last_error}\n{diagnostics}")

    try:
        yield endpoint
    finally:
        process.terminate()
        try:
            process.wait(timeout=min(10.0, test_timeout))
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=min(5.0, test_timeout))


@pytest.fixture(scope="session")
def project_id() -> str:
    configured = os.getenv("BQEMU_TEST_PROJECT")
    return configured or f"bqemu-contract-{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="session")
def bq_client(bqemu_endpoint: str, project_id: str, test_timeout: float) -> Iterator[bigquery.Client]:
    try:
        _json_request(
            bqemu_endpoint + "/bqemu/v1/projects",
            "POST",
            test_timeout,
            {"projectId": project_id, "friendlyName": "Python contract"},
        )
    except urllib.error.HTTPError as error:
        if error.code != 409:
            raise
    client = bigquery.Client(
        project=project_id,
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint=bqemu_endpoint),
    )
    try:
        yield client
    finally:
        client.close()
        try:
            _json_request(bqemu_endpoint + f"/bqemu/v1/projects/{project_id}", "DELETE", test_timeout)
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise


@pytest.fixture(scope="session")
def media_runtime(
    tmp_path_factory: pytest.TempPathFactory, test_timeout: float
) -> Iterator[MediaRuntime]:
    """Run the dataframe contract against a controlled BQEMU and fake-GCS pair."""
    if os.getenv("BQEMU_TEST_ENDPOINT"):
        raise pytest.UsageError(
            "the dataframe media contract requires its controlled fake-GCS runtime"
        )

    work = tmp_path_factory.mktemp("bqemu-media")
    base_settings = Settings.from_environment()
    settings = replace(
        base_settings,
        artifact_root=work,
        project="media-e2e-" + uuid.uuid4().hex[:12],
    )
    infrastructure = read_locked_json(
        settings.infrastructure_lock, "bqemu-load-infrastructure/v1"
    )
    image = ensure_fake_gcs_image(
        settings, validate_infrastructure_lock(settings, infrastructure)
    )
    seed = work / "fake-gcs-seed"
    seed.mkdir(mode=0o755)
    fake_gcs = FakeGCSRuntime(
        settings, image, seed, work / "fake-gcs.safe.json"
    )
    binary = work / "go-bemu"
    emulator: EmulatorRuntime | None = None
    client: bigquery.Client | None = None
    dataset_id = "media_" + uuid.uuid4().hex[:12]
    table_id = "events"
    try:
        run_process(
            [settings.go_binary, "build", "-trimpath", "-o", str(binary), "./cmd/emulator"],
            operation="build-emulator",
            service="bqemu",
            model_version="current-source",
            timeout=settings.build_timeout,
        )
        gcs_endpoint = fake_gcs.start()
        emulator_work = work / "emulator"
        emulator_work.mkdir(mode=0o700)
        emulator = EmulatorRuntime(
            settings,
            binary,
            emulator_work,
            gcs_endpoint,
            emulator_work / "server.log",
            work / "emulator.safe.json",
        )
        endpoint = emulator.start()
        _create_media_dataset_and_table(
            endpoint,
            settings.project,
            dataset_id,
            table_id,
            settings.location,
            test_timeout,
        )
        client = bigquery.Client(
            project=settings.project,
            credentials=AnonymousCredentials(),
            client_options=ClientOptions(api_endpoint=endpoint),
        )
        yield MediaRuntime(
            endpoint=endpoint,
            gcs_endpoint=gcs_endpoint,
            project_id=settings.project,
            dataset_id=dataset_id,
            table_id=table_id,
            location=settings.location,
            client=client,
            timeout=test_timeout,
        )
    finally:
        if client is not None:
            client.close()
        if emulator is not None and emulator.endpoint:
            try:
                _phase_json_request(
                    emulator.endpoint,
                    "DELETE",
                    f"/bigquery/v2/projects/{settings.project}/datasets/{dataset_id}?deleteContents=true",
                    test_timeout,
                    phase="setup",
                )
            except urllib.error.HTTPError:
                pass
            try:
                _phase_json_request(
                    emulator.endpoint,
                    "DELETE",
                    f"/bqemu/v1/projects/{settings.project}",
                    test_timeout,
                    phase="setup",
                )
            except urllib.error.HTTPError:
                pass
            emulator.stop()
        fake_gcs.stop()
