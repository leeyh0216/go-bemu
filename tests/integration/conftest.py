"""Dedicated real-process fixtures for external consumer integration tests."""

from __future__ import annotations

import os
from pathlib import Path
import socket
import subprocess
import time
import urllib.request
import uuid

from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery
import pytest


ROOT = Path(__file__).resolve().parents[2]


def _port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


@pytest.fixture(scope="session")
def bqemu_endpoint(tmp_path_factory: pytest.TempPathFactory) -> str:
    work = tmp_path_factory.mktemp("query-parameters")
    prebuilt = os.environ.get("BQEMU_INTEGRATION_BINARY")
    binary = Path(prebuilt) if prebuilt else work / "go-bemu"
    if not prebuilt:
        # CGO-enabled DuckDB linking can take longer than an individual contract
        # test on a cold CI worker. Keep the build bound below the CI job timeout;
        # pytest's per-test timeout remains the guard for a running emulator.
        subprocess.run(["go", "build", "-trimpath", "-o", str(binary), "./cmd/emulator"], cwd=ROOT, check=True, timeout=300)
    if not binary.is_file():
        pytest.fail(f"BQEMU_INTEGRATION_BINARY is not an executable file: {binary}")
    http_port, grpc_port = _port(), _port()
    endpoint = f"http://127.0.0.1:{http_port}"
    environment = os.environ | {
        "BQEMU_HTTP_ADDRESS": f"127.0.0.1:{http_port}",
        "BQEMU_GRPC_ADDRESS": f"127.0.0.1:{grpc_port}",
        "BQEMU_PUBLIC_URL": endpoint,
        "BQEMU_STATE_DSN": str(work / "state.sqlite"),
        "BQEMU_DATABASE_DSN": str(work / "warehouse.duckdb"),
        "BQEMU_UI_ENABLED": "false",
        "BQEMU_LOAD_ENABLED": "true",
    }
    process = subprocess.Popen([str(binary)], cwd=ROOT, env=environment)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if process.poll() is not None:
            pytest.fail("go-bemu exited before readiness")
        try:
            import urllib.request
            with urllib.request.urlopen(endpoint + "/readyz", timeout=1):
                break
        except OSError:
            time.sleep(0.1)
    else:
        pytest.fail("go-bemu readiness timed out")
    previous_emulator_host = os.environ.get("BIGQUERY_EMULATOR_HOST")
    os.environ["BIGQUERY_EMULATOR_HOST"] = endpoint
    try:
        yield endpoint
    finally:
        if previous_emulator_host is None:
            os.environ.pop("BIGQUERY_EMULATOR_HOST", None)
        else:
            os.environ["BIGQUERY_EMULATOR_HOST"] = previous_emulator_host
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


@pytest.fixture()
def project_id() -> str:
    return f"parameter-integration-{uuid.uuid4().hex[:8]}"


@pytest.fixture()
def bq_client(bqemu_endpoint: str, project_id: str) -> bigquery.Client:
    request = urllib.request.Request(
        bqemu_endpoint + "/bqemu/v1/projects",
        data=(f'{{"projectId":"{project_id}"}}').encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=5):
        pass
    return bigquery.Client(project=project_id, credentials=AnonymousCredentials(), client_options=ClientOptions(api_endpoint=bqemu_endpoint))
