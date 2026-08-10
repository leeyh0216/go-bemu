#!/usr/bin/env python3
"""Run the released Flink CDC sink through its unmodified TLS gRPC transport.

The connector has no public endpoint option.  This harness maps its documented
BigQuery Storage hostname to an ephemeral local BQEMU container, gives that
container a certificate for the hostname, and preserves the connector's normal
TLS client path.  No connector source checkout, build, mock service, or custom
protocol parser participates in the execution.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.request


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.integration.framework.fetch_flink_artifacts import (  # noqa: E402
    DEFAULT_LOCK,
    DEFAULT_OUTPUT,
    fetch,
    load_lock,
)


FLINK_HOSTNAME = "bigquerystorage.googleapis.com"
PROJECT, DATASET, TABLE = "flink-cdc-contract", "cdc", "items"


class ContractError(RuntimeError):
    pass


def command(arguments: list[str], stage: str, *, timeout: int = 300) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            arguments,
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ContractError(f"stage={stage} shape=process-error fix_hint=inspect-runtime") from error
    if result.returncode != 0:
        raise ContractError(
            f"stage={stage} shape=exit-{result.returncode} fix_hint=inspect-runtime\n"
            f"stdout={result.stdout[-4000:]}\nstderr={result.stderr[-4000:]}"
        )
    return result


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def request(context: ssl.SSLContext, endpoint: str, path: str, payload: dict[str, object]) -> dict[str, object]:
    body = json.dumps(payload).encode("utf-8")
    http = urllib.request.Request(
        endpoint + path,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(http, context=context, timeout=15) as response:
            decoded = json.loads(response.read().decode("utf-8"))
    except Exception as error:
        raise ContractError(
            f"stage=rest operation={path} shape=request-failed fix_hint=inspect-bqemu-container"
        ) from error
    if not isinstance(decoded, dict):
        raise ContractError(f"stage=rest operation={path} shape=non-object fix_hint=inspect-bqemu-response")
    return decoded


def wait_ready(context: ssl.SSLContext, endpoint: str) -> None:
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(endpoint + "/readyz", context=context, timeout=2) as response:
                if response.status == 200:
                    return
        except OSError:
            time.sleep(0.5)
    raise ContractError("stage=ready shape=timeout fix_hint=inspect-bqemu-container")


def bootstrap(context: ssl.SSLContext, endpoint: str) -> None:
    request(context, endpoint, "/bqemu/v1/projects", {"projectId": PROJECT})
    request(
        context,
        endpoint,
        f"/bigquery/v2/projects/{PROJECT}/datasets",
        {"datasetReference": {"projectId": PROJECT, "datasetId": DATASET}, "location": "US"},
    )
    request(
        context,
        endpoint,
        f"/bigquery/v2/projects/{PROJECT}/datasets/{DATASET}/tables",
        {
            "tableReference": {"projectId": PROJECT, "datasetId": DATASET, "tableId": TABLE},
            "schema": {"fields": [
                {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "value", "type": "STRING", "mode": "REQUIRED"},
                {"name": "sequence", "type": "INTEGER", "mode": "REQUIRED"},
            ]},
            "tableConstraints": {"primaryKey": {"columns": ["id"]}},
        },
    )


def assert_result(context: ssl.SSLContext, endpoint: str) -> None:
    result = request(
        context,
        endpoint,
        f"/bigquery/v2/projects/{PROJECT}/queries",
        {"query": f"SELECT value, sequence FROM `{PROJECT}.{DATASET}.{TABLE}`", "useLegacySql": False},
    )
    rows = result.get("rows")
    if rows != [{"f": [{"v": "new"}, {"v": "17"}]}]:
        raise ContractError(
            "stage=assert operation=flink-cdc-upsert shape=unexpected-result "
            "fix_hint=inspect-cdc-transport-or-sequence-ledger"
        )


def run(arguments: argparse.Namespace) -> int:
    lock = load_lock(arguments.lock)
    runtime = lock["runtime"]
    assert isinstance(runtime, dict)
    image = str(runtime["image"])
    connector = fetch(lock, arguments.artifact_output, arguments.timeout)
    command(["docker", "version", "--format", "{{.Server.Version}}"], "docker-availability", timeout=10)
    with tempfile.TemporaryDirectory(prefix="bqemu-flink-cdc-") as directory:
        work = Path(directory)
        fixture_binary = work / "fixture"
        fixture_dir = work / "credentials"
        image_tag = f"go-bemu-flink-cdc-{os.getpid()}"
        container = f"bqemu-flink-cdc-{os.getpid()}"
        http_port, issuer_port = free_port(), free_port()
        command(["go", "build", "-trimpath", "-o", str(fixture_binary), "./cmd/bqemu-auth-fixture"], "build-fixture")
        command(
            [
                str(fixture_binary), "generate", "--output", str(fixture_dir),
                "--base-url", f"https://localhost:{issuer_port}",
                "--tls-dns-name", FLINK_HOSTNAME,
            ],
            "generate-tls-fixture",
        )
        manifest = json.loads((fixture_dir / "manifest.json").read_text(encoding="utf-8"))
        fixture_dir.chmod(0o755)
        for path in fixture_dir.iterdir():
            if path.is_file():
                path.chmod(0o644)
        context = ssl.create_default_context(cafile=manifest["ca_certificate"])
        endpoint = f"https://localhost:{http_port}"
        try:
            command(["docker", "build", "--tag", image_tag, "."], "build-bqemu-image", timeout=600)
            command(
                [
                    "docker", "run", "--detach", "--rm", "--name", container,
                    "-p", f"127.0.0.1:{http_port}:9050", "-p", "443:9060",
                    "-e", "BQEMU_TLS_CERT_FILE=/auth/server.pem",
                    "-e", "BQEMU_TLS_KEY_FILE=/auth/server-key.pem",
                    "-e", f"BQEMU_PUBLIC_URL={endpoint}",
                    "-e", "BQEMU_DATABASE_DSN=/tmp/bqemu/bqemu.duckdb",
                    "-e", "BQEMU_STATE_DSN=/tmp/bqemu/bqemu-state.sqlite",
                    "-e", "BQEMU_TEMP_DIRECTORY=/tmp/bqemu",
                    "-v", f"{fixture_dir}:/auth:ro",
                    image_tag,
                ],
                "start-bqemu-container",
            )
            wait_ready(context, endpoint)
            bootstrap(context, endpoint)
            java_options = " ".join(
                (
                    f"-Djavax.net.ssl.trustStore=/auth/{Path(manifest['java_truststore']).name}",
                    f"-Djavax.net.ssl.trustStorePassword={manifest['truststore_password']}",
                    "-Djavax.net.ssl.trustStoreType=PKCS12",
                )
            )
            run_java = (
                "mkdir -p /tmp/flink-contract-classes && "
                "javac -cp '/opt/flink/lib/*:/work/connector.jar' -d /tmp/flink-contract-classes /work/FlinkCdcMain.java && "
                "java -cp '/opt/flink/lib/*:/work/connector.jar:/tmp/flink-contract-classes' "
                f"flinkcontract.FlinkCdcMain {PROJECT} {DATASET} {TABLE}"
            )
            result = command(
                [
                    "docker", "run", "--rm", "--add-host", f"{FLINK_HOSTNAME}:host-gateway",
                    "-e", f"JAVA_TOOL_OPTIONS={java_options}",
                    "-v", f"{connector}:/work/connector.jar:ro",
                    "-v", f"{ROOT / 'tests' / 'integration' / 'flink' / 'FlinkCdcMain.java'}:/work/FlinkCdcMain.java:ro",
                    "-v", f"{fixture_dir}:/auth:ro", image, "bash", "-lc", run_java,
                ],
                "run-released-flink-cdc", timeout=arguments.timeout,
            )
            if "BQEMU_FLINK_CDC_OK" not in result.stdout:
                raise ContractError("stage=flink shape=missing-success-marker fix_hint=inspect-flink-output")
            assert_result(context, endpoint)
        finally:
            subprocess.run(["docker", "rm", "--force", container], capture_output=True, text=True, check=False)
            subprocess.run(["docker", "image", "rm", image_tag], capture_output=True, text=True, check=False)
    print("version=flink-bigquery-connector-1.2.0 operation=cdc-default-stream stage=e2e shape=tls-grpc status=verified fix_hint=none")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lock", type=Path, default=DEFAULT_LOCK)
    parser.add_argument("--artifact-output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--timeout", type=int, default=900)
    arguments = parser.parse_args()
    if arguments.timeout <= 0:
        raise ValueError("timeout must be positive")
    return run(arguments)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(
            "version=flink-bigquery-connector-1.2.0 operation=cdc-default-stream "
            f"stage=failed shape={type(error).__name__} status=failed "
            "fix_hint=inspect-flink-cdc-contract",
            file=sys.stderr,
        )
        raise
