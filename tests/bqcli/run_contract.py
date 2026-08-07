#!/usr/bin/env python3
"""Real-process contract for the official bq CLI.

Protocol and CLI sources:
  https://cloud.google.com/bigquery/docs/bq-command-line-tool
  https://cloud.google.com/bigquery/docs/reference/bq-cli-reference
  https://cloud.google.com/bigquery/docs/reference/rest/v2

The runner logs operation, duration, byte length, and digest. It deliberately
does not log the access token, raw request payloads, query text, or command
output. Every subprocess and readiness request has an explicit timeout.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
from typing import Any
import urllib.error
import urllib.request
import uuid


ROOT = Path(__file__).resolve().parents[2]
EXPECTED_VERSION = os.getenv("BQEMU_BQCLI_VERSION", "2.1.31")


class ContractError(RuntimeError):
    """A payload-safe failure with a stable operation and fix hint."""


def positive_seconds(name: str, default: str) -> float:
    raw = os.getenv(name, default)
    try:
        value = float(raw)
    except ValueError as error:
        raise ContractError(
            f"stage=config operation=parse_timeout shape={name} "
            "fix_hint=set-a-positive-number-of-seconds"
        ) from error
    if value <= 0:
        raise ContractError(
            f"stage=config operation=parse_timeout shape={name} "
            "fix_hint=set-a-positive-number-of-seconds"
        )
    return value


TIMEOUT = 120.0


def digest(value: str | bytes) -> str:
    encoded = value.encode("utf-8", errors="replace") if isinstance(value, str) else value
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def event(**fields: Any) -> None:
    print(json.dumps(fields, sort_keys=True, separators=(",", ":")), flush=True)


def run_process(
    args: list[str],
    operation: str,
    *,
    expected_codes: tuple[int, ...] = (0,),
    environment: dict[str, str] | None = None,
    cwd: Path = ROOT,
) -> subprocess.CompletedProcess[str]:
    started = time.monotonic()
    try:
        result = subprocess.run(
            args,
            cwd=cwd,
            env=environment,
            capture_output=True,
            text=True,
            timeout=TIMEOUT,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise ContractError(
            f"stage=process operation={operation} shape=subprocess-timeout "
            f"consumer_version={EXPECTED_VERSION} fix_hint=increase-BQEMU_BQCLI_TIMEOUT_SECONDS"
        ) from error
    except OSError as error:
        raise ContractError(
            f"stage=process operation={operation} shape=process-unavailable "
            f"consumer_version={EXPECTED_VERSION} fix_hint=install-pinned-bq-cli"
        ) from error
    event(
        boundary="process",
        consumer="bq-cli",
        consumer_version=EXPECTED_VERSION,
        duration_ms=round((time.monotonic() - started) * 1000),
        operation=operation,
        return_code=result.returncode,
        stderr_bytes=len(result.stderr.encode()),
        stderr_digest=digest(result.stderr),
        stdout_bytes=len(result.stdout.encode()),
        stdout_digest=digest(result.stdout),
    )
    if result.returncode not in expected_codes:
        raise ContractError(
            f"stage=process operation={operation} shape=exit-{result.returncode} "
            f"consumer_version={EXPECTED_VERSION} stdout_fingerprint={digest(result.stdout)} "
            f"stderr_fingerprint={digest(result.stderr)} fix_hint=inspect-payload-safe-server-log"
        )
    return result


def decode_json(result: subprocess.CompletedProcess[str], operation: str) -> Any:
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise ContractError(
            f"stage=decode operation={operation} shape=invalid-json "
            f"consumer_version={EXPECTED_VERSION} stdout_fingerprint={digest(result.stdout)} "
            "fix_hint=compare-bq-wire-profile"
        ) from error


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def request_json(endpoint: str, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
    encoded = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(endpoint + path, data=encoded, method=method)
    if encoded is not None:
        request.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(request, timeout=min(TIMEOUT, 5.0)) as response:
        body = response.read()
    return None if not body else json.loads(body)


class Runtime(AbstractContextManager[str]):
    def __init__(self) -> None:
        self.endpoint = os.getenv("BQEMU_TEST_ENDPOINT", "").rstrip("/")
        self._temporary: tempfile.TemporaryDirectory[str] | None = None
        self._process: subprocess.Popen[bytes] | None = None
        self._log = None

    def __enter__(self) -> str:
        if self.endpoint:
            return self.endpoint
        self._temporary = tempfile.TemporaryDirectory(prefix="bqemu-bqcli-")
        work = Path(self._temporary.name)
        binary = work / "go-bemu"
        run_process(
            ["go", "build", "-trimpath", "-o", str(binary), "./cmd/emulator"],
            "build_emulator",
        )
        http_port, grpc_port = free_port(), free_port()
        self.endpoint = f"http://127.0.0.1:{http_port}"
        artifact_directory = Path(os.getenv("BQEMU_BQCLI_ARTIFACT_DIR", work / "artifacts"))
        artifact_directory.mkdir(parents=True, exist_ok=True)
        self._log = (artifact_directory / "server.log").open("wb")
        environment = os.environ.copy()
        environment.update(
            {
                "BQEMU_HTTP_ADDRESS": f"127.0.0.1:{http_port}",
                "BQEMU_GRPC_ADDRESS": f"127.0.0.1:{grpc_port}",
                "BQEMU_PUBLIC_URL": self.endpoint,
                "BQEMU_DATABASE_DSN": str(work / "contract.duckdb"),
                "BQEMU_TEMP_DIRECTORY": str(work / "tmp"),
                "BQEMU_UI_ENABLED": "false",
            }
        )
        self._process = subprocess.Popen(
            [str(binary)], cwd=ROOT, env=environment, stdout=self._log, stderr=subprocess.STDOUT
        )
        deadline = time.monotonic() + TIMEOUT
        while time.monotonic() < deadline:
            if self._process.poll() is not None:
                break
            try:
                request_json(self.endpoint, "GET", "/readyz")
                return self.endpoint
            except (OSError, urllib.error.URLError):
                time.sleep(0.05)
        self._stop()
        raise ContractError(
            "stage=readiness operation=start_emulator shape=http-readyz "
            "fix_hint=inspect-payload-safe-server-log"
        )

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self._stop()

    def _stop(self) -> None:
        if self._process is not None and self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=min(TIMEOUT, 10.0))
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=min(TIMEOUT, 5.0))
        if self._log is not None:
            self._log.close()
        if self._temporary is not None:
            self._temporary.cleanup()
            self._temporary = None


def require(condition: bool, operation: str, shape: str, fix_hint: str) -> None:
    if not condition:
        raise ContractError(
            f"stage=assert operation={operation} shape={shape} "
            f"consumer_version={EXPECTED_VERSION} fix_hint={fix_hint}"
        )


def main() -> int:
    global TIMEOUT
    TIMEOUT = positive_seconds("BQEMU_BQCLI_TIMEOUT_SECONDS", "120")
    bq = os.getenv("BQEMU_BQCLI_BIN", "bq")
    version = run_process([bq, "version"], "version")
    require(
        f"BigQuery CLI {EXPECTED_VERSION}" in version.stdout,
        "version",
        "unexpected-version",
        "update-exact-bq-profile-and-goldens",
    )

    with Runtime() as endpoint:
        project = os.getenv("BQEMU_TEST_PROJECT", "local-project")
        location = os.getenv("BQEMU_TEST_LOCATION", "US")
        token = os.getenv("BQEMU_BQCLI_ACCESS_TOKEN", "bqemu-contract-token")
        try:
            request_json(
                endpoint,
                "POST",
                "/bqemu/v1/projects",
                {"projectId": project, "friendlyName": "bq CLI contract"},
            )
        except urllib.error.HTTPError as error:
            if error.code != 409:
                raise

        base = [
            bq,
            f"--api={endpoint}",
            f"--project_id={project}",
            "--use_gcloud_config=false",
            f"--oauth_access_token={token}",
            "--format=json",
        ]
        suffix = uuid.uuid4().hex[:10]
        dataset = f"bqcli_{suffix}"
        table = f"{project}:{dataset}.events"
        dataset_ref = f"{project}:{dataset}"
        created_dataset = False
        created_table = False
        try:
            projects = decode_json(run_process(base + ["ls", "--projects"], "list_projects"), "list_projects")
            require(
                any(item.get("id") == project for item in projects),
                "list_projects",
                "project-list",
                "compare-projects-list-resource",
            )

            run_process(
                base
                + [f"--location={location}", "mk", "--dataset", "--description=CLI contract dataset", dataset_ref],
                "create_dataset",
            )
            created_dataset = True
            shown_dataset = decode_json(run_process(base + ["show", dataset_ref], "get_dataset"), "get_dataset")
            require(
                shown_dataset.get("datasetReference", {}).get("datasetId") == dataset
                and shown_dataset.get("location") == location,
                "get_dataset",
                "dataset-resource",
                "compare-datasets-get-response",
            )

            run_process(base + ["mk", "--table", table, "id:INTEGER,name:STRING"], "create_table")
            created_table = True
            schema_file = Path(tempfile.gettempdir()) / f"bqemu-schema-{suffix}.json"
            schema_file.write_text(
                json.dumps(
                    [
                        {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
                        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "active", "type": "BOOLEAN", "mode": "NULLABLE"},
                    ]
                ),
                encoding="utf-8",
            )
            try:
                run_process(base + ["update", table, str(schema_file)], "add_nullable_column")
            finally:
                schema_file.unlink(missing_ok=True)
            shown_table = decode_json(run_process(base + ["show", table], "get_table"), "get_table")
            fields = shown_table.get("schema", {}).get("fields", [])
            require(
                [field.get("name") for field in fields] == ["id", "name", "active"],
                "get_table",
                "additive-schema",
                "compare-schema-evolution-rules",
            )

            query = decode_json(
                run_process(base + ["query", "--use_legacy_sql=false", "SELECT 1 AS answer"], "query"),
                "query",
            )
            require(query == [{"answer": "1"}], "query", "single-int64-row", "compare-query-row-codec")

            jobs = decode_json(
                run_process(base + ["ls", "--jobs", "--max_results=10"], "list_jobs"),
                "list_jobs",
            )
            require(
                any(job.get("status", {}).get("state") == "DONE" for job in jobs),
                "list_jobs",
                "done-query-job",
                "compare-jobs-list-resource",
            )
            tables = decode_json(run_process(base + ["ls", dataset_ref], "list_tables"), "list_tables")
            require(len(tables) == 1, "list_tables", "single-table", "compare-tables-list-resource")

            missing = run_process(
                base + ["show", f"{project}:missing_{suffix}"],
                "missing_dataset_error",
                expected_codes=(2,),
            )
            require(
                "resource not found" in (missing.stdout + missing.stderr).lower(),
                "missing_dataset_error",
                "bigquery-not-found",
                "compare-google-error-envelope",
            )
        finally:
            if created_table:
                run_process(base + ["rm", "-f", "-t", table], "delete_table", expected_codes=(0, 2))
            if created_dataset:
                run_process(base + ["rm", "-f", "-d", dataset_ref], "delete_dataset", expected_codes=(0, 2))

    event(status="passed", suite="bq-cli-contract", consumer_version=EXPECTED_VERSION)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ContractError as error:
        event(status="failed", error=str(error))
        raise SystemExit(1) from None
