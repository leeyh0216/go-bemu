"""Bounded runtimes and complete diagnostics for the load E2E.

Protocol references:
  https://cloud.google.com/storage/docs/json_api/v1/objects/get
  https://cloud.google.com/storage/docs/json_api/v1/objects/list
  https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert
  https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/get
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, BinaryIO, Mapping, Sequence, cast
import urllib.error
import urllib.parse
import urllib.request
import uuid


ROOT = Path(__file__).resolve().parents[2]
MAX_LOCK_BYTES = 1 << 20
MAX_HTTP_RESPONSE_BYTES = 4 << 20
LOAD_MODEL_VERSION = "bqemu-load-public-process/v1"


class ContractError(RuntimeError):
    """A stable failure signature with the original observed value."""

    def __init__(
        self,
        *,
        stage: str,
        service: str,
        model_version: str,
        operation: str,
        shape: str,
        fingerprint: str,
        observed: str,
        fix_hint: str,
    ) -> None:
        self.fields = {
            "stage": stage,
            "service": service,
            "model_version": model_version,
            "operation": operation,
            "shape": shape,
            "fingerprint": fingerprint,
            "observed": observed,
            "fix_hint": fix_hint,
        }
        super().__init__(" ".join(f"{key}={value}" for key, value in self.fields.items()))


def digest(value: bytes | str | Any) -> str:
    if isinstance(value, bytes):
        encoded = value
    elif isinstance(value, str):
        encoded = value.encode("utf-8", errors="replace")
    else:
        encoded = json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("ascii")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def raw_file_digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1 << 20):
            hasher.update(chunk)
    return hasher.hexdigest()


def file_metadata(path: Path) -> dict[str, Any]:
    try:
        size = path.stat().st_size
        fingerprint = "sha256:" + raw_file_digest(path)
    except FileNotFoundError:
        size = 0
        fingerprint = digest(b"")
    return {"bytes": size, "sha256": fingerprint}


def payload_metadata(payload: bytes) -> dict[str, Any]:
    return {
        "bytes": len(payload),
        "sha256": digest(payload),
        "text": payload.decode("utf-8", errors="replace"),
    }


def write_diagnostic_summary(path: Path, streams: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    encoded = (
        json.dumps(
            {"schemaVersion": "bqemu-diagnostic/v1", "streams": streams},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        + "\n"
    ).encode("utf-8")
    temporary = path.with_name(path.name + ".diagnostic")
    temporary.write_bytes(encoded)
    os.chmod(temporary, 0o600)
    temporary.replace(path)
    return file_metadata(path)


def event(**fields: Any) -> None:
    print(
        json.dumps(fields, sort_keys=True, separators=(",", ":"), ensure_ascii=True),
        flush=True,
    )


def failure(
    *,
    stage: str,
    service: str,
    model_version: str,
    operation: str,
    shape: str,
    observed: bytes | str | Any,
    fix_hint: str,
) -> ContractError:
    if isinstance(observed, bytes):
        observed_text = observed.decode("utf-8", errors="replace")
    elif isinstance(observed, str):
        observed_text = observed
    else:
        observed_text = json.dumps(observed, sort_keys=True, default=str, ensure_ascii=False)
    return ContractError(
        stage=stage,
        service=service,
        model_version=model_version,
        operation=operation,
        shape=shape,
        fingerprint=digest(observed),
        observed=observed_text,
        fix_hint=fix_hint,
    )


def positive_seconds(name: str, default: str) -> float:
    raw = os.getenv(name, default)
    try:
        value = float(raw)
    except ValueError as error:
        raise failure(
            stage="configuration",
            service="load-e2e",
            model_version="bqemu-load-e2e/v1",
            operation="parse-timeout",
            shape=name,
            observed=raw,
            fix_hint="set-a-positive-number-of-seconds",
        ) from error
    if value <= 0:
        raise failure(
            stage="configuration",
            service="load-e2e",
            model_version="bqemu-load-e2e/v1",
            operation="parse-timeout",
            shape=name,
            observed=raw,
            fix_hint="set-a-positive-number-of-seconds",
        )
    return value


def positive_integer(name: str, default: str) -> int:
    raw = os.getenv(name, default)
    try:
        value = int(raw)
    except ValueError as error:
        raise failure(
            stage="configuration",
            service="load-e2e",
            model_version="bqemu-load-e2e/v1",
            operation="parse-limit",
            shape=name,
            observed=raw,
            fix_hint="set-a-positive-integer",
        ) from error
    if value <= 0:
        raise failure(
            stage="configuration",
            service="load-e2e",
            model_version="bqemu-load-e2e/v1",
            operation="parse-limit",
            shape=name,
            observed=raw,
            fix_hint="set-a-positive-integer",
        )
    return value


@dataclass(frozen=True)
class Settings:
    infrastructure_lock: Path
    fixture_lock: Path
    config: Path
    artifact_root: Path
    go_binary: str
    docker_binary: str
    bq_binary: str
    spark_python: Path
    project: str
    dataset: str
    location: str
    artifact_timeout: float
    build_timeout: float
    fixture_timeout: float
    fake_gcs_start_timeout: float
    emulator_start_timeout: float
    client_timeout: float
    shutdown_timeout: float
    http_timeout: float
    proxy_max_request_bytes: int
    proxy_max_response_bytes: int

    @classmethod
    def from_environment(cls) -> "Settings":
        return cls(
            infrastructure_lock=Path(
                os.getenv(
                    "BQEMU_LOAD_INFRASTRUCTURE_LOCK",
                    str(ROOT / "tests/load/infrastructure.lock.json"),
                )
            ).resolve(),
            fixture_lock=Path(
                os.getenv(
                    "BQEMU_LOAD_FIXTURE_LOCK",
                    str(ROOT / "tests/load/fixtures.lock.json"),
                )
            ).resolve(),
            config=Path(
                os.getenv(
                    "BQEMU_LOAD_CONFIG", str(ROOT / "tests/load/bqemu.yaml")
                )
            ).resolve(),
            artifact_root=Path(
                os.getenv(
                    "BQEMU_LOAD_ARTIFACT_DIR", str(ROOT / ".artifacts/load")
                )
            ).resolve(),
            go_binary=os.getenv("BQEMU_LOAD_GO_BIN", "go"),
            docker_binary=os.getenv("BQEMU_LOAD_DOCKER_BIN", "docker"),
            bq_binary=os.getenv(
                "BQEMU_LOAD_BQCLI_BIN", os.getenv("BQEMU_BQCLI_BIN", "bq")
            ),
            spark_python=Path(
                os.getenv(
                    "BQEMU_LOAD_SPARK_PYTHON",
                    os.getenv(
                        "BQEMU_SPARK_PYTHON",
                        str(ROOT / ".artifacts/spark/venv/bin/python"),
                    ),
                )
            ).absolute(),
            project=os.getenv("BQEMU_LOAD_PROJECT", "load-e2e-project"),
            dataset=os.getenv("BQEMU_LOAD_DATASET", "load_e2e"),
            location=os.getenv("BQEMU_LOAD_LOCATION", "US"),
            artifact_timeout=positive_seconds(
                "BQEMU_LOAD_ARTIFACT_TIMEOUT_SECONDS", "180"
            ),
            build_timeout=positive_seconds(
                "BQEMU_LOAD_BUILD_TIMEOUT_SECONDS", "180"
            ),
            fixture_timeout=positive_seconds(
                "BQEMU_LOAD_FIXTURE_TIMEOUT_SECONDS", "60"
            ),
            fake_gcs_start_timeout=positive_seconds(
                "BQEMU_LOAD_FAKE_GCS_START_TIMEOUT_SECONDS", "45"
            ),
            emulator_start_timeout=positive_seconds(
                "BQEMU_LOAD_EMULATOR_START_TIMEOUT_SECONDS", "45"
            ),
            client_timeout=positive_seconds(
                "BQEMU_LOAD_CLIENT_TIMEOUT_SECONDS", "120"
            ),
            shutdown_timeout=positive_seconds(
                "BQEMU_LOAD_SHUTDOWN_TIMEOUT_SECONDS", "15"
            ),
            http_timeout=positive_seconds(
                "BQEMU_LOAD_HTTP_TIMEOUT_SECONDS", "5"
            ),
            proxy_max_request_bytes=positive_integer(
                "BQEMU_LOAD_PROXY_MAX_REQUEST_BYTES", str(MAX_HTTP_RESPONSE_BYTES)
            ),
            proxy_max_response_bytes=positive_integer(
                "BQEMU_LOAD_PROXY_MAX_RESPONSE_BYTES", str(MAX_HTTP_RESPONSE_BYTES)
            ),
        )


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def read_locked_json(path: Path, expected_schema: str) -> dict[str, Any]:
    try:
        with path.open("rb") as source:
            payload = source.read(MAX_LOCK_BYTES + 1)
    except OSError as error:
        raise failure(
            stage="provenance",
            service="load-e2e",
            model_version=expected_schema,
            operation="read-lock",
            shape="unreadable-lock",
            observed={"type": type(error).__name__, "error": repr(error)},
            fix_hint="restore-the-versioned-lock-file",
        ) from error
    if len(payload) > MAX_LOCK_BYTES:
        raise failure(
            stage="provenance",
            service="load-e2e",
            model_version=expected_schema,
            operation="read-lock",
            shape="oversize-lock",
            observed=len(payload),
            fix_hint="reduce-lock-below-one-mebibyte",
        )
    try:
        decoded = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
    except (ValueError, json.JSONDecodeError) as error:
        raise failure(
            stage="provenance",
            service="load-e2e",
            model_version=expected_schema,
            operation="decode-lock",
            shape="invalid-json",
            observed={"type": type(error).__name__, "error": repr(error)},
            fix_hint="repair-the-versioned-lock-file",
        ) from error
    if not isinstance(decoded, dict) or decoded.get("schemaVersion") != expected_schema:
        raise failure(
            stage="provenance",
            service="load-e2e",
            model_version=expected_schema,
            operation="validate-lock",
            shape="schema-version",
            observed=decoded.get("schemaVersion") if isinstance(decoded, dict) else type(decoded).__name__,
            fix_hint="update-lock-reader-and-schema-together",
        )
    return decoded


def require_exact_keys(
    value: Mapping[str, Any], expected: set[str], operation: str, model_version: str
) -> None:
    if set(value) != expected:
        raise failure(
            stage="provenance",
            service="load-e2e",
            model_version=model_version,
            operation=operation,
            shape="object-keys",
            observed=sorted(value),
            fix_hint="update-lock-schema-and-reader-together",
        )


def run_process(
    args: Sequence[str],
    *,
    operation: str,
    service: str,
    model_version: str,
    timeout: float,
    cwd: Path = ROOT,
    environment: Mapping[str, str] | None = None,
    expected_codes: tuple[int, ...] = (0,),
) -> subprocess.CompletedProcess[bytes]:
    started = time.monotonic()
    try:
        result = subprocess.run(
            list(args),
            cwd=cwd,
            env=None if environment is None else dict(environment),
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise failure(
            stage="process",
            service=service,
            model_version=model_version,
            operation=operation,
            shape="timeout",
            observed=timeout,
            fix_hint="increase-the-operation-specific-timeout",
        ) from error
    except OSError as error:
        raise failure(
            stage="process",
            service=service,
            model_version=model_version,
            operation=operation,
            shape="unavailable",
            observed={"type": type(error).__name__, "error": repr(error)},
            fix_hint="install-the-pinned-runtime-or-fix-its-path",
        ) from error
    event(
        boundary="process",
        duration_ms=round((time.monotonic() - started) * 1000),
        model_version=model_version,
        operation=operation,
        return_code=result.returncode,
        service=service,
        stderr_bytes=len(result.stderr),
        stderr_sha256=digest(result.stderr),
        stderr=result.stderr.decode("utf-8", errors="replace"),
        stdout_bytes=len(result.stdout),
        stdout_sha256=digest(result.stdout),
        stdout=result.stdout.decode("utf-8", errors="replace"),
    )
    if result.returncode not in expected_codes:
        raise failure(
            stage="process",
            service=service,
            model_version=model_version,
            operation=operation,
            shape=f"exit-{result.returncode}",
            observed=result.stdout + b"\x00" + result.stderr,
            fix_hint="inspect-runtime-logs-and-locked-provenance",
        )
    return result


def validate_infrastructure_lock(
    settings: Settings, locked: Mapping[str, Any]
) -> dict[str, Any]:
    model = "bqemu-load-infrastructure/v1"
    require_exact_keys(
        locked,
        {"schemaVersion", "fakeGCS"},
        "infrastructure-lock",
        model,
    )
    fake = cast(Mapping[str, Any], locked["fakeGCS"])
    require_exact_keys(
        fake, {"image", "version", "revision", "source"}, "fake-gcs-lock", model
    )
    image = str(fake["image"])
    version = str(fake["version"])
    revision = str(fake["revision"])
    source = str(fake["source"])
    image_pattern = re.compile(
        r"^[a-z0-9][a-z0-9._/-]*@sha256:[0-9a-f]{64}$"
    )
    if (
        image_pattern.fullmatch(image) is None
        or re.fullmatch(r"[0-9]+(?:\.[0-9]+){2}", version) is None
        or re.fullmatch(r"[0-9a-f]{40}", revision) is None
        or source
        != f"https://github.com/fsouza/fake-gcs-server/tree/v{version}"
    ):
        raise failure(
            stage="provenance",
            service="fake-gcs-server",
            model_version=model,
            operation="validate-infrastructure-lock",
            shape="immutable-identity",
            observed=fake,
            fix_hint="review-the-pinned-fake-gcs-release-and-digest",
        )
    return {
        "infrastructureLockSha256": "sha256:"
        + raw_file_digest(settings.infrastructure_lock),
        "fakeGCSImage": image,
        "fakeGCSVersion": version,
        "fakeGCSRevision": revision,
    }


def ensure_fake_gcs_image(
    settings: Settings, provenance: Mapping[str, Any]
) -> dict[str, Any]:
    image = str(provenance["fakeGCSImage"])
    version = str(provenance["fakeGCSVersion"])
    revision = str(provenance["fakeGCSRevision"])
    inspect = run_process(
        [settings.docker_binary, "image", "inspect", image],
        operation="inspect-fake-gcs-image",
        service="fake-gcs-server",
        model_version=version,
        timeout=settings.artifact_timeout,
        expected_codes=(0, 1),
    )
    if inspect.returncode != 0:
        run_process(
            [settings.docker_binary, "pull", image],
            operation="pull-fake-gcs-image",
            service="fake-gcs-server",
            model_version=version,
            timeout=settings.artifact_timeout,
        )
        inspect = run_process(
            [settings.docker_binary, "image", "inspect", image],
            operation="inspect-pulled-fake-gcs-image",
            service="fake-gcs-server",
            model_version=version,
            timeout=settings.artifact_timeout,
        )
    try:
        decoded = json.loads(inspect.stdout)
        record = decoded[0]
        labels = record["Config"]["Labels"]
        repo_digests = record["RepoDigests"]
    except (KeyError, IndexError, TypeError, json.JSONDecodeError) as error:
        raise failure(
            stage="provenance",
            service="fake-gcs-server",
            model_version=version,
            operation="inspect-image",
            shape="docker-inspect-json",
            observed=inspect.stdout,
            fix_hint="restore-the-pinned-image-or-update-the-lock",
        ) from error
    if image not in repo_digests:
        raise failure(
            stage="provenance",
            service="fake-gcs-server",
            model_version=version,
            operation="inspect-image",
            shape="repo-digest-drift",
            observed=repo_digests,
            fix_hint="pull-the-image-by-locked-digest",
        )
    expected_labels = {
        "org.opencontainers.image.version": version,
        "org.opencontainers.image.revision": revision,
    }
    if not isinstance(labels, dict) or any(
        labels.get(key) != value for key, value in expected_labels.items()
    ):
        raise failure(
            stage="provenance",
            service="fake-gcs-server",
            model_version=version,
            operation="inspect-image",
            shape="oci-label-drift",
            observed=labels,
            fix_hint="review-the-upstream-image-before-updating-the-lock",
        )
    return {
        "image": image,
        "imageId": str(record["Id"]),
        "version": version,
        "revision": revision,
    }


@dataclass(frozen=True)
class AuditEntry:
    sequence: int
    time_unix_nanos: int
    actor: str
    operation: str
    method: str
    status: int
    request_bytes: int
    response_bytes: int
    response_sha256: str
    request_sha256: str
    target_sha256: str
    target_shape: str
    transport_normalization: str | None

    def evidence(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "timeUnixNanos": self.time_unix_nanos,
            "actor": self.actor,
            "operation": self.operation,
            "method": self.method,
            "status": self.status,
            "requestBytes": self.request_bytes,
            "responseBytes": self.response_bytes,
            "responseSha256": self.response_sha256,
            "requestSha256": self.request_sha256,
            "targetSha256": self.target_sha256,
            "targetShape": self.target_shape,
            "transportNormalization": self.transport_normalization,
        }


class _AuditHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        timeout: float,
        max_request_bytes: int,
        max_response_bytes: int,
        actor: str,
        model_version: str,
    ) -> None:
        super().__init__(address, _AuditHandler)
        self.upstream: tuple[str, int] | None = None
        self.upstream_timeout = timeout
        self.max_request_bytes = max_request_bytes
        self.max_response_bytes = max_response_bytes
        self.actor = actor
        self.model_version = model_version
        self.entries: list[AuditEntry] = []
        self.entries_lock = threading.Lock()

    def record(
        self,
        operation: str,
        method: str,
        status: int,
        request_body: bytes,
        response_body: bytes,
        request_target: str,
        transport_normalization: str | None = None,
    ) -> None:
        with self.entries_lock:
            entry = AuditEntry(
                sequence=len(self.entries) + 1,
                time_unix_nanos=time.time_ns(),
                actor=self.actor,
                operation=operation,
                method=method,
                status=status,
                request_bytes=len(request_body),
                response_bytes=len(response_body),
                response_sha256=digest(response_body),
                request_sha256=digest(request_body),
                target_sha256=digest(request_target),
                target_shape=_gcs_target_shape(method, request_target),
                transport_normalization=transport_normalization,
            )
            self.entries.append(entry)
        event(
            boundary="gcs-json",
            actor=entry.actor,
            model_version=self.model_version,
            operation=operation,
            method=method,
            request_bytes=entry.request_bytes,
            request_sha256=entry.request_sha256,
            target_sha256=entry.target_sha256,
            target_shape=entry.target_shape,
            transport_normalization=entry.transport_normalization,
            response_bytes=entry.response_bytes,
            response_sha256=entry.response_sha256,
            sequence=entry.sequence,
            service="storage",
            status=status,
        )


class _AuditHandler(BaseHTTPRequestHandler):
    server: _AuditHTTPServer

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self._proxy()

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self._proxy()

    def do_PUT(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self._proxy()

    def do_PATCH(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self._proxy()

    def do_DELETE(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self._proxy()

    def _proxy(self) -> None:
        server = cast(_AuditHTTPServer, self.server)
        request_body = self._read_bounded_body(server.max_request_bytes)
        if request_body is None:
            server.record("request.too-large", self.command, 413, b"", b"", self.path)
            self.send_error(413)
            return
        operation = _gcs_operation(self.command, self.path)
        normalize_empty_gzip = (
            request_body == b""
            and operation in {"objects.copy", "objects.rewrite"}
            and self.headers.get("Content-Encoding", "").lower() == "gzip"
        )
        transport_normalization = (
            "empty-gzip-body" if normalize_empty_gzip else None
        )
        if server.upstream is None:
            server.record(operation, self.command, 503, request_body, b"", self.path)
            self.send_error(503)
            return
        connection = http.client.HTTPConnection(
            server.upstream[0], server.upstream[1], timeout=server.upstream_timeout
        )
        try:
            headers = {
                name: value
                for name, value in self.headers.items()
                if name.lower()
                not in {"connection", "content-length", "host", "proxy-connection", "transfer-encoding"}
                and not (normalize_empty_gzip and name.lower() == "content-encoding")
            }
            connection.request(self.command, self.path, body=request_body, headers=headers)
            response = connection.getresponse()
            payload = response.read(server.max_response_bytes + 1)
            if len(payload) > server.max_response_bytes:
                server.record(operation, self.command, 502, request_body, b"", self.path)
                self.send_error(502)
                return
            server.record(
                operation,
                self.command,
                response.status,
                request_body,
                payload,
                self.path,
                transport_normalization,
            )
            self.send_response(response.status)
            for name in (
                "Content-Type",
                "ETag",
                "Last-Modified",
                "Location",
                "Range",
                "x-goog-generation",
                "x-goog-hash",
            ):
                value = response.getheader(name)
                if value is not None:
                    self.send_header(name, value)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except (OSError, http.client.HTTPException):
            server.record(operation, self.command, 502, request_body, b"", self.path)
            self.send_error(502)
        finally:
            connection.close()

    def _read_bounded_body(self, maximum: int) -> bytes | None:
        raw_length = self.headers.get("Content-Length", "0")
        try:
            length = int(raw_length)
        except ValueError:
            return None
        if length < 0 or length > maximum:
            return None
        payload = self.rfile.read(length)
        if len(payload) != length:
            return None
        return payload

    def log_message(self, format: str, *args: Any) -> None:
        return


def _gcs_operation(method: str, request_target: str) -> str:
    parsed = urllib.parse.urlsplit(request_target)
    if method == "GET" and parsed.path == "/storage/v1/b":
        return "buckets.list"
    if method == "GET" and re.fullmatch(r"/storage/v1/b/[^/]+", parsed.path):
        return "buckets.get"
    if method == "GET" and re.fullmatch(r"/storage/v1/b/[^/]+/o", parsed.path):
        return "objects.list"
    if method == "GET" and re.fullmatch(r"/storage/v1/b/[^/]+/o/.+", parsed.path):
        query = urllib.parse.parse_qs(parsed.query)
        if query.get("alt") == ["media"]:
            return "objects.get.media"
        return "objects.get.metadata"
    if method == "POST" and parsed.path.startswith("/upload/storage/v1/b/"):
        return "objects.upload"
    if method == "PUT" and parsed.path.startswith("/upload/"):
        return "objects.upload.chunk"
    if method == "POST" and re.fullmatch(
        r"/storage/v1/b/[^/]+/o/.+/rewriteTo/b/[^/]+/o/.+", parsed.path
    ):
        return "objects.rewrite"
    if method == "POST" and re.fullmatch(
        r"/storage/v1/b/[^/]+/o/.+/copyTo/b/[^/]+/o/.+", parsed.path
    ):
        return "objects.copy"
    if method == "POST" and re.fullmatch(
        r"/storage/v1/b/[^/]+/o/.+/moveTo/o/.+", parsed.path
    ):
        return "objects.move"
    if method == "POST" and re.fullmatch(
        r"/storage/v1/b/[^/]+/o/.+/compose", parsed.path
    ):
        return "objects.compose"
    if method == "DELETE" and re.fullmatch(r"/storage/v1/b/[^/]+/o/.+", parsed.path):
        return "objects.delete"
    return "unexpected"


def _gcs_target_shape(method: str, request_target: str) -> str:
    parsed = urllib.parse.urlsplit(request_target)
    operation = _gcs_operation(method, request_target)
    if operation in {"objects.copy", "objects.rewrite"}:
        match = re.fullmatch(
            r"/storage/v1/b/[^/]+/o/(.+)/(?:copyTo|rewriteTo)/b/[^/]+/o/(.+)",
            parsed.path,
        )
        if match is not None:
            source = urllib.parse.unquote(match.group(1))
            destination = urllib.parse.unquote(match.group(2))
            query_keys = sorted(urllib.parse.parse_qs(parsed.query, keep_blank_values=True))
            return ";".join(
                (
                    operation,
                    f"source={_gcs_object_kind(source)}",
                    f"destination={_gcs_object_kind(destination)}",
                    f"sourceDepth={len(source.split('/'))}",
                    f"destinationDepth={len(destination.split('/'))}",
                    "query=" + ",".join(query_keys),
                )
            )
    if operation != "unexpected":
        return operation
    path = parsed.path
    if path.startswith("/storage/v1/b/"):
        return "storage.object-action"
    if path.startswith("/upload/"):
        return "upload.unknown"
    return "unknown"


def _gcs_object_kind(name: str) -> str:
    segments = name.split("/")
    basename = segments[-1]
    if "_temporary" in segments:
        return "temporary"
    if basename.startswith("part-") and basename.endswith(".parquet"):
        return "parquet-part"
    if basename == "_SUCCESS":
        return "success-marker"
    if basename.startswith(".spark-bigquery-"):
        return "connector-prefix"
    if basename == "":
        return "directory-marker"
    return "other"


class AuditProxy:
    def __init__(
        self,
        timeout: float,
        max_request_bytes: int,
        max_response_bytes: int,
        *,
        actor: str,
        model_version: str,
    ) -> None:
        self._server = _AuditHTTPServer(
            ("127.0.0.1", 0),
            timeout,
            max_request_bytes,
            max_response_bytes,
            actor,
            model_version,
        )
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="bqemu-load-gcs-audit",
            daemon=True,
        )

    @property
    def endpoint(self) -> str:
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    @property
    def entries(self) -> list[AuditEntry]:
        with self._server.entries_lock:
            return list(self._server.entries)

    def set_upstream(self, upstream_endpoint: str) -> None:
        parsed = urllib.parse.urlsplit(upstream_endpoint)
        if parsed.scheme != "http" or parsed.hostname is None or parsed.port is None:
            raise ValueError("audit proxy upstream must be an absolute HTTP URL")
        if self._thread.is_alive():
            raise RuntimeError("audit proxy upstream must be set before start")
        self._server.upstream = (parsed.hostname, parsed.port)

    def start(self) -> None:
        self._thread.start()

    def stop(self, timeout: float) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=timeout)
        if self._thread.is_alive():
            raise failure(
                stage="shutdown",
                service="gcs-audit-proxy",
                model_version="bqemu-load-e2e/v1",
                operation="join",
                shape="thread-timeout",
                observed=timeout,
                fix_hint="inspect-in-flight-object-store-requests",
            )


class FakeGCSRuntime:
    def __init__(
        self,
        settings: Settings,
        image: Mapping[str, Any],
        seed_root: Path,
        log_path: Path,
    ) -> None:
        self.settings = settings
        self.image = image
        self.seed_root = seed_root
        self.log_path = log_path
        self.container_id: str | None = None
        self.endpoint = ""

    def start(self, external_url: str = "") -> str:
        name = f"bqemu-load-e2e-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        result = run_process(
            [
                self.settings.docker_binary,
                "run",
                "--detach",
                "--rm",
                "--name",
                name,
                "--read-only",
                "--security-opt",
                "no-new-privileges:true",
                "--cap-drop",
                "ALL",
                "--tmpfs",
                "/storage:mode=0700",
                "--tmpfs",
                "/tmp:mode=0700",
                "--publish",
                "127.0.0.1::4443/tcp",
                "--volume",
                f"{self.seed_root}:/data:ro",
                str(self.image["image"]),
                "-backend",
                "memory",
                "-scheme",
                "http",
                "-port",
                "4443",
                "-data",
                "/data",
                *(["-external-url", external_url] if external_url else []),
                "-log-level",
                "info",
            ],
            operation="start-container",
            service="fake-gcs-server",
            model_version=str(self.image["version"]),
            timeout=self.settings.fake_gcs_start_timeout,
        )
        container_id = result.stdout.decode("ascii", errors="strict").strip()
        if not re.fullmatch(r"[0-9a-f]{64}", container_id):
            raise failure(
                stage="runtime",
                service="fake-gcs-server",
                model_version=str(self.image["version"]),
                operation="start-container",
                shape="container-id",
                observed=result.stdout,
                fix_hint="inspect-docker-runtime-output",
            )
        self.container_id = container_id
        port = run_process(
            [self.settings.docker_binary, "port", container_id, "4443/tcp"],
            operation="discover-random-port",
            service="fake-gcs-server",
            model_version=str(self.image["version"]),
            timeout=self.settings.fake_gcs_start_timeout,
        )
        match = re.fullmatch(
            rb"(?:127\.0\.0\.1|0\.0\.0\.0):(\d+)\s*", port.stdout
        )
        if match is None:
            raise failure(
                stage="runtime",
                service="fake-gcs-server",
                model_version=str(self.image["version"]),
                operation="discover-random-port",
                shape="docker-port-output",
                observed=port.stdout,
                fix_hint="inspect-docker-port-publication",
            )
        self.endpoint = f"http://127.0.0.1:{int(match.group(1))}"
        deadline = time.monotonic() + self.settings.fake_gcs_start_timeout
        last_shape = "not-ready"
        while time.monotonic() < deadline:
            try:
                request_json(
                    self.endpoint,
                    "GET",
                    "/storage/v1/b",
                    operation="fake-gcs-readiness",
                    service="storage",
                    model_version=str(self.image["version"]),
                    timeout=min(self.settings.http_timeout, max(0.1, deadline - time.monotonic())),
                )
                return self.endpoint
            except ContractError as error:
                last_shape = error.fields["shape"]
                time.sleep(0.05)
        raise failure(
            stage="readiness",
            service="fake-gcs-server",
            model_version=str(self.image["version"]),
            operation="storage-buckets-list",
            shape=last_shape,
            observed=self._log_diagnostics(),
            fix_hint="inspect-fake-gcs-log",
        )

    def stop(self) -> None:
        if self.container_id is None:
            return
        logs = run_process(
            [self.settings.docker_binary, "logs", "--tail", "2000", self.container_id],
            operation="capture-logs",
            service="fake-gcs-server",
            model_version=str(self.image["version"]),
            timeout=self.settings.shutdown_timeout,
            expected_codes=(0, 1),
        )
        summary = write_diagnostic_summary(
            self.log_path,
            {"stdout": payload_metadata(logs.stdout), "stderr": payload_metadata(logs.stderr)},
        )
        run_process(
            [self.settings.docker_binary, "rm", "--force", self.container_id],
            operation="remove-container",
            service="fake-gcs-server",
            model_version=str(self.image["version"]),
            timeout=self.settings.shutdown_timeout,
            expected_codes=(0, 1),
        )
        self.container_id = None
        event(
            boundary="diagnostic",
            model_version=str(self.image["version"]),
            operation="fake-gcs-log",
            service="fake-gcs-server",
            **summary,
        )

    def _log_diagnostics(self) -> dict[str, Any]:
        if self.container_id is None:
            return {"container": "absent"}
        result = run_process(
            [self.settings.docker_binary, "logs", "--tail", "200", self.container_id],
            operation="readiness-diagnostics",
            service="fake-gcs-server",
            model_version=str(self.image["version"]),
            timeout=self.settings.shutdown_timeout,
            expected_codes=(0, 1),
        )
        return {
            "stdoutBytes": len(result.stdout),
            "stdoutSha256": digest(result.stdout),
            "stdout": result.stdout.decode("utf-8", errors="replace"),
            "stderrBytes": len(result.stderr),
            "stderrSha256": digest(result.stderr),
            "stderr": result.stderr.decode("utf-8", errors="replace"),
        }


class _ReservedPort:
    def __init__(self) -> None:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(("127.0.0.1", 0))
        self.port = int(self.socket.getsockname()[1])

    def release(self) -> None:
        self.socket.close()


@dataclass(frozen=True)
class RuntimeEvent:
    time_unix_nanos: int
    actor: str
    protocol: str
    phase: str
    operation: str
    status: str | int | None

    def evidence(self) -> dict[str, Any]:
        return {
            "timeUnixNanos": self.time_unix_nanos,
            "actor": self.actor,
            "protocol": self.protocol,
            "phase": self.phase,
            "operation": self.operation,
            "status": self.status,
        }


def _log_time_unix_nanos(value: Any) -> int:
    if not isinstance(value, str):
        return 0
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return 0
    return int(parsed.timestamp() * 1_000_000_000)


class EmulatorRuntime:
    def __init__(
        self,
        settings: Settings,
        binary: Path,
        work: Path,
        gcs_endpoint: str,
        log_path: Path,
        diagnostic_path: Path,
    ) -> None:
        self.settings = settings
        self.binary = binary
        self.work = work
        self.gcs_endpoint = gcs_endpoint
        self.log_path = log_path
        self.diagnostic_path = diagnostic_path
        self.process: subprocess.Popen[bytes] | None = None
        self.log: BinaryIO | None = None
        self.endpoint = ""

    def start(self) -> str:
        http_port, grpc_port = _ReservedPort(), _ReservedPort()
        self.endpoint = f"http://127.0.0.1:{http_port.port}"
        temporary = self.work / "tmp"
        home = self.work / "home"
        temporary.mkdir(mode=0o700)
        home.mkdir(mode=0o700)
        environment = {
            "PATH": os.environ.get("PATH", ""),
            "HOME": str(home),
            "TMPDIR": str(temporary),
            "BQEMU_PROJECT": self.settings.project,
            "BQEMU_LOCATION": self.settings.location,
            "BQEMU_HTTP_ADDRESS": f"127.0.0.1:{http_port.port}",
            "BQEMU_GRPC_ADDRESS": f"127.0.0.1:{grpc_port.port}",
            "BQEMU_PUBLIC_URL": self.endpoint,
            "BQEMU_DATABASE_DSN": str(self.work / "load-e2e.duckdb"),
            "BQEMU_TEMP_DIRECTORY": str(temporary),
            "BQEMU_LOAD_GCS_ENDPOINT": self.gcs_endpoint,
        }
        self.log = self.log_path.open("wb")
        http_port.release()
        grpc_port.release()
        try:
            self.process = subprocess.Popen(
                [str(self.binary), "--config", str(self.settings.config)],
                cwd=ROOT,
                env=environment,
                stdout=self.log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except OSError as error:
            self.log.close()
            self.log = None
            raise failure(
                stage="runtime",
                service="bqemu",
                model_version="current-source",
                operation="start-emulator",
                shape="process-unavailable",
                observed={"type": type(error).__name__, "error": repr(error)},
                fix_hint="inspect-current-source-build-and-runtime-libraries",
            ) from error
        deadline = time.monotonic() + self.settings.emulator_start_timeout
        last_shape = "not-ready"
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                last_shape = f"exit-{self.process.returncode}"
                break
            try:
                request_json(
                    self.endpoint,
                    "GET",
                    "/readyz",
                    operation="emulator-readiness",
                    service="bigquery",
                    model_version="current-source",
                    timeout=min(self.settings.http_timeout, max(0.1, deadline - time.monotonic())),
                )
                return self.endpoint
            except ContractError as error:
                last_shape = error.fields["shape"]
                time.sleep(0.05)
        self.stop()
        raise failure(
            stage="readiness",
            service="bqemu",
            model_version="current-source",
            operation="http-readyz",
            shape=last_shape,
            observed={
                **file_metadata(self.log_path),
                "log": self.log_path.read_text(encoding="utf-8", errors="replace")
                if self.log_path.exists()
                else "",
            },
            fix_hint="inspect-emulator-log",
        )

    def log_position(self) -> int:
        try:
            return self.log_path.stat().st_size
        except FileNotFoundError:
            return 0

    def runtime_events(
        self, since: int = 0, until: int | None = None
    ) -> list[RuntimeEvent]:
        maximum = 16 << 20
        try:
            with self.log_path.open("rb") as source:
                source.seek(since)
                limit = maximum + 1 if until is None else min(maximum + 1, until - since)
                payload = source.read(limit)
        except OSError as error:
            raise failure(
                stage="diagnostic",
                service="bqemu",
                model_version="current-source",
                operation="read-runtime-events",
                shape="unreadable-log",
                observed={"type": type(error).__name__, "error": repr(error)},
                fix_hint="inspect-emulator-runtime-ownership",
            ) from error
        if len(payload) > maximum:
            raise failure(
                stage="diagnostic",
                service="bqemu",
                model_version="current-source",
                operation="read-runtime-events",
                shape="oversize-log",
                observed=len(payload),
                fix_hint="reduce-test-workload-or-log-volume",
            )
        events: list[RuntimeEvent] = []
        internal_operations = {
            "commit_load_job",
            "load_parquet",
            "cleanup_load_workspace",
        }
        for encoded in payload.splitlines():
            try:
                record = json.loads(encoded)
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            event_name = record.get("event")
            timestamp = _log_time_unix_nanos(record.get("time"))
            if timestamp == 0:
                continue
            if event_name == "boundary.enter":
                boundary = record.get("boundary")
                if boundary == "http":
                    operation = _http_operation_id(
                        record.get("method"), record.get("path")
                    )
                    protocol = "rest"
                elif boundary in {"grpc.unary", "grpc.stream"}:
                    operation = _grpc_operation_id(record.get("rpc"))
                    protocol = "grpc"
                else:
                    continue
                if operation is None:
                    raise failure(
                        stage="diagnostic",
                        service="bqemu",
                        model_version="current-source",
                        operation="map-public-operation",
                        shape="unregistered-boundary",
                        observed={
                            "boundary": boundary,
                            "method": record.get("method"),
                            "path": record.get("path"),
                            "rpc": record.get("rpc"),
                        },
                        fix_hint="register-the-public-operation-before-claiming-compatibility",
                    )
                events.append(
                    RuntimeEvent(
                        time_unix_nanos=timestamp,
                        actor="consumer",
                        protocol=protocol,
                        phase="request",
                        operation=operation,
                        status=None,
                    )
                )
            elif (
                event_name in {"side_effect.pre", "side_effect.post"}
                and record.get("operation") in internal_operations
            ):
                events.append(
                    RuntimeEvent(
                        time_unix_nanos=timestamp,
                        actor="bqemu",
                        protocol="internal",
                        phase="before" if event_name == "side_effect.pre" else "after",
                        operation="internal." + str(record["operation"]),
                        status=record.get("tx_state"),
                    )
                )
        return events

    def public_operations(
        self, since: int = 0, until: int | None = None
    ) -> list[str]:
        return [
            event.operation
            for event in self.runtime_events(since, until)
            if event.protocol in {"rest", "grpc"} and event.phase == "request"
        ]
    def stop(self) -> None:
        process = self.process
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=self.settings.shutdown_timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                try:
                    process.wait(timeout=self.settings.shutdown_timeout)
                except subprocess.TimeoutExpired as error:
                    raise failure(
                        stage="shutdown",
                        service="bqemu",
                        model_version="current-source",
                        operation="wait-process",
                        shape="kill-timeout",
                        observed=self.settings.shutdown_timeout,
                        fix_hint="inspect-runtime-shutdown-lifecycle",
                    ) from error
        self.process = None
        if self.log is not None:
            self.log.close()
            self.log = None
        if self.log_path.exists():
            payload = self.log_path.read_bytes()
            captured = payload_metadata(payload[-MAX_DIAGNOSTIC_FILE_BYTES:])
            captured["originalBytes"] = len(payload)
            captured["truncated"] = len(payload) > MAX_DIAGNOSTIC_FILE_BYTES
            summary = write_diagnostic_summary(
                self.diagnostic_path, {"combined": captured}
            )
            event(
                boundary="diagnostic",
                model_version="current-source",
                operation="emulator-log",
                service="bqemu",
                **summary,
            )


def request_json(
    endpoint: str,
    method: str,
    path: str,
    *,
    operation: str,
    service: str,
    model_version: str,
    timeout: float,
    payload: Mapping[str, Any] | None = None,
) -> Any:
    encoded = (
        None
        if payload is None
        else json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    )
    request = urllib.request.Request(endpoint + path, data=encoded, method=method)
    if encoded is not None:
        request.add_header("Content-Type", "application/json")
    started = time.monotonic()
    status = 0
    response_payload = b""
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = response.status
            response_payload = response.read(MAX_HTTP_RESPONSE_BYTES + 1)
    except urllib.error.HTTPError as error:
        status = error.code
        response_payload = error.read(MAX_HTTP_RESPONSE_BYTES + 1)
        event(
            boundary="http",
            duration_ms=round((time.monotonic() - started) * 1000),
            method=method,
            model_version=model_version,
            operation=operation,
            request_bytes=0 if encoded is None else len(encoded),
            request_sha256=digest(b"" if encoded is None else encoded),
            response_bytes=len(response_payload),
            response_sha256=digest(response_payload),
            service=service,
            status=status,
        )
        raise failure(
            stage="http",
            service=service,
            model_version=model_version,
            operation=operation,
            shape=f"status-{status}",
            observed=response_payload,
            fix_hint="compare-the-pinned-wire-contract",
        ) from error
    except (OSError, urllib.error.URLError) as error:
        raise failure(
            stage="http",
            service=service,
            model_version=model_version,
            operation=operation,
            shape="transport-error",
            observed={"type": type(error).__name__, "error": repr(error)},
            fix_hint="inspect-runtime-readiness-and-timeout",
        ) from error
    event(
        boundary="http",
        duration_ms=round((time.monotonic() - started) * 1000),
        method=method,
        model_version=model_version,
        operation=operation,
        request_bytes=0 if encoded is None else len(encoded),
        request_sha256=digest(b"" if encoded is None else encoded),
        response_bytes=len(response_payload),
        response_sha256=digest(response_payload),
        service=service,
        status=status,
    )
    if len(response_payload) > MAX_HTTP_RESPONSE_BYTES:
        raise failure(
            stage="http",
            service=service,
            model_version=model_version,
            operation=operation,
            shape="oversize-response",
            observed=len(response_payload),
            fix_hint="inspect-response-pagination-and-limits",
        )
    if not response_payload:
        return None
    try:
        return json.loads(response_payload, object_pairs_hook=_reject_duplicate_keys)
    except (ValueError, json.JSONDecodeError) as error:
        raise failure(
            stage="http",
            service=service,
            model_version=model_version,
            operation=operation,
            shape="invalid-json",
            observed=response_payload,
            fix_hint="compare-the-pinned-wire-contract",
        ) from error


def _public_operation_routes() -> tuple[
    tuple[tuple[str, str, re.Pattern[str]], ...], Mapping[str, str]
]:
    path = ROOT / "contract/operations.normalized.json"
    try:
        payload = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
        )
        operations = payload["operations"]
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise failure(
            stage="provenance",
            service="load-e2e",
            model_version=LOAD_MODEL_VERSION,
            operation="load-operation-manifest",
            shape="invalid-normalized-operation-manifest",
            observed={"type": type(error).__name__, "error": repr(error)},
            fix_hint="regenerate-the-public-operation-contract",
        ) from error
    rest: list[tuple[str, str, re.Pattern[str]]] = []
    grpc: dict[str, str] = {}
    for operation in operations:
        if not isinstance(operation, dict) or not isinstance(operation.get("id"), str):
            raise failure(
                stage="provenance",
                service="load-e2e",
                model_version=LOAD_MODEL_VERSION,
                operation="load-operation-manifest",
                shape="invalid-operation-record",
                observed=operation,
                fix_hint="regenerate-the-public-operation-contract",
            )
        operation_id = operation["id"]
        rest_shape = operation.get("rest")
        if isinstance(rest_shape, dict):
            declared_path = rest_shape.get("path")
            method = rest_shape.get("method")
            if not isinstance(declared_path, str) or not isinstance(method, str):
                raise failure(
                    stage="provenance",
                    service="load-e2e",
                    model_version=LOAD_MODEL_VERSION,
                    operation="load-operation-manifest",
                    shape="invalid-rest-operation",
                    observed=operation_id,
                    fix_hint="regenerate-the-public-operation-contract",
                )
            pattern = re.sub(r"\\\{[^/]+\\\}", "[^/]+", re.escape(declared_path))
            rest.append((operation_id, method, re.compile(f"^{pattern}$")))
        grpc_shape = operation.get("grpc")
        if isinstance(grpc_shape, dict):
            service = grpc_shape.get("service")
            rpc_method = grpc_shape.get("method")
            if not isinstance(service, str) or not isinstance(rpc_method, str):
                raise failure(
                    stage="provenance",
                    service="load-e2e",
                    model_version=LOAD_MODEL_VERSION,
                    operation="load-operation-manifest",
                    shape="invalid-grpc-operation",
                    observed=operation_id,
                    fix_hint="regenerate-the-public-operation-contract",
                )
            rpc = f"/{service}/{rpc_method}"
            if rpc in grpc:
                raise failure(
                    stage="provenance",
                    service="load-e2e",
                    model_version=LOAD_MODEL_VERSION,
                    operation="load-operation-manifest",
                    shape="duplicate-grpc-route",
                    observed=rpc,
                    fix_hint="regenerate-the-public-operation-contract",
                )
            grpc[rpc] = operation_id
    return tuple(rest), grpc


_REST_OPERATION_ROUTES, _GRPC_OPERATION_ROUTES = _public_operation_routes()


def _http_operation_id(method: Any, path: Any) -> str | None:
    if not isinstance(method, str) or not isinstance(path, str):
        return None
    for operation, expected_method, pattern in _REST_OPERATION_ROUTES:
        if method == expected_method and pattern.fullmatch(path):
            return operation
    return None


def _grpc_operation_id(rpc: Any) -> str | None:
    if not isinstance(rpc, str):
        return None
    return _GRPC_OPERATION_ROUTES.get(rpc)


def source_fingerprint() -> dict[str, Any]:
    paths = [ROOT / "go.mod", ROOT / "go.sum"]
    for directory in (ROOT / "cmd", ROOT / "internal"):
        paths.extend(directory.rglob("*.go"))
    paths = sorted({path.resolve() for path in paths})
    hasher = hashlib.sha256()
    for path in paths:
        relative = path.relative_to(ROOT).as_posix().encode()
        hasher.update(len(relative).to_bytes(4, "big"))
        hasher.update(relative)
        payload = path.read_bytes()
        hasher.update(len(payload).to_bytes(8, "big"))
        hasher.update(payload)
    return {"fileCount": len(paths), "sha256": "sha256:" + hasher.hexdigest()}


def write_evidence(path: Path, evidence: Mapping[str, Any]) -> dict[str, Any]:
    encoded = (
        json.dumps(evidence, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
    ).encode("ascii")
    path.write_bytes(encoded)
    os.chmod(path, 0o600)
    metadata = file_metadata(path)
    event(
        boundary="evidence",
        model_version="bqemu-load-e2e/v1",
        operation="write-evidence",
        service="load-e2e",
        **metadata,
    )
    return metadata
