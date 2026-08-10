#!/usr/bin/env python3
"""Fetch the reviewed Flink BigQuery connector artifact without source checkout.

The CDC contract intentionally consumes the released Maven jar.  This keeps
the test input reproducible and prevents an upstream git clone or build from
silently changing the client under test.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import urllib.request


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LOCK = ROOT / "tests" / "integration" / "flink" / "artifacts.lock.json"
DEFAULT_OUTPUT = ROOT / ".artifacts" / "flink"


def _timeout() -> float:
    try:
        value = float(os.getenv("BQEMU_ARTIFACT_TIMEOUT_SECONDS", "120"))
    except ValueError as error:
        raise ValueError("BQEMU_ARTIFACT_TIMEOUT_SECONDS must be positive") from error
    if value <= 0:
        raise ValueError("BQEMU_ARTIFACT_TIMEOUT_SECONDS must be positive")
    return value


def _event(stage: str, shape: str, fingerprint: str, status: str, fix_hint: str) -> None:
    print(
        " ".join(
            (
                "version=flink-bigquery-connector-1.2.0",
                "operation=fetch-flink-artifact",
                f"stage={stage}",
                f"shape={shape}",
                f"fingerprint={fingerprint}",
                f"status={status}",
                f"fix_hint={fix_hint}",
            )
        ),
        flush=True,
    )


def load_lock(path: Path) -> dict[str, object]:
    with path.open(encoding="utf-8") as stream:
        lock = json.load(stream)
    artifact = lock.get("artifact")
    runtime = lock.get("runtime")
    source = lock.get("source")
    if not isinstance(artifact, dict) or not isinstance(runtime, dict) or not isinstance(source, dict):
        raise ValueError("unreviewed Flink artifact lock shape")
    required = {
        "schemaVersion": "1",
        "connectorVersion": "1.2.0",
        "flinkVersion": "1.17.1",
    }
    if any(lock.get(name) != value for name, value in required.items()):
        raise ValueError("unreviewed Flink artifact lock version")
    if (
        artifact.get("id") != "flink-1.17-connector-bigquery-shaded"
        or artifact.get("kind") != "maven-jar"
        or not isinstance(artifact.get("url"), str)
        or not artifact["url"].startswith("https://repo.maven.apache.org/")
        or not isinstance(artifact.get("output"), str)
        or not isinstance(artifact.get("size"), int)
        or artifact["size"] <= 0
        or not isinstance(artifact.get("sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", artifact["sha256"]) is None
        or runtime.get("id") != "apache-flink-1.17.1-scala_2.12-java11-linux-amd64"
        or runtime.get("kind") != "oci-image"
        or runtime.get("image")
        != "flink@sha256:d50dd931a53add0125d35e6cc47d13c15fa6bbb65050b975b95d4d89c2a82581"
        or source.get("kind") != "release-tag"
        or not isinstance(source.get("url"), str)
        or not isinstance(source.get("cdcSchemaProvider"), str)
    ):
        raise ValueError("unreviewed Flink artifact binding")
    return lock


def fetch(lock: dict[str, object], output: Path, timeout: float) -> Path:
    artifact = lock["artifact"]
    assert isinstance(artifact, dict)
    target = output / str(artifact["output"])
    expected_size, expected_hash = int(artifact["size"]), str(artifact["sha256"])
    if target.is_file():
        digest = hashlib.sha256(target.read_bytes()).hexdigest()
        if target.stat().st_size == expected_size and digest == expected_hash:
            _event("cache-check", "maven-jar", f"sha256:{expected_hash}", "hit", "none")
            return target
    output.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(prefix=target.name + ".", dir=output, delete=False) as stream:
            temporary = Path(stream.name)
            digest, size = hashlib.sha256(), 0
            request = urllib.request.Request(str(artifact["url"]), headers={"User-Agent": "bqemu-contract-fetch/1"})
            with urllib.request.urlopen(request, timeout=timeout) as response:
                while chunk := response.read(1024 * 1024):
                    size += len(chunk)
                    if size > expected_size:
                        raise ValueError("artifact exceeds locked byte size")
                    digest.update(chunk)
                    stream.write(chunk)
        if size != expected_size or digest.hexdigest() != expected_hash:
            raise ValueError(f"artifact mismatch size={size} fingerprint=sha256:{digest.hexdigest()}")
        temporary.replace(target)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    _event("checksum", "maven-jar", f"sha256:{expected_hash}", "verified", "none")
    return target


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lock", type=Path, default=DEFAULT_LOCK)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    arguments = parser.parse_args()
    print(fetch(load_lock(arguments.lock), arguments.output, _timeout()))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        _event("download", type(error).__name__, "sha256:none", "failed", "verify-network-or-refresh-reviewed-lock")
        raise
