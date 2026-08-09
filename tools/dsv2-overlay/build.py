#!/usr/bin/env python3
"""Build the reviewed one-class DSv2 streaming visibility overlay.

The build consumes Maven binaries only; it never clones or builds the upstream
connector. The lock binds the input JAR, target class, existing batch methods,
new hook bytecode, Javassist release, and deterministic output byte-for-byte.

Official sources:
https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/719817782a214b8ca72be520870013a3e0253d92/spark-bigquery-connector-common/src/main/java/com/google/cloud/spark/bigquery/write/context/DataSourceWriterContext.java#L38-L50
https://www.javassist.org/tutorial/tutorial.html
https://repo.maven.apache.org/maven2/org/javassist/javassist/3.30.2-GA/
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
import urllib.request
from zipfile import BadZipFile, ZipFile


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
TOOL_ROOT = Path(__file__).resolve().parent
DEFAULT_LOCK = TOOL_ROOT / "overlay.lock.json"
DEFAULT_CACHE = REPOSITORY_ROOT / ".artifacts" / "spark"
SHA256 = re.compile(r"^[a-f0-9]{64}$")
SAFE_FIELD = re.compile(r"^[A-Za-z0-9][A-Za-z0-9:._-]{0,127}$")
TARGET_CLASS = (
    "com.google.cloud.spark.bigquery.write.context."
    "BigQueryDirectDataSourceWriterContext"
)
TARGET_ENTRY = TARGET_CLASS.replace(".", "/") + ".class"
MESSAGE_ARRAY = (
    "[Lcom/google/cloud/spark/bigquery/write/context/"
    "WriterCommitMessageContext;"
)
BATCH_DESCRIPTOR = f"({MESSAGE_ARRAY})V"
STREAMING_DESCRIPTOR = f"(J{MESSAGE_ARRAY})V"


class BuildFailure(RuntimeError):
    def __init__(self, stage: str, shape: str, fingerprint: str, fix_hint: str):
        self.stage = stage
        self.shape = shape
        self.fingerprint = fingerprint
        self.fix_hint = fix_hint
        super().__init__(stage)


def _event(
    *, stage: str, shape: str, fingerprint: str, status: str, fix_hint: str
) -> None:
    print(
        " ".join(
            (
                "version=0.44.2",
                "operation=build-dsv2-streaming-overlay",
                f"stage={stage}",
                f"shape={shape}",
                f"fingerprint={fingerprint}",
                f"status={status}",
                f"fix_hint={fix_hint}",
            )
        ),
        flush=True,
    )


def _positive_seconds(value: object) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise BuildFailure(
            "configuration", "timeout:not-number", "sha256:none", "set-positive-timeout"
        ) from error
    if not math.isfinite(parsed) or parsed <= 0:
        raise BuildFailure(
            "configuration", "timeout:not-positive", "sha256:none", "set-positive-timeout"
        )
    return parsed


def _digest(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
                size += len(chunk)
    except OSError as error:
        raise BuildFailure(
            "artifact-read",
            type(error).__name__,
            "sha256:none",
            "fetch-reviewed-build-inputs",
        ) from None
    return digest.hexdigest(), size


def _require_artifact(path: Path, spec: dict[str, object], stage: str) -> None:
    actual_hash, actual_size = _digest(path)
    if actual_hash != spec["sha256"] or actual_size != spec["size"]:
        raise BuildFailure(
            stage,
            f"bytes:{actual_size}",
            f"sha256:{actual_hash}",
            "refresh-only-after-source-and-bytecode-review",
        )


def _load_lock(path: Path) -> dict[str, object]:
    try:
        lock = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise BuildFailure(
            "lock", type(error).__name__, "sha256:none", "restore-reviewed-overlay-lock"
        ) from None
    expected_top = {
        "schemaVersion",
        "overlayId",
        "connectorVersion",
        "sourceCommit",
        "inputArtifact",
        "targetClass",
        "patch",
        "outputArtifact",
        "builderSource",
        "buildTool",
    }
    if set(lock) != expected_top:
        raise BuildFailure("lock", "top-level-shape", "sha256:none", "review-lock-schema")
    _validate_lock_binding(lock)
    return lock


def _validate_lock_binding(lock: dict[str, object]) -> None:
    def require_mapping(value: object, stage: str) -> dict[str, object]:
        if not isinstance(value, dict):
            raise BuildFailure(stage, "not-object", "sha256:none", "review-lock-schema")
        return value

    def require_keys(value: dict[str, object], expected: set[str], stage: str) -> None:
        if set(value) != expected:
            raise BuildFailure(stage, "key-set-drift", "sha256:none", "review-lock-schema")

    if (
        lock.get("schemaVersion") != "1"
        or lock.get("overlayId") != "dsv2-spark-3.5-streaming-visibility-0.44.2"
        or lock.get("connectorVersion") != "0.44.2"
        or lock.get("sourceCommit")
        != "719817782a214b8ca72be520870013a3e0253d92"
    ):
        raise BuildFailure(
            "lock-version", "connector-source-binding", "sha256:none", "review-lock-version"
        )

    artifact = require_mapping(lock.get("inputArtifact"), "input-artifact-lock")
    require_keys(
        artifact,
        {"kind", "url", "output", "size", "sha256"},
        "input-artifact-lock",
    )
    target = require_mapping(lock.get("targetClass"), "target-class-lock")
    require_keys(
        target,
        {"name", "entry", "size", "sha256", "requiredMethods", "forbiddenMethods"},
        "target-class-lock",
    )
    patch = require_mapping(lock.get("patch"), "patch-lock")
    require_keys(patch, {"commitGuard", "methods"}, "patch-lock")
    commit_guard = require_mapping(patch.get("commitGuard"), "commit-guard-lock")
    require_keys(
        commit_guard,
        {"writeAtLeastOnce", "tableToWriteDeleteOnAbort", "failure"},
        "commit-guard-lock",
    )
    output = require_mapping(lock.get("outputArtifact"), "output-artifact-lock")
    require_keys(
        output,
        {"kind", "output", "entries", "classSize", "classSha256", "size", "sha256"},
        "output-artifact-lock",
    )
    builder_source = require_mapping(lock.get("builderSource"), "builder-source-lock")
    require_keys(builder_source, {"ref", "size", "sha256"}, "builder-source-lock")
    tool = require_mapping(lock.get("buildTool"), "build-tool-lock")
    require_keys(
        tool, {"javaRelease", "runtimeJavaMajor", "javassist"}, "build-tool-lock"
    )
    javassist = require_mapping(tool.get("javassist"), "javassist-lock")
    require_keys(
        javassist,
        {"version", "url", "output", "size", "sha256"},
        "javassist-lock",
    )
    required = target.get("requiredMethods")
    forbidden = target.get("forbiddenMethods")
    methods = patch.get("methods")
    if not all(isinstance(value, list) for value in (required, forbidden, methods)):
        raise BuildFailure(
            "method-descriptor", "method-list-shape", "sha256:none", "review-method-lock"
        )
    for item in required:
        require_keys(
            require_mapping(item, "method-descriptor"),
            {"name", "descriptor", "codeBytes", "codeSha256"},
            "method-descriptor",
        )
    for item in forbidden:
        require_keys(
            require_mapping(item, "method-descriptor"),
            {"name", "descriptor"},
            "method-descriptor",
        )
    for item in methods:
        require_keys(
            require_mapping(item, "patched-method-descriptor"),
            {"name", "descriptor", "delegate", "codeBytes", "codeSha256"},
            "patched-method-descriptor",
        )

    if artifact != {
        "kind": "maven-jar",
        "url": "https://repo.maven.apache.org/maven2/com/google/cloud/spark/spark-3.5-bigquery/0.44.2/spark-3.5-bigquery-0.44.2.jar",
        "output": "spark-3.5-bigquery-0.44.2.jar",
        "size": 42618495,
        "sha256": "2e6bbb41bcaf56ae17a5488dd4453698bd35f13e9849f4daed744ca7b57b053f",
    }:
        raise BuildFailure(
            "input-artifact-lock",
            "maven-coordinate-or-bytes-drift",
            "sha256:none",
            "review-input-artifact",
        )
    if (
        target.get("name") != TARGET_CLASS
        or target.get("entry") != TARGET_ENTRY
        or target.get("size") != 20291
        or target.get("sha256")
        != "3df68a5c1912fee08a1099399f2616bb1918566abf93a3f31194412584d31a63"
    ):
        raise BuildFailure(
            "target-class-binding", "name-or-entry-drift", "sha256:none", "review-target-class"
        )
    if required != [
        {
            "name": "commit",
            "descriptor": BATCH_DESCRIPTOR,
            "codeBytes": 436,
            "codeSha256": "04246c9daf6eb684c0704a9005e4346b9f2755f834ca949177ab54f9b8335cc6",
        },
        {
            "name": "abort",
            "descriptor": BATCH_DESCRIPTOR,
            "codeBytes": 56,
            "codeSha256": "b9b627f168c61c5c8e7e5c64225a8b1a55318c106758d3c084edb6f60eafa747",
        },
    ]:
        raise BuildFailure(
            "method-descriptor", "batch-method-drift", "sha256:none", "review-method-lock"
        )
    if forbidden != [
        {"name": "onDataStreamingWriterCommit", "descriptor": STREAMING_DESCRIPTOR},
        {"name": "onDataStreamingWriterAbort", "descriptor": STREAMING_DESCRIPTOR},
    ]:
        raise BuildFailure(
            "method-descriptor", "upstream-hook-drift", "sha256:none", "review-method-lock"
        )
    if methods != [
        {
            "name": "onDataStreamingWriterCommit",
            "descriptor": STREAMING_DESCRIPTOR,
            "delegate": "commit",
            "codeBytes": 34,
            "codeSha256": "6174e9886129f75e6e0ecb894887605ab3ce4d430e2c9c76ae3eec79d615e8e1",
        },
        {
            "name": "onDataStreamingWriterAbort",
            "descriptor": STREAMING_DESCRIPTOR,
            "delegate": "abort",
            "codeBytes": 6,
            "codeSha256": "c2f4cab39d82fb3d4459c1c1d4f6d37ceacf45e381b3aeb4b9260dd0d2f0a3ee",
        },
    ]:
        raise BuildFailure(
            "patched-method-descriptor",
            "delegation-drift",
            "sha256:none",
            "review-patch-method-lock",
        )
    if commit_guard != {
        "writeAtLeastOnce": False,
        "tableToWriteDeleteOnAbort": False,
        "failure": "java.lang.IllegalStateException",
    }:
        raise BuildFailure(
            "commit-guard-lock",
            "mode-policy-drift",
            "sha256:none",
            "keep-exact-pre-existing-append-only",
        )
    if output != {
        "kind": "one-class-overlay-jar",
        "output": "spark-3.5-bigquery-0.44.2-dsv2-streaming-overlay.jar",
        "entries": [TARGET_ENTRY],
        "classSize": 20673,
        "classSha256": "1f41fa60279c39e9fbc144ff2d6252b78dffa1a505f329e51af60dcee91af67d",
        "size": 20949,
        "sha256": "1e4d3705834745aa662442eb41f4aa99e6f7a1a89aa51b8aae1eb93c7c6c5bd3",
    }:
        raise BuildFailure(
            "output-artifact-lock", "entry-policy-drift", "sha256:none", "keep-one-class-entry"
        )
    if builder_source != {
        "ref": "tools/dsv2-overlay/src/dev/bqemu/overlay/OverlayBuilder.java",
        "size": 14332,
        "sha256": "405950777605aa26440e59aa2c99da50b65734c3d33970eb4f232ba6da610c4a",
    }:
        raise BuildFailure(
            "builder-source-lock",
            "source-binding-drift",
            "sha256:none",
            "review-builder-source",
        )
    if (
        tool.get("javaRelease") != "11"
        or tool.get("runtimeJavaMajor") != "17"
        or javassist
        != {
            "version": "3.30.2-GA",
            "url": "https://repo.maven.apache.org/maven2/org/javassist/javassist/3.30.2-GA/javassist-3.30.2-GA.jar",
            "output": "javassist-3.30.2-GA.jar",
            "size": 794714,
            "sha256": "eba37290994b5e4868f3af98ff113f6244a6b099385d9ad46881307d3cb01aaf",
        }
    ):
        raise BuildFailure(
            "build-tool-lock", "toolchain-version-drift", "sha256:none", "review-build-tool"
        )
    hashes = [
        artifact.get("sha256"),
        target.get("sha256"),
        *(item.get("codeSha256") for item in required),
        *(item.get("codeSha256") for item in methods),
        output.get("classSha256"),
        output.get("sha256"),
        builder_source.get("sha256"),
        javassist.get("sha256"),
    ]
    if any(not isinstance(value, str) or not SHA256.fullmatch(value) for value in hashes):
        raise BuildFailure(
            "fingerprint-lock", "non-sha256", "sha256:none", "review-artifact-fingerprints"
        )


def _fetch_javassist(
    spec: dict[str, object], cache: Path, timeout: float, configured: Path | None
) -> Path:
    target = configured or cache / str(spec["output"])
    if target.is_file():
        _require_artifact(target, spec, "javassist-artifact")
        return target
    if configured is not None:
        _require_artifact(target, spec, "javassist-artifact")
        return target
    cache.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(dir=cache, prefix="javassist.", delete=False) as stream:
            temporary = Path(stream.name)
            request = urllib.request.Request(
                str(spec["url"]), headers={"User-Agent": "bqemu-overlay-builder/1"}
            )
            deadline = time.monotonic() + timeout
            with urllib.request.urlopen(request, timeout=timeout) as response:
                content_length = response.headers.get("Content-Length")
                if content_length is not None and int(content_length) != spec["size"]:
                    raise BuildFailure(
                        "javassist-download",
                        "content-length-drift",
                        "sha256:none",
                        "use-reviewed-maven-artifact",
                    )
                written = 0
                while chunk := response.read(1024 * 1024):
                    written += len(chunk)
                    if written > spec["size"] or time.monotonic() > deadline:
                        raise BuildFailure(
                            "javassist-download",
                            "size-or-deadline-exceeded",
                            "sha256:none",
                            "use-reviewed-maven-artifact-and-timeout",
                        )
                    stream.write(chunk)
        _require_artifact(temporary, spec, "javassist-artifact")
        temporary.replace(target)
        temporary = None
        return target
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _java_arguments(
    lock: dict[str, object], input_jar: Path, temporary_output: Path
) -> list[str]:
    artifact = lock["inputArtifact"]
    target = lock["targetClass"]
    required = {item["name"]: item for item in target["requiredMethods"]}
    patched = {item["delegate"]: item for item in lock["patch"]["methods"]}
    output = lock["outputArtifact"]
    values = {
        "input": str(input_jar),
        "output": str(temporary_output),
        "input-size": str(artifact["size"]),
        "input-sha": str(artifact["sha256"]),
        "target-class": str(target["name"]),
        "target-entry": str(target["entry"]),
        "target-size": str(target["size"]),
        "target-sha": str(target["sha256"]),
        "commit-name": "commit",
        "commit-desc": str(required["commit"]["descriptor"]),
        "commit-code-size": str(required["commit"]["codeBytes"]),
        "commit-code-sha": str(required["commit"]["codeSha256"]),
        "abort-name": "abort",
        "abort-desc": str(required["abort"]["descriptor"]),
        "abort-code-size": str(required["abort"]["codeBytes"]),
        "abort-code-sha": str(required["abort"]["codeSha256"]),
        "commit-hook": str(patched["commit"]["name"]),
        "commit-hook-desc": str(patched["commit"]["descriptor"]),
        "commit-hook-code-size": str(patched["commit"]["codeBytes"]),
        "commit-hook-code-sha": str(patched["commit"]["codeSha256"]),
        "abort-hook": str(patched["abort"]["name"]),
        "abort-hook-desc": str(patched["abort"]["descriptor"]),
        "abort-hook-code-size": str(patched["abort"]["codeBytes"]),
        "abort-hook-code-sha": str(patched["abort"]["codeSha256"]),
        "output-class-size": str(output["classSize"]),
        "output-class-sha": str(output["classSha256"]),
    }
    return [part for key, value in values.items() for part in ("--" + key, value)]


def _run(command: list[str], timeout: float, stage: str) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            cwd=REPOSITORY_ROOT,
            check=True,
            timeout=timeout,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        output = getattr(error, "stdout", "") or getattr(error, "output", "") or ""
        safe_line = next((line for line in output.splitlines() if line.startswith("version=")), None)
        if safe_line is not None:
            try:
                fields = _parse_builder_event(safe_line)
            except BuildFailure:
                pass
            else:
                raise BuildFailure(
                    fields["stage"], fields["shape"], fields["fingerprint"], fields["fix_hint"]
                ) from None
        raise BuildFailure(
            stage,
            type(error).__name__,
            "sha256:" + hashlib.sha256(type(error).__name__.encode()).hexdigest(),
            "inspect-version-locked-builder-contract",
        ) from None


def _parse_builder_event(line: str) -> dict[str, str]:
    expected = {"version", "operation", "stage", "shape", "fingerprint", "status", "fix_hint"}
    fields: dict[str, str] = {}
    for token in line.split():
        if token.count("=") != 1:
            raise BuildFailure(
                "builder-diagnostic", "invalid-token", "sha256:none", "inspect-builder-contract"
            )
        key, value = token.split("=", 1)
        if key in fields:
            raise BuildFailure(
                "builder-diagnostic", "duplicate-key", "sha256:none", "inspect-builder-contract"
            )
        fields[key] = value
    if set(fields) != expected or fields["version"] != "0.44.2" or fields["operation"] != "build-dsv2-streaming-overlay":
        raise BuildFailure(
            "builder-diagnostic", "schema-drift", "sha256:none", "inspect-builder-contract"
        )
    if fields["status"] not in {"built", "failed"} or not re.fullmatch(
        r"sha256:(?:[a-f0-9]{64}|none)", fields["fingerprint"]
    ):
        raise BuildFailure(
            "builder-diagnostic", "value-drift", "sha256:none", "inspect-builder-contract"
        )
    for key in ("stage", "shape", "fix_hint"):
        if not SAFE_FIELD.fullmatch(fields[key]):
            raise BuildFailure(
                "builder-diagnostic", "unsafe-value", "sha256:none", "inspect-builder-contract"
            )
    return fields


def _verify_java_runtime(timeout: float, expected_major: str) -> None:
    result = _run(
        ["java", "-XshowSettings:properties", "-version"], timeout, "java-runtime"
    )
    match = re.search(r"^\s*java\.specification\.version\s*=\s*([^\s]+)\s*$", result.stdout, re.MULTILINE)
    if match is None or match.group(1) != expected_major:
        raise BuildFailure(
            "java-runtime",
            "major-version-drift",
            "sha256:none",
            "use-the-reviewed-java-runtime",
        )


def _validate_output(path: Path, lock: dict[str, object]) -> None:
    output = lock["outputArtifact"]
    _require_artifact(path, output, "output-artifact")
    try:
        with ZipFile(path) as archive:
            entries = archive.namelist()
            class_bytes = archive.read(entries[0]) if len(entries) == 1 else b""
    except (BadZipFile, KeyError, OSError):
        raise BuildFailure(
            "output-artifact", "invalid-jar", "sha256:none", "rebuild-reviewed-overlay"
        ) from None
    class_hash = hashlib.sha256(class_bytes).hexdigest()
    if (
        entries != output["entries"]
        or len(class_bytes) != output["classSize"]
        or class_hash != output["classSha256"]
    ):
        raise BuildFailure(
            "output-artifact",
            f"entries:{len(entries)},class-bytes:{len(class_bytes)}",
            f"sha256:{class_hash}",
            "keep-exactly-one-reviewed-class-entry",
        )


def build(
    *,
    lock_path: Path,
    input_jar: Path,
    output_path: Path,
    cache: Path,
    javassist_path: Path | None,
    timeout: float,
) -> Path:
    lock = _load_lock(lock_path)
    _require_artifact(input_jar, lock["inputArtifact"], "input-artifact")
    source = REPOSITORY_ROOT / str(lock["builderSource"]["ref"])
    _require_artifact(source, lock["builderSource"], "builder-source")
    javassist = _fetch_javassist(
        lock["buildTool"]["javassist"], cache, timeout, javassist_path
    )
    _verify_java_runtime(timeout, str(lock["buildTool"]["runtimeJavaMajor"]))
    with tempfile.TemporaryDirectory(prefix="bqemu-overlay-build-") as directory:
        work = Path(directory)
        classes = work / "classes"
        classes.mkdir()
        _run(
            [
                "javac",
                "--release",
                str(lock["buildTool"]["javaRelease"]),
                "-cp",
                str(javassist),
                "-d",
                str(classes),
                str(source),
            ],
            timeout,
            "compile-builder",
        )
        temporary_output = work / str(lock["outputArtifact"]["output"])
        result = _run(
            [
                "java",
                "-cp",
                os.pathsep.join((str(classes), str(javassist))),
                "dev.bqemu.overlay.OverlayBuilder",
                *_java_arguments(lock, input_jar, temporary_output),
            ],
            timeout,
            "run-builder",
        )
        events = [
            _parse_builder_event(line)
            for line in result.stdout.splitlines()
            if line.startswith("version=")
        ]
        if len(events) != 1 or events[0]["status"] != "built":
            raise BuildFailure(
                "builder-diagnostic",
                f"event-count:{len(events)}",
                "sha256:none",
                "inspect-builder-contract",
            )
        event = events[0]
        _event(
            stage=event["stage"],
            shape=event["shape"],
            fingerprint=event["fingerprint"],
            status=event["status"],
            fix_hint=event["fix_hint"],
        )
        _validate_output(temporary_output, lock)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_output.replace(output_path)
    output_hash, _ = _digest(output_path)
    _event(
        stage="output-verified",
        shape="entries:1",
        fingerprint=f"sha256:{output_hash}",
        status="verified",
        fix_hint="none",
    )
    return output_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lock", type=Path, default=DEFAULT_LOCK)
    parser.add_argument("--input", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--javassist", type=Path)
    parser.add_argument("--timeout-seconds")
    arguments = parser.parse_args()
    timeout = _positive_seconds(
        arguments.timeout_seconds
        or os.getenv("BQEMU_DSV2_OVERLAY_BUILD_TIMEOUT_SECONDS", "120")
    )
    lock = _load_lock(arguments.lock)
    input_jar = arguments.input or DEFAULT_CACHE / str(lock["inputArtifact"]["output"])
    output = arguments.output or DEFAULT_CACHE / str(lock["outputArtifact"]["output"])
    build(
        lock_path=arguments.lock,
        input_jar=input_jar,
        output_path=output,
        cache=arguments.cache,
        javassist_path=arguments.javassist,
        timeout=timeout,
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except BuildFailure as error:
        _event(
            stage=error.stage,
            shape=error.shape,
            fingerprint=error.fingerprint,
            status="failed",
            fix_hint=error.fix_hint,
        )
        sys.exit(1)
    except Exception as error:
        _event(
            stage="builder",
            shape=type(error).__name__,
            fingerprint="sha256:" + hashlib.sha256(type(error).__name__.encode()).hexdigest(),
            status="failed",
            fix_hint="inspect-version-locked-builder-contract",
        )
        sys.exit(1)
