#!/usr/bin/env python3
"""Execute one normalized consumer case through an explicit runner adapter."""

from __future__ import annotations

from abc import ABC, abstractmethod
import argparse
from dataclasses import dataclass
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any, Sequence
import urllib.request
import xml.etree.ElementTree as ET


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = REPOSITORY_ROOT / "contract" / "consumers.normalized.json"


class ContractError(RuntimeError):
    pass


@dataclass(frozen=True)
class CaseContext:
    case: dict[str, Any]
    repository_root: Path
    artifact_root: Path

    @property
    def case_id(self) -> str:
        return str(self.case["id"])

    @property
    def versions(self) -> dict[str, str]:
        return self.case["runtimeProfile"]["versions"]

    @property
    def runner_id(self) -> str:
        return str(self.case["runnerAdapter"]["id"])


class RunnerAdapter(ABC):
    def __init__(self, context: CaseContext) -> None:
        self.context = context
        self.started_at = time.time()
        self.result: subprocess.CompletedProcess[str] | None = None

    def prepare(self) -> None:
        self.context.artifact_root.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def verify_identity(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def execute_scenario(self) -> subprocess.CompletedProcess[str]:
        raise NotImplementedError

    def collect_evidence(self) -> None:
        scenarios = self.context.case["scenarioSet"]["scenarios"]
        events = collect_actual_events(self.context, scenarios)
        evidence = {
            "schemaVersion": "1",
            "caseId": self.context.case_id,
            "runnerAdapterId": self.context.runner_id,
            "scenarioIds": [scenario["id"] for scenario in scenarios],
            "exitCode": self.result.returncode if self.result else 1,
            "durationMillis": round((time.time() - self.started_at) * 1000),
            "events": events,
        }
        _write_json(self.context.artifact_root / "evidence.json", evidence)
        if self.result is not None and self.result.returncode != 0:
            expected = [operation for scenario in scenarios for operation in scenario["operationIds"]]
            observed = [event["operationId"] for event in events]
            difference = first_contract_difference(expected, observed)
            _write_json(
                self.context.artifact_root / "diff.json",
                {
                    "schemaVersion": "1",
                    "caseId": self.context.case_id,
                    "scenarioId": _scenario_for_operation(scenarios, difference) if difference else None,
                    "phase": "runner",
                    "operationId": difference,
                    "field": "process.exitCode",
                    "expected": 0,
                    "actual": self.result.returncode,
                },
            )

    def cleanup(self) -> None:
        return

    def run(self) -> int:
        try:
            self.prepare()
            self.verify_identity()
            self.result = self.execute_scenario()
            return self.result.returncode
        except Exception as error:
            self.result = subprocess.CompletedProcess([], 1, "", str(error))
            (self.context.artifact_root / "runner-error.txt").write_text(
                f"{type(error).__name__}: {error}\n", encoding="utf-8"
            )
            return 1
        finally:
            self.collect_evidence()
            self.cleanup()

    def _run(self, command: Sequence[str], *, environment: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        process_environment = (environment or os.environ).copy()
        process_environment["BQEMU_CONSUMER_CASE_ID"] = self.context.case_id
        process_environment["BQEMU_RUNTIME_VERSIONS_JSON"] = json.dumps(
            self.context.versions, sort_keys=True, separators=(",", ":")
        )
        result = subprocess.run(
            list(command),
            cwd=self.context.repository_root,
            env=process_environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        (self.context.artifact_root / "runner.log").write_text(
            _redact(result.stdout), encoding="utf-8"
        )
        return result


class PythonPytestAdapter(RunnerAdapter):
    def prepare(self) -> None:
        super().prepare()
        expected = self.context.versions["client"]
        wheel = next(
            (artifact for artifact in self.context.case["artifacts"] if artifact["uri"].endswith(".whl")),
            None,
        )
        if wheel is None:
            raise ContractError("Python client case has no immutable wheel artifact")
        wheel_path = _materialize_artifact(self.context, wheel)
        try:
            actual = importlib.metadata.version("google-cloud-bigquery")
        except importlib.metadata.PackageNotFoundError:
            actual = ""
        if actual == expected:
            return
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "--no-deps", str(wheel_path)],
            cwd=self.context.repository_root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        if result.returncode != 0:
            raise ContractError("installing the case-declared Python wheel failed")

    def verify_identity(self) -> None:
        actual = importlib.metadata.version("google-cloud-bigquery")
        _require_equal("google-cloud-bigquery", actual, self.context.versions["client"])
        _require_equal("Python", f"{sys.version_info.major}.{sys.version_info.minor}", self.context.versions["python"])

    def execute_scenario(self) -> subprocess.CompletedProcess[str]:
        return self._run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-c",
                "tests/python/pytest.ini",
                "tests/python",
                f"--basetemp={self.context.artifact_root / 'pytest'}",
                f"--junitxml={self.context.artifact_root / 'junit.xml'}",
            ]
        )


class BQCLIAdapter(RunnerAdapter):
    def verify_identity(self) -> None:
        result = self._run([os.getenv("BQEMU_BQCLI_BIN", "bq"), "version"])
        if result.returncode != 0:
            raise ContractError("bq version command failed")
        expected = f"This is BigQuery CLI {self.context.versions['bq']}"
        _require_equal("bq", result.stdout.strip(), expected)

    def execute_scenario(self) -> subprocess.CompletedProcess[str]:
        environment = os.environ.copy()
        environment["BQEMU_BQCLI_VERSION"] = self.context.versions["bq"]
        environment["BQEMU_BQCLI_ARTIFACT_DIR"] = str(self.context.artifact_root / "bqcli")
        result = self._run([sys.executable, "tests/bqcli/run_contract.py"], environment=environment)
        _write_junit(self.context.artifact_root / "junit.xml", self.context.case_id, result)
        return result


class SparkAdapter(RunnerAdapter):
    connector_path: Path
    connector_spec: dict[str, Any]

    def prepare(self) -> None:
        super().prepare()
        artifact = next(
            (
                candidate
                for candidate in self.context.case["artifacts"]
                if candidate["id"].startswith("spark-bigquery-connector-")
            ),
            None,
        )
        if artifact is None:
            raise ContractError("Spark case has no connector JAR artifact")
        self.connector_path = _materialize_artifact(self.context, artifact)
        self.connector_spec = {
            "variant": "dsv1-with-dependencies-2.12",
            "output": self.connector_path.name,
            "size": self.connector_path.stat().st_size,
            "sha256": artifact["sha256"],
            "provider": "com.google.cloud.spark.bigquery.Scala212BigQueryRelationProvider",
            "connectorVersion": self.context.versions["connector"],
        }

    def spark_environment(self) -> dict[str, str]:
        environment = os.environ.copy()
        environment["BQEMU_SPARK_CONNECTOR_JAR"] = str(self.connector_path)
        environment["BQEMU_SPARK_CONNECTOR_SPEC_JSON"] = json.dumps(
            self.connector_spec, sort_keys=True, separators=(",", ":")
        )
        return environment


class SparkPytestAdapter(SparkAdapter):
    def verify_identity(self) -> None:
        _require_equal("PySpark", importlib.metadata.version("pyspark"), self.context.versions["spark"])
        _verify_case_artifacts(self.context)

    def execute_scenario(self) -> subprocess.CompletedProcess[str]:
        return self._run(
            _spark_pytest_command(self.context, ["tests/spark/test_public_edge.py", "tests/spark/test_dsv2_streaming.py"]),
            environment=self.spark_environment(),
        )


class SparkScalaShellAdapter(SparkAdapter):
    def verify_identity(self) -> None:
        _require_equal("Spark distribution", importlib.metadata.version("pyspark"), self.context.versions["spark"])
        _verify_case_artifacts(self.context)

    def execute_scenario(self) -> subprocess.CompletedProcess[str]:
        return self._run(
            _spark_pytest_command(self.context, ["tests/spark/test_scala_public_edge.py"]),
            environment=self.spark_environment(),
        )


ADAPTERS: dict[str, type[RunnerAdapter]] = {
    "python-pytest-v1": PythonPytestAdapter,
    "bq-cli-v1": BQCLIAdapter,
    "spark-pyspark-pytest-v1": SparkPytestAdapter,
    "spark-scala-shell-v1": SparkScalaShellAdapter,
}


def _spark_pytest_command(context: CaseContext, paths: list[str]) -> list[str]:
    return [
        sys.executable,
        "-m",
        "pytest",
        "-c",
        "tests/spark/pytest.ini",
        *paths,
        f"--basetemp={context.artifact_root / 'pytest'}",
        f"--junitxml={context.artifact_root / 'junit.xml'}",
    ]


def load_manifest(manifest_path: Path) -> dict[str, Any]:
    with manifest_path.open("r", encoding="utf-8") as stream:
        manifest = json.load(stream)
    if set(manifest) != {"schemaVersion", "cases"} or manifest["schemaVersion"] != "1":
        raise ContractError("unsupported normalized consumer manifest")
    return manifest


def load_case(manifest_path: Path, case_id: str) -> dict[str, Any]:
    manifest = load_manifest(manifest_path)
    matches = [case for case in manifest["cases"] if case.get("id") == case_id]
    if len(matches) != 1:
        raise ContractError(f"consumer case {case_id!r} was not found exactly once")
    return matches[0]


def build_adapter(context: CaseContext) -> RunnerAdapter:
    adapter_type = ADAPTERS.get(context.runner_id)
    if adapter_type is None:
        raise ContractError(f"unknown runner adapter {context.runner_id!r}")
    return adapter_type(context)


def _verify_case_artifacts(context: CaseContext) -> None:
    for artifact in context.case["artifacts"]:
        if artifact["id"].startswith("spark-bigquery-connector-"):
            path = _materialize_artifact(context, artifact)
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            _require_equal(f"artifact {artifact['id']}", digest, artifact["sha256"])


def _materialize_artifact(context: CaseContext, artifact: dict[str, str]) -> Path:
    uri = artifact["uri"]
    if not uri.startswith("https://"):
        raise ContractError(f"artifact {artifact['id']} is not an HTTPS download")
    filename = uri.rsplit("/", 1)[-1]
    cache = context.repository_root / ".artifacts" / "consumer-downloads"
    cache.mkdir(parents=True, exist_ok=True)
    target = cache / f"{artifact['sha256']}-{filename}"
    if target.is_file() and hashlib.sha256(target.read_bytes()).hexdigest() == artifact["sha256"]:
        return target
    temporary = cache / f".{target.name}.{os.getpid()}.tmp"
    digest = hashlib.sha256()
    try:
        with urllib.request.urlopen(uri, timeout=float(os.getenv("BQEMU_ARTIFACT_TIMEOUT_SECONDS", "180"))) as response:
            with temporary.open("wb") as output:
                while chunk := response.read(1024 * 1024):
                    digest.update(chunk)
                    output.write(chunk)
        _require_equal(f"artifact {artifact['id']}", digest.hexdigest(), artifact["sha256"])
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def collect_actual_events(context: CaseContext, scenarios: list[dict[str, Any]]) -> list[dict[str, Any]]:
    operation_path = context.repository_root / "contract" / "operations.normalized.json"
    with operation_path.open("r", encoding="utf-8") as stream:
        operations = json.load(stream)["operations"]
    rest_routes: list[tuple[str, str, re.Pattern[str], str]] = []
    grpc_routes: dict[str, str] = {}
    for operation in operations:
        rest = operation.get("rest")
        if rest:
            pattern = re.sub(r"\\\{[^/]+\\\}", "[^/]+", re.escape(rest["path"]))
            rest_routes.append((operation["id"], rest["method"], re.compile(f"^{pattern}$"), rest["path"]))
        grpc = operation.get("grpc")
        if grpc:
            grpc_routes[f"/{grpc['service']}/{grpc['method']}"] = operation["id"]
    scenario_by_operation = {
        operation_id: scenario["id"]
        for scenario in scenarios
        for operation_id in scenario["operationIds"]
    }
    events: list[dict[str, Any]] = []
    for log_path in sorted(context.artifact_root.rglob("server.log")):
        for line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("event") != "boundary.exit":
                continue
            operation_id: str | None = None
            protocol: str | None = None
            request_shape: str | None = None
            response_status: Any = None
            if event.get("boundary") == "http":
                for candidate, method, pattern, declared_path in rest_routes:
                    if event.get("method") == method and pattern.fullmatch(str(event.get("path", ""))):
                        operation_id = candidate
                        protocol = "REST"
                        request_shape = f"{method} {declared_path}"
                        response_status = event.get("status")
                        break
            elif str(event.get("boundary", "")).startswith("grpc."):
                rpc = str(event.get("rpc", ""))
                operation_id = grpc_routes.get(rpc)
                protocol = "gRPC"
                request_shape = rpc
                response_status = event.get("grpc_code")
            if operation_id not in scenario_by_operation:
                continue
            events.append(
                {
                    "seq": len(events) + 1,
                    "phase": "observed-response",
                    "operationId": operation_id,
                    "protocol": protocol,
                    "requestShape": request_shape,
                    "responseStatus": response_status,
                    "correlationGroup": scenario_by_operation[operation_id],
                }
            )
    return events


def first_contract_difference(expected: list[str], observed: list[str]) -> str | None:
    cursor = 0
    for operation_id in observed:
        if cursor < len(expected) and operation_id == expected[cursor]:
            cursor += 1
    return expected[cursor] if cursor < len(expected) else None


def _scenario_for_operation(scenarios: list[dict[str, Any]], operation_id: str | None) -> str | None:
    for scenario in scenarios:
        if operation_id in scenario["operationIds"]:
            return str(scenario["id"])
    return None


def _require_equal(label: str, actual: str, expected: str) -> None:
    if actual != expected:
        raise ContractError(f"{label} identity mismatch: actual={actual!r} expected={expected!r}")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_junit(path: Path, case_id: str, result: subprocess.CompletedProcess[str]) -> None:
    suite = ET.Element("testsuite", name=case_id, tests="1", failures="1" if result.returncode else "0")
    test = ET.SubElement(suite, "testcase", classname="consumer.bq", name=case_id)
    if result.returncode:
        failure = ET.SubElement(test, "failure", message="bq consumer contract failed")
        failure.text = _redact(result.stdout)[-4000:]
    path.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(suite).write(path, encoding="unicode", xml_declaration=True)


def _redact(value: str) -> str:
    value = re.sub(r"(?i)(authorization:\s*bearer\s+)[^\s]+", r"\1[REDACTED]", value)
    value = re.sub(r"(?i)(access[_-]?token[=:]\s*)[^\s,]+", r"\1[REDACTED]", value)
    return value


def main(arguments: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--case", dest="case_id")
    selection.add_argument("--family")
    parser.add_argument("--lane", default="required")
    parser.add_argument("--all", action="store_true", dest="run_all")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--artifact-root", type=Path)
    options = parser.parse_args(arguments)
    if options.case_id:
        cases = [load_case(options.manifest, options.case_id)]
    else:
        manifest = load_manifest(options.manifest)
        cases = [
            case
            for case in manifest["cases"]
            if case["family"] == options.family and case["lane"] == options.lane
        ]
        if not cases:
            raise ContractError(f"no consumer cases for family={options.family!r} lane={options.lane!r}")
        if len(cases) != 1 and not options.run_all:
            raise ContractError("family selection returned multiple cases; pass --all explicitly")
    exit_code = 0
    for case in cases:
        artifact_root = options.artifact_root or REPOSITORY_ROOT / ".artifacts" / "consumers" / case["id"]
        context = CaseContext(case=case, repository_root=REPOSITORY_ROOT, artifact_root=artifact_root)
        exit_code = max(exit_code, build_adapter(context).run())
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
