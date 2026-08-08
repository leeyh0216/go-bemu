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
        self.artifact_evidence: list[dict[str, Any]] = []

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
        difference = compare_contract(scenarios, events)
        if self.result is not None and self.result.returncode == 0 and difference is not None:
            self.result = subprocess.CompletedProcess(
                self.result.args,
                1,
                self.result.stdout,
                "successful runner violated its normalized wire contract",
            )
            _write_junit(
                self.context.artifact_root / "junit.xml", self.context.case_id, self.result
            )
        evidence = {
            "schemaVersion": "1",
            "caseId": self.context.case_id,
            "runnerAdapterId": self.context.runner_id,
            "scenarioIds": [scenario["id"] for scenario in scenarios],
            "exitCode": self.result.returncode if self.result else 1,
            "durationMillis": round((time.time() - self.started_at) * 1000),
            "artifactEvidence": self.artifact_evidence,
            "comparison": {
                "status": "matched" if difference is None else "different",
                "expectedOperationCount": sum(len(scenario["operationExpectations"]) for scenario in scenarios),
                "observedEventCount": len([event for event in events if event["phase"] == "observed-response"]),
            },
            "events": events,
        }
        _write_json(self.context.artifact_root / "evidence.json", evidence)
        if self.result is not None and self.result.returncode != 0:
            if difference is None:
                difference = {
                    "scenarioId": None,
                    "phase": "runner",
                    "operationId": None,
                    "field": "process.exitCode",
                    "expected": 0,
                    "actual": self.result.returncode,
                }
            _write_json(
                self.context.artifact_root / "diff.json",
                {
                    "schemaVersion": "1",
                    "caseId": self.context.case_id,
                    **difference,
                },
            )

    def cleanup(self) -> None:
        return

    def run(self) -> int:
        try:
            self.prepare()
            self.verify_identity()
            self.result = self.execute_scenario()
        except Exception as error:
            self.result = subprocess.CompletedProcess([], 1, "", str(error))
            self._record_runner_error(error)
            _write_junit(
                self.context.artifact_root / "junit.xml", self.context.case_id, self.result
            )
        finally:
            try:
                self.collect_evidence()
            except Exception as error:
                self.result = subprocess.CompletedProcess([], 1, "", "evidence collection failed")
                self._record_runner_error(error)
                _write_junit(
                    self.context.artifact_root / "junit.xml", self.context.case_id, self.result
                )
            finally:
                try:
                    self.cleanup()
                except Exception as error:
                    self.result = subprocess.CompletedProcess([], 1, "", "runner cleanup failed")
                    self._record_runner_error(error)
                    _write_junit(
                        self.context.artifact_root / "junit.xml", self.context.case_id, self.result
                    )
        return self.result.returncode if self.result is not None else 1

    def _record_runner_error(self, error: Exception) -> None:
        self.context.artifact_root.mkdir(parents=True, exist_ok=True)
        error_text = str(error).encode("utf-8", errors="replace")
        (self.context.artifact_root / "runner-error.txt").write_text(
            f"error_type={type(error).__name__} error_bytes={len(error_text)} "
            f"error_digest=sha256:{hashlib.sha256(error_text).hexdigest()}\n",
            encoding="utf-8",
        )

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
        output = result.stdout.encode("utf-8", errors="replace")
        (self.context.artifact_root / "runner.log").write_text(
            f"return_code={result.returncode} output_bytes={len(output)} output_digest=sha256:{hashlib.sha256(output).hexdigest()}\n",
            encoding="utf-8",
        )
        return result


class PythonPytestAdapter(RunnerAdapter):
    def prepare(self) -> None:
        super().prepare()
        wheel = _require_artifact(self.context, "python-wheel")
        wheel_path = _materialize_artifact(self.context, wheel)
        result = subprocess.run(
            [
                os.getenv("BQEMU_UV_BIN", "uv"),
                "pip",
                "install",
                "--python",
                sys.executable,
                "--force-reinstall",
                "--no-deps",
                str(wheel_path),
            ],
            cwd=self.context.repository_root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        if result.returncode != 0:
            raise ContractError("installing the case-declared Python wheel failed")
        evidence = _materialized_evidence(wheel, wheel_path)
        evidence["installed"] = True
        self.artifact_evidence.append(evidence)

    def verify_identity(self) -> None:
        actual = importlib.metadata.version("google-cloud-bigquery")
        _require_equal("google-cloud-bigquery", actual, self.context.versions["client"])
        _require_equal("Python", f"{sys.version_info.major}.{sys.version_info.minor}", self.context.versions["python"])

    def execute_scenario(self) -> subprocess.CompletedProcess[str]:
        result = self._run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-c",
                "tests/python/pytest.ini",
                *_scenario_selectors(self.context, "pytest"),
                f"--basetemp={self.context.artifact_root / 'pytest'}",
                f"--junitxml={self.context.artifact_root / 'junit.xml'}",
            ]
        )
        junit_valid = _sanitize_junit(
            self.context.artifact_root / "junit.xml", self.context.case_id, result
        )
        if result.returncode == 0 and not junit_valid:
            return subprocess.CompletedProcess(result.args, 1, result.stdout, "pytest did not produce valid JUnit")
        return result


class BQCLIAdapter(RunnerAdapter):
    def verify_identity(self) -> None:
        result = self._run([os.getenv("BQEMU_BQCLI_BIN", "bq"), "version"])
        if result.returncode != 0:
            raise ContractError("bq version command failed")
        expected = f"This is BigQuery CLI {self.context.versions['bq']}"
        _require_equal("bq", result.stdout.strip(), expected)
        artifact = _require_artifact(self.context, "cloud-sdk-image")
        self.artifact_evidence.append(
            {
                "id": artifact["id"],
                "role": artifact["role"],
                "usage": artifact["usage"],
                "sha256": artifact["sha256"],
                "status": "tool-version-identity-matched",
                "materialized": False,
                "note": "The OCI digest is release provenance; setup-gcloud supplies the version-verified executable.",
            }
        )

    def execute_scenario(self) -> subprocess.CompletedProcess[str]:
        selectors = _scenario_selectors(self.context, "bq")
        if selectors != ["tests/bqcli/run_contract.py:main"]:
            raise ContractError(f"bq-cli-v1 does not implement selectors {selectors!r}")
        environment = os.environ.copy()
        environment["BQEMU_BQCLI_VERSION"] = self.context.versions["bq"]
        environment["BQEMU_BQCLI_ARTIFACT_DIR"] = str(self.context.artifact_root / "bqcli")
        result = self._run([sys.executable, "tests/bqcli/run_contract.py"], environment=environment)
        _write_junit(self.context.artifact_root / "junit.xml", self.context.case_id, result)
        return result


class SparkAdapter(RunnerAdapter):
    connector_path: Path
    connector_spec: dict[str, Any]
    dsv2_connector_path: Path | None = None
    dsv2_connector_spec: dict[str, Any] | None = None

    def prepare(self) -> None:
        super().prepare()
        artifact = _require_artifact(self.context, "spark-connector-dsv1-jar")
        self.connector_path = _materialize_artifact(self.context, artifact)
        self.artifact_evidence.append(_materialized_evidence(artifact, self.connector_path))
        self.connector_spec = {
            "variant": "dsv1-with-dependencies-2.12",
            "output": self.connector_path.name,
            "size": self.connector_path.stat().st_size,
            "sha256": artifact["sha256"],
            "provider": "com.google.cloud.spark.bigquery.Scala212BigQueryRelationProvider",
            "connectorVersion": self.context.versions["connector"],
        }
        if "spark-connector-dsv2-jar" in self.context.case["runnerAdapter"]["requiredArtifactUsages"]:
            dsv2_artifact = _require_artifact(self.context, "spark-connector-dsv2-jar")
            self.dsv2_connector_path = _materialize_artifact(self.context, dsv2_artifact)
            self.artifact_evidence.append(
                _materialized_evidence(dsv2_artifact, self.dsv2_connector_path)
            )
            self.dsv2_connector_spec = {
                "variant": "dsv2-spark-3.5-raw",
                "output": self.dsv2_connector_path.name,
                "size": self.dsv2_connector_path.stat().st_size,
                "sha256": dsv2_artifact["sha256"],
                "provider": "com.google.cloud.spark.bigquery.v2.Spark35BigQueryTableProvider",
                "connectorVersion": self.context.versions["connector"],
            }

    def spark_environment(self) -> dict[str, str]:
        environment = os.environ.copy()
        environment["BQEMU_SPARK_CONNECTOR_JAR"] = str(self.connector_path)
        environment["BQEMU_SPARK_CONNECTOR_SPEC_JSON"] = json.dumps(
            self.connector_spec, sort_keys=True, separators=(",", ":")
        )
        if self.dsv2_connector_path is not None and self.dsv2_connector_spec is not None:
            environment["BQEMU_SPARK_DSV2_CONNECTOR_JAR"] = str(self.dsv2_connector_path)
            environment["BQEMU_SPARK_DSV2_CONNECTOR_SPEC_JSON"] = json.dumps(
                self.dsv2_connector_spec, sort_keys=True, separators=(",", ":")
            )
        return environment


class SparkPytestAdapter(SparkAdapter):
    def verify_identity(self) -> None:
        _require_equal("PySpark", importlib.metadata.version("pyspark"), self.context.versions["spark"])
        self.artifact_evidence.extend(_verify_spark_runtime_artifacts(self.context))

    def execute_scenario(self) -> subprocess.CompletedProcess[str]:
        result = self._run(
            _spark_pytest_command(self.context, _scenario_selectors(self.context, "pytest")),
            environment=self.spark_environment(),
        )
        junit_valid = _sanitize_junit(
            self.context.artifact_root / "junit.xml", self.context.case_id, result
        )
        if result.returncode == 0 and not junit_valid:
            return subprocess.CompletedProcess(result.args, 1, result.stdout, "pytest did not produce valid JUnit")
        return result


class SparkScalaShellAdapter(SparkAdapter):
    def verify_identity(self) -> None:
        _require_equal("Spark distribution", importlib.metadata.version("pyspark"), self.context.versions["spark"])
        self.artifact_evidence.extend(_verify_spark_runtime_artifacts(self.context))

    def execute_scenario(self) -> subprocess.CompletedProcess[str]:
        result = self._run(
            _spark_pytest_command(self.context, _scenario_selectors(self.context, "pytest")),
            environment=self.spark_environment(),
        )
        junit_valid = _sanitize_junit(
            self.context.artifact_root / "junit.xml", self.context.case_id, result
        )
        if result.returncode == 0 and not junit_valid:
            return subprocess.CompletedProcess(result.args, 1, result.stdout, "pytest did not produce valid JUnit")
        return result


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


def _require_artifact(context: CaseContext, usage: str) -> dict[str, str]:
    matches = [artifact for artifact in context.case["artifacts"] if artifact["usage"] == usage]
    if len(matches) != 1:
        raise ContractError(
            f"case {context.case_id} must provide exactly one artifact with usage {usage!r}; "
            f"found {len(matches)}"
        )
    return matches[0]


def _verify_spark_runtime_artifacts(context: CaseContext) -> list[dict[str, Any]]:
    lock = (context.repository_root / "tests" / "spark" / "requirements.lock").read_text(encoding="utf-8")
    evidence: list[dict[str, Any]] = []
    artifact = _require_artifact(context, "spark-runtime")
    if artifact["uri"] not in lock or f"--hash=sha256:{artifact['sha256']}" not in lock:
        raise ContractError(f"artifact {artifact['id']} is not selected by the Spark hash lock")
    evidence.append(
        {
            "id": artifact["id"],
            "role": artifact["role"],
            "usage": artifact["usage"],
            "status": "hash-locked-runtime-identity-matched",
            "materialized": False,
            "installed": True,
            "version": importlib.metadata.version("pyspark"),
            "sha256": artifact["sha256"],
        }
    )
    return evidence


def _scenario_selectors(context: CaseContext, adapter_prefix: str) -> list[str]:
    selectors: list[str] = []
    seen: set[str] = set()
    for scenario in context.case["scenarioSet"]["scenarios"]:
        for encoded in scenario["selectors"]:
            prefix, separator, selector = encoded.partition(":")
            if separator == "" or prefix != adapter_prefix or selector == "":
                raise ContractError(
                    f"scenario {scenario['id']} selector {encoded!r} is not implemented "
                    f"by {context.runner_id}"
                )
            if selector not in seen:
                seen.add(selector)
                selectors.append(selector)
    if not selectors:
        raise ContractError(f"case {context.case_id} has no {adapter_prefix} selectors")
    return selectors


def _materialized_evidence(artifact: dict[str, str], path: Path) -> dict[str, Any]:
    return {
        "id": artifact["id"],
        "role": artifact["role"],
        "usage": artifact["usage"],
        "status": "digest-verified",
        "materialized": True,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "bytes": path.stat().st_size,
    }


def _materialize_artifact(context: CaseContext, artifact: dict[str, str]) -> Path:
    uri = artifact["uri"]
    if not uri.startswith("https://"):
        raise ContractError(f"artifact {artifact['id']} is not an HTTPS download")
    filename = uri.rsplit("/", 1)[-1]
    cache = context.repository_root / ".artifacts" / "consumer-downloads"
    target = cache / artifact["sha256"] / filename
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.is_file() and hashlib.sha256(target.read_bytes()).hexdigest() == artifact["sha256"]:
        return target
    temporary = target.parent / f".{target.name}.{os.getpid()}.tmp"
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
    setup_operations = set(context.case["runnerAdapter"].get("setupOperationIds", []))
    events: list[dict[str, Any]] = []
    for log_path in sorted(context.artifact_root.rglob("server.log")):
        relative_log = str(log_path.relative_to(context.artifact_root)).encode("utf-8")
        run_id = hashlib.sha256(relative_log).hexdigest()[:16]
        run_sequence = 0
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        decoded_lines: list[dict[str, Any]] = []
        for line in lines:
            try:
                decoded = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(decoded, dict):
                decoded_lines.append(decoded)
        startup_count = sum(
            event.get("event") == "runtime.configuration.loaded" for event in decoded_lines
        )
        if startup_count != 1:
            events.append(
                {
                    "seq": len(events) + 1,
                    "runId": run_id,
                    "runSeq": 0,
                    "phase": "unexpected-response",
                    "operationId": None,
                    "protocol": None,
                    "requestShape": "ambiguous server run boundary",
                    "responseStatus": None,
                    "correlationGroup": "unexpected",
                }
            )
        for event in decoded_lines:
            if event.get("event") != "boundary.exit":
                continue
            boundary = str(event.get("boundary", ""))
            if boundary != "http" and not boundary.startswith("grpc."):
                continue
            operation_id: str | None = None
            protocol: str | None = None
            request_shape: str | None = None
            response_status: Any = None
            if boundary == "http":
                for candidate, method, pattern, declared_path in rest_routes:
                    if event.get("method") == method and pattern.fullmatch(str(event.get("path", ""))):
                        operation_id = candidate
                        protocol = "REST"
                        request_shape = f"{method} {declared_path}"
                        response_status = event.get("status")
                        break
            elif boundary.startswith("grpc."):
                rpc = str(event.get("rpc", ""))
                operation_id = grpc_routes.get(rpc)
                protocol = "gRPC"
                request_shape = rpc
                response_status = event.get("grpc_code")
            if operation_id in scenario_by_operation:
                phase = "observed-response"
                correlation_group = scenario_by_operation[operation_id]
            elif operation_id in setup_operations:
                phase = "setup-response"
                correlation_group = "runner-setup"
            else:
                phase = "unexpected-response"
                correlation_group = "unexpected"
            run_sequence += 1
            events.append(
                {
                    "seq": len(events) + 1,
                    "runId": run_id,
                    "runSeq": run_sequence,
                    "phase": phase,
                    "operationId": operation_id,
                    "protocol": protocol,
                    "requestShape": request_shape,
                    "responseStatus": response_status,
                    "correlationGroup": correlation_group,
                }
            )
    return events


def compare_contract(
    scenarios: list[dict[str, Any]], events: list[dict[str, Any]]
) -> dict[str, Any] | None:
    for event in events:
        if event["phase"] == "unexpected-response":
            return {
                "scenarioId": None,
                "phase": "wire",
                "operationId": event["operationId"],
                "field": "unexpectedOperation",
                "expected": "not observed",
                "actual": event["requestShape"] or "unmapped boundary",
            }

    for scenario in scenarios:
        scenario_id = scenario["id"]
        observed = [
            event
            for event in events
            if event["phase"] == "observed-response" and event["correlationGroup"] == scenario_id
        ]
        positions: dict[str, list[tuple[str, int]]] = {}
        for event in observed:
            positions.setdefault(event["operationId"], []).append((event["runId"], event["runSeq"]))
        for expectation in scenario["operationExpectations"]:
            operation_id = expectation["operationId"]
            count = len(positions.get(operation_id, []))
            minimum = expectation["min"]
            maximum = expectation["max"]
            if count < minimum or (maximum != 0 and count > maximum):
                return {
                    "scenarioId": scenario_id,
                    "phase": "wire",
                    "operationId": operation_id,
                    "field": "cardinality",
                    "expected": {"min": minimum, "max": maximum},
                    "actual": count,
                }
        for expectation in scenario["operationExpectations"]:
            operation_id = expectation["operationId"]
            for predecessor in expectation["after"]:
                operation_by_run = _positions_by_run(positions.get(operation_id, []))
                predecessor_by_run = _positions_by_run(positions.get(predecessor, []))
                for run_id, operation_positions in operation_by_run.items():
                    predecessor_positions = predecessor_by_run.get(run_id, [])
                    if predecessor_positions and min(operation_positions) > min(predecessor_positions):
                        continue
                    return {
                        "scenarioId": scenario_id,
                        "phase": "wire",
                        "operationId": operation_id,
                        "field": "partialOrder",
                        "expected": {"after": predecessor},
                        "actual": {
                            "runId": run_id,
                            "firstPosition": min(operation_positions),
                            "predecessorFirstPosition": min(predecessor_positions) if predecessor_positions else None,
                        },
                    }
    return None


def _positions_by_run(positions: list[tuple[str, int]]) -> dict[str, list[int]]:
    result: dict[str, list[int]] = {}
    for run_id, position in positions:
        result.setdefault(run_id, []).append(position)
    return result


def _require_equal(label: str, actual: str, expected: str) -> None:
    if actual != expected:
        raise ContractError(f"{label} identity mismatch: actual={actual!r} expected={expected!r}")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_junit(path: Path, case_id: str, result: subprocess.CompletedProcess[str]) -> None:
    suite = ET.Element("testsuite", name=case_id, tests="1", failures="1" if result.returncode else "0")
    test = ET.SubElement(suite, "testcase", classname="consumer.runner", name=case_id)
    if result.returncode:
        failure = ET.SubElement(test, "failure", message="bq consumer contract failed")
        output = result.stdout.encode("utf-8", errors="replace")
        failure.text = f"output_bytes={len(output)} output_digest=sha256:{hashlib.sha256(output).hexdigest()}"
    path.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(suite).write(path, encoding="unicode", xml_declaration=True)


def _sanitize_junit(
    path: Path, case_id: str, result: subprocess.CompletedProcess[str]
) -> bool:
    raw = path.read_bytes() if path.is_file() else b""
    valid = path.is_file()
    try:
        if not valid:
            raise ET.ParseError("missing JUnit")
        tree = ET.parse(path)
    except ET.ParseError:
        valid = False
        suite = ET.Element("testsuite", name=case_id, tests="1", failures="1")
        test = ET.SubElement(suite, "testcase", classname="consumer.runner", name=case_id)
        failure = ET.SubElement(test, "failure", message="missing or invalid JUnit was redacted")
        output = result.stdout.encode("utf-8", errors="replace") if not raw else raw
        failure.text = f"output_bytes={len(output)} output_digest=sha256:{hashlib.sha256(output).hexdigest()}"
        tree = ET.ElementTree(suite)
    for element in tree.iter():
        if element.tag not in {"failure", "error", "system-out", "system-err"}:
            continue
        output = (element.text or "").encode("utf-8", errors="replace")
        element.text = f"output_bytes={len(output)} output_digest=sha256:{hashlib.sha256(output).hexdigest()}"
        if element.tag in {"failure", "error"}:
            element.set("message", "redacted consumer failure")
    tree.write(path, encoding="unicode", xml_declaration=True)
    return valid


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
