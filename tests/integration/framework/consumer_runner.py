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
import traceback
from typing import Any, Sequence
import xml.etree.ElementTree as ET


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from .consumer_runtime import (  # noqa: E402
    ArtifactSpec,
    ConsumerRuntimeError,
    NormalizedConsumerCase,
    NormalizedConsumerExecution,
    check_python_dependencies,
    install_python_artifact,
    load_normalized_case,
    load_normalized_execution,
    load_normalized_manifest,
    materialize_artifact,
    require_artifact,
    require_execution_artifact,
    select_normalized_cases,
    select_normalized_executions,
)


DEFAULT_MANIFEST = REPOSITORY_ROOT / "tests" / "integration" / "contract" / "consumers.normalized.json"
ContractError = ConsumerRuntimeError


@dataclass(frozen=True)
class CaseContext:
    case: NormalizedConsumerCase
    repository_root: Path
    artifact_root: Path
    execution: NormalizedConsumerExecution | None = None
    manifest_path: Path = DEFAULT_MANIFEST

    def __post_init__(self) -> None:
        if self.execution is not None:
            return
        public = [
            execution
            for execution in self.case.executions
            if execution.execution_id == "public"
        ]
        if len(public) != 1:
            raise ContractError("consumer case has no unique public execution")
        object.__setattr__(self, "execution", public[0])

    @property
    def case_id(self) -> str:
        return self.case.case_id

    @property
    def versions(self) -> dict[str, str]:
        return dict(self.case.versions)

    @property
    def runner_id(self) -> str:
        assert self.execution is not None
        return self.execution.runner_adapter_id

    @property
    def execution_id(self) -> str:
        assert self.execution is not None
        return self.execution.execution_id


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
        scenarios = list(self.context.execution.scenarios)
        events = collect_actual_events(self.context, scenarios)
        difference = compare_contract(scenarios, events)
        if self.result is not None and self.result.returncode == 0 and difference is not None:
            self.result = subprocess.CompletedProcess(
                self.result.args,
                1,
                self.result.stdout,
                "successful runner violated its normalized wire contract",
            )
        evidence = {
            "schemaVersion": "1",
            "caseId": self.context.case_id,
            "executionId": self.context.execution_id,
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
                    "executionId": self.context.execution_id,
                    **difference,
                },
            )
        else:
            (self.context.artifact_root / "diff.json").unlink(missing_ok=True)

    def cleanup(self) -> None:
        return

    def run(self) -> int:
        try:
            self.prepare()
            self.verify_identity()
            self.result = self.execute_scenario()
        except Exception as error:
            self._append_runner_failure("execution", error)
            self._record_runner_error(error)
        try:
            self.cleanup()
        except Exception as error:
            self._append_runner_failure("cleanup", error)
            self._record_runner_error(error)
        try:
            self.collect_evidence()
        except Exception as error:
            self._append_runner_failure("evidence collection", error)
            self._record_runner_error(error)
            self._write_minimal_evidence("evidence.collection")
        if self.result is None:
            self.result = subprocess.CompletedProcess([], 1, "", "runner produced no result")
            self._write_minimal_evidence("runner.result")
        _write_junit(
            self.context.artifact_root / "junit.xml",
            f"{self.context.case_id}:{self.context.execution_id}",
            self.result,
        )
        if self.result.returncode == 0:
            (self.context.artifact_root / "runner-error.txt").unlink(missing_ok=True)
        return self.result.returncode if self.result is not None else 1

    def _append_runner_failure(self, stage: str, error: Exception) -> None:
        previous = self.result
        output: list[str] = []
        if previous is not None:
            if previous.stdout:
                output.append(previous.stdout)
            if previous.stderr:
                output.append(previous.stderr)
        output.append(
            f"{stage}: {type(error).__name__}: {error}\n{traceback.format_exc()}"
        )
        self.result = subprocess.CompletedProcess(
            previous.args if previous is not None else [],
            1,
            "\n".join(output),
            "",
        )

    def _write_minimal_evidence(self, field: str) -> None:
        scenarios = list(self.context.execution.scenarios)
        _write_json(
            self.context.artifact_root / "evidence.json",
            {
                "schemaVersion": "1",
                "caseId": self.context.case_id,
                "executionId": self.context.execution_id,
                "runnerAdapterId": self.context.runner_id,
                "scenarioIds": [scenario["id"] for scenario in scenarios],
                "exitCode": 1,
                "durationMillis": round((time.time() - self.started_at) * 1000),
                "artifactEvidence": self.artifact_evidence,
                "comparison": {
                    "status": "unavailable",
                    "expectedOperationCount": sum(
                        len(scenario["operationExpectations"])
                        for scenario in scenarios
                    ),
                    "observedEventCount": 0,
                },
                "events": [],
            },
        )
        _write_json(
            self.context.artifact_root / "diff.json",
            {
                "schemaVersion": "1",
                "caseId": self.context.case_id,
                "executionId": self.context.execution_id,
                "scenarioId": None,
                "phase": "runner",
                "operationId": None,
                "field": field,
                "expected": "completed",
                "actual": "failed",
            },
        )

    def _record_runner_error(self, error: Exception) -> None:
        self.context.artifact_root.mkdir(parents=True, exist_ok=True)
        with (self.context.artifact_root / "runner-error.txt").open(
            "a", encoding="utf-8"
        ) as stream:
            stream.write(f"error_type={type(error).__name__} error={error!s}\n")
            stream.write(traceback.format_exc())

    def _run(self, command: Sequence[str], *, environment: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        process_environment = (environment or os.environ).copy()
        process_environment["BQEMU_CONSUMER_CASE_ID"] = self.context.case_id
        process_environment["BQEMU_CONSUMER_EXECUTION_ID"] = self.context.execution_id
        process_environment["BQEMU_CONSUMER_ARTIFACT_ROOT"] = str(
            self.context.artifact_root
        )
        process_environment["BQEMU_CONSUMER_MANIFEST"] = str(
            self.context.manifest_path.resolve()
        )
        process_environment["BQEMU_RUNTIME_VERSIONS_JSON"] = json.dumps(
            dict(self.context.versions), sort_keys=True, separators=(",", ":")
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
            f"return_code={result.returncode} output_bytes={len(output)} "
            f"output_digest=sha256:{hashlib.sha256(output).hexdigest()}\n{result.stdout}",
            encoding="utf-8",
        )
        return result


class PythonPytestAdapter(RunnerAdapter):
    def prepare(self) -> None:
        super().prepare()
        wheel = require_execution_artifact(
            self.context.case, self.context.execution, "python-wheel"
        )
        wheel_path = _materialize_case_artifact(self.context, wheel)
        _install_case_python_artifact(self.context, wheel_path, "Python client")
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
                "tests/integration/python/pytest.ini",
                *_scenario_selectors(self.context, "pytest"),
                f"--basetemp={self.context.artifact_root / 'pytest'}",
                f"--junitxml={self.context.artifact_root / 'junit.xml'}",
            ]
        )
        junit_valid = _normalize_junit(
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
        cloud_sdk = self._run(
            [os.getenv("BQEMU_GCLOUD_BIN", "gcloud"), "version", "--format=json"]
        )
        if cloud_sdk.returncode != 0:
            raise ContractError("gcloud version command failed")
        try:
            cloud_versions = json.loads(cloud_sdk.stdout)
        except json.JSONDecodeError as error:
            raise ContractError("gcloud version command returned invalid JSON") from error
        if not isinstance(cloud_versions, dict):
            raise ContractError("gcloud version command returned an invalid shape")
        _require_equal(
            "Google Cloud SDK",
            str(cloud_versions.get("Google Cloud SDK", "")),
            self.context.versions["cloudSdk"],
        )
        _require_equal(
            "gcloud bq component",
            str(cloud_versions.get("bq", "")),
            self.context.versions["bq"],
        )
        artifact = require_execution_artifact(
            self.context.case,
            self.context.execution,
            "cloud-sdk-release-provenance",
        )
        self.artifact_evidence.append(
            {
                "id": artifact.artifact_id,
                "role": artifact.role,
                "usage": artifact.usage,
                "sha256": artifact.sha256,
                "status": "tool-version-identity-matched",
                "materialized": False,
                "note": "The OCI digest is release provenance; setup-gcloud supplies executables whose Cloud SDK and bq identities are verified separately.",
            }
        )

    def execute_scenario(self) -> subprocess.CompletedProcess[str]:
        selectors = _scenario_selectors(self.context, "bq")
        if selectors != ["tests/integration/bqcli/run_contract.py:main"]:
            raise ContractError(f"bq-cli-v1 does not implement selectors {selectors!r}")
        environment = os.environ.copy()
        environment["BQEMU_BQCLI_VERSION"] = self.context.versions["bq"]
        environment["BQEMU_BQCLI_ARTIFACT_DIR"] = str(self.context.artifact_root / "bqcli")
        result = self._run([sys.executable, "tests/integration/bqcli/run_contract.py"], environment=environment)
        _write_junit(self.context.artifact_root / "junit.xml", self.context.case_id, result)
        return result


class SparkAdapter(RunnerAdapter):
    connector_path: Path
    connector_spec: dict[str, Any]
    hadoop_gcs_path: Path | None = None
    dsv2_connector_path: Path | None = None
    dsv2_connector_spec: dict[str, Any] | None = None

    def prepare(self) -> None:
        super().prepare()
        bridge = require_execution_artifact(
            self.context.case, self.context.execution, "spark-python-bridge"
        )
        bridge_path = _materialize_case_artifact(self.context, bridge)
        _install_case_python_artifact(
            self.context, bridge_path, "Spark Python bridge", check_dependencies=False
        )
        bridge_evidence = _materialized_evidence(bridge, bridge_path)
        bridge_evidence["installed"] = True
        self.artifact_evidence.append(bridge_evidence)
        runtime = require_execution_artifact(
            self.context.case, self.context.execution, "spark-runtime"
        )
        runtime_path = _materialize_case_artifact(self.context, runtime)
        _install_case_python_artifact(self.context, runtime_path, "Spark runtime")
        runtime_evidence = _materialized_evidence(runtime, runtime_path)
        runtime_evidence["installed"] = True
        self.artifact_evidence.append(runtime_evidence)
        artifact = require_execution_artifact(
            self.context.case, self.context.execution, "spark-connector-dsv1-jar"
        )
        self.connector_path = _materialize_case_artifact(self.context, artifact)
        self.artifact_evidence.append(_materialized_evidence(artifact, self.connector_path))
        self.connector_spec = {
            "variant": "dsv1-with-dependencies-2.12",
            "output": self.connector_path.name,
            "size": self.connector_path.stat().st_size,
            "sha256": artifact.sha256,
            "provider": "com.google.cloud.spark.bigquery.Scala212BigQueryRelationProvider",
            "connectorVersion": self.context.versions["connector"],
        }
        if "hadoop-gcs-connector-jar" in self.context.execution.required_artifact_usages:
            hadoop_gcs = require_execution_artifact(
                self.context.case,
                self.context.execution,
                "hadoop-gcs-connector-jar",
            )
            self.hadoop_gcs_path = _materialize_case_artifact(
                self.context, hadoop_gcs
            )
            self.artifact_evidence.append(
                _materialized_evidence(hadoop_gcs, self.hadoop_gcs_path)
            )
        if "spark-connector-dsv2-jar" in self.context.execution.required_artifact_usages:
            dsv2_artifact = require_execution_artifact(
                self.context.case,
                self.context.execution,
                "spark-connector-dsv2-jar",
            )
            self.dsv2_connector_path = _materialize_case_artifact(
                self.context, dsv2_artifact
            )
            self.artifact_evidence.append(
                _materialized_evidence(dsv2_artifact, self.dsv2_connector_path)
            )
            self.dsv2_connector_spec = {
                "variant": "dsv2-spark-3.5-raw",
                "output": self.dsv2_connector_path.name,
                "size": self.dsv2_connector_path.stat().st_size,
                "sha256": dsv2_artifact.sha256,
                "provider": "com.google.cloud.spark.bigquery.v2.Spark35BigQueryTableProvider",
                "connectorVersion": self.context.versions["connector"],
            }

    def spark_environment(self) -> dict[str, str]:
        environment = os.environ.copy()
        environment["BQEMU_SPARK_CONNECTOR_JAR"] = str(self.connector_path)
        environment["BQEMU_SPARK_CONNECTOR_SPEC_JSON"] = json.dumps(
            self.connector_spec, sort_keys=True, separators=(",", ":")
        )
        if self.hadoop_gcs_path is not None:
            environment["BQEMU_HADOOP_GCS_CONNECTOR_JAR"] = str(
                self.hadoop_gcs_path
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
        _require_equal("Python", f"{sys.version_info.major}.{sys.version_info.minor}", self.context.versions["python"])

    def execute_scenario(self) -> subprocess.CompletedProcess[str]:
        result = self._run(
            _spark_pytest_command(self.context, _scenario_selectors(self.context, "pytest")),
            environment=self.spark_environment(),
        )
        junit_valid = _normalize_junit(
            self.context.artifact_root / "junit.xml", self.context.case_id, result
        )
        if result.returncode == 0 and not junit_valid:
            return subprocess.CompletedProcess(result.args, 1, result.stdout, "pytest did not produce valid JUnit")
        return result


class SparkScalaShellAdapter(SparkAdapter):
    def verify_identity(self) -> None:
        _require_equal("Spark distribution", importlib.metadata.version("pyspark"), self.context.versions["spark"])
        _require_equal(
            "Python bootstrap",
            f"{sys.version_info.major}.{sys.version_info.minor}",
            self.context.versions["python"],
        )

    def execute_scenario(self) -> subprocess.CompletedProcess[str]:
        result = self._run(
            _spark_pytest_command(self.context, _scenario_selectors(self.context, "pytest")),
            environment=self.spark_environment(),
        )
        junit_valid = _normalize_junit(
            self.context.artifact_root / "junit.xml", self.context.case_id, result
        )
        if result.returncode == 0 and not junit_valid:
            return subprocess.CompletedProcess(result.args, 1, result.stdout, "pytest did not produce valid JUnit")
        return result


def _child_failure(output: str) -> dict[str, Any] | None:
    for encoded in reversed(output.splitlines()):
        try:
            decoded = json.loads(encoded)
        except json.JSONDecodeError:
            continue
        if not isinstance(decoded, dict) or decoded.get("status") != "failed":
            continue
        return decoded
    return None


class IndirectLoadAdapter:
    load_case: str

    def execute_scenario(self) -> subprocess.CompletedProcess[str]:
        context = self.context  # type: ignore[attr-defined]
        selectors = _scenario_selectors(context, "load")
        if selectors != [self.load_case]:
            raise ContractError(
                f"{context.runner_id} does not implement selectors {selectors!r}"
            )
        environment = (
            self.spark_environment()  # type: ignore[attr-defined]
            if isinstance(self, SparkAdapter)
            else os.environ.copy()
        )
        environment["BQEMU_LOAD_ARTIFACT_DIR"] = str(
            context.artifact_root / "load"
        )
        environment["BQEMU_LOAD_JUNIT"] = str(context.artifact_root / "load-junit.xml")
        environment["BQEMU_SPARK_PYTHON"] = sys.executable
        environment["BQEMU_LOAD_SPARK_PYTHON"] = environment["BQEMU_SPARK_PYTHON"]
        environment["BQEMU_LOAD_BQCLI_BIN"] = os.getenv("BQEMU_BQCLI_BIN", "bq")
        return self._run(  # type: ignore[attr-defined]
            [sys.executable, "tests/integration/load/run_contract.py", "--case", self.load_case],
            environment=environment,
        )

    def collect_evidence(self) -> None:
        context = self.context  # type: ignore[attr-defined]
        path = context.artifact_root / "load" / self.load_case / "evidence.json"
        try:
            child = json.loads(path.read_text(encoding="utf-8"))
            operations = child["consumerOperations"]
        except (OSError, json.JSONDecodeError, KeyError, TypeError) as error:
            result = self.result  # type: ignore[attr-defined]
            if result is not None and result.returncode != 0:
                self._write_failed_child_evidence(result)
                return
            raise ContractError("indirect-load evidence is missing or invalid") from error
        if not isinstance(operations, list) or any(
            not isinstance(operation, str) for operation in operations
        ):
            raise ContractError("indirect-load public operations are invalid")
        scenarios = list(context.execution.scenarios)
        if len(scenarios) != 1:
            raise ContractError("indirect-load execution must select exactly one scenario")
        scenario_id = scenarios[0]["id"]
        scenario_operations = {
            expectation["operationId"]
            for expectation in scenarios[0]["operationExpectations"]
        }
        setup_operations = set(context.execution.setup_operation_ids)
        events = []
        for index, operation in enumerate(operations, start=1):
            if operation in scenario_operations:
                phase = "observed-response"
                correlation_group = scenario_id
            elif operation in setup_operations:
                phase = "setup-response"
                correlation_group = "runner-setup"
            else:
                phase = "unexpected-response"
                correlation_group = "unexpected"
            events.append({
                "seq": index,
                "runId": "indirect-load",
                "runSeq": index,
                "phase": phase,
                "operationId": operation,
                "protocol": "REST",
                "requestShape": "captured by tests/integration/load",
                "responseStatus": 200,
                "correlationGroup": correlation_group,
            })
        difference = compare_contract(scenarios, events)
        result = self.result  # type: ignore[attr-defined]
        if result is not None and result.returncode == 0 and difference is not None:
            self.result = subprocess.CompletedProcess(  # type: ignore[attr-defined]
                result.args, 1, result.stdout, "indirect-load wire contract drifted"
            )
        _write_json(
            context.artifact_root / "evidence.json",
            {
                "schemaVersion": "1",
                "caseId": context.case_id,
                "executionId": context.execution_id,
                "runnerAdapterId": context.runner_id,
                "scenarioIds": [scenario["id"] for scenario in scenarios],
                "exitCode": self.result.returncode if self.result else 1,  # type: ignore[attr-defined]
                "durationMillis": round((time.time() - self.started_at) * 1000),  # type: ignore[attr-defined]
                "artifactEvidence": self.artifact_evidence,  # type: ignore[attr-defined]
                "comparison": {
                    "status": "matched" if difference is None else "different",
                    "expectedOperationCount": sum(
                        len(scenario["operationExpectations"]) for scenario in scenarios
                    ),
                    "observedEventCount": len(
                        [event for event in events if event["phase"] == "observed-response"]
                    ),
                },
                "events": events,
            },
        )

    def _write_failed_child_evidence(
        self, result: subprocess.CompletedProcess[str]
    ) -> None:
        context = self.context  # type: ignore[attr-defined]
        scenarios = list(context.execution.scenarios)
        child_failure = _child_failure(result.stdout)
        evidence: dict[str, Any] = {
            "schemaVersion": "1",
            "caseId": context.case_id,
            "executionId": context.execution_id,
            "runnerAdapterId": context.runner_id,
            "scenarioIds": [scenario["id"] for scenario in scenarios],
            "exitCode": result.returncode,
            "durationMillis": round((time.time() - self.started_at) * 1000),  # type: ignore[attr-defined]
            "artifactEvidence": self.artifact_evidence,  # type: ignore[attr-defined]
            "comparison": {
                "status": "unavailable",
                "expectedOperationCount": sum(
                    len(scenario["operationExpectations"])
                    for scenario in scenarios
                ),
                "observedEventCount": 0,
            },
            "events": [],
            "childOutput": result.stdout,
        }
        if child_failure is not None:
            evidence["childFailure"] = child_failure
        _write_json(context.artifact_root / "evidence.json", evidence)
        difference: dict[str, Any] = {
            "schemaVersion": "1",
            "caseId": context.case_id,
            "executionId": context.execution_id,
            "scenarioId": None,
            "phase": "child-runner",
            "operationId": None,
            "field": "process.exitCode",
            "expected": 0,
            "actual": result.returncode,
        }
        if child_failure is not None:
            difference["childFailure"] = child_failure
        _write_json(context.artifact_root / "diff.json", difference)
        if self.result is not None and self.result.returncode != 0:  # type: ignore[attr-defined]
            _write_json(
                context.artifact_root / "diff.json",
                {
                    "schemaVersion": "1",
                    "caseId": context.case_id,
                    "executionId": context.execution_id,
                    **(
                        difference
                        or {
                            "scenarioId": None,
                            "phase": "runner",
                            "operationId": None,
                            "field": "process.exitCode",
                            "expected": 0,
                            "actual": self.result.returncode,  # type: ignore[attr-defined]
                        }
                    ),
                },
            )
        else:
            (context.artifact_root / "diff.json").unlink(missing_ok=True)


class PythonIndirectLoadAdapter(IndirectLoadAdapter, PythonPytestAdapter):
    load_case = "python"


class BQIndirectLoadAdapter(IndirectLoadAdapter, BQCLIAdapter):
    load_case = "bq"


class SparkPyIndirectLoadAdapter(IndirectLoadAdapter, SparkPytestAdapter):
    load_case = "pyspark"


class SparkScalaIndirectLoadAdapter(IndirectLoadAdapter, SparkScalaShellAdapter):
    load_case = "scala-spark"


ADAPTERS: dict[str, type[RunnerAdapter]] = {
    "python-pytest-v1": PythonPytestAdapter,
    "bq-cli-v1": BQCLIAdapter,
    "spark-pyspark-pytest-v1": SparkPytestAdapter,
    "spark-scala-shell-v1": SparkScalaShellAdapter,
    "python-indirect-load-v1": PythonIndirectLoadAdapter,
    "bq-indirect-load-v1": BQIndirectLoadAdapter,
    "spark-pyspark-indirect-load-v1": SparkPyIndirectLoadAdapter,
    "spark-scala-indirect-load-v1": SparkScalaIndirectLoadAdapter,
}


def _spark_pytest_command(context: CaseContext, paths: list[str]) -> list[str]:
    return [
        sys.executable,
        "-m",
        "pytest",
        "-c",
        "tests/integration/spark/pytest.ini",
        *paths,
        f"--basetemp={context.artifact_root / 'pytest'}",
        f"--junitxml={context.artifact_root / 'junit.xml'}",
    ]


def load_manifest(manifest_path: Path):
    return load_normalized_manifest(manifest_path)


def load_case(manifest_path: Path, case_id: str) -> NormalizedConsumerCase:
    return load_normalized_case(manifest_path, case_id)


def load_execution(
    manifest_path: Path, case_id: str, execution_id: str
) -> tuple[NormalizedConsumerCase, NormalizedConsumerExecution]:
    return load_normalized_execution(manifest_path, case_id, execution_id)


def build_adapter(context: CaseContext) -> RunnerAdapter:
    adapter_type = ADAPTERS.get(context.runner_id)
    if adapter_type is None:
        raise ContractError(f"unknown runner adapter {context.runner_id!r}")
    return adapter_type(context)


def _install_case_python_artifact(
    context: CaseContext,
    artifact_path: Path,
    label: str,
    *,
    check_dependencies: bool = True,
) -> None:
    uv = os.getenv("BQEMU_UV_BIN", "uv")

    def run_checked(command: Sequence[str], operation: str) -> None:
        result = subprocess.run(
            list(command),
            cwd=context.repository_root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        if result.returncode != 0:
            raise ContractError(f"case-declared {label} {operation} failed")

    install_python_artifact(
        Path(sys.executable),
        artifact_path,
        "installation",
        run_checked,
        uv_executable=uv,
    )
    if check_dependencies:
        check_python_dependencies(
            Path(sys.executable),
            "dependency check",
            run_checked,
            uv_executable=uv,
        )


def _scenario_selectors(context: CaseContext, adapter_prefix: str) -> list[str]:
    selectors: list[str] = []
    seen: set[str] = set()
    for scenario in context.execution.scenarios:
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


def _materialized_evidence(artifact: ArtifactSpec, path: Path) -> dict[str, Any]:
    return {
        "id": artifact.artifact_id,
        "role": artifact.role,
        "usage": artifact.usage,
        "status": "digest-verified",
        "materialized": True,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "bytes": path.stat().st_size,
    }


def _materialize_case_artifact(
    context: CaseContext, artifact: ArtifactSpec
) -> Path:
    return materialize_artifact(
        context.repository_root,
        artifact,
        timeout_seconds=float(os.getenv("BQEMU_ARTIFACT_TIMEOUT_SECONDS", "180")),
    )


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
    setup_operations = set(context.execution.setup_operation_ids)
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
        failure = ET.SubElement(test, "failure", message="consumer contract failed")
        failure.text = result.stdout
    path.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(suite).write(path, encoding="unicode", xml_declaration=True)


def _normalize_junit(
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
        failure = ET.SubElement(test, "failure", message="missing or invalid JUnit")
        failure.text = result.stdout if not raw else raw.decode("utf-8", errors="replace")
        tree = ET.ElementTree(suite)
    tree.write(path, encoding="unicode", xml_declaration=True)
    return valid


def main(arguments: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--case", dest="case_id")
    selection.add_argument("--family")
    parser.add_argument("--lane", default="required")
    parser.add_argument("--execution", default="public")
    parser.add_argument("--all", action="store_true", dest="run_all")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--artifact-root", type=Path)
    options = parser.parse_args(arguments)
    if options.case_id:
        selections = [
            load_execution(options.manifest, options.case_id, options.execution)
        ]
    else:
        selections = list(
            select_normalized_executions(
                options.manifest,
                family=options.family,
                lane=options.lane,
                execution_id=options.execution,
            )
        )
        if not selections:
            raise ContractError(
                f"no consumer executions for family={options.family!r} "
                f"lane={options.lane!r} execution={options.execution!r}"
            )
        if len(selections) != 1 and not options.run_all:
            raise ContractError("family selection returned multiple cases; pass --all explicitly")
    exit_code = 0
    for case, execution in selections:
        artifact_root = (
            options.artifact_root
            or REPOSITORY_ROOT
            / ".artifacts"
            / "consumers"
            / case.case_id
            / execution.execution_id
        )
        context = CaseContext(
            case=case,
            execution=execution,
            repository_root=REPOSITORY_ROOT,
            artifact_root=artifact_root,
            manifest_path=options.manifest,
        )
        exit_code = max(exit_code, build_adapter(context).run())
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
