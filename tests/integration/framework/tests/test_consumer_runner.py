from __future__ import annotations

from dataclasses import replace
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

from tests.integration.framework.consumer_runner import (
    ADAPTERS,
    DEFAULT_MANIFEST,
    CaseContext,
    ContractError,
    _normalize_junit,
    _scenario_selectors,
    build_adapter,
    collect_actual_events,
    compare_contract,
    load_case,
    load_manifest,
)
from tests.integration.framework.consumer_runtime import ArtifactSpec


class ConsumerRunnerTest(unittest.TestCase):
    def test_every_normalized_case_uses_a_typed_runner_adapter(self) -> None:
        manifest = load_manifest(DEFAULT_MANIFEST)
        unknown = sorted(
            {
                execution.runner_adapter_id
                for case in manifest.cases
                for execution in case.executions
                if execution.runner_adapter_id not in ADAPTERS
            }
        )
        self.assertEqual(unknown, [])

    def test_load_case_reads_normalized_json_and_rejects_unknown_case(self) -> None:
        known = load_manifest(DEFAULT_MANIFEST).cases[0]
        self.assertEqual(load_case(DEFAULT_MANIFEST, known.case_id).case_id, known.case_id)
        with self.assertRaises(ContractError):
            load_case(DEFAULT_MANIFEST, "missing")

    def test_runner_adapter_is_selected_by_explicit_id_not_version(self) -> None:
        case = replace(
            _python_case(),
            case_id="case",
            versions={"client": "99.0.0", "python": "99.0"},
        )
        context = CaseContext(case, Path("."), Path(".artifacts/test"))
        self.assertEqual(type(build_adapter(context)).__name__, "PythonPytestAdapter")
        assert context.execution is not None
        context = replace(
            context,
            execution=replace(
                context.execution, runner_adapter_id="version-inferred-adapter"
            ),
        )
        with self.assertRaises(ContractError):
            build_adapter(context)

    def test_bq_adapter_verifies_bq_and_cloud_sdk_identities(self) -> None:
        case = load_case(DEFAULT_MANIFEST, "bq-cli-2.1.31")
        adapter = build_adapter(CaseContext(case, Path("."), Path(".artifacts/test")))
        cloud_versions = {
            "Google Cloud SDK": case.versions["cloudSdk"],
            "bq": case.versions["bq"],
        }
        with mock.patch.object(
            adapter,
            "_run",
            side_effect=[
                subprocess.CompletedProcess(
                    [], 0, f"This is BigQuery CLI {case.versions['bq']}\n", ""
                ),
                subprocess.CompletedProcess([], 0, json.dumps(cloud_versions), ""),
            ],
        ):
            adapter.verify_identity()

        self.assertEqual(
            adapter.artifact_evidence[0]["usage"],
            "cloud-sdk-release-provenance",
        )
        self.assertEqual(
            adapter.artifact_evidence[0]["status"],
            "tool-version-identity-matched",
        )

    def test_failed_runner_writes_structured_first_operation_diff(self) -> None:
        case = _with_public_execution(
            replace(_python_case(), case_id="case"),
            setup_operation_ids=(),
            scenarios=(
                {
                    "id": "query",
                    "operationIds": ["bigquery.jobs.query", "bigquery.jobs.getQueryResults"],
                    "selectors": ["pytest:tests/integration/python/test_query_contract.py"],
                    "operationExpectations": [
                        {"operationId": "bigquery.jobs.query", "min": 1, "max": 0, "after": []},
                        {
                            "operationId": "bigquery.jobs.getQueryResults",
                            "min": 1,
                            "max": 0,
                            "after": [],
                        },
                    ],
                },
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            context = CaseContext(case, Path("."), Path(directory))
            adapter = build_adapter(context)
            with (
                mock.patch.object(adapter, "prepare"),
                mock.patch.object(
                    adapter, "verify_identity", side_effect=ContractError("drift")
                ),
            ):
                self.assertEqual(adapter.run(), 1)
            diff = json.loads((Path(directory) / "diff.json").read_text(encoding="utf-8"))
            self.assertEqual(diff["scenarioId"], "query")
            self.assertEqual(diff["operationId"], "bigquery.jobs.query")
            self.assertEqual(diff["field"], "cardinality")

    def test_comparator_reports_partial_order_mutation(self) -> None:
        scenario = _scenario(
            [
                {"operationId": "bigquery.jobs.insert", "min": 1, "max": 0, "after": []},
                {
                    "operationId": "bigquery.jobs.get",
                    "min": 1,
                    "max": 0,
                    "after": ["bigquery.jobs.insert"],
                },
            ]
        )
        difference = compare_contract(
            [scenario],
            [_event("bigquery.jobs.get", run_seq=1), _event("bigquery.jobs.insert", run_seq=2)],
        )
        self.assertEqual(difference["scenarioId"], "query")
        self.assertEqual(difference["operationId"], "bigquery.jobs.get")
        self.assertEqual(difference["field"], "partialOrder")

    def test_comparator_reports_cardinality_mutation(self) -> None:
        scenario = _scenario(
            [{"operationId": "bigquery.jobs.query", "min": 1, "max": 1, "after": []}]
        )
        difference = compare_contract(
            [scenario],
            [_event("bigquery.jobs.query", run_seq=1), _event("bigquery.jobs.query", run_seq=2)],
        )
        self.assertEqual(difference["operationId"], "bigquery.jobs.query")
        self.assertEqual(difference["field"], "cardinality")
        self.assertEqual(difference["actual"], 2)

    def test_comparator_rejects_operation_outside_scenario_and_setup(self) -> None:
        scenario = _scenario(
            [{"operationId": "bigquery.jobs.query", "min": 1, "max": 0, "after": []}]
        )
        unexpected = _event("bigquery.tables.delete")
        unexpected["phase"] = "unexpected-response"
        unexpected["correlationGroup"] = "unexpected"
        difference = compare_contract([scenario], [unexpected])
        self.assertEqual(difference["operationId"], "bigquery.tables.delete")
        self.assertEqual(difference["field"], "unexpectedOperation")

    def test_missing_predecessor_reports_cardinality_before_order(self) -> None:
        scenario = _scenario(
            [
                {"operationId": "bigquery.jobs.insert", "min": 1, "max": 0, "after": []},
                {
                    "operationId": "bigquery.jobs.get",
                    "min": 1,
                    "max": 0,
                    "after": ["bigquery.jobs.insert"],
                },
            ]
        )
        difference = compare_contract([scenario], [_event("bigquery.jobs.get")])
        self.assertEqual(difference["operationId"], "bigquery.jobs.insert")
        self.assertEqual(difference["field"], "cardinality")

    def test_partial_order_never_correlates_different_server_runs(self) -> None:
        scenario = _scenario(
            [
                {"operationId": "bigquery.jobs.insert", "min": 1, "max": 0, "after": []},
                {
                    "operationId": "bigquery.jobs.get",
                    "min": 1,
                    "max": 0,
                    "after": ["bigquery.jobs.insert"],
                },
            ]
        )
        difference = compare_contract(
            [scenario],
            [
                _event("bigquery.jobs.insert", run_id="run-a"),
                _event("bigquery.jobs.get", run_id="run-b"),
            ],
        )
        self.assertEqual(difference["operationId"], "bigquery.jobs.get")
        self.assertEqual(difference["field"], "partialOrder")
        self.assertIsNone(difference["actual"]["predecessorFirstPosition"])

    def test_collector_rejects_multiple_server_startups_in_one_log(self) -> None:
        scenario = _scenario(
            [{"operationId": "bigquery.jobs.query", "min": 1, "max": 0, "after": []}]
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "contract").mkdir()
            (root / "contract" / "operations.normalized.json").write_text(
                json.dumps(
                    {
                        "operations": [
                            {
                                "id": "bigquery.jobs.query",
                                "rest": {"method": "POST", "path": "/bigquery/v2/projects/{projectId}/queries"},
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            artifacts = root / "artifacts"
            artifacts.mkdir()
            (artifacts / "server.log").write_text(
                "\n".join(
                    [
                        json.dumps({"event": "runtime.configuration.loaded"}),
                        json.dumps({"event": "runtime.configuration.loaded"}),
                        json.dumps(
                            {
                                "event": "boundary.exit",
                                "boundary": "http",
                                "method": "POST",
                                "path": "/bigquery/v2/projects/p/queries",
                                "status": 200,
                            }
                        ),
                    ]
                ),
                encoding="utf-8",
            )
            case = _with_public_execution(
                replace(_python_case(), case_id="case"), setup_operation_ids=()
            )
            events = collect_actual_events(CaseContext(case, root, artifacts), [scenario])
            self.assertEqual(events[0]["phase"], "unexpected-response")
            self.assertEqual(events[0]["requestShape"], "ambiguous server run boundary")

    def test_collector_accepts_canonical_configuration_transition(self) -> None:
        scenario = _scenario(
            [{"operationId": "bigquery.jobs.query", "min": 1, "max": 0, "after": []}]
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "contract").mkdir()
            (root / "contract" / "operations.normalized.json").write_text(
                json.dumps({"operations": []}), encoding="utf-8"
            )
            artifacts = root / "artifacts"
            artifacts.mkdir()
            (artifacts / "server.log").write_text(
                json.dumps(
                    {
                        "event": "domain.transition",
                        "state_from": "CONFIGURING",
                        "state_to": "CONFIGURED",
                    }
                ),
                encoding="utf-8",
            )
            case = _with_public_execution(
                replace(_python_case(), case_id="case"), setup_operation_ids=()
            )
            events = collect_actual_events(CaseContext(case, root, artifacts), [scenario])
            self.assertEqual(events, [])

    def test_successful_process_fails_when_declared_operation_was_not_observed(self) -> None:
        case = _with_public_execution(
            replace(_python_case(), case_id="case"),
            setup_operation_ids=(),
            scenarios=(
                _scenario(
                    [{"operationId": "bigquery.jobs.query", "min": 1, "max": 0, "after": []}]
                ),
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            context = CaseContext(case, Path("."), Path(directory))
            adapter = build_adapter(context)
            adapter.result = subprocess.CompletedProcess([], 0, "", "")
            adapter.collect_evidence()
            self.assertEqual(adapter.result.returncode, 1)
            diff = json.loads((Path(directory) / "diff.json").read_text(encoding="utf-8"))
            self.assertEqual(diff["operationId"], "bigquery.jobs.query")
            self.assertEqual(diff["field"], "cardinality")

    def test_indirect_load_evidence_classifies_scenario_and_setup_operations(self) -> None:
        case = load_case(DEFAULT_MANIFEST, "bq-cli-2.1.31")
        execution = next(
            execution
            for execution in case.executions
            if execution.execution_id == "indirect-load"
        )
        with tempfile.TemporaryDirectory() as directory:
            artifact_root = Path(directory)
            child_evidence = artifact_root / "load" / "bq" / "evidence.json"
            child_evidence.parent.mkdir(parents=True)
            child_evidence.write_text(
                json.dumps(
                    {
                        "consumerOperations": [
                            "bqemu.discovery.get",
                            "bigquery.jobs.insert",
                            "bigquery.jobs.get",
                        ]
                    }
                ),
                encoding="utf-8",
            )
            adapter = build_adapter(
                CaseContext(case, Path("."), artifact_root, execution=execution)
            )
            adapter.result = subprocess.CompletedProcess([], 0, "", "")

            adapter.collect_evidence()

            self.assertEqual(adapter.result.returncode, 0)
            evidence = json.loads(
                (artifact_root / "evidence.json").read_text(encoding="utf-8")
            )
            self.assertEqual(evidence["comparison"]["status"], "matched")
            self.assertEqual(
                [event["phase"] for event in evidence["events"]],
                ["setup-response", "observed-response", "observed-response"],
            )
            self.assertEqual(
                [event["correlationGroup"] for event in evidence["events"]],
                ["runner-setup", "bq-indirect-load", "bq-indirect-load"],
            )

    def test_indirect_load_preserves_complete_child_failure_and_output(self) -> None:
        case = load_case(DEFAULT_MANIFEST, "bq-cli-2.1.31")
        execution = next(
            execution
            for execution in case.executions
            if execution.execution_id == "indirect-load"
        )
        with tempfile.TemporaryDirectory() as directory:
            artifact_root = Path(directory)
            adapter = build_adapter(
                CaseContext(case, Path("."), artifact_root, execution=execution)
            )
            first_failure = {
                "status": "failed",
                "stage": "contract",
                "service": "load-e2e",
                "model_version": "bqemu-load-public-process/v1",
                "operation": "cross-protocol-trace",
                "shape": "load-order",
                "fix_hint": "compare-the-pinned-load-contract",
                "observed": "raw-payload-is-retained",
            }
            last_failure = {
                **first_failure,
                "shape": "SECRET_MARKER_IS_RETAINED",
            }
            adapter.result = subprocess.CompletedProcess(
                [], 1, "\n".join((json.dumps(first_failure), json.dumps(last_failure))), ""
            )

            adapter.collect_evidence()

            evidence = json.loads(
                (artifact_root / "evidence.json").read_text(encoding="utf-8")
            )
            self.assertEqual(evidence["exitCode"], 1)
            self.assertEqual(evidence["childFailure"]["shape"], "SECRET_MARKER_IS_RETAINED")
            encoded = json.dumps(evidence, sort_keys=True)
            self.assertIn("raw-payload", encoded)
            self.assertIn("SECRET_MARKER", encoded)

    def test_scenario_selectors_are_deduplicated_and_adapter_typed(self) -> None:
        case = _with_public_execution(
            replace(_python_case(), case_id="case"),
            scenarios=(
                {"id": "one", "selectors": ["pytest:tests/integration/python/test_one.py"]},
                {
                    "id": "two",
                    "selectors": [
                        "pytest:tests/integration/python/test_one.py",
                        "pytest:tests/integration/python/test_two.py",
                    ],
                },
            ),
        )
        context = CaseContext(case, Path("."), Path(".artifacts/test"))
        self.assertEqual(
            _scenario_selectors(context, "pytest"),
            ["tests/integration/python/test_one.py", "tests/integration/python/test_two.py"],
        )
        with self.assertRaises(ContractError):
            _scenario_selectors(context, "bq")

    def test_new_connector_patch_row_uses_same_adapter_and_materializes_declared_jar(self) -> None:
        base = load_case(DEFAULT_MANIFEST, "spark-pyspark-3.5.8-connector-0.44.2")
        contents = b"new connector patch"
        digest = hashlib.sha256(contents).hexdigest()
        artifacts = tuple(
            ArtifactSpec(
                artifact_id=artifact.artifact_id + "-patch",
                role=artifact.role,
                usage=artifact.usage,
                uri=f"https://example.invalid/{artifact.usage}.artifact",
                sha256=digest,
            )
            for artifact in base.artifacts
        )
        versions = dict(base.versions)
        versions["connector"] = "0.44.3"
        case = replace(
            base,
            case_id="spark-pyspark-3.5.8-connector-0.44.3",
            versions=versions,
            artifacts=artifacts,
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context = CaseContext(case, root, root / "evidence")
            adapter = build_adapter(context)
            self.assertEqual(type(adapter).__name__, "SparkPytestAdapter")
            with (
                mock.patch(
                    "tests.integration.framework.consumer_runtime.urllib.request.urlopen",
                    side_effect=[io.BytesIO(contents) for _ in artifacts],
                ),
                mock.patch("tests.integration.framework.consumer_runner._install_case_python_artifact"),
            ):
                adapter.prepare()
            self.assertEqual(adapter.connector_path.read_bytes(), contents)
            self.assertEqual(adapter.artifact_evidence[0]["usage"], "spark-python-bridge")
            self.assertEqual(adapter.artifact_evidence[1]["usage"], "spark-runtime")
            self.assertEqual(adapter.artifact_evidence[2]["sha256"], digest)
            self.assertEqual(adapter.dsv2_connector_path.read_bytes(), contents)
            self.assertEqual(adapter.artifact_evidence[3]["sha256"], digest)

    def test_junit_normalization_retains_consumer_output(self) -> None:
        secret = "authorization: Bearer future-credential"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "junit.xml"
            path.write_text(
                f'<testsuite><testcase><failure message="{secret}">{secret}</failure>'
                f"<system-out>{secret}</system-out></testcase></testsuite>",
                encoding="utf-8",
            )
            valid = _normalize_junit(
                path, "case", subprocess.CompletedProcess([], 1, secret, "")
            )
            self.assertTrue(valid)
            normalized = path.read_text(encoding="utf-8")
            self.assertIn(secret, normalized)

    def test_missing_junit_is_synthesized_with_raw_output(self) -> None:
        secret = "access_token=future-credential"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "junit.xml"
            valid = _normalize_junit(
                path, "case", subprocess.CompletedProcess([], 1, secret, "")
            )
            self.assertFalse(valid)
            contents = path.read_text(encoding="utf-8")
            self.assertIn(secret, contents)

    def test_cleanup_runs_when_evidence_collection_fails(self) -> None:
        case = replace(_python_case(), case_id="case")
        with tempfile.TemporaryDirectory() as directory:
            context = CaseContext(case, Path("."), Path(directory))
            adapter = build_adapter(context)
            with (
                mock.patch.object(adapter, "prepare"),
                mock.patch.object(adapter, "verify_identity"),
                mock.patch.object(
                    adapter,
                    "execute_scenario",
                    return_value=subprocess.CompletedProcess([], 0, "", ""),
                ),
                mock.patch.object(
                    adapter, "collect_evidence", side_effect=ContractError("evidence secret")
                ),
                mock.patch.object(adapter, "cleanup") as cleanup,
            ):
                self.assertEqual(adapter.run(), 1)
            cleanup.assert_called_once_with()
            self.assertTrue((Path(directory) / "junit.xml").is_file())
            evidence = json.loads(
                (Path(directory) / "evidence.json").read_text(encoding="utf-8")
            )
            difference = json.loads(
                (Path(directory) / "diff.json").read_text(encoding="utf-8")
            )
            self.assertEqual(evidence["exitCode"], 1)
            self.assertEqual(evidence["comparison"]["status"], "unavailable")
            self.assertEqual(difference["field"], "evidence.collection")
            self.assertIn('failures="1"', (Path(directory) / "junit.xml").read_text())
            error = (Path(directory) / "runner-error.txt").read_text(encoding="utf-8")
            self.assertIn("evidence secret", error)
            self.assertIn(
                "evidence secret",
                (Path(directory) / "junit.xml").read_text(encoding="utf-8"),
            )

    def test_cleanup_failure_is_reflected_in_evidence_diff_and_junit(self) -> None:
        scenario = _scenario(
            [
                {
                    "operationId": "bigquery.jobs.query",
                    "min": 0,
                    "max": 0,
                    "after": [],
                }
            ]
        )
        case = _with_public_execution(
            replace(_python_case(), case_id="case"), scenarios=(scenario,)
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "contract").mkdir()
            (root / "contract" / "operations.normalized.json").write_text(
                json.dumps({"operations": []}), encoding="utf-8"
            )
            context = CaseContext(case, root, root / "artifacts")
            adapter = build_adapter(context)
            with (
                mock.patch.object(adapter, "prepare"),
                mock.patch.object(adapter, "verify_identity"),
                mock.patch.object(
                    adapter,
                    "execute_scenario",
                    return_value=subprocess.CompletedProcess([], 0, "", ""),
                ),
                mock.patch.object(
                    adapter, "cleanup", side_effect=ContractError("cleanup secret")
                ),
            ):
                self.assertEqual(adapter.run(), 1)

            evidence = json.loads(
                (context.artifact_root / "evidence.json").read_text(encoding="utf-8")
            )
            difference = json.loads(
                (context.artifact_root / "diff.json").read_text(encoding="utf-8")
            )
            junit = (context.artifact_root / "junit.xml").read_text(encoding="utf-8")
            self.assertEqual(evidence["exitCode"], 1)
            self.assertEqual(difference["field"], "process.exitCode")
            self.assertIn('failures="1"', junit)
            self.assertIn("cleanup secret", junit)


def _scenario(expectations: list[dict[str, object]]) -> dict[str, object]:
    return {
        "id": "query",
        "operationIds": [expectation["operationId"] for expectation in expectations],
        "selectors": ["pytest:tests/integration/python/test_query_contract.py"],
        "operationExpectations": expectations,
    }


def _python_case():
    return load_case(DEFAULT_MANIFEST, "google-cloud-bigquery-python-3.43.0")


def _with_public_execution(case, **changes):
    return replace(
        case,
        executions=tuple(
            replace(execution, **changes)
            if execution.execution_id == "public"
            else execution
            for execution in case.executions
        ),
    )


def _event(operation_id: str, *, run_id: str = "run", run_seq: int = 1) -> dict[str, object]:
    return {
        "seq": 1,
        "runId": run_id,
        "runSeq": run_seq,
        "phase": "observed-response",
        "operationId": operation_id,
        "protocol": "REST",
        "requestShape": "GET /example",
        "responseStatus": 200,
        "correlationGroup": "query",
    }


if __name__ == "__main__":
    unittest.main()
