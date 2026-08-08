from __future__ import annotations

import copy
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

from scripts.consumer_runner import (
    ADAPTERS,
    DEFAULT_MANIFEST,
    CaseContext,
    ContractError,
    _require_artifact,
    _sanitize_junit,
    _scenario_selectors,
    build_adapter,
    collect_actual_events,
    compare_contract,
    load_case,
    load_manifest,
)


class ConsumerRunnerTest(unittest.TestCase):
    def test_every_normalized_case_uses_a_typed_runner_adapter(self) -> None:
        manifest = load_manifest(DEFAULT_MANIFEST)
        unknown = sorted(
            {
                case["runnerAdapter"]["id"]
                for case in manifest["cases"]
                if case["runnerAdapter"]["id"] not in ADAPTERS
            }
        )
        self.assertEqual(unknown, [])

    def test_load_case_reads_normalized_json_and_rejects_unknown_case(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "normalized.json"
            path.write_text(
                json.dumps({"schemaVersion": "1", "cases": [{"id": "known"}]}),
                encoding="utf-8",
            )
            self.assertEqual(load_case(path, "known")["id"], "known")
            with self.assertRaises(ContractError):
                load_case(path, "missing")

    def test_runner_adapter_is_selected_by_explicit_id_not_version(self) -> None:
        case = {
            "id": "case",
            "runnerAdapter": {"id": "python-pytest-v1"},
            "runtimeProfile": {"versions": {"client": "99.0.0", "python": "99.0"}},
        }
        context = CaseContext(case, Path("."), Path(".artifacts/test"))
        self.assertEqual(type(build_adapter(context)).__name__, "PythonPytestAdapter")
        case["runnerAdapter"]["id"] = "version-inferred-adapter"
        with self.assertRaises(ContractError):
            build_adapter(context)

    def test_failed_runner_writes_structured_first_operation_diff(self) -> None:
        case = {
            "id": "case",
            "runnerAdapter": {"id": "python-pytest-v1", "setupOperationIds": []},
            "runtimeProfile": {"versions": {"client": "3.43.0", "python": "3.13"}},
            "scenarioSet": {
                "scenarios": [
                    {
                        "id": "query",
                        "operationIds": ["bigquery.jobs.query", "bigquery.jobs.getQueryResults"],
                        "selectors": ["pytest:tests/python/test_query_contract.py"],
                        "operationExpectations": [
                            {"operationId": "bigquery.jobs.query", "min": 1, "max": 0, "after": []},
                            {
                                "operationId": "bigquery.jobs.getQueryResults",
                                "min": 1,
                                "max": 0,
                                "after": [],
                            },
                        ],
                    }
                ]
            },
            "artifacts": [],
        }
        with tempfile.TemporaryDirectory() as directory:
            context = CaseContext(case, Path("."), Path(directory))
            adapter = build_adapter(context)
            with mock.patch.object(adapter, "verify_identity", side_effect=ContractError("drift")):
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
            case = {
                "id": "case",
                "runnerAdapter": {"id": "python-pytest-v1", "setupOperationIds": []},
                "runtimeProfile": {"versions": {}},
            }
            events = collect_actual_events(CaseContext(case, root, artifacts), [scenario])
            self.assertEqual(events[0]["phase"], "unexpected-response")
            self.assertEqual(events[0]["requestShape"], "ambiguous server run boundary")

    def test_successful_process_fails_when_declared_operation_was_not_observed(self) -> None:
        case = {
            "id": "case",
            "runnerAdapter": {"id": "python-pytest-v1", "setupOperationIds": []},
            "runtimeProfile": {"versions": {"client": "3.43.0", "python": "3.13"}},
            "scenarioSet": {
                "scenarios": [
                    _scenario(
                        [{"operationId": "bigquery.jobs.query", "min": 1, "max": 0, "after": []}]
                    )
                ]
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            context = CaseContext(case, Path("."), Path(directory))
            adapter = build_adapter(context)
            adapter.result = subprocess.CompletedProcess([], 0, "", "")
            adapter.collect_evidence()
            self.assertEqual(adapter.result.returncode, 1)
            diff = json.loads((Path(directory) / "diff.json").read_text(encoding="utf-8"))
            self.assertEqual(diff["operationId"], "bigquery.jobs.query")
            self.assertEqual(diff["field"], "cardinality")

    def test_scenario_selectors_are_deduplicated_and_adapter_typed(self) -> None:
        case = {
            "id": "case",
            "runnerAdapter": {"id": "python-pytest-v1"},
            "runtimeProfile": {"versions": {}},
            "scenarioSet": {
                "scenarios": [
                    {"id": "one", "selectors": ["pytest:tests/python/test_one.py"]},
                    {
                        "id": "two",
                        "selectors": [
                            "pytest:tests/python/test_one.py",
                            "pytest:tests/python/test_two.py",
                        ],
                    },
                ]
            },
        }
        context = CaseContext(case, Path("."), Path(".artifacts/test"))
        self.assertEqual(
            _scenario_selectors(context, "pytest"),
            ["tests/python/test_one.py", "tests/python/test_two.py"],
        )
        with self.assertRaises(ContractError):
            _scenario_selectors(context, "bq")

    def test_typed_artifact_lookup_rejects_zero_or_multiple_matches(self) -> None:
        case = {
            "id": "case",
            "runnerAdapter": {"id": "python-pytest-v1"},
            "runtimeProfile": {"versions": {}},
            "artifacts": [],
        }
        context = CaseContext(case, Path("."), Path(".artifacts/test"))
        with self.assertRaises(ContractError):
            _require_artifact(context, "python-wheel")
        case["artifacts"] = [
            {"id": "one", "usage": "python-wheel"},
            {"id": "two", "usage": "python-wheel"},
        ]
        with self.assertRaises(ContractError):
            _require_artifact(context, "python-wheel")

    def test_new_connector_patch_row_uses_same_adapter_and_materializes_declared_jar(self) -> None:
        base = load_case(DEFAULT_MANIFEST, "spark-pyspark-3.5.8-connector-0.44.2")
        case = copy.deepcopy(base)
        case["id"] = "spark-pyspark-3.5.8-connector-0.44.3"
        case["runtimeProfile"]["versions"]["connector"] = "0.44.3"
        contents = b"new connector patch"
        digest = hashlib.sha256(contents).hexdigest()
        case["artifacts"][0] = {
            "id": "spark-bigquery-connector-0.44.3-jar",
            "role": "execution",
            "usage": "spark-connector-dsv1-jar",
            "uri": "https://example.invalid/spark-bigquery-0.44.3.jar",
            "sha256": digest,
        }
        case["artifacts"][1] = {
            "id": "spark-bigquery-connector-0.44.3-dsv2-jar",
            "role": "execution",
            "usage": "spark-connector-dsv2-jar",
            "uri": "https://example.invalid/spark-bigquery-dsv2-0.44.3.jar",
            "sha256": digest,
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context = CaseContext(case, root, root / "evidence")
            adapter = build_adapter(context)
            self.assertEqual(type(adapter).__name__, "SparkPytestAdapter")
            with mock.patch(
                "scripts.consumer_runner.urllib.request.urlopen",
                side_effect=[io.BytesIO(contents), io.BytesIO(contents)],
            ):
                adapter.prepare()
            self.assertEqual(adapter.connector_path.read_bytes(), contents)
            self.assertEqual(adapter.artifact_evidence[0]["sha256"], digest)
            self.assertEqual(adapter.dsv2_connector_path.read_bytes(), contents)
            self.assertEqual(adapter.artifact_evidence[1]["sha256"], digest)

    def test_junit_redaction_never_persists_consumer_output(self) -> None:
        secret = "authorization: Bearer future-credential"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "junit.xml"
            path.write_text(
                f'<testsuite><testcase><failure message="{secret}">{secret}</failure>'
                f"<system-out>{secret}</system-out></testcase></testsuite>",
                encoding="utf-8",
            )
            valid = _sanitize_junit(
                path, "case", subprocess.CompletedProcess([], 1, secret, "")
            )
            self.assertTrue(valid)
            redacted = path.read_text(encoding="utf-8")
            self.assertNotIn(secret, redacted)
            self.assertIn("output_digest=sha256:", redacted)

    def test_missing_junit_is_synthesized_without_raw_output(self) -> None:
        secret = "access_token=future-credential"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "junit.xml"
            valid = _sanitize_junit(
                path, "case", subprocess.CompletedProcess([], 1, secret, "")
            )
            self.assertFalse(valid)
            contents = path.read_text(encoding="utf-8")
            self.assertNotIn(secret, contents)
            self.assertIn("output_digest=sha256:", contents)

    def test_cleanup_runs_when_evidence_collection_fails(self) -> None:
        case = {
            "id": "case",
            "runnerAdapter": {"id": "python-pytest-v1"},
            "runtimeProfile": {"versions": {}},
        }
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
            error = (Path(directory) / "runner-error.txt").read_text(encoding="utf-8")
            self.assertNotIn("evidence secret", error)


def _scenario(expectations: list[dict[str, object]]) -> dict[str, object]:
    return {
        "id": "query",
        "operationIds": [expectation["operationId"] for expectation in expectations],
        "selectors": ["pytest:tests/python/test_query_contract.py"],
        "operationExpectations": expectations,
    }


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
