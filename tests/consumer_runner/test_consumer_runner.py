from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts.consumer_runner import (
    CaseContext,
    ContractError,
    build_adapter,
    first_contract_difference,
    load_case,
)


class ConsumerRunnerTest(unittest.TestCase):
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
            "runnerAdapter": {"id": "python-pytest-v1"},
            "runtimeProfile": {"versions": {"client": "3.43.0", "python": "3.13"}},
            "scenarioSet": {
                "scenarios": [
                    {"id": "query", "operationIds": ["bigquery.jobs.query", "bigquery.jobs.getQueryResults"]}
                ]
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            context = CaseContext(case, Path("."), Path(directory))
            adapter = build_adapter(context)
            with mock.patch.object(adapter, "verify_identity", side_effect=ContractError("drift")):
                self.assertEqual(adapter.run(), 1)
            diff = json.loads((Path(directory) / "diff.json").read_text(encoding="utf-8"))
            self.assertEqual(diff["scenarioId"], "query")
            self.assertEqual(diff["operationId"], "bigquery.jobs.query")
            self.assertEqual(diff["field"], "process.exitCode")

    def test_first_contract_difference_reports_changed_operation_order(self) -> None:
        expected = ["bigquery.jobs.insert", "bigquery.jobs.get", "bigquery.jobs.getQueryResults"]
        observed = ["bigquery.jobs.get", "bigquery.jobs.insert", "bigquery.jobs.getQueryResults"]
        self.assertEqual(first_contract_difference(expected, observed), "bigquery.jobs.get")


if __name__ == "__main__":
    unittest.main()
