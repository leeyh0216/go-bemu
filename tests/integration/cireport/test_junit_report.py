"""Regression tests for the standalone CI JUnit report renderer."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = REPOSITORY_ROOT / "scripts" / "ci_junit_report.py"
SPEC = importlib.util.spec_from_file_location("ci_junit_report", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
REPORT = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = REPORT
SPEC.loader.exec_module(REPORT)


class JUnitReportTest(unittest.TestCase):
    def test_renders_summary_and_html_with_all_terminal_statuses(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "result.xml"
            source.write_text(
                """<?xml version=\"1.0\"?>
<testsuite name=\"sample\">
  <testcase classname=\"suite\" name=\"passes\" time=\"0.125\"/>
  <testcase classname=\"suite\" name=\"fails\"><failure message=\"expected 1\">actual 2</failure></testcase>
  <testcase classname=\"suite\" name=\"errors\"><error message=\"broken\">trace</error></testcase>
  <testcase classname=\"suite\" name=\"skips\"><skipped message=\"not applicable\"/></testcase>
</testsuite>
""",
                encoding="utf-8",
            )
            output = root / "report"

            self.assertEqual(
                0,
                REPORT.main(
                    [
                        "--title",
                        "Sample result",
                        "--junit-files",
                        str(source),
                        "--output-dir",
                        str(output),
                    ]
                ),
            )

            summary = (output / "summary.md").read_text(encoding="utf-8")
            html = (output / "index.html").read_text(encoding="utf-8")
            self.assertIn("| 4 | 1 | 1 | 1 | 1 | 0 |", summary)
            self.assertIn("actual 2", html)
            self.assertIn("not applicable", html)

    def test_reports_missing_junit_without_masking_the_job_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "report"
            missing = root / "missing.xml"

            self.assertEqual(
                0,
                REPORT.main(
                    [
                        "--title",
                        "Missing result",
                        "--junit-files",
                        str(missing),
                        "--output-dir",
                        str(output),
                    ]
                ),
            )

            summary = (output / "summary.md").read_text(encoding="utf-8")
            self.assertIn("JUnit XML was not produced", summary)


if __name__ == "__main__":
    unittest.main()
