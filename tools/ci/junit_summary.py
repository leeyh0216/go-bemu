#!/usr/bin/env python3
"""Write a compact, payload-safe JUnit summary to GitHub Actions output."""
from __future__ import annotations

import os
import sys
from html import escape
import xml.etree.ElementTree as etree
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit("usage: junit_summary.py <suite> <junit.xml>")
    suite, path = sys.argv[1], Path(sys.argv[2])
    if not path.is_file():
        write_missing_report(suite, path)
        return 0
    root = etree.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else root.findall(".//testsuite")
    counts = {name: sum(int(node.get(name, "0")) for node in suites) for name in ("tests", "failures", "errors", "skipped")}
    duration = sum(float(node.get("time", "0")) for node in suites)
    line = f"| {suite} | {counts['tests']} | {counts['failures']} | {counts['errors']} | {counts['skipped']} | {duration:.2f}s |"
    cases = []
    for case in root.findall(".//testcase"):
        status = "failed" if case.find("failure") is not None else "error" if case.find("error") is not None else "skipped" if case.find("skipped") is not None else "passed"
        cases.append(f"<tr><td>{escape(case.get('classname', ''))}</td><td>{escape(case.get('name', ''))}</td><td>{status}</td><td>{escape(case.get('time', '0'))}s</td></tr>")
    path.with_suffix(".html").write_text("<!doctype html><meta charset=utf-8><title>JUnit report</title><h1>" + escape(suite) + "</h1><p>tests=" + str(counts["tests"]) + " failures=" + str(counts["failures"]) + " errors=" + str(counts["errors"]) + " skipped=" + str(counts["skipped"]) + "</p><table><thead><tr><th>Class</th><th>Case</th><th>Status</th><th>Duration</th></tr></thead><tbody>" + "".join(cases) + "</tbody></table>", encoding="utf-8")
    summary = os.getenv("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as output:
            output.write("\n## Test summary\n\n| Suite | Tests | Failures | Errors | Skipped | Duration |\n| --- | ---: | ---: | ---: | ---: | ---: |\n" + line + "\n")
    else:
        print(line)
    return 0


def write_missing_report(suite: str, path: Path) -> None:
    """Keep the job summary readable when setup fails before pytest writes XML."""
    path.with_suffix(".html").write_text(
        "<!doctype html><meta charset=utf-8><title>JUnit report unavailable</title>"
        "<h1>" + escape(suite) + "</h1>"
        "<p>The test command did not produce JUnit XML. Inspect the job log; no raw output is copied into this report.</p>",
        encoding="utf-8",
    )
    line = f"| {suite} | 0 | 0 | 1 | 0 | unavailable |"
    summary = os.getenv("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as output:
            output.write("\n## Test summary\n\n| Suite | Tests | Failures | Errors | Skipped | Duration |\n| --- | ---: | ---: | ---: | ---: | ---: |\n" + line + "\n")
    else:
        print(line)


if __name__ == "__main__":
    raise SystemExit(main())
