#!/usr/bin/env python3
"""Run one CI command and publish a compact, payload-safe suite report."""
from __future__ import annotations

import argparse
import os
import subprocess
import time
from html import escape
from pathlib import Path


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--suite", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--artifact", required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    arguments = parser.parse_args()
    if not arguments.command or arguments.command[0] != "--" or len(arguments.command) == 1:
        parser.error("a command is required after --")
    arguments.command = arguments.command[1:]
    return arguments


def write_summary(suite: str, status: str, duration: float, artifact: str) -> None:
    summary = os.getenv("GITHUB_STEP_SUMMARY")
    passed = 1 if status == "passed" else 0
    failed = 1 if status == "failed" else 0
    line = f"| {suite} | {passed} | {failed} | 0 | 0 | {duration:.2f}s | `{artifact}` |"
    if summary:
        with open(summary, "a", encoding="utf-8") as output:
            output.write(
                "\n## Test summary\n\n"
                "| Suite | Passed | Failed | Errors | Skipped | Duration | Report artifact |\n"
                "| --- | ---: | ---: | ---: | ---: | ---: | --- |\n"
                + line
                + "\n"
            )
    else:
        print(line)


def write_report(path: Path, suite: str, status: str, duration: float, artifact: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "<!doctype html><meta charset=utf-8><title>CI suite report</title>"
        f"<h1>{escape(suite)}</h1>"
        "<table><thead><tr><th>Suite</th><th>Passed</th><th>Failed</th><th>Errors</th><th>Skipped</th><th>Duration</th><th>Artifact</th>"
        "</tr></thead><tbody>"
        f"<tr><td>{escape(suite)}</td><td>{1 if status == 'passed' else 0}</td>"
        f"<td>{1 if status == 'failed' else 0}</td><td>0</td><td>0</td>"
        f"<td>{duration:.2f}s</td><td>{escape(artifact)}</td></tr>"
        "</tbody></table>",
        encoding="utf-8",
    )


def main() -> int:
    arguments = parse_arguments()
    started = time.monotonic()
    completed = subprocess.run(arguments.command, check=False)
    duration = time.monotonic() - started
    status = "passed" if completed.returncode == 0 else "failed"
    write_report(arguments.report, arguments.suite, status, duration, arguments.artifact)
    write_summary(arguments.suite, status, duration, arguments.artifact)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
