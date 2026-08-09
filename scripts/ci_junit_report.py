#!/usr/bin/env python3
"""Render JUnit XML into a GitHub Actions summary and a standalone HTML report."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from glob import glob
from html import escape
from pathlib import Path
import os
import sys
import xml.etree.ElementTree as ET


@dataclass(frozen=True)
class TestCase:
    source: str
    suite: str
    classname: str
    name: str
    duration_seconds: float
    status: str
    detail: str


@dataclass(frozen=True)
class SourceReport:
    path: str
    cases: tuple[TestCase, ...]
    parse_error: str = ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--title", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--junit-files",
        default="",
        help="newline-separated JUnit XML paths or glob patterns",
    )
    args = parser.parse_args(argv)

    reports = tuple(_read_source(path) for path in _expand_paths(args.junit_files))
    summary = _render_summary(args.title, reports)
    html = _render_html(args.title, reports)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "summary.md").write_text(summary, encoding="utf-8")
    (args.output_dir / "index.html").write_text(html, encoding="utf-8")

    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if summary_path:
        with Path(summary_path).open("a", encoding="utf-8") as destination:
            destination.write(summary)
            destination.write("\n")
    else:
        sys.stdout.write(summary)
    return 0


def _expand_paths(raw: str) -> tuple[Path, ...]:
    paths: list[Path] = []
    seen: set[Path] = set()
    for pattern in (line.strip() for line in raw.splitlines()):
        if not pattern:
            continue
        matched = [Path(value) for value in glob(pattern, recursive=True)]
        if not matched and not any(character in pattern for character in "*?["):
            matched = [Path(pattern)]
        for path in sorted(matched):
            resolved = path.resolve()
            if resolved not in seen:
                seen.add(resolved)
                paths.append(path)
    return tuple(paths)


def _read_source(path: Path) -> SourceReport:
    display_path = path.as_posix()
    if not path.is_file():
        return SourceReport(display_path, (), "JUnit XML was not produced")
    try:
        root = ET.parse(path).getroot()
    except (ET.ParseError, OSError) as error:
        return SourceReport(display_path, (), f"Could not read JUnit XML: {error}")

    cases: list[TestCase] = []
    for testcase in root.iter("testcase"):
        cases.append(
            TestCase(
                source=display_path,
                suite=_suite_name(root, testcase),
                classname=testcase.get("classname", ""),
                name=testcase.get("name", "unnamed test"),
                duration_seconds=_float_attribute(testcase, "time"),
                status=_case_status(testcase),
                detail=_case_detail(testcase),
            )
        )
    return SourceReport(display_path, tuple(cases))


def _suite_name(root: ET.Element, testcase: ET.Element) -> str:
    for suite in root.iter("testsuite"):
        if testcase in list(suite):
            return suite.get("name", "JUnit suite")
    return root.get("name", "JUnit suite")


def _float_attribute(element: ET.Element, name: str) -> float:
    try:
        return float(element.get(name, "0"))
    except ValueError:
        return 0.0


def _case_status(testcase: ET.Element) -> str:
    if testcase.find("failure") is not None:
        return "failed"
    if testcase.find("error") is not None:
        return "error"
    if testcase.find("skipped") is not None:
        return "skipped"
    return "passed"


def _case_detail(testcase: ET.Element) -> str:
    for tag in ("failure", "error", "skipped"):
        node = testcase.find(tag)
        if node is None:
            continue
        message = node.get("message", "").strip()
        body = (node.text or "").strip()
        return "\n".join(part for part in (message, body) if part)
    return ""


def _counts(reports: tuple[SourceReport, ...]) -> dict[str, int]:
    counts = {"total": 0, "passed": 0, "failed": 0, "error": 0, "skipped": 0, "unavailable": 0}
    for report in reports:
        if report.parse_error:
            counts["unavailable"] += 1
        for case in report.cases:
            counts["total"] += 1
            counts[case.status] += 1
    return counts


def _render_summary(title: str, reports: tuple[SourceReport, ...]) -> str:
    counts = _counts(reports)
    lines = [
        f"## Test report: {title}",
        "",
        "| Tests | Passed | Failed | Errors | Skipped | Missing or invalid JUnit |",
        "| ---: | ---: | ---: | ---: | ---: | ---: |",
        "| {total} | {passed} | {failed} | {error} | {skipped} | {unavailable} |".format(**counts),
        "",
    ]
    if reports:
        lines.extend(["| JUnit source | Result |", "| --- | --- |"])
        for report in reports:
            if report.parse_error:
                result = report.parse_error
            else:
                source_counts = _counts((report,))
                result = (
                    f"{source_counts['total']} tests; {source_counts['failed']} failed; "
                    f"{source_counts['error']} errors; {source_counts['skipped']} skipped"
                )
            lines.append(f"| `{report.path}` | {result} |")
    else:
        lines.append("No JUnit producer was configured for this job.")
    lines.extend(
        [
            "",
            "Download the matching `test-report-*` artifact for the standalone HTML report and JUnit XML.",
        ]
    )
    return "\n".join(lines)


def _render_html(title: str, reports: tuple[SourceReport, ...]) -> str:
    counts = _counts(reports)
    rows: list[str] = []
    failures: list[str] = []
    for report in reports:
        if report.parse_error:
            failures.append(
                "<section class=\"notice unavailable\"><h2>{}</h2><p>{}</p></section>".format(
                    escape(report.path), escape(report.parse_error)
                )
            )
        for case in report.cases:
            detail = ""
            if case.detail:
                detail = f"<details><summary>Details</summary><pre>{escape(case.detail)}</pre></details>"
            rows.append(
                "<tr class=\"{status}\"><td>{source}</td><td>{suite}</td><td>{name}</td>"
                "<td>{duration:.3f}s</td><td>{status}</td><td>{detail}</td></tr>".format(
                    source=escape(case.source),
                    suite=escape(" ".join(value for value in (case.suite, case.classname) if value)),
                    name=escape(case.name),
                    duration=case.duration_seconds,
                    status=case.status,
                    detail=detail,
                )
            )
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    body {{ font-family: system-ui, sans-serif; color: #17202a; margin: 2rem; max-width: 1200px; }}
    h1, h2 {{ margin-bottom: .4rem; }}
    .counts {{ display: grid; grid-template-columns: repeat(6, minmax(7rem, 1fr)); gap: .75rem; margin: 1.5rem 0; }}
    .count {{ border: 1px solid #ccd1d1; padding: .75rem; }}
    .count strong {{ display: block; font-size: 1.4rem; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ border: 1px solid #d5dbdb; padding: .55rem; text-align: left; vertical-align: top; }}
    th {{ background: #f4f6f6; }}
    tr.failed, tr.error {{ background: #fdedec; }}
    tr.skipped {{ background: #fef9e7; }}
    .notice {{ border-left: 4px solid #d68910; background: #fef9e7; padding: .75rem 1rem; margin: 1rem 0; }}
    pre {{ white-space: pre-wrap; overflow-wrap: anywhere; }}
    @media (max-width: 720px) {{ .counts {{ grid-template-columns: repeat(2, minmax(7rem, 1fr)); }} table {{ font-size: .85rem; }} }}
  </style>
</head>
<body>
  <h1>{title}</h1>
  <p>Generated from JUnit XML produced by this workflow job.</p>
  <section class="counts">
    {count_cards}
  </section>
  {notices}
  <table>
    <thead><tr><th>Source</th><th>Suite</th><th>Test</th><th>Duration</th><th>Status</th><th>Failure detail</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
</body>
</html>
""".format(
        title=escape(title),
        count_cards="".join(
            f'<div class="count"><span>{escape(label)}</span><strong>{counts[key]}</strong></div>'
            for key, label in (
                ("total", "Tests"),
                ("passed", "Passed"),
                ("failed", "Failed"),
                ("error", "Errors"),
                ("skipped", "Skipped"),
                ("unavailable", "Unavailable"),
            )
        ),
        notices="\n  ".join(failures),
        rows="\n      ".join(rows) or "<tr><td colspan=\"6\">No JUnit test cases were produced.</td></tr>",
    )


if __name__ == "__main__":
    raise SystemExit(main())
