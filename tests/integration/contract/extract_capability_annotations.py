#!/usr/bin/env python3
"""Extract literal integration contract_case decorators with Python's AST."""

from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
import sys
from typing import Any


class ExtractionError(ValueError):
    pass


def _name(node: ast.AST, value: str) -> bool:
    return isinstance(node, ast.Name) and node.id == value


def _pytest_marker(node: ast.AST, marker: str) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == marker
        and isinstance(node.value, ast.Attribute)
        and node.value.attr == "mark"
        and _name(node.value.value, "pytest")
    )


def _literal_string(node: ast.AST, label: str) -> str:
    if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
        raise ExtractionError(f"{label} must be one literal string")
    return node.value


def _literal_bool(node: ast.AST, label: str) -> bool:
    if not isinstance(node, ast.Constant) or type(node.value) is not bool:
        raise ExtractionError(f"{label} must be literal True or False")
    return node.value


def _location(path: Path, node: ast.AST) -> str:
    return f"{path.as_posix()}:{getattr(node, 'lineno', '?')}"


def _literal_operation_ids(node: ast.AST, label: str) -> list[str]:
    if not isinstance(node, (ast.List, ast.Tuple)):
        raise ExtractionError(f"{label} must be a literal tuple or list of operation IDs")
    operation_ids = [_literal_string(value, label) for value in node.elts]
    if not operation_ids:
        raise ExtractionError(f"{label} must not be empty")
    if len(operation_ids) != len(set(operation_ids)):
        raise ExtractionError(f"{label} repeats an operation ID")
    return sorted(operation_ids)


def _parse_case(path: Path, node: ast.Call, test_id: str) -> dict[str, Any]:
    if len(node.args) != 1:
        raise ExtractionError(f"{_location(path, node)} contract_case requires one literal capability ID")
    capability_id = _literal_string(node.args[0], f"{_location(path, node)} contract_case capability ID")
    values: dict[str, str] = {}
    operation_ids: list[str] = []
    strict_xfail = False
    for keyword in node.keywords:
        if keyword.arg is None:
            raise ExtractionError(f"{_location(path, node)} contract_case cannot use **metadata")
        if keyword.arg == "strict_xfail":
            strict_xfail = _literal_bool(keyword.value, f"{_location(path, node)} strict_xfail")
            continue
        if keyword.arg == "operations":
            if operation_ids:
                raise ExtractionError(f"{_location(path, node)} contract_case repeats operations")
            operation_ids = _literal_operation_ids(keyword.value, f"{_location(path, node)} contract_case operations")
            continue
        if keyword.arg in values:
            raise ExtractionError(f"{_location(path, node)} contract_case repeats {keyword.arg}")
        values[keyword.arg] = _literal_string(
            keyword.value, f"{_location(path, node)} contract_case {keyword.arg}"
        )
    allowed = {"state", "category", "summary", "profile", "wire_flow", "issue", "limitation"}
    unknown = sorted(set(values) - allowed)
    if unknown:
        raise ExtractionError(f"{_location(path, node)} contract_case has unknown metadata {unknown[0]}")
    for key in ("state", "category", "summary", "profile"):
        if not values.get(key):
            raise ExtractionError(f"{_location(path, node)} contract_case {capability_id} must define {key}")
    return {
        "id": capability_id,
        "state": values["state"],
        "category": values["category"],
        "summary": values["summary"],
        "profile": values["profile"],
        "wireFlow": values.get("wire_flow", ""),
        "operationIds": operation_ids,
        "issue": values.get("issue", ""),
        "limitation": values.get("limitation", ""),
        "strictXfail": strict_xfail,
        "test": test_id,
    }


def _extract_file(root: Path, path: Path) -> list[dict[str, Any]]:
    try:
        module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError as error:
        raise ExtractionError(f"{path.as_posix()}:{error.lineno} invalid Python syntax: {error.msg}") from error
    relative = path.relative_to(root)
    decorator_calls: set[int] = set()
    annotations: list[dict[str, Any]] = []
    for node in module.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or not node.name.startswith("test_"):
            continue
        for decorator in node.decorator_list:
            for call in ast.walk(decorator):
                if not isinstance(call, ast.Call) or not _name(call.func, "contract_case"):
                    continue
                decorator_calls.add(id(call))
                annotations.append(
                    _parse_case(
                        relative,
                        call,
                        f"spark:{relative.as_posix()}:{node.name}",
                    )
                )
    for node in ast.walk(module):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "contract_case":
            raise ExtractionError(f"{_location(relative, node)} contract_case must be imported, not redefined")
        if isinstance(node, ast.Assign) and _name(node.value, "contract_case"):
            raise ExtractionError(f"{_location(relative, node)} contract_case must not be aliased")
        if isinstance(node, ast.ImportFrom):
            for imported in node.names:
                if imported.name == "contract_case" and imported.asname:
                    raise ExtractionError(f"{_location(relative, node)} contract_case must not be imported with an alias")
        if not isinstance(node, ast.Call):
            continue
        if _pytest_marker(node.func, "operation"):
            raise ExtractionError(
                f"{_location(relative, node)} Spark operation IDs belong in contract_case(..., operations=(...))"
            )
        if _pytest_marker(node.func, "capability"):
            raise ExtractionError(f"{_location(relative, node)} still uses the retired pytest.mark.capability marker")
        if _name(node.func, "contract_case") and id(node) not in decorator_calls:
            raise ExtractionError(f"{_location(relative, node)} contract_case must appear in a test decorator or pytest.param metadata")
    return annotations


def extract(root: Path) -> list[dict[str, Any]]:
    directory = root / "tests" / "integration" / "spark"
    paths = sorted(directory.glob("test_*.py"))
    if not paths:
        raise ExtractionError(f"no Spark integration test sources under {directory.relative_to(root).as_posix()}")
    annotations: list[dict[str, Any]] = []
    for path in paths:
        annotations.extend(_extract_file(root, path))
    return annotations


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    arguments = parser.parse_args()
    try:
        annotations = extract(arguments.root.resolve())
    except ExtractionError as error:
        print(f"contract_case extraction failed: {error}", file=sys.stderr)
        return 1
    json.dump(annotations, sys.stdout, sort_keys=True, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
