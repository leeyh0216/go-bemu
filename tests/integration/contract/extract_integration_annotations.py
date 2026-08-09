#!/usr/bin/env python3
"""Extract literal integration operation and Spark contract annotations with AST."""

from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
import sys
from typing import Any


class ExtractionError(ValueError):
    pass


_DIRECTORIES = (
    ("python", Path("tests/integration/python")),
    ("spark", Path("tests/integration/spark")),
    ("bq", Path("tests/integration/bqcli")),
)


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
    if len(operation_ids) != len(set(operation_ids)):
        raise ExtractionError(f"{label} repeats an operation ID")
    return sorted(operation_ids)


def _parse_contract_case(path: Path, node: ast.Call, test_id: str) -> dict[str, Any]:
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
        "family": "spark",
        "test": test_id,
        "operationIds": operation_ids,
        "scenario": "",
        "capabilityId": capability_id,
        "state": values["state"],
        "category": values["category"],
        "summary": values["summary"],
        "profile": values["profile"],
        "wireFlow": values.get("wire_flow", ""),
        "issue": values.get("issue", ""),
        "limitation": values.get("limitation", ""),
        "strictXfail": strict_xfail,
    }


def _parse_pytest_operation(path: Path, node: ast.Call) -> str:
    if len(node.args) != 1 or node.keywords:
        raise ExtractionError(f"{_location(path, node)} operation marker must contain one literal operation ID")
    return _literal_string(node.args[0], f"{_location(path, node)} operation marker")


def _parse_bq_operation(path: Path, node: ast.Call) -> tuple[str, str]:
    if len(node.args) != 1:
        raise ExtractionError(f"{_location(path, node)} operation marker must contain one literal operation ID")
    operation_id = _literal_string(node.args[0], f"{_location(path, node)} operation marker")
    if len(node.keywords) != 1 or node.keywords[0].arg != "scenario":
        raise ExtractionError(f"{_location(path, node)} bq operation marker must declare one literal scenario")
    scenario = _literal_string(node.keywords[0].value, f"{_location(path, node)} operation scenario")
    if not scenario:
        raise ExtractionError(f"{_location(path, node)} operation scenario must not be empty")
    return operation_id, scenario


def _audit_aliases(path: Path, module: ast.Module, family: str) -> None:
    for node in ast.walk(module):
        if family == "spark":
            if (
                path.name != "conftest.py"
                and isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == "contract_case"
            ):
                raise ExtractionError(f"{_location(path, node)} contract_case must be imported, not redefined")
            if isinstance(node, ast.Assign) and _name(node.value, "contract_case"):
                raise ExtractionError(f"{_location(path, node)} contract_case must not be aliased")
            if isinstance(node, ast.ImportFrom):
                for imported in node.names:
                    if imported.name == "contract_case" and imported.asname:
                        raise ExtractionError(f"{_location(path, node)} contract_case must not be imported with an alias")
        if family == "bq":
            if (
                path.name != "operation_contract.py"
                and isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == "operation"
            ):
                raise ExtractionError(f"{_location(path, node)} operation must be imported, not redefined")
            if isinstance(node, ast.Assign) and _name(node.value, "operation"):
                raise ExtractionError(f"{_location(path, node)} operation must not be aliased")
            if isinstance(node, ast.ImportFrom):
                for imported in node.names:
                    if imported.name == "operation" and imported.asname:
                        raise ExtractionError(f"{_location(path, node)} operation must not be imported with an alias")


def _extract_python(path: Path, module: ast.Module, family: str) -> list[dict[str, Any]]:
    annotations: list[dict[str, Any]] = []
    for node in module.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or not node.name.startswith("test_"):
            continue
        operation_ids = [
            _parse_pytest_operation(path, decorator)
            for decorator in node.decorator_list
            if isinstance(decorator, ast.Call) and _pytest_marker(decorator.func, "operation")
        ]
        if len(operation_ids) != len(set(operation_ids)):
            raise ExtractionError(f"{_location(path, node)} test function repeats an operation marker")
        if operation_ids:
            annotations.append(
                {
                    "family": family,
                    "test": f"{family}:{path.as_posix()}:{node.name}",
                    "operationIds": sorted(operation_ids),
                    "scenario": "",
                }
            )
    return annotations


def _extract_spark(path: Path, module: ast.Module) -> list[dict[str, Any]]:
    annotations: list[dict[str, Any]] = []
    decorator_calls: set[int] = set()
    for node in module.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or not node.name.startswith("test_"):
            continue
        test_id = f"spark:{path.as_posix()}:{node.name}"
        for decorator in node.decorator_list:
            for call in ast.walk(decorator):
                if not isinstance(call, ast.Call) or not _name(call.func, "contract_case"):
                    continue
                decorator_calls.add(id(call))
                annotations.append(_parse_contract_case(path, call, test_id))
    for node in ast.walk(module):
        if not isinstance(node, ast.Call):
            continue
        if _pytest_marker(node.func, "operation"):
            raise ExtractionError(
                f"{_location(path, node)} Spark operation IDs belong in contract_case(..., operations=(...))"
            )
        if _pytest_marker(node.func, "capability"):
            raise ExtractionError(f"{_location(path, node)} still uses the retired pytest.mark.capability marker")
        if _name(node.func, "contract_case") and id(node) not in decorator_calls:
            raise ExtractionError(f"{_location(path, node)} contract_case must appear in a test decorator or pytest.param metadata")
    return annotations


def _extract_bq(path: Path, module: ast.Module) -> list[dict[str, Any]]:
    annotations: list[dict[str, Any]] = []
    for node in module.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        by_scenario: dict[str, list[str]] = {}
        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call) or not _name(decorator.func, "operation"):
                continue
            operation_id, scenario = _parse_bq_operation(path, decorator)
            by_scenario.setdefault(scenario, []).append(operation_id)
        for scenario, operation_ids in by_scenario.items():
            if len(operation_ids) != len(set(operation_ids)):
                raise ExtractionError(f"{_location(path, node)} function repeats a scenario operation marker")
            annotations.append(
                {
                    "family": "bq",
                    "test": f"bq:{path.as_posix()}:{node.name}",
                    "operationIds": sorted(operation_ids),
                    "scenario": scenario,
                }
            )
    return annotations


def _extract_file(root: Path, family: str, absolute_path: Path) -> list[dict[str, Any]]:
    relative = absolute_path.relative_to(root)
    try:
        module = ast.parse(absolute_path.read_text(encoding="utf-8"), filename=str(relative))
    except SyntaxError as error:
        raise ExtractionError(f"{relative.as_posix()}:{error.lineno} invalid Python syntax: {error.msg}") from error
    _audit_aliases(relative, module, family)
    if family == "spark":
        return _extract_spark(relative, module)
    if family == "bq":
        return _extract_bq(relative, module)
    return _extract_python(relative, module, family)


def extract(root: Path) -> list[dict[str, Any]]:
    annotations: list[dict[str, Any]] = []
    for family, directory in _DIRECTORIES:
        source_directory = root / directory
        if not source_directory.is_dir():
            continue
        for path in sorted(source_directory.rglob("*.py")):
            if "__pycache__" in path.parts or any(part.startswith(".") for part in path.relative_to(source_directory).parts):
                continue
            annotations.extend(_extract_file(root, family, path))
    annotations.sort(
        key=lambda annotation: (
            annotation["family"],
            annotation["test"],
            annotation["scenario"],
            annotation.get("capabilityId", ""),
            tuple(annotation["operationIds"]),
        )
    )
    return annotations


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    arguments = parser.parse_args()
    try:
        annotations = extract(arguments.root.resolve())
    except ExtractionError as error:
        print(f"integration annotation extraction failed: {error}", file=sys.stderr)
        return 1
    json.dump(annotations, sys.stdout, sort_keys=True, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
