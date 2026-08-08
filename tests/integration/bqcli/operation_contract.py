"""Fail-closed operation markers for the standalone bq CLI runner."""

from __future__ import annotations

from collections.abc import Callable
from functools import lru_cache
import json
from pathlib import Path
from typing import Any, TypeVar


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
OPERATION_MANIFEST_PATH = (
    REPOSITORY_ROOT / "contract" / "operations.normalized.json"
)
F = TypeVar("F", bound=Callable[..., Any])


class OperationContractError(RuntimeError):
    """Raised when a bq marker drifts from the canonical operation manifest."""


@lru_cache(maxsize=1)
def _known_operation_ids() -> frozenset[str]:
    try:
        with OPERATION_MANIFEST_PATH.open("r", encoding="utf-8") as stream:
            manifest = json.load(stream)
        operations = manifest["operations"]
        operation_ids = [operation["id"] for operation in operations]
    except (OSError, json.JSONDecodeError, KeyError, TypeError) as error:
        raise OperationContractError(
            "cannot read canonical contract/operations.normalized.json"
        ) from error
    if (
        not operation_ids
        or any(not isinstance(operation_id, str) for operation_id in operation_ids)
        or len(operation_ids) != len(set(operation_ids))
    ):
        raise OperationContractError(
            "canonical operation manifest has missing or duplicate operation IDs"
        )
    return frozenset(operation_ids)


def operation(operation_id: str, /) -> Callable[[F], F]:
    """Attach one canonical operation ID to a runner function."""

    if not isinstance(operation_id, str) or operation_id not in _known_operation_ids():
        raise OperationContractError(f"unknown operation ID: {operation_id!r}")

    def decorate(function: F) -> F:
        declared = tuple(getattr(function, "__bqemu_operation_ids__", ()))
        if operation_id in declared:
            raise OperationContractError(
                f"duplicate operation ID {operation_id!r} on {function.__name__}"
            )
        setattr(function, "__bqemu_operation_ids__", declared + (operation_id,))
        return function

    return decorate


def validate_declared_operations(*functions: Callable[..., Any]) -> None:
    """Require runner entrypoints to declare canonical operation IDs."""

    known = _known_operation_ids()
    for function in functions:
        declared = tuple(getattr(function, "__bqemu_operation_ids__", ()))
        if not declared:
            raise OperationContractError(
                f"bq contract entrypoint {function.__name__} has no operation IDs"
            )
        unknown = set(declared) - known
        if unknown:
            raise OperationContractError(
                f"bq contract entrypoint {function.__name__} has unknown IDs: "
                + ",".join(sorted(unknown))
            )
