"""Strict non-claims for the deliberately narrow one-class overlay.

Spark permits repeated commit calls for an epoch and null commit-message slots
on abort. The connector wrapper and batch abort behavior require more than the
single direct-context class patched here, so those contracts remain explicit
gaps rather than being inferred from the successful normal-path test.

Official contracts:
https://spark.apache.org/docs/3.5.8/api/java/org/apache/spark/sql/connector/write/streaming/StreamingWrite.html
https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/719817782a214b8ca72be520870013a3e0253d92/spark-bigquery-dsv2/spark-3.1-bigquery-lib/src/main/java/com/google/cloud/spark/bigquery/v2/BigQueryStreamingWrite.java#L23-L41
https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/719817782a214b8ca72be520870013a3e0253d92/spark-bigquery-connector-common/src/main/java/com/google/cloud/spark/bigquery/write/context/BigQueryDirectDataSourceWriterContext.java#L199-L312
"""

from __future__ import annotations

import json
from pathlib import Path
from zipfile import ZipFile

import pytest

from conftest import REPOSITORY_ROOT, record_capability_gap


LOCK = REPOSITORY_ROOT / "tools" / "dsv2-overlay" / "overlay.lock.json"
WRAPPER_ENTRY = "com/google/cloud/spark/bigquery/v2/BigQueryStreamingWrite.class"


def _lock() -> dict[str, object]:
    return json.loads(LOCK.read_text(encoding="utf-8"))


def _patched_method(delegate: str) -> dict[str, object]:
    methods = _lock()["patch"]["methods"]
    matches = [method for method in methods if method["delegate"] == delegate]
    assert len(matches) == 1
    return matches[0]


@pytest.mark.capability("SBQ-DSV2-OVERLAY-SAME-EPOCH-REPLAY-V1")
def test_same_epoch_commit_replay_has_no_epoch_ledger(
    dsv2_overlay_jar: Path,
) -> None:
    method = _patched_method("commit")
    assert method["codeBytes"] == 34
    assert method["codeSha256"] == (
        "6174e9886129f75e6e0ecb894887605ab3ce4d430e2c9c76ae3eec79d615e8e1"
    )
    record_capability_gap(
        "SBQ-DSV2-OVERLAY-SAME-EPOCH-REPLAY-V1",
        "one-class-hook:batch-commit-delegate epoch-id-read:0 durable-ledger:0",
        "add-durable-epoch-reconciliation-before-exactly-once-claim",
    )


@pytest.mark.capability("SBQ-DSV2-OVERLAY-CHECKPOINT-FAILURE-REPLAY-V1")
def test_commit_success_before_checkpoint_has_no_reconciliation(
    dsv2_overlay_jar: Path,
) -> None:
    method = _patched_method("commit")
    assert method["delegate"] == "commit"
    assert len(_lock()["outputArtifact"]["entries"]) == 1
    record_capability_gap(
        "SBQ-DSV2-OVERLAY-CHECKPOINT-FAILURE-REPLAY-V1",
        "commit-checkpoint-window:unreconciled durable-epoch-state:0",
        "add-idempotent-epoch-ledger-and-ambiguous-commit-recovery",
    )


@pytest.mark.capability("SBQ-DSV2-OVERLAY-PARTIAL-ABORT-V1")
def test_partial_abort_wrapper_remains_unpatched(
    dsv2_overlay_jar: Path,
) -> None:
    with ZipFile(dsv2_overlay_jar) as archive:
        entries = archive.namelist()
    assert entries == _lock()["outputArtifact"]["entries"]
    assert WRAPPER_ENTRY not in entries
    record_capability_gap(
        "SBQ-DSV2-OVERLAY-PARTIAL-ABORT-V1",
        "wrapper-patched:false null-slot-normalization:false",
        "patch-or-upstream-the-dsv2-wrapper-before-partial-abort-claim",
    )


@pytest.mark.capability("SBQ-DSV2-OVERLAY-NEW-TABLE-ABORT-V1")
def test_new_table_abort_still_delegates_batch_deletion_logic(
    dsv2_overlay_jar: Path,
) -> None:
    method = _patched_method("abort")
    assert method["codeBytes"] == 6
    assert method["codeSha256"] == (
        "c2f4cab39d82fb3d4459c1c1d4f6d37ceacf45e381b3aeb4b9260dd0d2f0a3ee"
    )
    record_capability_gap(
        "SBQ-DSV2-OVERLAY-NEW-TABLE-ABORT-V1",
        "one-class-hook:batch-abort-delegate table-lifecycle-reconciliation:0",
        "separate-epoch-abort-from-whole-writer-table-cleanup",
    )
