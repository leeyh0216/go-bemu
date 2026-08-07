"""Unsupported jobs remain visible as strict xfails; they are never skipped."""

import pytest


@pytest.mark.parametrize(
    ("gap_id", "operation"),
    [
        pytest.param(
            "GAP-JOB-LOAD-001",
            "jobs.insert configuration.load",
            marks=pytest.mark.xfail(strict=True, reason="GAP-JOB-LOAD-001: load jobs not implemented"),
        ),
        pytest.param(
            "GAP-JOB-COPY-001",
            "jobs.insert configuration.copy",
            marks=pytest.mark.xfail(strict=True, reason="GAP-JOB-COPY-001: copy jobs not implemented"),
        ),
        pytest.param(
            "GAP-JOB-EXTRACT-001",
            "jobs.insert configuration.extract",
            marks=pytest.mark.xfail(strict=True, reason="GAP-JOB-EXTRACT-001: extract jobs not implemented"),
        ),
        pytest.param(
            "GAP-TABLEDATA-INSERTALL-001",
            "tabledata.insertAll",
            marks=pytest.mark.xfail(
                strict=True,
                reason="GAP-TABLEDATA-INSERTALL-001: insertAll not implemented",
            ),
        ),
    ],
)
@pytest.mark.gap("capability-gap")
def test_unsupported_operation_is_an_explicit_contract_gap(gap_id: str, operation: str) -> None:
    pytest.fail(f"{gap_id}: {operation} is not implemented")
