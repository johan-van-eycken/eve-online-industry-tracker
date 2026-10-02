"""A contract violation reaches the user as a specific, persistent red banner.

Before this, a PlannerInputError surfaced only as a generic message, and the
status bar showed a failure only on the poll that saw running -> failed: a
page reload hid it, leaving the user looking at the previous plan.
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.application.daily_planner.input_row import PlannerInputError  # noqa: E402
from eve_online_industry_tracker.application.daily_planner.service import DailyPlannerService  # noqa: E402


def _bare_service():
    return DailyPlannerService(
        industry_service=SimpleNamespace(get_cached_overview_rows=lambda: []),
        corporations_service=SimpleNamespace(list_corporations=lambda: []),
        characters_service=SimpleNamespace(list_characters=lambda: []),
        sales_history_service=SimpleNamespace(),
        market_pricing_service=SimpleNamespace(),
        realized_profit_service=SimpleNamespace(),
        repo=SimpleNamespace(),
        admin_settings=SimpleNamespace(),
        session_provider=SimpleNamespace(),
    )


def _fail_with(svc, exc):
    def boom():
        raise exc
    svc._phase_1_collect = boom
    svc._run_compute()
    return svc.get_compute_status()


def test_a_contract_violation_fails_the_compute_with_a_specific_message():
    status = _fail_with(_bare_service(), PlannerInputError(
        type_id=12345, field="quantity", detail="is missing"))
    assert status["status"] == "failed"
    assert "'quantity'" in status["error"]
    assert "type_id=12345" in status["error"]
    assert "is missing" in status["error"]
    assert "refresh the product overview" in status["error"]


def test_a_contract_violation_without_a_type_id_names_an_unidentified_item():
    status = _fail_with(_bare_service(), PlannerInputError(
        type_id=None, field="type_id", detail="is missing"))
    assert status["status"] == "failed"
    assert "None" not in status["error"]
    assert "an unidentified item" in status["error"]


# --- Status bar: a failed compute is a red banner, every time ------------------

def test_a_failed_status_produces_a_banner_message():
    from streamlit_ui.components.daily_planner.status_bar import compute_failure_banner

    msg = compute_failure_banner({"status": "failed", "error": "Product overview is bad."},
                                 has_plan=False)
    assert msg is not None
    assert "Product overview is bad." in msg


def test_a_failed_status_over_an_older_plan_says_the_plan_is_the_previous_one():
    from streamlit_ui.components.daily_planner.status_bar import compute_failure_banner

    msg = compute_failure_banner({"status": "failed", "error": "x"}, has_plan=True)
    assert "previous" in msg.lower()


def test_a_failed_status_without_an_error_still_produces_a_banner():
    from streamlit_ui.components.daily_planner.status_bar import compute_failure_banner

    assert compute_failure_banner({"status": "failed", "error": None}, has_plan=False)


@pytest.mark.parametrize("status", ["idle", "running", "done"])
def test_no_banner_unless_the_compute_failed(status):
    from streamlit_ui.components.daily_planner.status_bar import compute_failure_banner

    assert compute_failure_banner({"status": status, "error": "stale"}, has_plan=True) is None
