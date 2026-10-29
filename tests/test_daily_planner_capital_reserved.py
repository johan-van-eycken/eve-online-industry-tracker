"""Capital Reserved: pending purchases plus estimated job install fees (D4, A2)."""
from __future__ import annotations

from streamlit_ui.components.daily_planner import status_bar  # noqa: E402


def _a(action_type, cost, status="pending", fee=None):
    return {"action_type": action_type, "estimated_cost_isk": cost, "status": status,
            "estimated_job_fee_isk": fee}


def test_manufacture_material_cost_is_not_counted_on_top_of_buy_materials():
    actions = [
        _a("manufacture", 300.0, fee=20.0),
        _a("sub_manufacture", 100.0, fee=5.0),
        _a("buy_materials", 250.0),
        _a("buy_materials", 150.0),
    ]
    reserved = status_bar.compute_capital_reserved(actions)
    # 250 + 150 purchases + 20 + 5 fees; the jobs' material costs are not added.
    assert reserved == status_bar.CapitalReserved(425.0, 0, 0)


def test_buy_bpo_counts_and_non_pending_are_ignored_and_unpriced_purchases_are_counted():
    actions = [
        _a("buy_bpo", 1000.0),
        _a("buy_materials", 50.0, status="done"),
        _a("manufacture", 50.0, status="done", fee=None),
        _a("buy_materials", None),
        _a("deliver", 9.0),
    ]
    reserved = status_bar.compute_capital_reserved(actions)
    assert (reserved.total, reserved.unpriced_purchases, reserved.unknown_fee_jobs) == (1000.0, 1, 0)


def test_empty_plan_reserves_nothing():
    assert status_bar.compute_capital_reserved([]) == status_bar.CapitalReserved(0.0, 0, 0)
    assert status_bar.capital_reserved_display(status_bar.CapitalReserved(0.0, 0, 0)) == ("—", None)


def test_every_job_type_adds_its_fee_including_research():
    actions = [
        _a("manufacture", 300.0, fee=25.0),
        _a("invent", 900.0, fee=4.0),       # cost = inputs + fee; only the fee is added
        _a("copy", None, fee=0.5),
        _a("me_research", None, fee=10.0),
        _a("te_research", None, fee=20.0),
        _a("buy_materials", 400.0),
    ]
    reserved = status_bar.compute_capital_reserved(actions)
    assert reserved == status_bar.CapitalReserved(459.5, 0, 0)
    assert status_bar.capital_reserved_display(reserved) == ("460 ISK", None)


def test_an_unpriced_purchase_makes_the_total_unknown():
    reserved = status_bar.CapitalReserved(total=1_000.0, unpriced_purchases=2, unknown_fee_jobs=0)
    assert status_bar.capital_reserved_display(reserved) == ("unknown", "2 purchase(s) have no price")


def test_an_unknown_fee_makes_the_total_unknown_too_never_a_partial_sum():
    reserved = status_bar.compute_capital_reserved([_a("buy_materials", 2_500_000.0), _a("me_research", None)])
    assert reserved == status_bar.CapitalReserved(2_500_000.0, 0, 1)
    assert status_bar.capital_reserved_display(reserved) == (
        "unknown", "install fee unknown for 1 job(s)" + status_bar.STALE_FEE_HINT,
    )


def test_both_unknowns_are_named():
    reserved = status_bar.CapitalReserved(total=5.0, unpriced_purchases=1, unknown_fee_jobs=3)
    assert status_bar.capital_reserved_display(reserved) == (
        "unknown", "1 purchase(s) have no price; install fee unknown for 3 job(s)" + status_bar.STALE_FEE_HINT,
    )


def test_a_legacy_action_without_the_fee_column_counts_as_unknown():
    legacy = {"action_type": "manufacture", "estimated_cost_isk": 300.0, "status": "pending"}
    assert status_bar.compute_capital_reserved([legacy]).unknown_fee_jobs == 1


def test_capital_reserved_help_states_scope():
    h = status_bar.CAPITAL_RESERVED_HELP
    assert "install fee" in h and "unknown" in h and "research" in h
    assert "≥" not in h and "count as 0" not in h


def test_an_unknown_fee_caption_says_how_to_fix_an_old_cache_or_plan():
    """Fix round 1: a plan or overview from before the fee estimates has no
    fees at all; the caption names the fix."""
    assert "recompute the plan" in status_bar.STALE_FEE_HINT
    assert "refresh the product overview" in status_bar.STALE_FEE_HINT
    caption = status_bar.capital_reserved_display(status_bar.CapitalReserved(0.0, 0, 2))[1]
    assert caption.endswith(status_bar.STALE_FEE_HINT)
    # An unpriced purchase alone does not get the fee hint.
    assert status_bar.capital_reserved_display(status_bar.CapitalReserved(0.0, 1, 0))[1] == (
        "1 purchase(s) have no price"
    )


# --- Task 8b fix round 1: name the cause; the refresh hint only for stale fees ---

def _r(action_type, fee=None, reason=None):
    return {**_a(action_type, None, fee=fee), "estimated_job_fee_unknown_reason": reason}


def test_an_unknown_fee_with_a_reason_names_the_cause_without_the_refresh_hint():
    reserved = status_bar.compute_capital_reserved([
        _r("manufacture", reason="the industry profile has no manufacturing cost index"),
        _r("copy", reason="the industry profile has no manufacturing cost index"),
        _r("invent", reason="the facility tax is unknown"),
    ])
    assert reserved.unknown_fee_jobs == 3
    value, caption = status_bar.capital_reserved_display(reserved)
    assert value == "unknown"
    assert caption == (
        "install fee unknown for 3 job(s): the facility tax is unknown (1 job(s)); "
        "the industry profile has no manufacturing cost index (2 job(s))"
    )
    assert status_bar.STALE_FEE_HINT not in caption


def test_the_refresh_hint_shows_only_for_the_unknowns_without_a_reason():
    reserved = status_bar.compute_capital_reserved([
        _r("manufacture", reason="no cost index"),
        _r("te_research"),  # an old plan / overview: no reason recorded
    ])
    caption = status_bar.capital_reserved_display(reserved)[1]
    assert "no cost index (1 job(s))" in caption
    assert caption.endswith(status_bar.STALE_FEE_HINT)


# --- Task 8c fix round 5: the queued TE's fee is shown apart ------------------------

def test_a_queued_te_fee_stays_in_the_total_and_is_named_in_the_caption():
    actions = [
        _a("buy_materials", 400.0),
        _a("me_research", None, fee=10.0),
        {**_a("te_research", None, fee=20.0), "queued_after_me": True},
    ]
    reserved = status_bar.compute_capital_reserved(actions)
    assert reserved.total == 430.0
    assert (reserved.queued_fee, reserved.queued_jobs) == (20.0, 1)
    assert status_bar.capital_reserved_display(reserved) == (
        "430 ISK", "incl. 20 ISK for 1 queued job(s)",
    )


def test_without_queued_jobs_there_is_no_queued_caption():
    reserved = status_bar.compute_capital_reserved([_a("te_research", None, fee=20.0)])
    assert (reserved.queued_fee, reserved.queued_jobs) == (0.0, 0)
    assert status_bar.capital_reserved_display(reserved) == ("20 ISK", None)
