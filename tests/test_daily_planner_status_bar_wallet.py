from datetime import datetime, timezone

from streamlit_ui.components.daily_planner.status_bar import free_slots_by_character, plan_summary, status_tiles
from streamlit_ui.pages.corporations import _wallet_age

W = {"age_hours": 1.5, "budget": 766_390_384, "reserve_pct": 10, "blocked_reason": None}
NOW = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)


def _tiles(summary, **kw):
    args = dict(summary=summary, corp_wallet=851_544_871.0, actions=[], freshness_display="100% (fresh) · 5/5 items",
                market_status={}, market_refresh_hours=3.0, now=NOW)
    return {label: (value, sub) for label, value, sub in status_tiles(**{**args, **kw})}


def test_budget_tile_ages_with_the_plan():
    stored = {**W, "refreshed_at": "2026-11-16T08:30:00+00:00"}
    assert _tiles({"wallet": stored})["Budget"] == (
        "766.39M ISK", "wallet 851.54M ISK · 10% reserve · read 3.5 h ago")
    assert _tiles({"wallet": {**W, "refreshed_at": None}})["Budget"][1].endswith("read 1.5 h ago")


def test_budget_tile_old_plan_shows_the_wallet():
    assert _tiles({})["Budget"] == ("851.54M ISK", "corp wallet (no budget in this plan)")


def test_freshness_and_market_tiles():
    tiles = _tiles({}, market_status={"last_completed_at": "2026-11-16T11:00:00"})
    assert tiles["Freshness"] == ("100% (fresh)", "5/5 items")
    assert tiles["Market data"] == ("1.0h ago", "fresh")
    assert _tiles({})["Market data"] == ("—", "no completed run")


def test_to_buy_tile_counts_pending_purchases():
    actions = [{"action_type": "buy_materials", "status": "pending", "estimated_cost_isk": 2_000_000.0},
               {"action_type": "buy_materials", "status": "done", "estimated_cost_isk": 9.0}]
    assert _tiles({}, actions=actions)["To buy today"] == ("2.00M ISK", "1 purchase(s) plus job install fees")


def test_free_slots_lines():
    free = {
        "a": {"character_name": "Duke X1", "manufacturing": 3, "science": 2,
              "manufacturing_reason": "wallet", "science_reason": "no eligible job"},
        "b": {"character_name": "Only Sci", "manufacturing": 0, "science": 3,
              "manufacturing_reason": "x", "science_reason": None},
        "c": {"character_name": "Full", "manufacturing": 0, "science": 0, "science_reason": "x"},
    }
    assert free_slots_by_character(free) == {
        "Duke X1": "3 mfg free (wallet), 2 science free (no eligible job)", "Only Sci": "3 science free",
    }
    assert free_slots_by_character(None) == {}


def test_free_slots_lines_old_single_reason_summary():
    old = {"a": {"character_name": "Duke X1", "manufacturing": 2, "science": 1, "reason": "no eligible job"}}
    assert free_slots_by_character(old) == {"Duke X1": "2 mfg free, 1 science free (no eligible job)"}


def test_plan_summary_old_or_broken():
    assert plan_summary({}) == {}
    assert plan_summary({"plan_summary_json": "{nope"}) == {}
    assert plan_summary({"plan_summary_json": '{"wallet": {}}'}) == {"wallet": {}}


def test_corporations_wallet_age_missing_is_never_read_and_stale():
    text, stale = _wallet_age(float("nan"), 6.0)
    assert text == "never read" and stale
