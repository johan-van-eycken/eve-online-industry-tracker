from streamlit_ui.components.daily_planner.status_bar import free_slots_lines, plan_summary, wallet_caption
from streamlit_ui.pages.corporations import _wallet_age

W = {"age_hours": 1.5, "budget": 766_390_384, "reserve_pct": 10, "blocked_reason": None}


def test_wallet_caption_fresh():
    assert wallet_caption(W) == "read 1.5 h ago · budget 766,390,384 ISK after 10% reserve"


def test_wallet_caption_blocked():
    text = wallet_caption({**W, "budget": 0, "blocked_reason": "corp wallet last read 7.0 h ago (limit 6 h)"})
    assert text.startswith("⚠ corp wallet last read")


def test_wallet_caption_old_plan():
    assert wallet_caption(None) is None


def test_free_slots_lines():
    free = {
        "a": {"character_name": "Duke X1", "manufacturing": 2, "science": 1, "reason": "no eligible job"},
        "b": {"character_name": "Only Sci", "manufacturing": 0, "science": 3, "reason": None},
        "c": {"character_name": "Full", "manufacturing": 0, "science": 0, "reason": "x"},
    }
    assert free_slots_lines(free) == ["Duke X1: 2 mfg, 1 science free (no eligible job)", "Only Sci: 3 science free"]
    assert free_slots_lines(None) == []


def test_plan_summary_old_or_broken():
    assert plan_summary({}) == {}
    assert plan_summary({"plan_summary_json": "{nope"}) == {}
    assert plan_summary({"plan_summary_json": '{"wallet": {}}'}) == {"wallet": {}}


def test_corporations_wallet_age_missing_is_never_read_and_stale():
    text, stale = _wallet_age(float("nan"), 6.0)
    assert text == "never read" and stale
