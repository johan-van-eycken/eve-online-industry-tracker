"""F5: the status bar shows the LIVE freshness, not the stored one.

The stored plan.freshness_score is written in Phase 9 against the same market
cache the plan was just computed from, so it is always 1.0; the live value is
the top-level `freshness_score` that get_active_plan recomputes on every GET.
`or 1.0` on top of that turned a real 0.0 into "100% (fresh)".
"""
from __future__ import annotations

import os
import sys
from contextlib import contextmanager

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from streamlit_ui.components.daily_planner import status_bar  # noqa: E402
from streamlit_ui.state.daily_planner_page import DailyPlannerPageState  # noqa: E402


def _plan(score, comparable=4, total=5, stored=1.0):
    return {
        "plan": {"id": 1, "freshness_score": stored, "created_at": None,
                 "corp_wallet_snapshot": None},
        "items": [], "actions": [],
        "freshness_score": score,
        "freshness_comparable_items": comparable,
        "freshness_total_items": total,
    }


# --- the rendering helper -------------------------------------------------------

def test_the_live_score_is_used_not_the_stored_one():
    label, score = status_bar.freshness_summary(_plan(0.5, stored=1.0))
    assert score == 0.5
    assert label.startswith("50% (stale)")


def test_a_real_zero_is_zero_percent_not_one_hundred():
    label, score = status_bar.freshness_summary(_plan(0.0))
    assert score == 0.0
    assert label.startswith("0% (stale)")


def test_comparable_and_total_counts_are_shown():
    label, _ = status_bar.freshness_summary(_plan(0.95, comparable=4, total=5))
    assert label == "95% (fresh) · 4/5 items"


def test_a_missing_score_is_unknown():
    label, score = status_bar.freshness_summary(_plan(None))
    assert score is None
    assert label == "unknown"


def test_nothing_comparable_is_unknown_not_fresh():
    """compute_freshness_stats returns 1.0 when nothing could be compared;
    the counts are what say it was never checked."""
    label, score = status_bar.freshness_summary(_plan(1.0, comparable=0, total=5))
    assert score is None
    assert label == "unknown · 0/5 items comparable"


# --- render_status_bar's freshness path, with a stubbed `st` -----------------------

class _Col:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeSt:
    def __init__(self):
        self.session_state = {}
        self.metrics: dict[str, str] = {}
        self.warnings: list[str] = []
        self.captions: list[str] = []
        self.buttons: dict[str, str | None] = {}

    def metric(self, label, value):
        self.metrics[label] = value

    def warning(self, text):
        self.warnings.append(text)

    def caption(self, text):
        self.captions.append(text)

    def columns(self, spec):
        n = spec if isinstance(spec, int) else len(spec)
        return [_Col() for _ in range(n)]

    def button(self, label, type=None, key=None, **kw):
        self.buttons[key] = type
        return False

    def error(self, *a, **k):
        pass

    info = success = markdown = error

    @contextmanager
    def spinner(self, *a, **k):
        yield

    def rerun(self):
        raise AssertionError("no rerun expected")


def _render(monkeypatch, plan):
    fake = _FakeSt()
    monkeypatch.setattr(status_bar, "st", fake)
    monkeypatch.setattr(status_bar, "get_status", lambda: {"status": "idle"})
    monkeypatch.setattr(status_bar, "get_plan", lambda: plan)
    monkeypatch.setattr(status_bar, "get_market_intel_status", lambda: {})
    status_bar.render_status_bar(DailyPlannerPageState(plan=plan))
    return fake


def test_render_shows_the_live_drift_and_warns(monkeypatch):
    fake = _render(monkeypatch, _plan(0.0, comparable=5, total=5, stored=1.0))
    assert fake.metrics["Freshness"] == "0% (stale) · 5/5 items"
    assert any("drifted significantly" in w for w in fake.warnings)
    assert fake.buttons["_recompute_plan_btn"] == "primary"


def test_render_shows_unknown_without_a_drift_warning(monkeypatch):
    fake = _render(monkeypatch, _plan(None, comparable=0, total=0))
    assert fake.metrics["Freshness"] == "unknown"
    assert fake.warnings == []
    assert fake.captions == []
