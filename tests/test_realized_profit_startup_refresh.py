"""The first Realized Profit load after an app start rebuilds both ledgers, once."""
import pytest

from streamlit_ui.api import realized_profit as rp


@pytest.fixture(autouse=True)
def _fresh_start(monkeypatch):
    rp._startup_refresh_state.clear()
    calls: list[str] = []
    monkeypatch.setattr(rp, "refresh_realized_profit", lambda *, owner_scope, owner_id=None: calls.append(owner_scope))
    monkeypatch.setattr(rp, "clear_realized_profit_cache", lambda: calls.append("clear"))
    yield calls
    rp._startup_refresh_state.clear()


def test_first_load_rebuilds_both_scopes_once(_fresh_start):
    assert rp.refresh_ledger_once_per_start() is None
    assert _fresh_start == ["character", "corporation", "clear"]
    assert rp.refresh_ledger_once_per_start() is None
    assert _fresh_start == ["character", "corporation", "clear"]


def test_a_failed_rebuild_is_reported_and_not_repeated(monkeypatch, _fresh_start):
    def boom(*, owner_scope, owner_id=None):
        raise RuntimeError("Refreshing the realized profit ledger failed (character): timeout")
    monkeypatch.setattr(rp, "refresh_realized_profit", boom)
    assert rp.refresh_ledger_once_per_start() == "Refreshing the realized profit ledger failed (character): timeout"
    assert rp.refresh_ledger_once_per_start() is None
    assert _fresh_start == ["clear"]
