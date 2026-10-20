"""Round J1.6: CharacterManager.get_assets / get_wallet_balances /
get_market_orders with a character_id (or name) refresh that character once
and return only its data. Before, they looped over every character,
refreshed the selected one once per character and returned everyone's
data. Without a selector the behaviour is unchanged. Fake values only.
"""
from __future__ import annotations

from collections import Counter

import pytest

from eve_online_industry_tracker.application.characters.character_manager import (
    CharacterManager,
    CharacterRefreshError,
    UnknownCharacterError,
)


class _Char:
    def __init__(self, name, character_id, refresh_error=None):
        self.character_name = name
        self.character_id = character_id
        self.refresh_error = refresh_error
        self.calls = Counter()

    def ensure_esi(self):
        pass

    def _refresh(self, kind):
        self.calls[kind] += 1
        if self.refresh_error:
            raise self.refresh_error

    def refresh_wallet_balance(self):
        self._refresh("refresh_wallet_balance")

    def refresh_assets(self):
        self._refresh("refresh_assets")

    def refresh_market_orders(self):
        self._refresh("refresh_market_orders")

    def get_wallet_balance(self):
        self.calls["get_wallet_balance"] += 1
        return {"character_id": self.character_id, "balance": 1.0}

    def get_assets(self):
        self.calls["get_assets"] += 1
        return {"character_id": self.character_id, "assets": []}

    def get_market_orders(self):
        self.calls["get_market_orders"] += 1
        return {"character_id": self.character_id, "orders": []}


def _manager():
    chars = [_Char("Alpha", 90000001), _Char("Bravo", 90000002), _Char("Charlie", 90000003)]
    mgr = CharacterManager.__new__(CharacterManager)
    mgr._character_list = list(chars)
    mgr.init_errors = []
    mgr._rebuild_indexes()
    return mgr, chars


_GETTERS = [
    ("get_wallet_balances", "refresh_wallet_balance", "get_wallet_balance"),
    ("get_assets", "refresh_assets", "get_assets"),
    ("get_market_orders", "refresh_market_orders", "get_market_orders"),
]


@pytest.mark.parametrize("method,refresh,getter", _GETTERS)
@pytest.mark.parametrize("selector", [{"character_id": 90000002}, {"character_name": "bravo"}])
def test_a_selected_character_is_refreshed_once_and_returned_alone(method, refresh, getter, selector):
    mgr, (alpha, bravo, charlie) = _manager()
    result = getattr(mgr, method)(**selector)
    assert [r["character_id"] for r in result] == [90000002]
    assert bravo.calls == Counter({refresh: 1, getter: 1})
    assert alpha.calls == charlie.calls == Counter()


@pytest.mark.parametrize("method,refresh,getter", _GETTERS)
def test_without_a_selector_every_character_is_refreshed_once_and_returned(method, refresh, getter):
    # Before, the whole batch was refreshed before each read: N x N refreshes.
    mgr, chars = _manager()
    result = getattr(mgr, method)()
    assert [r["character_id"] for r in result] == [90000001, 90000002, 90000003]
    assert all(c.calls == Counter({refresh: 1, getter: 1}) for c in chars)


def test_market_orders_without_a_selector_or_refresh_only_reads():
    mgr, chars = _manager()
    assert len(mgr.get_market_orders(refresh=False)) == 3
    assert all(c.calls == Counter({"get_market_orders": 1}) for c in chars)


def test_market_orders_without_refresh_does_not_refresh_the_selected_character():
    mgr, (alpha, bravo, charlie) = _manager()
    assert [r["character_id"] for r in mgr.get_market_orders(character_id=90000003, refresh=False)] == [90000003]
    assert charlie.calls == Counter({"get_market_orders": 1})
    assert alpha.calls == bravo.calls == Counter()


@pytest.mark.parametrize("method,refresh,getter", _GETTERS)
def test_an_unknown_selected_character_is_named(method, refresh, getter):
    mgr, chars = _manager()
    with pytest.raises(UnknownCharacterError, match="90009999"):
        getattr(mgr, method)(character_id=90009999)
    assert all(c.calls == Counter() for c in chars)


def test_a_failed_refresh_of_the_selected_character_names_it():
    mgr, (alpha, bravo, charlie) = _manager()
    bravo.refresh_error = RuntimeError("ESI down")
    with pytest.raises(CharacterRefreshError) as info:
        mgr.get_assets(character_id=90000002)
    assert set(info.value.errors) == {"Bravo"} and "ESI down" in str(info.value)
    assert bravo.calls["get_assets"] == 0
