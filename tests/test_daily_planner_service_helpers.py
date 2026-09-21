# tests/test_daily_planner_service_helpers.py
from __future__ import annotations

from types import SimpleNamespace

from eve_online_industry_tracker.application.daily_planner.service import (
    _select_division_one_balance,
    index_blueprint_assets,
)


class _BlueprintsAre:
    def __init__(self, *ids):
        self._ids = set(ids)

    def is_blueprint(self, type_id):
        return int(type_id) in self._ids


def test_bpos_and_bpcs_are_indexed_separately():
    bpo = SimpleNamespace(type_id=999, is_blueprint_copy=False, blueprint_runs=None, quantity=1)
    bpc = SimpleNamespace(type_id=999, is_blueprint_copy=True, blueprint_runs=10, quantity=1)
    module = SimpleNamespace(type_id=34, is_blueprint_copy=False, blueprint_runs=None, quantity=5)

    bpos, bpcs = index_blueprint_assets([bpo, bpc, module], _BlueprintsAre(999))

    assert list(bpos) == [999]
    assert list(bpcs) == [999]
    assert len(bpos[999]) == 1
    assert len(bpcs[999]) == 1


def test_non_blueprint_assets_are_excluded():
    module = SimpleNamespace(type_id=34, is_blueprint_copy=False, blueprint_runs=None, quantity=5)
    bpos, bpcs = index_blueprint_assets([module], _BlueprintsAre(999))
    assert bpos == {}
    assert bpcs == {}


def test_wallet_balance_from_a_list_of_divisions():
    wallets = [{"division": 1, "balance": "8,400,000.55"}, {"division": 2, "balance": "1"}]
    assert _select_division_one_balance(wallets) == 8_400_000.55


def test_wallet_balance_from_a_dict_keyed_by_division():
    assert _select_division_one_balance({"1": "500.5"}) == 500.5
    assert _select_division_one_balance({1: 500.5}) == 500.5


def test_wallet_balance_is_zero_when_division_one_is_absent():
    assert _select_division_one_balance([{"division": 2, "balance": "1"}]) == 0.0
    assert _select_division_one_balance(None) == 0.0
