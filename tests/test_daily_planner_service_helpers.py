# tests/test_daily_planner_service_helpers.py
from __future__ import annotations

from types import SimpleNamespace

from eve_online_industry_tracker.application.daily_planner.service import (
    _select_division_one_balance,
    index_blueprint_assets,
)
from eve_online_industry_tracker.application.industry.type_metadata import TypeMetadataResolver


class _BlueprintsAre:
    def __init__(self, *ids):
        self._ids = set(ids)

    def is_blueprint(self, type_id):
        return int(type_id) in self._ids

    def prefetch(self, type_ids):
        """No-op: this stub answers is_blueprint() from a fixed id set, not an SDE
        loader, so there is nothing to warm. Real batching behavior is proved
        separately below against an actual TypeMetadataResolver."""
        return None


class _RecordingLoader:
    """Mirrors tests/test_type_metadata.py's _FakeLoader: records every batch of
    type_ids it was asked to load, so callers can assert it was invoked once."""

    def __init__(self, data):
        self._data = data
        self.calls: list[list[int]] = []

    def __call__(self, session, language, type_ids):
        self.calls.append(sorted(type_ids))
        return {tid: self._data[tid] for tid in type_ids if tid in self._data}


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


def test_index_blueprint_assets_batches_the_sde_lookup_into_one_call():
    """Regression for a fix-round-1 finding: is_blueprint() inside the per-asset loop
    self-heals a resolver cache miss with a single-id prefetch, so calling it without a
    prior batched prefetch would open one SDE session per distinct asset type_id (881
    sessions for the live corp_assets table's 881 distinct types). index_blueprint_assets
    must prefetch every distinct type_id once, up front, so the loader is invoked exactly
    once regardless of how many assets or distinct types are passed in."""
    bpo = SimpleNamespace(type_id=999, is_blueprint_copy=False, blueprint_runs=None, quantity=1)
    bpc = SimpleNamespace(type_id=999, is_blueprint_copy=True, blueprint_runs=10, quantity=1)
    other_bp = SimpleNamespace(type_id=888, is_blueprint_copy=False, blueprint_runs=None, quantity=1)
    module = SimpleNamespace(type_id=34, is_blueprint_copy=False, blueprint_runs=None, quantity=5)

    loader = _RecordingLoader({
        999: {"type_id": 999, "category_id": 9, "meta_group_id": None, "category_name": "Blueprint"},
        888: {"type_id": 888, "category_id": 9, "meta_group_id": None, "category_name": "Blueprint"},
        34: {"type_id": 34, "category_id": 4, "meta_group_id": None, "category_name": "Material"},
    })
    resolver = TypeMetadataResolver(sde_session_provider=lambda: None, loader=loader)

    bpos, bpcs = index_blueprint_assets([bpo, bpc, other_bp, module], resolver)

    assert loader.calls == [[34, 888, 999]]
    assert set(bpos) == {888, 999}
    assert set(bpcs) == {999}


def test_wallet_balance_from_a_list_of_divisions():
    wallets = [{"division": 1, "balance": "8,400,000.55"}, {"division": 2, "balance": "1"}]
    assert _select_division_one_balance(wallets) == 8_400_000.55


def test_wallet_balance_from_a_dict_keyed_by_division():
    assert _select_division_one_balance({"1": "500.5"}) == 500.5
    assert _select_division_one_balance({1: 500.5}) == 500.5


def test_wallet_balance_is_zero_when_division_one_is_absent():
    assert _select_division_one_balance([{"division": 2, "balance": "1"}]) == 0.0
    assert _select_division_one_balance(None) == 0.0
