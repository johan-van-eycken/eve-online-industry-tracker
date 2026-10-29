"""Small fakes several test files share."""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError

from eve_online_industry_tracker.application.characters.realized_profit import CharacterRealizedProfitLedgerService
from eve_online_industry_tracker.application.daily_planner.models import ItemDecision
from eve_online_industry_tracker.application.daily_planner.service import DailyPlannerService
from eve_online_industry_tracker.application.industry.service import IndustryService
from eve_online_industry_tracker.config.admin_settings import AdminSettingsManager


def _raiser(exc):
    def _f(*a, **k):
        raise exc
    return _f


def _db_error():
    return OperationalError("SELECT", {}, Exception("database is locked"))


class _Admin:
    def get(self, section, key, default=None):
        return default


class _AdminStub:
    def get(self, section, key, default=None):
        raise KeyError(key)  # forces ChainPlanner's own fallbacks


class _AdminKeyError:
    def get(self, section, key):
        raise KeyError(key)


class _Db:
    def __init__(self, session):
        self.session = session


class _Esi:
    def __init__(self, jobs):
        self._jobs = jobs

    def esi_get(self, endpoint, params=None, paginate=False):
        if endpoint.endswith("/industry/jobs"):
            return list(self._jobs)
        if endpoint.endswith("/markets/prices"):
            return []
        raise AssertionError(endpoint)


def _manager(tmp_path, payload=None) -> AdminSettingsManager:
    path = tmp_path / "admin_settings.json"
    if payload is not None:
        path.write_text(json.dumps(payload), encoding="utf-8")
    return AdminSettingsManager(path)


class _NoBlueprints:
    def is_blueprint(self, type_id):
        return False

    def prefetch(self, type_ids):
        return None


class _Chars:
    def __init__(self, chars):
        self._chars = chars

    def list_characters(self):
        return self._chars


def _decision(row) -> ItemDecision:
    return ItemDecision(
        type_id=12345, type_name="Thing", decision="build", decision_reason="test",
        adjusted_score=1.0, absolute_profit_per_batch=1.0, isk_per_hour=1.0, margin_pct=1.0,
        days_of_supply_current=1.0, effective_velocity=1.0, meta_group_id=1,
        pipeline_stage="manufacturing", overview_row=row,
    )


def _bare_service():
    return DailyPlannerService(
        industry_service=SimpleNamespace(get_cached_overview_rows=lambda: []),
        corporations_service=SimpleNamespace(list_corporations=lambda: []),
        characters_service=SimpleNamespace(list_characters=lambda: []),
        sales_history_service=SimpleNamespace(),
        market_pricing_service=SimpleNamespace(),
        repo=SimpleNamespace(),
        admin_settings=SimpleNamespace(),
        session_provider=SimpleNamespace(),
    )


class _RecordingLoader:
    """Mirrors tests/test_type_metadata.py's _FakeLoader: records every batch
    of type_ids it was asked to load, so a test can assert it was invoked
    exactly once (batched), not once per deliver row."""

    def __init__(self, data):
        self._data = data
        self.calls: list[list[int]] = []

    def __call__(self, session, language, type_ids):
        self.calls.append(sorted(type_ids))
        return {tid: self._data[tid] for tid in type_ids if tid in self._data}


@pytest.fixture(autouse=True)
def _fresh_once_warnings():
    IndustryService._reset_once_warnings()
    yield
    IndustryService._reset_once_warnings()


def _ledger(session):
    return CharacterRealizedProfitLedgerService(app_session=session, sde_session=None, market_prices=[])


@pytest.fixture
def industry_client(monkeypatch):
    from flask_app.app import create_app
    import flask_app.routes.industry as routes

    monkeypatch.setattr(routes, "require_ready", lambda s: None)
    monkeypatch.setattr(routes, "require_sde_ready", lambda s: None)
    return create_app().test_client()
