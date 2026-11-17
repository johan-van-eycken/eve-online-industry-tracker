from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.application.corporations.corporation import Corporation
from eve_online_industry_tracker.application.corporations.wallet_age import wallet_age_hours, wallet_stale_reason
from eve_online_industry_tracker.infrastructure.database_manager import DatabaseManager
from eve_online_industry_tracker.infrastructure.esi_versions import ESI_CORP_WALLETS
from eve_online_industry_tracker.infrastructure.models import BaseApp, CorporationModel

NOW = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)
OLD = datetime(2026, 11, 16, 1, 0)


def test_wallet_age_hours():
    assert wallet_age_hours("2026-11-16T09:00:00+00:00", NOW) == 3.0
    assert wallet_age_hours(datetime(2026, 11, 16, 9, 0), NOW) == 3.0  # naive = UTC (sqlite)
    assert wallet_age_hours(None, NOW) is None
    assert wallet_age_hours("not a date", NOW) is None


def test_stale_wallet_is_unknown_for_planner():
    assert wallet_stale_reason("2026-11-16T05:00:00+00:00", NOW, 6) == "corp wallet last read 7.0 h ago (limit 6 h)"
    assert wallet_stale_reason("2026-11-16T07:00:00+00:00", NOW, 6) is None
    assert wallet_stale_reason(None, NOW, 6) == "corp wallet never read successfully"


class _Esi:
    def __init__(self, fail=False):
        self.endpoints = []
        self.fail = fail

    def esi_get(self, endpoint, *a, **k):
        self.endpoints.append(endpoint)
        if self.fail:
            raise RuntimeError("esi down")
        return [{"division": 1, "balance": 5.5}, {"division": 2, "balance": 7}]


@pytest.fixture()
def corp(tmp_path):
    db = DatabaseManager(f"sqlite:///{tmp_path / 'app.db'}")
    BaseApp.metadata.create_all(db.engine)
    db.session.add(CorporationModel(
        corporation_id=42, wallets=[{"division": "1", "division_name": "Master Wallet", "balance": "1.0"}],
        wallets_refreshed_at=OLD))
    db.session.commit()
    esi = _Esi()
    c = Corporation.__new__(Corporation)
    c._db_app = db
    c._default_esi_character = SimpleNamespace(_esi_client=esi, ensure_esi=lambda: None)
    c.corporation_id = 42
    c.corporation_name = "Test Corp"
    c.wallets = list(db.session.query(CorporationModel).one().wallets)
    c.wallets_refreshed_at = OLD
    yield c, esi, db
    db.engine.dispose()


def test_refresh_wallet_balances_makes_one_esi_call(corp):
    c, esi, db = corp
    c.refresh_wallet_balances()
    assert esi.endpoints == [ESI_CORP_WALLETS.format(corporation_id=42)]
    assert c.wallets == [
        {"division": "1", "division_name": "Master Wallet", "balance": "5.5"},
        {"division": "2", "division_name": "Division 2", "balance": "7"},
    ]
    rec = db.session.query(CorporationModel).one()
    assert rec.wallets[0]["balance"] == "5.5"
    assert rec.wallets_refreshed_at > OLD


def test_failed_wallet_read_keeps_old_timestamp(corp):
    c, esi, db = corp
    esi.fail = True
    with pytest.raises(Exception):
        c.refresh_wallet_balances()
    assert c.wallets_refreshed_at == OLD
    assert db.session.query(CorporationModel).one().wallets_refreshed_at == OLD
