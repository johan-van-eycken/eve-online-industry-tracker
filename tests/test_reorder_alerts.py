from __future__ import annotations

import os
import sys
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.application.industry.service import IndustryService  # noqa: E402
from eve_online_industry_tracker.db_models import (  # noqa: E402
    BaseApp,
    BaseSde,
    CorporationAssetsModel,
    CorporationIndustryJobsModel,
    CorporationWalletTransactionsModel,
)


class _SessionProvider:
    def __init__(self, app_factory, sde_factory):
        self._app_factory = app_factory
        self._sde_factory = sde_factory

    def app_session(self) -> Session:
        return self._app_factory()

    def sde_session(self) -> Session:
        return self._sde_factory()

    def oauth_session(self) -> Session:
        raise RuntimeError("oauth session not needed in this test")


class _CorpManager:
    def __init__(self, corporations):
        self._corporations = corporations

    def get_corporations(self):
        return list(self._corporations)


def _make_sessions() -> tuple[sessionmaker, sessionmaker]:
    app_engine = create_engine("sqlite:///:memory:")
    sde_engine = create_engine("sqlite:///:memory:")
    BaseApp.metadata.create_all(bind=app_engine)
    BaseSde.metadata.create_all(bind=sde_engine)
    return sessionmaker(bind=app_engine), sessionmaker(bind=sde_engine)


def _build_service(*, app_factory, sde_factory) -> IndustryService:
    service = object.__new__(IndustryService)
    service._sessions = _SessionProvider(app_factory, sde_factory)  # type: ignore[attr-defined]
    service._state = SimpleNamespace(  # type: ignore[attr-defined]
        esi_service=None,
        char_manager=None,
        corp_manager=_CorpManager([{"corporation_id": 1}]),
    )
    return service


def _add_asset(session, *, type_id: int, quantity: int, item_id: int = None) -> None:
    if item_id is None:
        item_id = type_id * 1000
    session.add(
        CorporationAssetsModel(
            corporation_id=1,
            item_id=item_id,
            type_id=type_id,
            location_id=60000001,
            is_singleton=False,
            is_blueprint_copy=False,
            quantity=quantity,
        )
    )


def _add_sell_tx(session, *, type_id: int, quantity: int, date: str, tx_id: int = None) -> None:
    if tx_id is None:
        import random
        tx_id = random.randint(100_000, 999_999_999)
    session.add(
        CorporationWalletTransactionsModel(
            corporation_id=1,
            transaction_id=tx_id,
            type_id=type_id,
            quantity=quantity,
            is_buy=False,
            date=date,
        )
    )


def _add_active_job(session, *, product_type_id: int, output_quantity: int, job_id: int = None) -> None:
    if job_id is None:
        import random
        job_id = random.randint(1_000_000, 9_999_999)
    session.add(
        CorporationIndustryJobsModel(
            corporation_id=1,
            job_id=job_id,
            status="active",
            product_type_id=product_type_id,
            output_quantity=output_quantity,
        )
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_reorder_urgent() -> None:
    """10 units in stock, 5/day velocity over 30 days → 2 days of stock → urgent."""
    app_factory, sde_factory = _make_sessions()
    session = app_factory()
    _add_asset(session, type_id=101, quantity=10)
    # 5/day * 30 days = 150 sold
    _add_sell_tx(session, type_id=101, quantity=150, date="2026-06-01T00:00:00Z", tx_id=1001)
    session.commit()
    session.close()

    service = _build_service(app_factory=app_factory, sde_factory=sde_factory)
    result = service.get_industry_reorder_alerts(type_ids=[101], today_iso="2026-06-30")

    assert 101 in result
    alert = result[101]
    assert alert["urgency"] == "urgent"
    assert alert["stock_qty"] == 10
    assert abs(alert["velocity_per_day"] - 5.0) < 0.01
    assert alert["days_until_reorder"] is not None
    assert abs(alert["days_until_reorder"] - 2.0) < 0.01


def test_reorder_soon() -> None:
    """20 units, 4/day → 5 days → soon."""
    app_factory, sde_factory = _make_sessions()
    session = app_factory()
    _add_asset(session, type_id=102, quantity=20)
    # 4/day * 30 = 120 sold
    _add_sell_tx(session, type_id=102, quantity=120, date="2026-06-01T00:00:00Z", tx_id=2001)
    session.commit()
    session.close()

    service = _build_service(app_factory=app_factory, sde_factory=sde_factory)
    result = service.get_industry_reorder_alerts(type_ids=[102], today_iso="2026-06-30")

    assert 102 in result
    alert = result[102]
    assert alert["urgency"] == "soon"
    assert alert["stock_qty"] == 20
    assert abs(alert["days_until_reorder"] - 5.0) < 0.01


def test_reorder_ok() -> None:
    """70 units, 5/day → 14 days → ok."""
    app_factory, sde_factory = _make_sessions()
    session = app_factory()
    _add_asset(session, type_id=103, quantity=70)
    # 5/day * 30 = 150 sold
    _add_sell_tx(session, type_id=103, quantity=150, date="2026-06-01T00:00:00Z", tx_id=3001)
    session.commit()
    session.close()

    service = _build_service(app_factory=app_factory, sde_factory=sde_factory)
    result = service.get_industry_reorder_alerts(type_ids=[103], today_iso="2026-06-30")

    assert 103 in result
    alert = result[103]
    assert alert["urgency"] == "ok"
    assert alert["stock_qty"] == 70
    assert abs(alert["days_until_reorder"] - 14.0) < 0.01


def test_reorder_no_sales() -> None:
    """50 units but 0 wallet transactions → no_data."""
    app_factory, sde_factory = _make_sessions()
    session = app_factory()
    _add_asset(session, type_id=104, quantity=50)
    # No transactions added
    session.commit()
    session.close()

    service = _build_service(app_factory=app_factory, sde_factory=sde_factory)
    result = service.get_industry_reorder_alerts(type_ids=[104], today_iso="2026-06-30")

    assert 104 in result
    alert = result[104]
    assert alert["urgency"] == "no_data"
    assert alert["stock_qty"] == 50
    assert alert["velocity_per_day"] is None
    assert alert["days_until_reorder"] is None


def test_reorder_includes_active_jobs() -> None:
    """5 units hangar + 15 units in active mfg jobs → total 20 stock."""
    app_factory, sde_factory = _make_sessions()
    session = app_factory()
    _add_asset(session, type_id=105, quantity=5)
    _add_active_job(session, product_type_id=105, output_quantity=15, job_id=9001)
    # 4/day * 30 = 120 sold → 20/4 = 5 days → soon
    _add_sell_tx(session, type_id=105, quantity=120, date="2026-06-01T00:00:00Z", tx_id=5001)
    session.commit()
    session.close()

    service = _build_service(app_factory=app_factory, sde_factory=sde_factory)
    result = service.get_industry_reorder_alerts(type_ids=[105], today_iso="2026-06-30")

    assert 105 in result
    alert = result[105]
    assert alert["stock_qty"] == 20  # 5 hangar + 15 in-progress
    assert abs(alert["days_until_reorder"] - 5.0) < 0.01
    assert alert["urgency"] == "soon"
