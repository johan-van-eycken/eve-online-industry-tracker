"""Unit tests for CorporationMarketOrdersModel upsert behaviour."""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.infrastructure.models import (  # noqa: E402
    BaseApp,
    CorporationMarketOrdersModel,
)


def _make_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    BaseApp.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine)()


def _make_order(order_id: int, corporation_id: int = 98765, price: float = 1_000_000.0) -> dict:
    return {
        "corporation_id": corporation_id,
        "order_id": order_id,
        "type_id": 34,
        "type_name": "Tritanium",
        "type_group_id": 18,
        "type_group_name": "Mineral",
        "type_category_id": 4,
        "type_category_name": "Material",
        "location_id": 60003760,
        "location_name": "Jita IV - Moon 4 - Caldari Navy Assembly Plant",
        "region_id": 10000002,
        "region_name": "The Forge",
        "price": price,
        "is_buy_order": False,
        "escrow": 0.0,
        "volume_total": 100,
        "volume_remain": 80,
        "duration": 90,
        "issued": "2026-06-01T00:00:00Z",
        "min_volume": 1,
        "range": "station",
        "updated_at": datetime.now(timezone.utc),
    }


def test_insert_new_order() -> None:
    """A new order should be persisted to the database."""
    session = _make_session()
    order = _make_order(order_id=111111)
    session.add(CorporationMarketOrdersModel(**order))
    session.commit()

    result = session.query(CorporationMarketOrdersModel).filter_by(order_id=111111).first()
    assert result is not None
    assert result.corporation_id == 98765
    assert result.type_name == "Tritanium"
    assert result.price == 1_000_000.0
    assert result.volume_remain == 80


def test_upsert_updates_existing_order() -> None:
    """Re-inserting an order with the same order_id (after delete) should reflect
    updated values, not create a duplicate.

    The Corporation.refresh_market_orders() method uses a delete-then-bulk-insert
    pattern (matching CharacterMarketOrdersModel).  This test verifies that after
    a simulated refresh cycle the updated price is stored and only one row exists.
    """
    session = _make_session()
    corp_id = 98765
    order_id = 222222

    # First insert
    session.add(CorporationMarketOrdersModel(**_make_order(order_id=order_id, price=500_000.0)))
    session.commit()

    # Simulate a refresh: delete + re-insert with a new price
    session.query(CorporationMarketOrdersModel).filter_by(corporation_id=corp_id).delete()
    session.add(CorporationMarketOrdersModel(**_make_order(order_id=order_id, price=750_000.0)))
    session.commit()

    rows = session.query(CorporationMarketOrdersModel).filter_by(order_id=order_id).all()
    assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
    assert rows[0].price == 750_000.0


def test_multiple_orders_for_same_corporation() -> None:
    """Multiple orders for the same corporation are all stored."""
    session = _make_session()
    corp_id = 98765

    for oid in [300001, 300002, 300003]:
        session.add(CorporationMarketOrdersModel(**_make_order(order_id=oid, corporation_id=corp_id)))
    session.commit()

    rows = session.query(CorporationMarketOrdersModel).filter_by(corporation_id=corp_id).all()
    assert len(rows) == 3


def test_order_id_is_unique() -> None:
    """Two rows with the same order_id cannot coexist (unique constraint enforced)."""
    import pytest
    from sqlalchemy.exc import IntegrityError

    session = _make_session()
    session.add(CorporationMarketOrdersModel(**_make_order(order_id=400001)))
    session.commit()

    session.add(CorporationMarketOrdersModel(**_make_order(order_id=400001)))
    with pytest.raises(IntegrityError):
        session.commit()


def test_refresh_names_location_and_region_from_the_order_with_one_lookup_each() -> None:
    """ESI orders carry region_id: no system/constellation walk, one name lookup per id."""
    from types import SimpleNamespace

    from eve_online_industry_tracker.application.corporations.corporation import Corporation
    from eve_online_industry_tracker.infrastructure.models import BaseSde

    app_session = _make_session()
    sde_engine = create_engine("sqlite:///:memory:")
    BaseSde.metadata.create_all(bind=sde_engine)
    names = {60003760: "Jita IV - Moon 4", 10000002: "The Forge"}
    looked_up: list[int] = []

    def _info(location_id):
        looked_up.append(location_id)
        return {"name": names[location_id]}

    esi_order = {"order_id": 1, "type_id": 34, "location_id": 60003760, "region_id": 10000002, "price": 5.0}
    corp = object.__new__(Corporation)
    corp.corporation_id = 98765
    corp.corporation_name = "Corp"
    corp._db_app = SimpleNamespace(session=app_session)
    corp._db_sde = SimpleNamespace(session=sessionmaker(bind=sde_engine)(), language="en")
    corp._default_esi_character = SimpleNamespace(
        ensure_esi=lambda: None,
        _esi_client=SimpleNamespace(esi_get=lambda *a, **kw: [dict(esi_order), dict(esi_order, order_id=2)]),
        esi_service=SimpleNamespace(get_location_info=_info),
    )

    corp.refresh_market_orders()

    rows = app_session.query(CorporationMarketOrdersModel).order_by(CorporationMarketOrdersModel.order_id).all()
    assert [(r.location_name, r.region_id, r.region_name) for r in rows] == [
        ("Jita IV - Moon 4", 10000002, "The Forge")
    ] * 2
    assert sorted(looked_up) == [10000002, 60003760]
