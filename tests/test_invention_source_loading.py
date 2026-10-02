# tests/test_invention_source_loading.py
"""The planner must load the T1 source blueprint of every T2 blueprint it
plans, even when the corp does not own that T1 BPO.

The invention activity (and so the datacores) lives on the T1 source
blueprint. DailyPlannerService._get_blueprint_data used to load only
overview-row blueprints plus owned BPOs, so inventing from a BPC without the
T1 BPO left the source out of blueprint_data and the shopping list could not
find the datacores.
"""
from __future__ import annotations

import logging
from types import SimpleNamespace

from sqlalchemy.orm import sessionmaker

from eve_online_industry_tracker.infrastructure.models import Blueprints
from eve_online_industry_tracker.infrastructure.sde import blueprints as sde_blueprints


def _activities(invented=()):
    acts = {"manufacturing": {"materials": [], "products": [{"typeID": 1, "quantity": 1}]}}
    if invented:
        acts["invention"] = {
            "materials": [{"typeID": 20416, "quantity": 2}],
            "products": [{"typeID": t, "quantity": 10, "probability": 0.34} for t in invented],
        }
    return acts


def test_sde_lookup_finds_the_t1_source_of_an_invented_blueprint(sde_engine):
    # Shape mirrors eve_sde.db: Damage Control I Blueprint 2047 invents into
    # Damage Control II Blueprint 2049; 2049 itself has no invention activity.
    session = sessionmaker(bind=sde_engine, future=True)()
    session.add_all([
        Blueprints(blueprintTypeID=2047, maxProductionLimit=300, activities=_activities(invented=[2049])),
        Blueprints(blueprintTypeID=2049, maxProductionLimit=10, activities=_activities()),
        Blueprints(blueprintTypeID=3001, maxProductionLimit=10, activities=_activities(invented=[3002])),
    ])
    session.commit()

    result = sde_blueprints.get_invention_source_blueprint_ids(session, [2049, 2047, 9999])
    session.close()

    assert result == {2049: 2047}


def test_sde_lookup_with_no_ids_returns_empty(sde_engine):
    session = sessionmaker(bind=sde_engine, future=True)()
    assert sde_blueprints.get_invention_source_blueprint_ids(session, []) == {}
    session.close()


def _row(blueprint_type_id):
    return {"type_id": 12345,
            "manufacturing_job": {"blueprint_sde": {"blueprint_type_id": blueprint_type_id}}}


def _fake_service():
    session = SimpleNamespace(close=lambda: None)
    return SimpleNamespace(_session_provider=SimpleNamespace(sde_session=lambda: session))


def test_service_also_loads_the_unowned_t1_source_blueprint(monkeypatch):
    from eve_online_industry_tracker.application.daily_planner.service import DailyPlannerService

    loaded: list[list[int]] = []
    looked_up: list[list[int]] = []

    def fake_sources(session, invented_ids):
        looked_up.append(sorted(invented_ids))
        return {1999: 998}

    def fake_loader(session, language, blueprint_type_ids):
        loaded.append(sorted(blueprint_type_ids))
        return {}

    monkeypatch.setattr(sde_blueprints, "get_invention_source_blueprint_ids", fake_sources)
    monkeypatch.setattr(sde_blueprints, "get_blueprint_manufacturing_data", fake_loader)

    DailyPlannerService._get_blueprint_data(
        _fake_service(), [_row(1999)], extra_blueprint_type_ids={888},
    )

    assert looked_up == [[888, 1999]]
    assert loaded == [[888, 998, 1999]]  # one load, source included


def test_a_failed_source_lookup_still_loads_the_rest_and_warns(monkeypatch, caplog):
    from eve_online_industry_tracker.application.daily_planner.service import DailyPlannerService

    loaded: list[list[int]] = []

    def broken_sources(session, invented_ids):
        raise RuntimeError("boom")

    def fake_loader(session, language, blueprint_type_ids):
        loaded.append(sorted(blueprint_type_ids))
        return {}

    monkeypatch.setattr(sde_blueprints, "get_invention_source_blueprint_ids", broken_sources)
    monkeypatch.setattr(sde_blueprints, "get_blueprint_manufacturing_data", fake_loader)

    with caplog.at_level(logging.WARNING):
        DailyPlannerService._get_blueprint_data(_fake_service(), [_row(1999)])

    assert loaded == [[1999]]
    assert any("T1 source" in r.getMessage() for r in caplog.records
               if r.levelno == logging.WARNING)
