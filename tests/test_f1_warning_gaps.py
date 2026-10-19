"""F1 item 10: the warnings that had no test of their own. The public and
corporation structure owner-name lookups (cosmetic: the structures keep
their IDs), the market depth collector's plan-velocity read failing on its
own (the sales-history fallback still answers) and the excluded blueprint
type IDs entry that is not a type ID. Fake values throughout. (The job's
ImportError path is in test_market_intelligence_job_start.py.)"""
from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError

from eve_online_industry_tracker.application.industry import service as service_module
from eve_online_industry_tracker.application.industry.service import IndustryService

SYSTEM = 30000001
CORP = 98000001


def _esi_names_failing():
    def _fail(ids):
        raise RuntimeError("ESI POST failed after retries: fake")

    return SimpleNamespace(get_universe_names=_fail)


def _service(state, sessions):
    svc = IndustryService.__new__(IndustryService)
    svc._state = state
    svc._sessions = sessions
    return svc


# --- the public structures' owner names -----------------------------------------------------------

def test_public_structure_owner_names_that_cannot_be_resolved_are_logged(monkeypatch, caplog):
    structures = [{"structure_id": 1035000000001, "type_id": 35825, "owner_id": CORP, "name": "Fake Raitaru"}]
    monkeypatch.setattr(service_module, "public_structures_cache_ttl_seconds", lambda: 3600)
    monkeypatch.setattr(service_module, "get_cached_public_structures", lambda **kw: (structures, True))
    monkeypatch.setattr(service_module, "get_type_data", lambda session, language, ids: {})
    svc = _service(SimpleNamespace(esi_service=_esi_names_failing(), db_sde=None),
                   SimpleNamespace(sde_session=lambda: None))
    with caplog.at_level(logging.WARNING):
        out, meta = svc.public_structures(system_id=SYSTEM)
    assert out[0]["owner_id"] == CORP and out[0]["owner_name"] is None       # keeps its owner ID
    assert "Public structures: owner names cannot be resolved" in caplog.text and "fake" in caplog.text
    assert meta == {"refreshing": False, "cache_fresh": True}


def test_a_programming_error_in_the_public_owner_lookup_propagates(monkeypatch):
    def _bug(ids):
        raise KeyError("fake bug")

    monkeypatch.setattr(service_module, "public_structures_cache_ttl_seconds", lambda: 3600)
    monkeypatch.setattr(service_module, "get_cached_public_structures",
                        lambda **kw: ([{"type_id": 35825, "owner_id": CORP}], True))
    monkeypatch.setattr(service_module, "get_type_data", lambda session, language, ids: {})
    svc = _service(SimpleNamespace(esi_service=SimpleNamespace(get_universe_names=_bug), db_sde=None),
                   SimpleNamespace(sde_session=lambda: None))
    with pytest.raises(KeyError):
        svc.public_structures(system_id=SYSTEM)


# --- the corporation structures' owner names ------------------------------------------------------

def test_corporation_structure_owner_names_that_cannot_be_resolved_are_logged(monkeypatch, caplog):
    rows = [SimpleNamespace(to_dict=lambda: {"structure_id": 1035000000002, "corporation_id": CORP})]
    monkeypatch.setattr(service_module, "corporation_structures_list_by_corporation_id", lambda session, cid: rows)
    character = SimpleNamespace(corporation_id=CORP)
    state = SimpleNamespace(char_manager=SimpleNamespace(get_character_by_id=lambda cid: character),
                            esi_service=_esi_names_failing(), db_sde=None)
    svc = _service(state, SimpleNamespace(app_session=lambda: None))
    with caplog.at_level(logging.WARNING):
        out = svc.corporation_structures(character_id=90000001)
    assert out[0]["corporation_id"] == CORP and "owner_name" not in out[0]      # keeps its corporation ID
    assert "Corporation structures: owner names cannot be resolved" in caplog.text and "fake" in caplog.text


# --- the market depth collector: the plan velocity read alone ---------------------------------------

def test_the_plan_velocity_read_failing_alone_warns_once_and_falls_back(monkeypatch, caplog):
    from eve_online_industry_tracker.application.industry import sales_history_service
    from eve_online_industry_tracker.application.market_intelligence.market_depth_collector import (
        MarketDepthCollector,
    )

    class _Session:
        def execute(self, *a, **k):
            raise OperationalError("SELECT fake", {}, Exception("fake plan table locked"))

        def close(self):
            pass

    class _Svc:
        def __init__(self, **k):
            pass

        def get_sold_history(self, **k):
            return [{"quantity": 60}]

    monkeypatch.setattr(sales_history_service, "SalesHistoryService", _Svc)
    c = MarketDepthCollector.__new__(MarketDepthCollector)
    c._state, c._admin = SimpleNamespace(), None
    c._sessions = SimpleNamespace(app_session=lambda: _Session())
    with caplog.at_level(logging.WARNING):
        assert c._get_effective_velocity(34, {CORP}) == pytest.approx(2.0)     # 60 units / 30 days
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "plan effective_velocity" in warnings[0] and "fake plan table locked" in warnings[0]
    assert "type_id=34" in warnings[0]


# --- the excluded blueprint type IDs -----------------------------------------------------------------

def test_an_excluded_blueprint_entry_that_is_not_a_type_id_is_logged_and_ignored(caplog):
    from eve_online_industry_tracker.application.industry.job_manager import IndustryJobManager

    cfg = {"defaults": {"industry": {"excluded_blueprint_type_ids": [1001, "1002", "not-an-id", None]}}}
    mgr = IndustryJobManager.__new__(IndustryJobManager)
    mgr._state = SimpleNamespace(cfg_manager=SimpleNamespace(all=lambda: cfg))
    with caplog.at_level(logging.WARNING):
        assert mgr._excluded_blueprint_type_ids() == {1001, 1002}
    messages = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(messages) == 2
    assert any("'not-an-id'" in m for m in messages) and any("None" in m for m in messages)
    assert all("excluded_blueprint_type_ids" in m for m in messages)
