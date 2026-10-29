"""F1 items 3-4: a non-numeric EIV quantity, a non-numeric system cost index
and a system without the activity's cost index make the job fee unknown,
with the reason (the job_fee_unknown_reasons path), never 0. The industry
profile payloads carry cost_index_unknown_reason. Fake values throughout."""
from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.application.industry.job_cost import JobSurchargeRates, job_install_cost
from eve_online_industry_tracker.application.industry.service import IndustryService
from industry_service_fakes import _profile_service

_RATES = JobSurchargeRates(facility_tax=0.01, scc=0.04, research_scc=0.02)
SYSTEM = 30000001


def _profile(*entries, **extra):
    return {"system_id": SYSTEM, "system_cost_indices": list(entries), **extra}


# --- item 3: the EIV quantity -----------------------------------------------------------------

def test_a_non_numeric_eiv_quantity_makes_the_eiv_unknown_with_the_reason(caplog):
    entries = [{"type_id": 34, "quantity": "many"}, {"type_id": 35, "quantity": 10}]
    prices = {34: {"adjusted_price": 5.0}, 35: {"adjusted_price": 2.0}}
    reasons: list[str] = []
    with caplog.at_level(logging.WARNING):
        total, priced, lines = IndustryService._sum_estimated_item_value_counted(
            entries, quantity_key="quantity", adjusted_price_map=prices, unknown_reasons=reasons)
    assert total is None                         # not 20 (the other line alone), not 0
    assert lines == 2
    assert len(reasons) == 1 and "many" in reasons[0] and "type_id=34" in reasons[0]
    assert "many" in caplog.text


def test_the_two_value_form_is_unknown_too():
    total, _ = IndustryService._sum_estimated_item_value(
        [{"type_id": 34, "quantity": "many"}], quantity_key="quantity",
        adjusted_price_map={34: {"adjusted_price": 5.0}})
    assert total is None


def test_the_fee_names_the_eiv_reason():
    fee = job_install_cost(activity="manufacturing", process_value=None, system_index=0.05,
                           structure_cost_reduction=0.0, surcharge_rates=_RATES,
                           eiv_unknown_reason="the quantity 'many' of type_id=34 is not a number")
    assert fee["total_job_cost"] is None
    assert "EIV" in fee["fee_unknown_reason"] and "'many'" in fee["fee_unknown_reason"]


def test_the_default_eiv_reason_is_unchanged():
    fee = job_install_cost(activity="manufacturing", process_value=None, system_index=0.05,
                           structure_cost_reduction=0.0, surcharge_rates=_RATES)
    assert fee["fee_unknown_reason"] == "no priced EIV for the manufacturing job (a material has no adjusted price)"


# --- items 3-4: the system cost index --------------------------------------------------------------

def test_a_non_numeric_cost_index_is_unknown_with_the_reason(caplog):
    profile = _profile({"activity": "manufacturing", "cost_index": "n/a"})
    with caplog.at_level(logging.WARNING):
        index, reason = IndustryService._system_cost_index_with_reason(profile_payload=profile,
                                                                       activity="manufacturing")
    assert index is None
    assert "'n/a'" in reason and "not a number" in reason and str(SYSTEM) in reason
    assert "n/a" in caplog.text
    assert IndustryService._system_cost_index(profile_payload=profile, activity="manufacturing") is None


def test_a_missing_cost_index_entry_is_unknown_not_zero():
    profile = _profile({"activity": "copying", "cost_index": 0.01})
    index, reason = IndustryService._system_cost_index_with_reason(profile_payload=profile, activity="manufacturing")
    assert index is None
    assert reason == "the industry profile has no manufacturing cost index"


def test_a_null_cost_index_value_is_unknown():
    profile = _profile({"activity": "manufacturing", "cost_index": None})
    index, reason = IndustryService._system_cost_index_with_reason(profile_payload=profile, activity="manufacturing")
    assert index is None and reason


def test_a_failed_cost_index_lookup_is_named():
    profile = _profile(industry_system={"solar_system_id": SYSTEM, "cost_indices": [],
                                        "lookup_error": "ESI GET failed: fake"})
    index, reason = IndustryService._system_cost_index_with_reason(profile_payload=profile, activity="manufacturing")
    assert index is None and "ESI GET failed: fake" in reason


def test_a_present_cost_index_has_no_reason():
    profile = _profile({"activity": "manufacturing", "cost_index": 0.0421})
    assert IndustryService._system_cost_index_with_reason(profile_payload=profile, activity="manufacturing") == \
        (pytest.approx(0.0421), None)


def test_no_profile_is_unknown():
    index, reason = IndustryService._system_cost_index_with_reason(profile_payload=None, activity="manufacturing")
    assert index is None and reason


def test_an_unknown_index_makes_the_fee_unknown_with_its_reason():
    fee = job_install_cost(activity="manufacturing", process_value=1_000.0, system_index=None,
                           structure_cost_reduction=0.0, surcharge_rates=_RATES,
                           index_unknown_reason="the manufacturing cost index 'n/a' of system 1 is not a number")
    assert fee["total_job_cost"] is None
    assert fee["fee_unknown_reason"] == "the manufacturing cost index 'n/a' of system 1 is not a number"
    assert IndustryService._job_fee_unknown_entries(("manufacturing", fee, True)) == [
        "manufacturing (the manufacturing cost index 'n/a' of system 1 is not a number)"]


def _overview_row(monkeypatch, profile):
    import industry_service_fakes as h
    import test_industry_unknown_fees_and_inventors as prf

    rows = h._rows_with(probability_pct=30.0, bpc_runs=10, t2_limit=10)
    prices = {**h._INVENTION_FAKE_PRICES, 5002: 2000.0}
    service = h._invention_service(monkeypatch, prices=prices, blueprint_rows=rows, profile=profile,
                                   real_confidence=True)
    return prf._t2(prf._overview(service))


def test_a_non_numeric_manufacturing_index_makes_the_overview_fee_unknown(monkeypatch):
    import test_industry_unknown_fees_and_inventors as prf

    IndustryService._reset_once_warnings()
    profile = {**prf.PROFILE, "system_cost_indices": [
        {"activity": "manufacturing", "cost_index": "n/a"},
        {"activity": "invention", "cost_index": 0.06},
        {"activity": "copying", "cost_index": 0.04},
    ]}
    mj = _overview_row(monkeypatch, profile)["manufacturing_job"]
    assert mj["activity_breakdown"]["manufacturing"]["total_job_cost"] is None      # not a 0 fee
    assert mj["activity_breakdown"]["manufacturing"]["cost_index"] is None
    assert "manufacturing (the manufacturing cost index 'n/a' is not a number)" in mj["job_fee_unknown_reasons"]
    assert mj["total_cost_excludes_unknown_job_fees"] is True


def test_a_missing_index_reads_unknown_in_the_activity_cost_indices(monkeypatch):
    import test_industry_unknown_fees_and_inventors as prf

    IndustryService._reset_once_warnings()
    row = _overview_row(monkeypatch, prf._F_PROFILE)          # manufacturing only
    indices = row["manufacturing_job"]["activity_cost_indices"]
    assert indices["manufacturing"] == pytest.approx(0.05)
    assert indices["invention"] is None and indices["copying"] is None             # unknown, not 0.00%


# --- item 4: the profile payloads ---------------------------------------------------------------------

def _esi(systems):
    return SimpleNamespace(get_industry_systems=lambda: systems)


def test_the_system_cost_index_payload_names_a_missing_manufacturing_index():
    systems = [{"solar_system_id": SYSTEM, "cost_indices": [{"activity": "copying", "cost_index": 0.01}]}]
    svc = IndustryService(state=SimpleNamespace(esi_service=_esi(systems)))
    payload = svc.industry_system_cost_index(system_id=SYSTEM)
    assert "manufacturing cost index" in payload["cost_index_unknown_reason"]
    assert "cost_index_unknown_reason" not in systems[0]          # the ESI cache row is not mutated


def test_the_system_cost_index_payload_names_an_unknown_system():
    svc = IndustryService(state=SimpleNamespace(esi_service=_esi([])))
    payload = svc.industry_system_cost_index(system_id=SYSTEM)
    assert payload["cost_indices"] == [] and str(SYSTEM) in payload["cost_index_unknown_reason"]


def test_the_system_cost_index_payload_has_no_reason_when_the_index_is_there():
    systems = [{"solar_system_id": SYSTEM, "cost_indices": [{"activity": "manufacturing", "cost_index": 0.05}]}]
    svc = IndustryService(state=SimpleNamespace(esi_service=_esi(systems)))
    assert svc.industry_system_cost_index(system_id=SYSTEM)["cost_index_unknown_reason"] is None


def test_the_profile_context_carries_the_cost_index_unknown_reason(app_session):
    svc, profile_id = _profile_service(app_session, system_id=SYSTEM)
    svc.industry_system_cost_index = lambda **kw: {"solar_system_id": SYSTEM,
                                                    "cost_indices": [{"activity": "copying", "cost_index": 0.01}]}
    payload = svc._resolve_industry_profile_context(character_id=None, industry_profile_id=profile_id)
    assert payload["cost_index_unknown_reason"] == "the industry profile has no manufacturing cost index"


def test_the_profile_context_names_a_failed_lookup(app_session):
    from eve_online_industry_tracker.application.errors import ServiceError

    svc, profile_id = _profile_service(app_session, system_id=SYSTEM)

    def _raise(**kw):
        raise ServiceError("ESI service not initialized.", status_code=503)

    svc.industry_system_cost_index = _raise
    payload = svc._resolve_industry_profile_context(character_id=None, industry_profile_id=profile_id)
    assert "ESI service not initialized" in payload["cost_index_unknown_reason"]
    assert "ESI service not initialized" in payload["industry_system"]["lookup_error"]


def test_the_profile_context_has_no_reason_with_an_index(app_session):
    svc, profile_id = _profile_service(app_session, system_id=SYSTEM)
    svc.industry_system_cost_index = lambda **kw: {"solar_system_id": SYSTEM,
                                                    "cost_indices": [{"activity": "manufacturing", "cost_index": 0.05}]}
    payload = svc._resolve_industry_profile_context(character_id=None, industry_profile_id=profile_id)
    assert payload["cost_index_unknown_reason"] is None


def test_a_profile_without_a_system_names_it(app_session):
    svc, profile_id = _profile_service(app_session)
    payload = svc._resolve_industry_profile_context(character_id=None, industry_profile_id=profile_id)
    assert payload["cost_index_unknown_reason"]
