"""The corp's own invention jobs as the realized invention cost source.

Cost per invented run = sum(runs x one attempt's inputs + job cost) /
sum(successful_runs x licensed_runs) over the delivered jobs for the
blueprint: every attempt is paid for, failed ones included, and only the
successful attempts yield runs. The decryptor of a job is read from its ESI
success chance (`probability` = SDE base x skill factor x decryptor
multiplier, the skill factor 1 + (sci1 + sci2)/30 + enc/40 at whole levels):
ESI's licensed_runs on an invention job is the source T1 copy's runs, not
the invented BPC's, so it names no decryptor. Several candidates: the
overview's published choice for the blueprint when it is one of them, else
"decryptor unknown: candidates ...". Never guessed.

Fake ids and prices only; the SDE rows are built in memory.
"""
from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from eve_online_industry_tracker.application.characters import asset_provenance as ap
from eve_online_industry_tracker.application.characters.asset_provenance import (
    RealizedInventionRefresh,
    build_invention_cost_per_run_by_blueprint_type,
    invention_cost_source_for,
    resolve_industry_job_cost_snapshot,
)
from eve_online_industry_tracker.application.industry.invention_model import (
    NO_DECRYPTOR,
    DecryptorOption,
    InventionContext,
)
from eve_online_industry_tracker.infrastructure.models import Blueprints, Groups, Types

_SOURCE_BP = 990001
_TARGET_BP = 990002
_PRODUCT = 990003
_DATACORE_A = 990011
_DATACORE_B = 990012
_MINERAL = 990021
_DECRYPTOR_PLUS_1 = 990041
_DECRYPTOR_PLUS_2A = 990042
_DECRYPTOR_PLUS_2B = 990043
_SCIENCE_A = 990031
_SCIENCE_B = 990032
_ENCRYPTION = 990033

#: One attempt: 2 x 100 + 3 x 50 = 350 ISK.
_PRICES = {_DATACORE_A: 100.0, _DATACORE_B: 50.0, _MINERAL: 5.0, _DECRYPTOR_PLUS_1: 300.0,
           _DECRYPTOR_PLUS_2A: 400.0, _DECRYPTOR_PLUS_2B: 500.0}
_PER_ATTEMPT = 350.0
_BASE_RUNS = 10
_BASE_PROBABILITY = 0.34
#: ESI licensed_runs of an invention job: the source T1 copy's runs.
_SOURCE_COPY_RUNS = 300
#: The inventor's skill factor: science 4 + 4, encryption 4.
_SKILL_FACTOR = 1 + 8 / 30 + 4 / 40

_PLUS_1 = DecryptorOption(type_id=_DECRYPTOR_PLUS_1, type_name="Fake +1", probability_multiplier=1.2,
                          max_run_modifier=1)
_PLUS_2A = DecryptorOption(type_id=_DECRYPTOR_PLUS_2A, type_name="Fake +2 A", probability_multiplier=1.9,
                           max_run_modifier=2)
#: Same success multiplier as no decryptor (Symmetry vs none on the real SDE).
_PLUS_2B = DecryptorOption(type_id=_DECRYPTOR_PLUS_2B, type_name="Fake +2 B", probability_multiplier=1.0,
                           max_run_modifier=2)
_OPTIONS = (NO_DECRYPTOR, _PLUS_1, _PLUS_2A)
_OPTIONS_WITH_TWIN = (NO_DECRYPTOR, _PLUS_1, _PLUS_2A, _PLUS_2B)


def _chance(decryptor: DecryptorOption, skill_factor: float = _SKILL_FACTOR) -> float:
    """The success chance ESI reports for a job (float32-rounded like ESI)."""
    import struct

    value = _BASE_PROBABILITY * skill_factor * decryptor.probability_multiplier
    return struct.unpack("f", struct.pack("f", value))[0]


@pytest.fixture()
def sde(sde_engine):
    with Session(sde_engine) as session:
        session.add(Groups(id=990900, name={"en": "Fake Group"}, categoryID=990, published=True))
        session.add(Groups(id=270, name={"en": "Science"}, categoryID=16, published=True))
        for type_id, name in (
            (_SOURCE_BP, "Fake T1 Blueprint"), (_TARGET_BP, "Fake T2 Blueprint"), (_PRODUCT, "Fake T2 Item"),
            (_DATACORE_A, "Fake Datacore A"), (_DATACORE_B, "Fake Datacore B"), (_MINERAL, "Fake Mineral"),
        ):
            session.add(Types(id=type_id, name={"en": name}, groupID=990900, published=True))
        for type_id, name in (
            (_SCIENCE_A, "Fake Science A"), (_SCIENCE_B, "Fake Science B"), (_ENCRYPTION, "Fake Encryption Methods"),
        ):
            session.add(Types(id=type_id, name={"en": name}, groupID=270, published=True))
        session.add(Blueprints(
            id=1, blueprintTypeID=_SOURCE_BP, maxProductionLimit=300,
            activities={"invention": {
                "time": 1000,
                "materials": [{"typeID": _DATACORE_A, "quantity": 2}, {"typeID": _DATACORE_B, "quantity": 3}],
                "products": [{"typeID": _TARGET_BP, "quantity": _BASE_RUNS, "probability": _BASE_PROBABILITY}],
                "skills": [
                    {"typeID": _SCIENCE_A, "level": 1}, {"typeID": _SCIENCE_B, "level": 1},
                    {"typeID": _ENCRYPTION, "level": 1},
                ],
            }},
        ))
        session.add(Blueprints(
            id=2, blueprintTypeID=_TARGET_BP, maxProductionLimit=10,
            activities={"manufacturing": {
                "time": 600,
                "materials": [{"typeID": _MINERAL, "quantity": 10}],
                "products": [{"typeID": _PRODUCT, "quantity": 1}],
            }},
        ))
        session.commit()
    with Session(sde_engine) as session:
        yield session


_UNSET = object()


def _job(job_id=1, *, runs=10, successful_runs=4, licensed_runs=_SOURCE_COPY_RUNS, cost=100.0, status="delivered",
         probability=_UNSET, **extra):
    job = {
        "job_id": job_id, "activity_id": 8, "status": status,
        "blueprint_type_id": _SOURCE_BP, "product_type_id": _TARGET_BP,
        "runs": runs, "licensed_runs": licensed_runs, "cost": cost,
        "completed_date": "2026-10-01T00:00:00Z",
    }
    if probability is _UNSET:
        probability = _chance(NO_DECRYPTOR)
    if probability is not None:
        job["probability"] = probability
    if successful_runs is not None:
        job["successful_runs"] = successful_runs
    job.update(extra)
    return job


def _inputs(choices) -> ap.RealizedInventionInputs:
    return ap.RealizedInventionInputs(
        invention_context=InventionContext(encryption_skill_type_ids=frozenset({_ENCRYPTION})),
        decryptor_by_blueprint_type_id=dict(choices), default_decryptor=None, probability_floor=0.01,
        decryptor_choices_published=True,
    )


def _refresh(options=_OPTIONS, *, overview_choices=None) -> RealizedInventionRefresh:
    return RealizedInventionRefresh(
        inputs=_inputs(overview_choices) if overview_choices is not None else None,
        inputs_unknown_reason="no inputs in this test", decryptor_options=options,
    )


def _sources(sde, jobs, *, options=_OPTIONS, prices=None, overview_choices=None):
    return build_invention_cost_per_run_by_blueprint_type(
        jobs=jobs, sde_session=sde, market_price_map=dict(_PRICES if prices is None else prices),
        invention_refresh=_refresh(options, overview_choices=overview_choices),
    )


def test_ten_attempts_four_successes_cost_every_attempt_over_the_successful_runs(sde):
    """The worked example: 10 attempts, 4 successes, 10 runs per BPC, 350 ISK
    per attempt, fee 100. Before: (350 + 100) / 10 = 45 ISK per run (one
    attempt per licensed BPC, the failed attempts free). Now: (10 x 350 +
    100) / (4 x 10) = 90 ISK per run."""
    source = _sources(sde, [_job()])[_TARGET_BP]
    assert source["source"] == "actual_invention_job"
    assert source["unit_cost_per_run"] == pytest.approx((10 * _PER_ATTEMPT + 100.0) / (4 * 10))
    assert source["unit_cost_per_run"] == pytest.approx(90.0)
    assert source["attempts"] == 10 and source["successful_runs"] == 4 and source["invented_runs"] == 40


def test_the_jobs_of_a_blueprint_are_summed_and_a_failed_job_still_costs(sde):
    jobs = [_job(1), _job(2, runs=5, successful_runs=0, cost=50.0)]
    source = _sources(sde, jobs)[_TARGET_BP]
    assert source["unit_cost_per_run"] == pytest.approx(
        (10 * _PER_ATTEMPT + 100.0 + 5 * _PER_ATTEMPT + 50.0) / (4 * 10)
    )


def test_undelivered_jobs_and_jobs_without_an_outcome_are_skipped(sde):
    jobs = [
        _job(1),
        _job(2, status="active", successful_runs=None),
        _job(3, status="ready", successful_runs=2),
        _job(4, successful_runs=None),
    ]
    source = _sources(sde, jobs)[_TARGET_BP]
    assert source["unit_cost_per_run"] == pytest.approx(90.0)
    assert source["jobs"] == 1


def test_zero_successes_is_no_cost_source_and_says_why(sde):
    source = _sources(sde, [_job(successful_runs=0)])[_TARGET_BP]
    assert source.get("unit_cost_per_run") is None
    assert "no successful" in source["unknown_reason"]


def test_the_decryptor_is_read_from_the_success_chance_and_bought_per_attempt(sde):
    """+1 runs: 11 runs per success, one decryptor per attempt."""
    source = _sources(sde, [_job(probability=_chance(_PLUS_1))])[_TARGET_BP]
    assert source["unit_cost_per_run"] == pytest.approx((10 * (_PER_ATTEMPT + 300.0) + 100.0) / (4 * 11))


def test_licensed_runs_are_the_source_copy_runs_and_name_no_decryptor(sde):
    """On the real ESI data licensed_runs is the source T1 copy's runs (300
    here). 10 base + 9 would have read as a +9 decryptor; the chance says
    no decryptor, and a success yields the SDE's 10 runs."""
    for licensed_runs in (_SOURCE_COPY_RUNS, _BASE_RUNS + 9, 1):
        source = _sources(sde, [_job(licensed_runs=licensed_runs)])[_TARGET_BP]
        assert source["unit_cost_per_run"] == pytest.approx(90.0)
        assert source["invented_runs"] == 4 * _BASE_RUNS


def test_an_ambiguous_decryptor_is_never_guessed(sde):
    source = _sources(sde, [_job()], options=_OPTIONS_WITH_TWIN)[_TARGET_BP]
    assert source.get("unit_cost_per_run") is None
    assert "decryptor unknown: candidates Fake +2 B, No decryptor" in source["unknown_reason"]


def test_the_overview_choice_disambiguates_when_it_is_a_candidate(sde):
    source = _sources(
        sde, [_job()], options=_OPTIONS_WITH_TWIN, overview_choices={_TARGET_BP: _PLUS_2B},
    )[_TARGET_BP]
    assert source["unit_cost_per_run"] == pytest.approx((10 * (_PER_ATTEMPT + 500.0) + 100.0) / (4 * 12))
    assert source["decryptor_from_overview_choice_job_ids"] == [1]


def test_the_overview_choice_disambiguates_to_no_decryptor(sde):
    source = _sources(
        sde, [_job()], options=_OPTIONS_WITH_TWIN, overview_choices={_TARGET_BP: NO_DECRYPTOR},
    )[_TARGET_BP]
    assert source["unit_cost_per_run"] == pytest.approx(90.0)
    assert source["decryptor_from_overview_choice_job_ids"] == [1]


def test_an_overview_choice_outside_the_candidates_does_not_decide(sde):
    source = _sources(
        sde, [_job()], options=_OPTIONS_WITH_TWIN, overview_choices={_TARGET_BP: _PLUS_1},
    )[_TARGET_BP]
    assert source.get("unit_cost_per_run") is None
    reason = source["unknown_reason"]
    assert "decryptor unknown: candidates Fake +2 B, No decryptor" in reason
    assert "the overview's choice (Fake +1) is not one of them" in reason


def test_another_blueprints_overview_choice_does_not_decide(sde):
    source = _sources(
        sde, [_job()], options=_OPTIONS_WITH_TWIN, overview_choices={990099: NO_DECRYPTOR},
    )[_TARGET_BP]
    assert source.get("unit_cost_per_run") is None
    assert "decryptor unknown: candidates" in source["unknown_reason"]


def test_an_unambiguous_chance_is_not_overridden_by_the_overview_choice(sde):
    source = _sources(
        sde, [_job(probability=_chance(_PLUS_1))], options=_OPTIONS_WITH_TWIN,
        overview_choices={_TARGET_BP: NO_DECRYPTOR},
    )[_TARGET_BP]
    assert source["unit_cost_per_run"] == pytest.approx((10 * (_PER_ATTEMPT + 300.0) + 100.0) / (4 * 11))
    assert source["decryptor_from_overview_choice_job_ids"] == []


def test_a_skill_factor_below_the_sde_required_levels_is_no_candidate(sde):
    """0.34 x 1.2: no decryptor at science 3 + 3, encryption 4 (factor 1.2),
    or +1 at all skills 0 (factor 1.0). The SDE requires level 1 in each
    skill, so only no decryptor fits."""
    source = _sources(sde, [_job(probability=_chance(NO_DECRYPTOR, 1.2))])[_TARGET_BP]
    assert source["unit_cost_per_run"] == pytest.approx(90.0)


def test_a_job_without_a_success_chance_has_an_unknown_decryptor(sde):
    source = _sources(sde, [_job(probability=None)])[_TARGET_BP]
    assert source.get("unit_cost_per_run") is None
    assert "decryptor unknown: the job gives no success chance" in source["unknown_reason"]


def test_a_chance_no_decryptor_and_skill_level_explains_is_unknown(sde):
    source = _sources(sde, [_job(probability=0.123)])[_TARGET_BP]
    assert source.get("unit_cost_per_run") is None
    assert "matches no decryptor at any skill level" in source["unknown_reason"]


def test_a_capped_chance_names_no_decryptor(sde):
    source = _sources(sde, [_job(probability=1.0)])[_TARGET_BP]
    assert source.get("unit_cost_per_run") is None
    assert "capped at 100%" in source["unknown_reason"]


def test_a_job_with_an_unknown_decryptor_is_left_out_of_the_sum(sde):
    source = _sources(sde, [_job(1), _job(2, probability=0.123)])[_TARGET_BP]
    assert source["unit_cost_per_run"] == pytest.approx(90.0)
    assert source["jobs"] == 1


def test_without_sde_decryptor_data_no_decryptor_is_assumed(sde):
    source = _sources(sde, [_job()], options=(NO_DECRYPTOR,))[_TARGET_BP]
    assert source.get("unit_cost_per_run") is None
    assert "decryptor unknown" in source["unknown_reason"]


def test_an_unpriced_input_is_no_cost_source(sde):
    prices = {k: v for k, v in _PRICES.items() if k != _DATACORE_B}
    source = _sources(sde, [_job()], prices=prices)[_TARGET_BP]
    assert source.get("unit_cost_per_run") is None
    assert str(_DATACORE_B) in source["unknown_reason"]


def test_the_sde_invention_is_read_once_per_refresh(sde, monkeypatch):
    calls: list[list[int]] = []
    real = ap.get_invention_source_blueprint_ids

    def _counting(session, ids):
        calls.append(list(ids))
        return real(session, ids)

    monkeypatch.setattr(ap, "get_invention_source_blueprint_ids", _counting)
    refresh = _refresh()
    sources = build_invention_cost_per_run_by_blueprint_type(
        jobs=[_job(1), _job(2), _job(3)], sde_session=sde, market_price_map=dict(_PRICES),
        invention_refresh=refresh,
    )
    job = _ManufacturingJob(blueprint_type_id=_TARGET_BP, product_type_id=_PRODUCT, runs=2, cost=10.0)
    for _ in range(3):
        resolve_industry_job_cost_snapshot(
            job=job, sde_session=sde, market_price_map=dict(_PRICES), invention_refresh=refresh,
            **invention_cost_source_for(sources, 990099),
        )
    assert calls == [[_TARGET_BP]]


class _ManufacturingJob:
    def __init__(self, **fields):
        self.raw = {"activity_id": 1, "runs": fields.get("runs")}
        for key, value in fields.items():
            setattr(self, key, value)


def test_a_manufacturing_job_uses_the_actual_cost_per_run(sde):
    sources = _sources(sde, [_job()])
    job = _ManufacturingJob(blueprint_type_id=_TARGET_BP, product_type_id=_PRODUCT, runs=3, cost=10.0)
    snapshot = resolve_industry_job_cost_snapshot(
        job=job, sde_session=sde, market_price_map=dict(_PRICES), invention_refresh=_refresh(),
        **invention_cost_source_for(sources, _TARGET_BP),
    )
    assert snapshot["invention_cost"] == pytest.approx(3 * 90.0)
    assert snapshot["invention_cost_unknown_reason"] is None
    assert snapshot["source"].endswith("+actual_invention_job")


def test_the_refresh_reason_is_passed_down_and_the_provider_is_not_asked_again(sde, monkeypatch):
    def _boom():
        raise AssertionError("the provider is read once per refresh, not per job")

    monkeypatch.setattr(ap, "_REALIZED_INVENTION_INPUTS_PROVIDER", _boom)
    refresh = RealizedInventionRefresh(inputs=None, inputs_unknown_reason="the producer's inputs could not be read (X)")
    job = _ManufacturingJob(blueprint_type_id=_TARGET_BP, product_type_id=_PRODUCT, runs=1, cost=10.0)
    snapshot = resolve_industry_job_cost_snapshot(
        job=job, sde_session=sde, market_price_map=dict(_PRICES), invention_refresh=refresh,
    )
    assert "could not be read (X)" in snapshot["invention_cost_unknown_reason"]


def test_without_an_actual_source_or_estimate_both_reasons_are_named(sde):
    sources = _sources(sde, [_job(successful_runs=0)])
    job = _ManufacturingJob(blueprint_type_id=_TARGET_BP, product_type_id=_PRODUCT, runs=1, cost=10.0)
    snapshot = resolve_industry_job_cost_snapshot(
        job=job, sde_session=sde, market_price_map=dict(_PRICES), invention_refresh=_refresh(),
        **invention_cost_source_for(sources, _TARGET_BP),
    )
    reason = snapshot["invention_cost_unknown_reason"]
    assert "no inputs in this test" in reason
    assert "no successful" in reason


def test_read_takes_the_provider_once(monkeypatch):
    calls = []
    monkeypatch.setattr(ap, "_REALIZED_INVENTION_INPUTS_PROVIDER", lambda: calls.append(1))
    refresh = RealizedInventionRefresh.read()
    assert calls == [1]
    assert refresh.inputs is None
    assert "no inventor skills or decryptor choice" in refresh.inputs_unknown_reason
