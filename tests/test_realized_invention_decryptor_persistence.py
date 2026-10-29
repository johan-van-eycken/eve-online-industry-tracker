"""The decryptor the overview chose survives a restart.

The realized invention estimate needs the producer's decryptor per invented
blueprint. The overview kept it in memory only, so after every restart the
startup job refresh found none and stored "no decryptor choice" on every
invented T2 job. The last published choices now live in the app DB, are read
when there is no in-memory overview, and the jobs stored without a choice are
re-snapshotted once the first overview of the process publishes. Fake ids only.
"""
from __future__ import annotations

import sqlite3
from types import MethodType, SimpleNamespace

import pytest
from sqlalchemy.orm import Session

from eve_online_industry_tracker.application.characters import asset_provenance as ap
from eve_online_industry_tracker.application.industry.invention_model import (
    NO_DECRYPTOR,
    NO_DECRYPTOR_DATA_REASON,
    DecryptorChoice,
    DecryptorOption,
)
from eve_online_industry_tracker.application.industry.service import IndustryService
from eve_online_industry_tracker.infrastructure.models import (
    Blueprints,
    CharacterAssetHistoryModel,
    CharacterIndustryJobsModel,
    Groups,
    RealizedInventionDecryptorChoiceModel,
    RealizedInventionDecryptorPublishModel,
    Types,
)
from eve_online_industry_tracker.infrastructure.schema_migrations import ensure_app_schema
from eve_online_industry_tracker.application.characters import character as character_module
from eve_online_industry_tracker.application.characters.asset_provenance import (
    BlueprintCopySighting,
    load_owner_invention_evidence,
)
from eve_online_industry_tracker.application.characters.character import Character
from eve_online_industry_tracker.infrastructure.models import CorporationAssetHistoryModel, CorporationIndustryJobsModel
from test_realized_invention_actual_jobs import (  # noqa: F401  (the sde fixture)
    _PER_ATTEMPT,
    _PLUS_1,
    _PLUS_2A,
    _PLUS_2B,
    _PRICES,
    _chance,
    _job,
    _mfg,
    _refresh,
    sde,
)
from fakes import _Db

_SOURCE_BP = 990001
_TARGET_BP = 990002
_PRODUCT = 990003
_DATACORE_A = 990011
_DATACORE_B = 990012
_MINERAL = 990021
_SCIENCE_A = 990031
_SCIENCE_B = 990032
_ENCRYPTION = 990033
_UNPLANNED_BP = 990052
_PILOT = 7001

_DECRYPTOR = DecryptorOption(type_id=990041, type_name="Fake Decryptor", probability_multiplier=1.2,
                             me_modifier=2, te_modifier=10, max_run_modifier=1)


class _Admin:
    def __init__(self, values):
        self._values = values

    def get(self, category, key):
        return self._values.get((category, key))


def _state(**extra):
    state = SimpleNamespace(
        admin_settings=_Admin({
            ("industry", "invention_decryptor_selection_enabled"): True,
            ("industry", "invention_probability_floor"): 0.01,
        }),
        char_manager=SimpleNamespace(get_characters=lambda: [{
            "character_id": _PILOT,
            "skills": {"skills": [
                {"skill_id": _SCIENCE_A, "trained_skill_level": 4},
                {"skill_id": _SCIENCE_B, "trained_skill_level": 4},
                {"skill_id": _ENCRYPTION, "trained_skill_level": 4},
            ]},
        }]),
        esi_service=SimpleNamespace(get_market_prices=lambda: [
            {"type_id": _DATACORE_A, "average_price": 100.0},
            {"type_id": _DATACORE_B, "average_price": 50.0},
            {"type_id": _MINERAL, "average_price": 5.0},
            {"type_id": _DECRYPTOR.type_id, "average_price": 300.0},
        ]),
    )
    for key, value in extra.items():
        setattr(state, key, value)
    return state


def _service(state, session_provider) -> IndustryService:
    service = IndustryService(state=state, sessions=session_provider)
    service._load_encryption_skill_type_ids = MethodType(lambda self: frozenset({_ENCRYPTION}), service)
    return service


def _seed_sde(engine) -> None:
    with Session(engine) as session:
        session.add(Groups(id=270, name={"en": "Science"}, categoryID=16, published=True))
        session.add(Groups(id=990900, name={"en": "Fake Group"}, categoryID=990, published=True))
        for type_id, name, group in (
            (_SOURCE_BP, "Fake T1 Blueprint", 990900), (_TARGET_BP, "Fake T2 Blueprint", 990900),
            (_PRODUCT, "Fake T2 Item", 990900), (_DATACORE_A, "Fake Datacore A", 990900),
            (_DATACORE_B, "Fake Datacore B", 990900), (_MINERAL, "Fake Mineral", 990900),
            (_SCIENCE_A, "Fake Science A", 270), (_SCIENCE_B, "Fake Science B", 270),
            (_ENCRYPTION, "Fake Encryption Methods", 270),
        ):
            session.add(Types(id=type_id, name={"en": name}, groupID=group, published=True))
        session.add(Blueprints(
            id=1, blueprintTypeID=_SOURCE_BP, maxProductionLimit=300,
            activities={"invention": {
                "time": 1000,
                "materials": [{"typeID": _DATACORE_A, "quantity": 2}, {"typeID": _DATACORE_B, "quantity": 3}],
                "products": [{"typeID": _TARGET_BP, "quantity": 10, "probability": 0.34}],
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


def test_the_published_choices_are_read_back_after_a_restart(session_provider):
    before = _service(_state(), session_provider)
    before.publish_decryptor_choices({
        _TARGET_BP: DecryptorChoice(_DECRYPTOR, "chosen"),
        990004: DecryptorChoice(NO_DECRYPTOR, "no decryptor beats the others"),
    })
    with session_provider.app_session() as session:
        stored = {row.blueprint_type_id: row for row in session.query(RealizedInventionDecryptorChoiceModel)}
    assert set(stored) == {_TARGET_BP, 990004}
    assert stored[_TARGET_BP].choice_reason == "chosen" and stored[_TARGET_BP].set_at is not None

    # A restart: a new state without the in-memory overview.
    after = _service(_state(), session_provider).realized_invention_inputs()
    assert after.decryptor_for(_TARGET_BP) == _DECRYPTOR
    assert after.decryptor_for(990004) == NO_DECRYPTOR
    assert after.decryptor_choices_published is True
    assert after.decryptor_for(_UNPLANNED_BP) is None


def test_the_startup_job_refresh_after_a_restart_estimates_with_the_persisted_choice(
    session_provider, sde_engine, monkeypatch,
):
    """Before the first overview of the new process the job refresh (the
    provider bootstrap registers) already knows the decryptor."""
    _seed_sde(sde_engine)
    _service(_state(), session_provider).publish_decryptor_choices({_TARGET_BP: DecryptorChoice(_DECRYPTOR, "chosen")})

    restarted = _service(_state(), session_provider)
    monkeypatch.setattr(ap, "_REALIZED_INVENTION_INPUTS_PROVIDER", None)
    ap.set_realized_invention_inputs_provider(restarted.realized_invention_inputs)
    refresh = ap.RealizedInventionRefresh.read()

    class _Job:
        raw = {"activity_id": 1, "runs": 2}
        blueprint_type_id = _TARGET_BP
        product_type_id = _PRODUCT
        runs = 2
        cost = 40.0

    with Session(sde_engine) as sde:
        snapshot = ap.resolve_industry_job_cost_snapshot(
            job=_Job(), sde_session=sde, invention_refresh=refresh,
            market_price_map={_DATACORE_A: 100.0, _DATACORE_B: 50.0, _MINERAL: 5.0, _DECRYPTOR.type_id: 300.0},
        )
    assert snapshot["invention_cost_unknown_reason"] is None
    assert snapshot["invention_cost"] > 0


def test_end_to_end_a_job_reason_reaches_the_ledger_allocation_and_feedback_skips_it(
    session_provider, sde_engine,
):
    """The startup refresh without a decryptor choice -> the job's reason ->
    the sale's allocation_details and cost_lower_bound_reason -> feedback
    skips the cost update."""
    from eve_online_industry_tracker.application.characters.realized_profit import (
        CharacterRealizedProfitLedgerService,
    )
    from eve_online_industry_tracker.application.daily_planner.feedback_processor import _industry_build_unit_cost
    from eve_online_industry_tracker.infrastructure.models import CharacterModel, CharacterWalletTransactionsModel

    _seed_sde(sde_engine)
    inputs = _service(_state(), session_provider).realized_invention_inputs()  # nothing published yet
    raw = {"job_id": 5001, "activity_id": 1, "runs": 2, "blueprint_type_id": _TARGET_BP,
           "product_type_id": _PRODUCT, "cost": 40.0}
    with Session(sde_engine) as sde:
        snapshot = ap.resolve_industry_job_cost_snapshot(
            job=type("IndustryJobPayload", (), raw)(), sde_session=sde,
            market_price_map={_DATACORE_A: 100.0, _DATACORE_B: 50.0, _MINERAL: 5.0},
            invention_refresh=ap.RealizedInventionRefresh(inputs=inputs),
        )
    reason = snapshot["invention_cost_unknown_reason"]
    assert "no decryptor choice" in reason

    with session_provider.app_session() as session:
        session.add(CharacterModel(character_name="Fake Pilot", character_id=_PILOT))
        session.add(CharacterIndustryJobsModel(
            character_id=_PILOT, job_id=5001, status="delivered", activity_id=1, runs=2,
            blueprint_type_id=_TARGET_BP, product_type_id=_PRODUCT, cost=40.0,
            completed_date="2026-10-01T00:00:00Z", output_quantity=snapshot["output_quantity"],
            total_build_cost=snapshot["total_cost"], unit_build_cost=snapshot["unit_cost"],
            invention_cost_unknown_reason=reason, raw=raw,
        ))
        session.add(CharacterWalletTransactionsModel(
            character_id=_PILOT, transaction_id=6001, date="2026-10-02T00:00:00Z", is_buy=False,
            is_personal=True, quantity=2, type_id=_PRODUCT, unit_price=200.0, total_price=400.0,
        ))
        session.commit()
        ledger = CharacterRealizedProfitLedgerService(app_session=session, sde_session=None, market_prices=[])
        ledger.rebuild(character_id=_PILOT)
        [row] = ledger.list_rows(character_id=_PILOT)
    assert row["allocation_details"][0]["cost_unknown_reason"] == reason
    assert row["cost_lower_bound_reason"] == reason
    cost, skip_reason = _industry_build_unit_cost(row["allocation_details"])
    assert cost is None and reason in skip_reason


def test_a_new_publish_replaces_the_stored_choices(session_provider):
    service = _service(_state(), session_provider)
    service.publish_decryptor_choices({_TARGET_BP: DecryptorChoice(_DECRYPTOR, "chosen")})
    service.publish_decryptor_choices({990004: DecryptorChoice(NO_DECRYPTOR, "no decryptor beats the others")})
    inputs = _service(_state(), session_provider).realized_invention_inputs()
    assert inputs.decryptor_for(_TARGET_BP) is None
    assert inputs.decryptor_for(990004) == NO_DECRYPTOR


def test_nothing_published_yet_keeps_the_old_reason(session_provider, sde_engine):
    _seed_sde(sde_engine)
    inputs = _service(_state(), session_provider).realized_invention_inputs()
    assert inputs.decryptor_choices_published is False
    with Session(sde_engine) as sde:
        _, reason = ap.estimate_invention_cost_per_run(
            sde_session=sde, target_blueprint_type_id=_TARGET_BP, market_price_map={}, invention_inputs=inputs,
        )
    assert "no product overview has planned it yet" in reason


def test_a_blueprint_the_overview_does_not_plan_says_so(session_provider, sde_engine):
    _seed_sde(sde_engine)
    service = _service(_state(), session_provider)
    service.publish_decryptor_choices({990004: DecryptorChoice(NO_DECRYPTOR, "no decryptor beats the others")})
    inputs = service.realized_invention_inputs()
    with Session(sde_engine) as sde:
        _, reason = ap.estimate_invention_cost_per_run(
            sde_session=sde, target_blueprint_type_id=_TARGET_BP, market_price_map={}, invention_inputs=inputs,
        )
    assert "no decryptor choice (this blueprint is not planned by the overview)" in reason


def test_a_no_decryptor_data_fallback_is_priced_without_a_decryptor_like_the_producer(session_provider):
    """The producer plans a "no decryptor data" fallback with NO_DECRYPTOR,
    so the realized side does too (it used to leave it out as unknown)."""
    choices = {
        _TARGET_BP: DecryptorChoice(_DECRYPTOR, "chosen"),
        990006: DecryptorChoice(NO_DECRYPTOR, NO_DECRYPTOR_DATA_REASON),
    }
    assert IndustryService._published_decryptor_choices(choices) == {_TARGET_BP: _DECRYPTOR, 990006: NO_DECRYPTOR}


def _store_job_without_a_choice(session_provider, *, job_id=5001, reason=None) -> None:
    raw = {
        "job_id": job_id, "activity_id": 1, "status": "delivered", "runs": 2,
        "blueprint_type_id": _TARGET_BP, "product_type_id": _PRODUCT, "cost": 40.0,
        "completed_date": "2026-10-01T00:00:00Z", "blueprint_id": 880001,
    }
    with session_provider.app_session() as session:
        session.add(CharacterIndustryJobsModel(
            character_id=_PILOT, job_id=job_id, status="delivered", activity_id=1, runs=2,
            blueprint_type_id=_TARGET_BP, product_type_id=_PRODUCT, cost=40.0,
            completed_date="2026-10-01T00:00:00Z", blueprint_is_blueprint_copy=True,
            output_quantity=2, materials_cost=100.0, total_build_cost=140.0, unit_build_cost=70.0,
            invention_cost_unknown_reason=reason or (
                f"invention cost unknown: no decryptor choice for blueprint {_TARGET_BP} "
                "(no product overview has planned it yet)"
            ),
            build_cost_source="market_snapshot_estimate", raw=raw,
        ))
        session.add(CharacterAssetHistoryModel(
            character_id=_PILOT, item_id=-2_000_000_000_000 - job_id, observed_at="2026-10-01T00:00:00Z",
            snapshot_source="historical_backfill", type_id=_PRODUCT, quantity=2, is_singleton=False,
            acquisition_source="industry_build", acquisition_unit_cost=70.0, acquisition_total_cost=140.0,
            acquisition_reference_type="industry_job", acquisition_reference_id=job_id,
        ))
        session.commit()


def test_the_first_publish_re_snapshots_the_jobs_stored_without_a_choice(session_provider, sde_engine):
    _seed_sde(sde_engine)
    _store_job_without_a_choice(session_provider)
    state = _state()
    service = _service(state, session_provider)
    service.publish_decryptor_choices({_TARGET_BP: DecryptorChoice(_DECRYPTOR, "chosen")})

    with session_provider.app_session() as session:
        job = session.query(CharacterIndustryJobsModel).filter_by(job_id=5001).one()
        history = session.query(CharacterAssetHistoryModel).filter_by(acquisition_reference_id=5001).one()
        assert job.invention_cost_unknown_reason is None
        assert job.invention_cost is not None and job.invention_cost > 0
        assert job.unit_build_cost == pytest.approx((2 * 10 * 5.0 + 40.0 + job.invention_cost) / 2)
        assert "expected_invention_material_cost" in job.build_cost_source
        assert history.acquisition_unit_cost == pytest.approx(job.unit_build_cost)
        assert history.acquisition_total_cost == pytest.approx(job.total_build_cost)


def test_a_later_publish_with_a_newly_planned_blueprint_re_snapshots_again(session_provider, sde_engine):
    """The re-snapshot is not once per process: a later overview that
    publishes a choice for a blueprint the earlier ones did not plan
    re-snapshots its stored jobs too."""
    _seed_sde(sde_engine)
    state = _state()
    service = _service(state, session_provider)
    service.publish_decryptor_choices({990004: DecryptorChoice(NO_DECRYPTOR, "no decryptor beats the others")})
    _store_job_without_a_choice(session_provider)
    service.publish_decryptor_choices({_TARGET_BP: DecryptorChoice(_DECRYPTOR, "chosen")})
    with session_provider.app_session() as session:
        job = session.query(CharacterIndustryJobsModel).filter_by(job_id=5001).one()
        assert job.invention_cost_unknown_reason is None
        assert job.invention_cost is not None and job.invention_cost > 0


def test_a_publish_without_a_newly_planned_blueprint_does_not_re_snapshot(
    session_provider, sde_engine, monkeypatch,
):
    from eve_online_industry_tracker.application.industry import service as service_module

    _seed_sde(sde_engine)
    runs = []
    real = service_module.resnapshot_jobs_without_decryptor_choice
    monkeypatch.setattr(service_module, "resnapshot_jobs_without_decryptor_choice",
                        lambda **kw: runs.append(1) or real(**kw))
    service = _service(_state(), session_provider)
    service.publish_decryptor_choices({_TARGET_BP: DecryptorChoice(_DECRYPTOR, "chosen")})
    service.publish_decryptor_choices({_TARGET_BP: DecryptorChoice(NO_DECRYPTOR, "changed")})
    service.publish_decryptor_choices({})
    assert runs == [1]


def test_an_unplanned_blueprint_job_gets_the_not_planned_reason(session_provider, sde_engine):
    _seed_sde(sde_engine)
    _store_job_without_a_choice(session_provider)
    service = _service(_state(), session_provider)
    service.publish_decryptor_choices({990004: DecryptorChoice(NO_DECRYPTOR, "no decryptor beats the others")})
    with session_provider.app_session() as session:
        job = session.query(CharacterIndustryJobsModel).filter_by(job_id=5001).one()
        assert "this blueprint is not planned by the overview" in job.invention_cost_unknown_reason


def test_other_reasons_are_left_alone(session_provider, sde_engine):
    _seed_sde(sde_engine)
    _store_job_without_a_choice(session_provider, reason="invention cost unknown: no corp character has its skills loaded")
    _service(_state(), session_provider).publish_decryptor_choices({_TARGET_BP: DecryptorChoice(_DECRYPTOR, "chosen")})
    with session_provider.app_session() as session:
        job = session.query(CharacterIndustryJobsModel).filter_by(job_id=5001).one()
        assert job.invention_cost_unknown_reason.endswith("skills loaded")
        assert job.unit_build_cost == 70.0


def test_market_prices_unavailable_skips_the_re_snapshot_with_a_warning_and_retries(session_provider, sde_engine, caplog):
    from eve_online_industry_tracker.infrastructure.esi_client import ESI_REQUEST_ERRORS

    _seed_sde(sde_engine)
    _store_job_without_a_choice(session_provider)
    error_type = ESI_REQUEST_ERRORS[0]

    def _down():
        raise error_type("ESI down")

    state = _state()
    state.esi_service = SimpleNamespace(get_market_prices=_down)
    service = _service(state, session_provider)
    with caplog.at_level("WARNING"):
        service.publish_decryptor_choices({_TARGET_BP: DecryptorChoice(_DECRYPTOR, "chosen")})
    assert any("re-snapshot" in r.getMessage() for r in caplog.records)
    state.esi_service = _state().esi_service
    service.publish_decryptor_choices({_TARGET_BP: DecryptorChoice(_DECRYPTOR, "chosen")})
    with session_provider.app_session() as session:
        assert session.query(CharacterIndustryJobsModel).filter_by(job_id=5001).one().invention_cost_unknown_reason is None


def _reason_for(inputs, sde_engine, blueprint_type_id=_TARGET_BP) -> str:
    with Session(sde_engine) as sde:
        _, reason = ap.estimate_invention_cost_per_run(
            sde_session=sde, target_blueprint_type_id=blueprint_type_id, market_price_map={}, invention_inputs=inputs,
        )
    return reason


def test_an_overview_that_planned_no_invention_is_told_from_no_overview_after_a_restart(
    session_provider, sde_engine,
):
    _seed_sde(sde_engine)
    assert "no product overview has planned it yet" in _reason_for(
        _service(_state(), session_provider).realized_invention_inputs(), sde_engine,
    )
    _service(_state(), session_provider).publish_decryptor_choices({})

    restarted = _service(_state(), session_provider).realized_invention_inputs()
    assert restarted.decryptor_choices_published is True
    assert dict(restarted.decryptor_by_blueprint_type_id) == {}
    reason = _reason_for(restarted, sde_engine)
    assert "no decryptor choice (the last product overview planned no invention)" in reason
    assert "no product overview has planned it yet" not in reason


def test_an_empty_publish_in_memory_says_the_overview_planned_no_invention(session_provider, sde_engine):
    _seed_sde(sde_engine)
    service = _service(_state(), session_provider)
    service.publish_decryptor_choices({})
    assert "the last product overview planned no invention" in _reason_for(
        service.realized_invention_inputs(), sde_engine,
    )


def test_each_publish_replaces_the_one_publish_marker(session_provider):
    service = _service(_state(), session_provider)
    service.publish_decryptor_choices({_TARGET_BP: DecryptorChoice(_DECRYPTOR, "chosen")})
    with session_provider.app_session() as session:
        [first] = session.query(RealizedInventionDecryptorPublishModel).all()
        first_at, first_count = first.published_at, first.choice_count
    service.publish_decryptor_choices({})
    with session_provider.app_session() as session:
        [marker] = session.query(RealizedInventionDecryptorPublishModel).all()
        assert first_count == 1 and marker.choice_count == 0
        assert marker.published_at >= first_at
        assert session.query(RealizedInventionDecryptorChoiceModel).count() == 0


def test_choices_stored_before_the_marker_existed_still_count_as_published(session_provider):
    """A DB written before the publish marker: its rows are a publish."""
    from datetime import datetime

    with session_provider.app_session() as session:
        session.add(RealizedInventionDecryptorChoiceModel(
            blueprint_type_id=_TARGET_BP, decryptor_type_id=None, decryptor_type_name="No decryptor",
            probability_multiplier=1.0, me_modifier=0, te_modifier=0, max_run_modifier=0,
            choice_reason="old", set_at=datetime(2026, 10, 1),
        ))
        session.commit()
    inputs = _service(_state(), session_provider).realized_invention_inputs()
    assert inputs.decryptor_choices_published is True
    assert inputs.decryptor_for(_TARGET_BP) == NO_DECRYPTOR


def test_the_re_snapshot_after_an_empty_publish_names_that_case(session_provider, sde_engine):
    _seed_sde(sde_engine)
    _store_job_without_a_choice(session_provider)
    _service(_state(), session_provider).publish_decryptor_choices({})
    with session_provider.app_session() as session:
        job = session.query(CharacterIndustryJobsModel).filter_by(job_id=5001).one()
        assert "the last product overview planned no invention" in job.invention_cost_unknown_reason


_ACTUAL_JOB_REASON = (
    "; the corp's own invention jobs give no cost either: the corp's 1 delivered invention job(s) had no "
    "successful attempt (10 attempts)"
)
#: Science 4 + 4, encryption 4: the pilot of _state().
_SKILL_FACTOR = 1 + 8 / 30 + 4 / 40
_TWIN = DecryptorOption(type_id=990042, type_name="Fake Symmetry", probability_multiplier=1.0,
                        me_modifier=1, te_modifier=8, max_run_modifier=2)


def _store_invention_job(session_provider, *, job_id=4001, successful_runs=0, probability=None) -> None:
    raw = {
        "job_id": job_id, "activity_id": 8, "status": "delivered", "runs": 10, "licensed_runs": 300,
        "successful_runs": successful_runs, "blueprint_type_id": _SOURCE_BP, "product_type_id": _TARGET_BP,
        "cost": 100.0, "completed_date": "2026-09-20T00:00:00Z",
        "probability": probability if probability is not None else 0.34 * _SKILL_FACTOR,
    }
    with session_provider.app_session() as session:
        session.add(CharacterIndustryJobsModel(
            character_id=_PILOT, job_id=job_id, status="delivered", activity_id=8, runs=10,
            blueprint_type_id=_SOURCE_BP, product_type_id=_TARGET_BP, cost=100.0,
            completed_date="2026-09-20T00:00:00Z", raw=raw,
        ))
        session.commit()


def _sde_decryptors(monkeypatch, *options: DecryptorOption) -> None:
    rows = [{
        "type_id": o.type_id, "type_name": o.type_name,
        "invention_probability_multiplier": o.probability_multiplier, "invention_me_modifier": o.me_modifier,
        "invention_te_modifier": o.te_modifier, "invention_max_run_modifier": o.max_run_modifier,
    } for o in options]
    monkeypatch.setattr(ap, "get_t2_invention_decryptors", lambda session, language: rows)


def test_the_re_snapshot_keeps_the_actual_invention_jobs_part_of_the_reason(
    session_provider, sde_engine, monkeypatch,
):
    """The job's reason named both sources; the re-snapshot re-reads the
    owner's stored invention jobs, so the actual part stays."""
    _seed_sde(sde_engine)
    _sde_decryptors(monkeypatch, _DECRYPTOR)
    _store_invention_job(session_provider, successful_runs=0)
    _store_job_without_a_choice(session_provider, reason=(
        f"invention cost unknown: no decryptor choice for blueprint {_TARGET_BP} "
        "(no product overview has planned it yet)" + _ACTUAL_JOB_REASON
    ))
    _service(_state(), session_provider).publish_decryptor_choices({990004: DecryptorChoice(NO_DECRYPTOR, "x")})
    with session_provider.app_session() as session:
        reason = session.query(CharacterIndustryJobsModel).filter_by(job_id=5001).one().invention_cost_unknown_reason
    assert "this blueprint is not planned by the overview" in reason
    assert reason.endswith(_ACTUAL_JOB_REASON)


def test_the_re_snapshot_does_not_settle_a_tie_with_the_overview_choice(
    session_provider, sde_engine, monkeypatch,
):
    """The stored invention job's chance fits no decryptor and Fake
    Symmetry; the published choice (no decryptor) is no evidence, so the
    actual source stays unknown and the expected estimate is used."""
    _seed_sde(sde_engine)
    _sde_decryptors(monkeypatch, _DECRYPTOR, _TWIN)
    _store_invention_job(session_provider, successful_runs=4)
    _store_job_without_a_choice(session_provider, reason=(
        f"invention cost unknown: no decryptor choice for blueprint {_TARGET_BP} "
        "(no product overview has planned it yet); the corp's own invention jobs give no cost either: "
        "no usable invention job (job 4001: decryptor unknown: candidates Fake Symmetry, No decryptor)"
    ))
    _service(_state(), session_provider).publish_decryptor_choices({_TARGET_BP: DecryptorChoice(NO_DECRYPTOR, "x")})
    with session_provider.app_session() as session:
        job = session.query(CharacterIndustryJobsModel).filter_by(job_id=5001).one()
        assert job.invention_cost_unknown_reason is None
        assert "actual_invention_job" not in str(job.build_cost_source)
        assert "expected_invention_material_cost" in str(job.build_cost_source)


def test_the_re_snapshot_settles_a_tie_from_the_owners_stored_manufacturing_jobs(
    session_provider, sde_engine, monkeypatch,
):
    """The same tie, with a stored manufacturing job on a BPC of the
    blueprint after the invention: its 12 runs (more than the 10 no
    decryptor gives) settle Fake Symmetry, and the re-snapshot uses the
    actual source: (10 x (350 + 400) + 100) / (4 x 12) ISK per run, 2 runs."""
    _seed_sde(sde_engine)
    _sde_decryptors(monkeypatch, _DECRYPTOR, _TWIN)
    _store_invention_job(session_provider, successful_runs=4)
    with session_provider.app_session() as session:
        session.add(CharacterIndustryJobsModel(
            character_id=_PILOT, job_id=4100, status="delivered", activity_id=1, runs=12,
            blueprint_type_id=_TARGET_BP, product_type_id=_PRODUCT, cost=1.0,
            raw={"job_id": 4100, "activity_id": 1, "status": "delivered", "runs": 12,
                 "blueprint_type_id": _TARGET_BP, "blueprint_id": 77, "product_type_id": _PRODUCT,
                 "start_date": "2026-09-21T00:00:00Z", "licensed_runs": 10},
        ))
        session.commit()
    _store_job_without_a_choice(session_provider)
    state = _state()
    prices = state.esi_service.get_market_prices() + [{"type_id": _TWIN.type_id, "average_price": 400.0}]
    state.esi_service = SimpleNamespace(get_market_prices=lambda: prices)
    _service(state, session_provider).publish_decryptor_choices({_TARGET_BP: DecryptorChoice(NO_DECRYPTOR, "x")})
    with session_provider.app_session() as session:
        job = session.query(CharacterIndustryJobsModel).filter_by(job_id=5001).one()
        assert job.invention_cost_unknown_reason is None
        assert job.invention_cost == pytest.approx(2 * (10 * 750.0 + 100.0) / (4 * 12))
        assert job.build_cost_source.endswith("+actual_invention_job")


def test_the_re_snapshot_reads_only_the_owners_own_invention_jobs(session_provider, sde_engine):
    """Another character's invention job is not this pilot's cost source
    (as in the job refresh, which reads the owner's own jobs)."""
    _seed_sde(sde_engine)
    with session_provider.app_session() as session:
        session.add(CharacterIndustryJobsModel(
            character_id=_PILOT + 1, job_id=4002, status="delivered", activity_id=8, runs=10,
            blueprint_type_id=_SOURCE_BP, product_type_id=_TARGET_BP, cost=100.0,
            raw={"job_id": 4002, "activity_id": 8, "status": "delivered", "runs": 10, "successful_runs": 0,
                 "product_type_id": _TARGET_BP, "cost": 100.0, "probability": 0.34 * _SKILL_FACTOR},
        ))
        session.commit()
    _store_job_without_a_choice(session_provider)
    _service(_state(), session_provider).publish_decryptor_choices({990004: DecryptorChoice(NO_DECRYPTOR, "x")})
    with session_provider.app_session() as session:
        reason = session.query(CharacterIndustryJobsModel).filter_by(job_id=5001).one().invention_cost_unknown_reason
    assert "own invention jobs" not in reason


def test_the_migration_adds_the_reason_column_and_the_choices_table():
    conn = sqlite3.connect(":memory:")
    for table in ("character_industry_jobs", "corporation_industry_jobs"):
        conn.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, job_id INTEGER)")

    class _Db:
        def execute(self, query, params=None):
            conn.execute(query, params or ())
            conn.commit()

        def query(self, query, params=None):
            return conn.execute(query, params or ()).fetchall()

    ensure_app_schema(_Db())
    for table in ("character_industry_jobs", "corporation_industry_jobs"):
        columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        assert "invention_cost_unknown_reason" in columns
    choice_columns = {row[1] for row in conn.execute("PRAGMA table_info(realized_invention_decryptor_choices)")}
    assert choice_columns == {
        "blueprint_type_id", "decryptor_type_id", "decryptor_type_name", "probability_multiplier",
        "me_modifier", "te_modifier", "max_run_modifier", "choice_reason", "set_at",
    }
    marker_columns = {row[1] for row in conn.execute("PRAGMA table_info(realized_invention_decryptor_publish)")}
    assert marker_columns == {"id", "published_at", "choice_count"}


# --- the ME/TE of owned T2 BPCs as decryptor evidence ------------------------------------------
#
# The ME/TE of the owner's T2 BPCs as decryptor evidence.
#
# An invented BPC has ME 2 / TE 4 plus its decryptor's modifiers, and a copy
# cannot be researched, so a BPC of the invented blueprint first seen after an
# invention job finished, with a run count a candidate can give, names the
# decryptor when no other of the owner's invention jobs can have made it. A
# tie is settled only when that evidence leaves exactly one candidate; a BPC
# whose ME/TE fits no candidate (a bought BPC) gives no evidence, with a
# reason. Fake ids and prices only.

#: Same success multiplier as no decryptor, a different ME/TE (Symmetry on the real SDE).
_SYM = DecryptorOption(type_id=990044, type_name="Fake Symmetry", probability_multiplier=1.0,
                       me_modifier=1, te_modifier=8, max_run_modifier=2)
_OPTIONS = (NO_DECRYPTOR, _PLUS_1, _PLUS_2A, _SYM)
_PRICES_SYM = {**_PRICES, _SYM.type_id: 600.0}


def _bpc(item_id=77, *, me=3, te=12, runs=12, seen="2026-10-02T00:00:00Z", blueprint=_TARGET_BP):
    return BlueprintCopySighting(item_id=item_id, blueprint_type_id=blueprint, material_efficiency=me,
                                 time_efficiency=te, runs=runs, first_seen_at=seen)


def _sources(sde, jobs, bpcs, *, options=_OPTIONS, known=None):
    return ap.build_invention_cost_per_run_by_blueprint_type(
        jobs=jobs, sde_session=sde, market_price_map=dict(_PRICES_SYM), invention_refresh=_refresh(options),
        blueprint_copies=bpcs, known_invention_jobs=known,
    )[_TARGET_BP]


def test_the_invented_me_te_of_a_bpc_seen_after_the_job_settles_the_tie(sde):
    source = _sources(sde, [_job()], [_bpc(me=3, te=12, runs=12)])
    assert source["unit_cost_per_run"] == pytest.approx((10 * (_PER_ATTEMPT + 600.0) + 100.0) / (4 * 12))
    assert source["decryptor_from_me_te_evidence_job_ids"] == [1]
    assert source["decryptor_from_runs_evidence_job_ids"] == []


def test_a_base_me_te_bpc_settles_it_as_no_decryptor(sde):
    source = _sources(sde, [_job()], [_bpc(me=2, te=4, runs=7)])
    assert source["unit_cost_per_run"] == pytest.approx((10 * _PER_ATTEMPT + 100.0) / (4 * 10))
    assert source["decryptor_from_me_te_evidence_job_ids"] == [1]


def test_a_bpc_seen_before_the_job_finished_is_no_evidence(sde):
    source = _sources(sde, [_job()], [_bpc(seen="2026-09-30T00:00:00Z")])
    assert source.get("unit_cost_per_run") is None
    assert "no ME/TE evidence" in source["unknown_reason"]


def test_a_bpc_whose_me_te_fits_no_candidate_gives_no_evidence_with_a_reason(sde):
    source = _sources(sde, [_job()], [_bpc(me=10, te=20, runs=5)])
    assert source.get("unit_cost_per_run") is None
    assert "fits no candidate" in source["unknown_reason"]
    assert "ME 10 / TE 20" in source["unknown_reason"]


def test_a_bpc_with_more_runs_than_its_me_te_candidate_gives_is_no_evidence(sde):
    source = _sources(sde, [_job()], [_bpc(me=3, te=12, runs=13)])
    assert source.get("unit_cost_per_run") is None
    assert "fits no candidate" in source["unknown_reason"]


def test_another_bpc_type_or_owner_is_not_considered(sde):
    source = _sources(sde, [_job()], [_bpc(blueprint=990099)])
    assert source.get("unit_cost_per_run") is None


def test_a_bpc_another_invention_job_can_have_made_is_no_evidence(sde):
    """Job 5 (a tie too) finished before the BPC was seen and can have made it."""
    other = _job(5, completed_date="2026-09-25T00:00:00Z")
    source = _sources(sde, [_job(), other], [_bpc()])
    assert source.get("unit_cost_per_run") is None
    assert "another invention job" in source["unknown_reason"]


def test_a_bpc_another_job_cannot_have_made_by_its_me_te_is_still_evidence(sde):
    """Job 5 used Fake +2 A (read from its chance: ME 2 / TE 4), so it cannot
    have made an ME 3 / TE 12 copy."""
    other = _job(5, probability=_chance(_PLUS_2A), completed_date="2026-09-25T00:00:00Z")
    source = _sources(sde, [_job(), other], [_bpc()])
    assert source["decryptor_from_me_te_evidence_job_ids"] == [1]
    assert source["jobs"] == 2


def test_a_stored_invention_job_outside_the_esi_list_counts_as_a_possible_maker(sde):
    stored = _job(5, completed_date="2026-09-25T00:00:00Z")
    source = _sources(sde, [_job()], [_bpc()], known=[{**stored, "raw": dict(stored)}])
    assert source.get("unit_cost_per_run") is None
    assert "another invention job" in source["unknown_reason"]


def test_bpcs_naming_different_candidates_settle_nothing(sde):
    source = _sources(sde, [_job()], [_bpc(77, me=3, te=12, runs=12), _bpc(78, me=2, te=4, runs=10)])
    assert source.get("unit_cost_per_run") is None
    assert "contradict" in source["unknown_reason"]


def test_more_fitting_bpcs_than_the_jobs_successes_are_no_evidence(sde):
    """Four successes make at most four BPCs: a fifth that fits says some
    came from elsewhere (bought, an unstored job), so none is evidence."""
    bpcs = [_bpc(77 + i) for i in range(5)]
    source = _sources(sde, [_job(successful_runs=4)], bpcs)
    assert source.get("unit_cost_per_run") is None
    assert "more than the job's 4 successful run(s)" in source["unknown_reason"]
    assert _sources(sde, [_job(successful_runs=4)], bpcs[:4])["decryptor_from_me_te_evidence_job_ids"] == [1]


def test_candidates_with_the_same_me_te_are_not_settled_by_it(sde):
    """No decryptor and Fake +2 B both give ME 2 / TE 4 (Parity and
    Optimized Attainment on the real SDE)."""
    source = _sources(sde, [_job()], [_bpc(me=2, te=4, runs=9)], options=(NO_DECRYPTOR, _PLUS_1, _PLUS_2A, _PLUS_2B))
    assert source.get("unit_cost_per_run") is None
    assert "fits several" in source["unknown_reason"]


def test_runs_and_me_te_evidence_that_disagree_settle_nothing(sde):
    """A 12-run BPC rules out no decryptor (runs evidence: Symmetry); an
    ME 2 / TE 4 copy says no decryptor."""
    source = _sources(sde, [_job(), _mfg(2, blueprint_id=88, runs=12)], [_bpc(77, me=2, te=4, runs=10)])
    assert source.get("unit_cost_per_run") is None
    assert "disagree" in source["unknown_reason"]


def test_runs_and_me_te_evidence_that_agree_settle_it(sde):
    source = _sources(sde, [_job(), _mfg(2, blueprint_id=88, runs=12)], [_bpc(77, me=3, te=12, runs=12)])
    assert source["unit_cost_per_run"] is not None
    assert source["decryptor_from_runs_evidence_job_ids"] == [1]


def test_without_bpc_data_the_runs_only_behaviour_is_kept(sde):
    source = _sources(sde, [_job()], None)
    assert source.get("unit_cost_per_run") is None
    assert "no runs evidence" in source["unknown_reason"]


# --- the loader ------------------------------------------------------------------


def _hist(model, owner_field, owner, item, *, observed, me=3, te=12, runs=12, type_id=_TARGET_BP, copy=True,
          source="asset_refresh"):
    return model(**{owner_field: owner}, item_id=item, observed_at=observed, snapshot_source=source,
                 type_id=type_id, quantity=1, is_blueprint_copy=copy, blueprint_runs=runs,
                 blueprint_material_efficiency=me, blueprint_time_efficiency=te)


def test_the_loader_reads_the_owners_bpc_sightings(app_session):
    H, f = CharacterAssetHistoryModel, "character_id"
    app_session.add_all([
        _hist(H, f, 7001, 77, observed="2026-10-03T00:00:00+00:00", runs=12),
        _hist(H, f, 7001, 77, observed="2026-10-05T00:00:00+00:00", runs=8),   # used since: runs go down
        _hist(H, f, 7001, 78, observed="2026-10-04T00:00:00+00:00", me=2, te=4, runs=10),
        _hist(H, f, 7001, 79, observed="2026-10-04T00:00:00+00:00", copy=False),          # an original
        _hist(H, f, 7001, 80, observed="2026-10-04T00:00:00+00:00", source="historical_backfill"),
        _hist(H, f, 7002, 81, observed="2026-10-04T00:00:00+00:00"),                      # another owner
        _hist(H, f, 7001, 82, observed="2026-10-04T00:00:00+00:00", me=3),
        _hist(H, f, 7001, 82, observed="2026-10-06T00:00:00+00:00", me=4),               # inconsistent: dropped
    ])
    app_session.add(CharacterIndustryJobsModel(
        character_id=7001, job_id=5, activity_id=8, product_type_id=_TARGET_BP, raw=_job(5)))
    app_session.add(CharacterIndustryJobsModel(
        character_id=7001, job_id=6, activity_id=1, product_type_id=990003, raw={"job_id": 6}))
    # Another owner's invention job: its BPC may have been moved here.
    app_session.add(CorporationIndustryJobsModel(
        corporation_id=98000001, job_id=7, activity_id=8, product_type_id=_TARGET_BP, raw=_job(7)))
    app_session.commit()

    evidence = load_owner_invention_evidence(app_session, owner_kind="character", owner_id=7001)

    bpcs = sorted(evidence["blueprint_copies"], key=lambda b: b.item_id)
    assert [(b.item_id, b.material_efficiency, b.time_efficiency, b.runs, b.first_seen_at[:10]) for b in bpcs] == [
        (77, 3, 12, 12, "2026-10-03"), (78, 2, 4, 10, "2026-10-04"),
    ]
    assert sorted(j["job_id"] for j in evidence["known_invention_jobs"]) == [5, 7]


def test_the_loader_reads_corporation_history(app_session):
    app_session.add(_hist(CorporationAssetHistoryModel, "corporation_id", 98000001, 90,
                          observed="2026-10-03T00:00:00+00:00"))
    app_session.commit()
    evidence = load_owner_invention_evidence(app_session, owner_kind="corporation", owner_id=98000001)
    assert [b.item_id for b in evidence["blueprint_copies"]] == [90]


def test_a_bpc_moved_from_a_pilot_is_first_seen_in_the_pilots_history(app_session):
    app_session.add(_hist(CorporationAssetHistoryModel, "corporation_id", 98000001, 90,
                          observed="2026-10-03T00:00:00+00:00"))
    app_session.add(_hist(CharacterAssetHistoryModel, "character_id", 7001, 90,
                          observed="2026-09-20T00:00:00+00:00"))
    app_session.commit()
    evidence = load_owner_invention_evidence(app_session, owner_kind="corporation", owner_id=98000001)
    assert [b.first_seen_at[:10] for b in evidence["blueprint_copies"]] == ["2026-09-20"]


# --- the paths pass it -----------------------------------------------------------


class _Esi:
    def esi_get(self, endpoint, params=None, paginate=False):
        if endpoint.endswith("/industry/jobs"):
            return [_job()]
        if endpoint.endswith("/markets/prices"):
            return []
        raise AssertionError(endpoint)


def test_the_character_job_refresh_passes_the_owners_bpcs(app_session, monkeypatch):
    app_session.add(_hist(CharacterAssetHistoryModel, "character_id", 7001, 77, observed="2026-10-03T00:00:00+00:00"))
    app_session.commit()
    seen = {}

    def _capture(**kw):
        seen.update(kw)
        return {}

    monkeypatch.setattr(character_module, "build_invention_cost_per_run_by_blueprint_type", _capture)
    monkeypatch.setattr(character_module, "backfill_historical_market_costs", lambda **kw: None)
    character = object.__new__(Character)
    character.character_id = 7001
    character.character_name = "Inventor"
    character._db_app = _Db(app_session)
    character._db_sde = _Db(None)
    character._esi_client = _Esi()
    character._esi_service = None
    character.ensure_esi = lambda: None

    character.refresh_industry_jobs()

    assert [b.item_id for b in seen["blueprint_copies"]] == [77]
    assert "known_invention_jobs" in seen


def test_the_resnapshot_passes_the_owners_bpcs(app_session, monkeypatch):
    app_session.add(_hist(CharacterAssetHistoryModel, "character_id", 7001, 77, observed="2026-10-03T00:00:00+00:00"))
    app_session.add(CharacterIndustryJobsModel(
        character_id=7001, job_id=9, activity_id=1, blueprint_type_id=_TARGET_BP, product_type_id=990003,
        invention_cost_unknown_reason=ap.NO_DECRYPTOR_CHOICE_REASON, raw={"job_id": 9, "activity_id": 1}))
    app_session.commit()
    seen = {}

    def _capture(**kw):
        seen.update(kw)
        return {}

    monkeypatch.setattr(ap, "build_invention_cost_per_run_by_blueprint_type", _capture)
    monkeypatch.setattr(ap, "industry_job_material_type_ids", lambda **kw: [])
    monkeypatch.setattr(ap, "resolve_industry_job_cost_snapshot", lambda **kw: {"unit_cost": None})
    from eve_online_industry_tracker.application.characters import asset_history
    monkeypatch.setattr(asset_history, "build_historical_input_cost_lookup", lambda **kw: {})

    ap.resnapshot_jobs_without_decryptor_choice(
        app_session=app_session, sde_session=None, market_price_map={},
        invention_refresh=ap.RealizedInventionRefresh(inputs=None))

    assert [b.item_id for b in seen["blueprint_copies"]] == [77]


def test_the_corporation_job_refresh_passes_the_corporations_bpcs(app_session, monkeypatch):
    from types import SimpleNamespace

    from eve_online_industry_tracker.application.corporations import corporation as corporation_module
    from eve_online_industry_tracker.application.corporations.corporation import Corporation

    app_session.add(_hist(CorporationAssetHistoryModel, "corporation_id", 98000001, 90,
                          observed="2026-10-03T00:00:00+00:00"))
    app_session.commit()
    seen = {}

    def _capture(**kw):
        seen.update(kw)
        return {}

    monkeypatch.setattr(corporation_module, "build_invention_cost_per_run_by_blueprint_type", _capture)
    monkeypatch.setattr(corporation_module, "backfill_historical_market_costs", lambda **kw: None)
    corp = object.__new__(Corporation)
    corp.corporation_id = 98000001
    corp.corporation_name = "Fake Corp"
    corp._db_app = _Db(app_session)
    corp._db_sde = _Db(None)
    corp._default_esi_character = SimpleNamespace(ensure_esi=lambda: None, _esi_client=_Esi(), esi_service=None)

    corp.refresh_industry_jobs()

    assert [b.item_id for b in seen["blueprint_copies"]] == [90]
