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


def test_only_the_first_publish_re_snapshots(session_provider, sde_engine):
    _seed_sde(sde_engine)
    state = _state()
    service = _service(state, session_provider)
    service.publish_decryptor_choices({990004: DecryptorChoice(NO_DECRYPTOR, "no decryptor beats the others")})
    _store_job_without_a_choice(session_provider)
    service.publish_decryptor_choices({_TARGET_BP: DecryptorChoice(_DECRYPTOR, "chosen")})
    with session_provider.app_session() as session:
        job = session.query(CharacterIndustryJobsModel).filter_by(job_id=5001).one()
        assert "no product overview has planned it yet" in job.invention_cost_unknown_reason


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
