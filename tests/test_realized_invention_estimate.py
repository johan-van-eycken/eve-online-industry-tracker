"""The realized side's invention estimate is sized like the producer's.

When an industry job has no persisted invention-job cost, asset_provenance
estimates the invention inputs one run of the job carried. It used the SDE base
chance and divided by the T2 blueprint's maxProductionLimit. It now uses the
runs an invention yields (the SDE invention product quantity), the producer's
chance model (the corp's best qualifying inventor) and the producer's decryptor
choice. An unknown input gives no estimate and a reason, and feedback then
skips the cost update for that sale.

Fake ids and prices only; the SDE rows are built in memory.
"""
from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from eve_online_industry_tracker.application.characters import asset_provenance as ap
from eve_online_industry_tracker.application.characters.asset_provenance import (
    FifoLot,
    RealizedInventionInputs,
    estimate_invention_cost_per_run,
    resolve_industry_job_cost_snapshot,
)
from eve_online_industry_tracker.application.characters.realized_profit import _consume_lots
from eve_online_industry_tracker.application.daily_planner.feedback_processor import _industry_build_unit_cost
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
_SCIENCE_A = 990031
_SCIENCE_B = 990032
_ENCRYPTION = 990033
_DECRYPTOR = 990041
_PILOT = 7001

_PRICES = {_DATACORE_A: 100.0, _DATACORE_B: 50.0, _MINERAL: 5.0, _DECRYPTOR: 300.0}
#: One attempt: 2 x 100 + 3 x 50.
_PER_ATTEMPT = 350.0
_BASE_CHANCE = 0.34
#: Skills 4/4/4: 1 + 8/30 + 4/40.
_SKILL_FACTOR = 1.0 + 8.0 / 30.0 + 4.0 / 40.0


def _seed(engine, *, runs_per_invention: int, max_production_limit: int, probability: float = _BASE_CHANCE) -> None:
    with Session(engine) as session:
        session.add(Groups(id=270, name={"en": "Science"}, categoryID=16, published=True))
        session.add(Groups(id=990900, name={"en": "Fake Group"}, categoryID=990, published=True))
        for type_id, name, group in (
            (_SOURCE_BP, "Fake T1 Blueprint", 990900), (_TARGET_BP, "Fake T2 Blueprint", 990900),
            (_PRODUCT, "Fake T2 Item", 990900), (_DATACORE_A, "Fake Datacore A", 990900),
            (_DATACORE_B, "Fake Datacore B", 990900), (_MINERAL, "Fake Mineral", 990900),
            (_SCIENCE_A, "Fake Science A", 270), (_SCIENCE_B, "Fake Science B", 270),
            (_ENCRYPTION, "Fake Encryption Methods", 270), (_DECRYPTOR, "Fake Decryptor", 990900),
        ):
            session.add(Types(id=type_id, name={"en": name}, groupID=group, published=True))
        session.add(Blueprints(
            id=1, blueprintTypeID=_SOURCE_BP, maxProductionLimit=300,
            activities={"invention": {
                "time": 1000,
                "materials": [{"typeID": _DATACORE_A, "quantity": 2}, {"typeID": _DATACORE_B, "quantity": 3}],
                "products": [{"typeID": _TARGET_BP, "quantity": runs_per_invention, "probability": probability}],
                "skills": [
                    {"typeID": _SCIENCE_A, "level": 1}, {"typeID": _SCIENCE_B, "level": 1},
                    {"typeID": _ENCRYPTION, "level": 1},
                ],
            }},
        ))
        session.add(Blueprints(
            id=2, blueprintTypeID=_TARGET_BP, maxProductionLimit=max_production_limit,
            activities={"manufacturing": {
                "time": 600,
                "materials": [{"typeID": _MINERAL, "quantity": 10}],
                "products": [{"typeID": _PRODUCT, "quantity": 1}],
            }},
        ))
        session.commit()


def _context(levels: dict[int, dict[int, int]] | None = None) -> InventionContext:
    if levels is None:
        levels = {_PILOT: {_SCIENCE_A: 4, _SCIENCE_B: 4, _ENCRYPTION: 4}}
    return InventionContext(
        inventor_skill_levels_by_character_id=levels,
        encryption_skill_type_ids=frozenset({_ENCRYPTION}),
    )


def _inputs(*, levels=None, decryptors=None, default=NO_DECRYPTOR, floor: float = 0.01) -> RealizedInventionInputs:
    return RealizedInventionInputs(
        invention_context=_context(levels),
        decryptor_by_blueprint_type_id=decryptors or {},
        default_decryptor=default,
        probability_floor=floor,
    )


def _estimate(engine, inputs, prices=None):
    with Session(engine) as session:
        return estimate_invention_cost_per_run(
            sde_session=session, target_blueprint_type_id=_TARGET_BP,
            market_price_map=dict(_PRICES if prices is None else prices), invention_inputs=inputs,
        )


def test_a_capital_t2_module_divides_by_the_5_runs_an_invention_yields_not_the_limit_of_10(sde_engine):
    _seed(sde_engine, runs_per_invention=5, max_production_limit=10)
    estimate, reason = _estimate(sde_engine, _inputs())
    assert reason is None
    assert estimate["runs_per_invented_blueprint"] == 5
    assert estimate["probability"] == pytest.approx(_BASE_CHANCE * _SKILL_FACTOR)
    assert estimate["inventor_character_id"] == _PILOT
    # Before: 350 / 0.34 / 10 = 102.94 per run. Now 350 / (0.4647 x 5) = 150.64.
    assert estimate["cost_per_run"] == pytest.approx(_PER_ATTEMPT / (_BASE_CHANCE * _SKILL_FACTOR * 5))
    assert estimate["cost_per_run"] == pytest.approx(150.64, abs=0.01)


def test_a_t3_subsystem_divides_by_the_20_runs_it_yields_not_the_limit_of_300(sde_engine):
    _seed(sde_engine, runs_per_invention=20, max_production_limit=300, probability=0.14)
    estimate, reason = _estimate(sde_engine, _inputs())
    assert reason is None
    assert estimate["runs_per_invented_blueprint"] == 20
    # Before: 350 / 0.14 / 300 = 8.33 per run. Now 350 / (0.14 x 1.3667 x 20) = 91.46.
    assert estimate["cost_per_run"] == pytest.approx(_PER_ATTEMPT / (0.14 * _SKILL_FACTOR * 20))
    assert estimate["cost_per_run"] == pytest.approx(91.46, abs=0.01)


def test_the_producers_decryptor_adds_its_runs_odds_and_one_per_attempt(sde_engine):
    _seed(sde_engine, runs_per_invention=10, max_production_limit=10)
    decryptor = DecryptorOption(
        type_id=_DECRYPTOR, type_name="Fake Decryptor", probability_multiplier=1.2, max_run_modifier=1,
    )
    estimate, reason = _estimate(sde_engine, _inputs(decryptors={_TARGET_BP: decryptor}, default=None))
    assert reason is None
    assert estimate["decryptor_type_id"] == _DECRYPTOR
    assert estimate["runs_per_invented_blueprint"] == 11
    assert estimate["cost_per_run"] == pytest.approx((_PER_ATTEMPT + 300.0) / (_BASE_CHANCE * _SKILL_FACTOR * 1.2 * 11))


def test_a_normal_t2_item_realized_and_predicted_agree_for_the_same_inputs(sde_engine):
    """The producer's _expected_invention_costs for one invented BPC of 10
    runs, priced at the same map, costs the same per run as the estimate."""
    from types import SimpleNamespace

    from eve_online_industry_tracker.application.industry.job_manager import IndustryJobManager
    from eve_online_industry_tracker.application.industry.service import IndustryService
    from eve_online_industry_tracker.infrastructure.sde.blueprints import get_blueprint_manufacturing_data

    _seed(sde_engine, runs_per_invention=10, max_production_limit=10)
    inputs = _inputs()
    estimate, reason = _estimate(sde_engine, inputs)
    assert reason is None

    with Session(sde_engine) as session:
        raw = get_blueprint_manufacturing_data(session, "en", [_SOURCE_BP, _TARGET_BP])
    rows = {int(r["blueprint_type_id"]): r for r in IndustryJobManager._build_blueprint_overview_rows(raw)}
    invention_job = rows[_SOURCE_BP]["invention_job"]
    service = IndustryService(state=SimpleNamespace(admin_settings=None))
    costs, materials = service._expected_invention_costs(
        compute_activity=lambda **_: {"duration_seconds": 0, "total_job_cost": 0.0},
        invention_job=invention_job,
        target_entry=IndustryService._invention_target_entry(invention_job, _TARGET_BP),
        blueprint_type_id=_TARGET_BP, invented_runs=10, max_production_limit=10,
        adjusted_price_map={}, decryptor=NO_DECRYPTOR, invention_context=inputs.invention_context,
        invented_blueprint_eiv=None,
    )
    predicted_per_run = sum(float(m["quantity"]) * _PRICES[int(m["type_id"])] for m in materials) / 10.0
    assert costs["success_probability"] == pytest.approx(estimate["probability"])
    assert estimate["cost_per_run"] == pytest.approx(predicted_per_run)


def test_the_snapshot_carries_the_estimate_for_the_jobs_runs(sde_engine):
    _seed(sde_engine, runs_per_invention=5, max_production_limit=10)
    job = SimpleJob(blueprint_type_id=_TARGET_BP, product_type_id=_PRODUCT, runs=4, cost=40.0)
    with Session(sde_engine) as session:
        snapshot = resolve_industry_job_cost_snapshot(
            job=job, sde_session=session, market_price_map=dict(_PRICES), invention_inputs=_inputs(),
        )
    per_run = _PER_ATTEMPT / (_BASE_CHANCE * _SKILL_FACTOR * 5)
    assert snapshot["invention_cost"] == pytest.approx(per_run * 4)
    assert snapshot["invention_cost_unknown_reason"] is None
    assert snapshot["total_cost"] == pytest.approx(4 * 10 * 5.0 + 40.0 + per_run * 4)


class SimpleJob:
    def __init__(self, **fields):
        self.raw = {"activity_id": 1, "runs": fields.get("runs")}
        for key, value in fields.items():
            setattr(self, key, value)


# --- unknown inputs: no estimate, a reason, and feedback skips the cost update


@pytest.mark.parametrize("inputs, expected", [
    (None, "no inventor skills or decryptor choice"),
    ("no_skills", "no corp character has its skills loaded"),
    ("no_decryptor_choice", "no decryptor choice"),
])
def test_an_unknown_input_gives_no_estimate_and_a_reason(sde_engine, inputs, expected):
    _seed(sde_engine, runs_per_invention=10, max_production_limit=10)
    resolved = {
        None: None,
        "no_skills": _inputs(levels={}),
        "no_decryptor_choice": _inputs(default=None),
    }[inputs]
    estimate, reason = _estimate(sde_engine, resolved)
    assert estimate is None
    assert expected in reason


def test_an_unpriced_datacore_gives_no_estimate_never_a_partial_sum(sde_engine):
    _seed(sde_engine, runs_per_invention=10, max_production_limit=10)
    prices = {k: v for k, v in _PRICES.items() if k != _DATACORE_B}
    estimate, reason = _estimate(sde_engine, _inputs(), prices=prices)
    assert estimate is None
    assert str(_DATACORE_B) in reason


def test_a_blueprint_nothing_invents_needs_no_estimate_and_no_reason(sde_engine):
    _seed(sde_engine, runs_per_invention=10, max_production_limit=10)
    with Session(sde_engine) as session:
        assert estimate_invention_cost_per_run(
            sde_session=session, target_blueprint_type_id=_SOURCE_BP,
            market_price_map=dict(_PRICES), invention_inputs=_inputs(),
        ) == (None, None)


def test_the_snapshot_names_the_unknown_invention_cost(sde_engine, caplog):
    _seed(sde_engine, runs_per_invention=10, max_production_limit=10)
    job = SimpleJob(blueprint_type_id=_TARGET_BP, product_type_id=_PRODUCT, runs=2, cost=40.0)
    with Session(sde_engine) as session, caplog.at_level("INFO"):
        snapshot = resolve_industry_job_cost_snapshot(
            job=job, sde_session=session, market_price_map=dict(_PRICES), invention_inputs=_inputs(levels={}),
        )
    assert snapshot["invention_cost"] is None
    assert "no corp character has its skills loaded" in snapshot["invention_cost_unknown_reason"]
    assert any("invention cost unknown" in r.getMessage() for r in caplog.records)


def test_a_persisted_unknown_reason_survives_the_persisted_snapshot_path():
    job = SimpleJob(
        blueprint_type_id=_TARGET_BP, product_type_id=_PRODUCT, runs=2, cost=40.0,
        output_quantity=2, total_build_cost=140.0, unit_build_cost=70.0,
        invention_cost_unknown_reason="no corp character has its skills loaded",
    )
    snapshot = resolve_industry_job_cost_snapshot(job=job, sde_session=None, market_price_map={})
    assert snapshot["unit_cost"] == 70.0
    assert snapshot["invention_cost_unknown_reason"] == "no corp character has its skills loaded"


def test_feedback_skips_the_cost_update_when_a_build_lot_has_an_unknown_invention_cost():
    lots = [FifoLot(
        quantity=2, unit_price=70.0, source="industry_build",
        cost_unknown_reason="invention cost unknown: no corp character has its skills loaded",
    )]
    allocation = _consume_lots(lots, quantity=2)["allocations"]
    assert allocation[0]["cost_unknown_reason"].startswith("invention cost unknown")
    cost, reason = _industry_build_unit_cost(allocation)
    assert cost is None
    assert "invention cost unknown" in reason


def test_a_known_build_lot_still_gives_its_cost():
    allocation = _consume_lots([FifoLot(quantity=2, unit_price=70.0, source="industry_build")], quantity=2)["allocations"]
    assert "cost_unknown_reason" not in allocation[0]
    assert _industry_build_unit_cost(allocation) == (70.0, None)


def test_without_a_provider_the_snapshot_has_no_inputs(monkeypatch):
    monkeypatch.setattr(ap, "_REALIZED_INVENTION_INPUTS_PROVIDER", None)
    inputs, reason = ap.current_realized_invention_inputs()
    assert inputs is None
    assert "no inventor skills or decryptor choice" in reason
