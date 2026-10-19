from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
import logging
import math
from typing import Any, Callable, Iterable, Mapping, Optional

from sqlalchemy import desc

from eve_online_industry_tracker.application.industry.invention_model import (
    NO_DECRYPTOR,
    DecryptorOption,
    InventionContext,
    decryptor_options_from_rows,
    effective_invention_probability,
    invented_runs_per_blueprint,
    invention_attempts,
    invention_skill_set,
    required_invention_skill_levels,
    resolve_invention_chance,
)
from eve_online_industry_tracker.infrastructure.models import (
    Blueprints,
    CharacterIndustryJobsModel,
    CorporationIndustryJobsModel,
    MarketHistoryModel,
    RealizedInventionDecryptorChoiceModel,
    RealizedInventionDecryptorPublishModel,
)
from eve_online_industry_tracker.infrastructure.sde.blueprints import (
    get_blueprint_manufacturing_data,
    get_invention_source_blueprint_ids,
)
from eve_online_industry_tracker.infrastructure.sde.decryptors import get_t2_invention_decryptors
from eve_online_industry_tracker.infrastructure.sde.skills import get_encryption_skill_type_ids


ASSET_SOURCE_INDUSTRY_BUILD = "industry_build"
ASSET_SOURCE_MARKET_BUY = "market_buy"
ASSET_SOURCE_UNKNOWN = "unknown"

REFERENCE_TYPE_INDUSTRY_JOB = "industry_job"
REFERENCE_TYPE_WALLET_TRANSACTION = "wallet_transaction"

@dataclass(frozen=True)
class CostInfo:
    source: str
    unit_cost: Optional[float]
    total_cost: Optional[float]
    reference_type: Optional[str]
    reference_id: Optional[int]
    acquisition_date: Optional[str]


@dataclass(frozen=True)
class FifoLot:
    quantity: int
    unit_price: float
    acquisition_date: Optional[str] = None
    reference_id: Optional[int] = None
    reference_type: Optional[str] = None
    source: Optional[str] = None
    #: Set when the lot's unit price is a lower bound because part of its
    #: build cost is unknown (an invention estimate that had no inputs).
    #: Feedback then skips the cost update for a sale that consumes it.
    cost_unknown_reason: Optional[str] = None


def _parse_date(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    # ESI timestamps are usually like: 2023-01-01T12:34:56Z
    # datetime.fromisoformat doesn't accept 'Z', so normalize.
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _industry_job_runs(job: Any) -> int:
    runs = _safe_int(getattr(job, "successful_runs", None)) or _safe_int(getattr(job, "runs", None)) or 1
    return max(1, int(runs))


def _industry_job_raw_payload(job: Any) -> dict[str, Any]:
    raw = getattr(job, "raw", None)
    return raw if isinstance(raw, dict) else {}


def _coalesce_float(payload: dict[str, Any], *keys: str) -> Optional[float]:
    for key in keys:
        value = _safe_float(payload.get(key))
        if value is not None:
            return value
    return None


def _coalesce_int(payload: dict[str, Any], *keys: str) -> Optional[int]:
    for key in keys:
        value = _safe_int(payload.get(key))
        if value is not None:
            return value
    return None


def _round_material_quantity(raw_quantity: float, *, minimum_quantity: int) -> int:
    if raw_quantity <= 0:
        return 0
    return max(int(minimum_quantity), int(math.ceil(raw_quantity)))


def build_market_price_map(rows: Iterable[dict[str, Any]] | None) -> dict[int, float]:
    price_map: dict[int, float] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        type_id = _safe_int(row.get("type_id"))
        if not type_id:
            continue
        average_price = _safe_float(row.get("average_price"))
        adjusted_price = _safe_float(row.get("adjusted_price"))
        candidates = [price for price in [average_price, adjusted_price] if price is not None and price > 0]
        if not candidates:
            continue
        price_map[int(type_id)] = float(min(candidates))
    return price_map


def _output_quantity_per_run_for_job(*, sde_session: Any, blueprint_type_id: int, product_type_id: int) -> Optional[int]:
    if not blueprint_type_id or not product_type_id:
        return None
    bp = sde_session.query(Blueprints).filter_by(blueprintTypeID=int(blueprint_type_id)).first()
    if bp is None:
        return None
    mfg = _get_mfg_activity(getattr(bp, "activities", None))
    if not mfg:
        return None
    products = mfg.get("products")
    if not isinstance(products, list) or not products:
        return None
    for p in products:
        if not isinstance(p, dict):
            continue
        if _safe_int(p.get("typeID")) == int(product_type_id):
            quantity = _safe_int(p.get("quantity"))
            return quantity if quantity and quantity > 0 else None
    first = products[0]
    if not isinstance(first, dict):
        return None
    quantity = _safe_int(first.get("quantity"))
    return quantity if quantity and quantity > 0 else None


def _estimate_industry_job_materials_cost_total(
    *,
    sde_session: Any,
    blueprint_type_id: int,
    product_type_id: int,
    runs: int,
    market_price_map: dict[int, float],
    blueprint_material_efficiency: int | None = None,
    owned_input_unit_cost_by_type_id: dict[int, dict[str, Any]] | None = None,
) -> Optional[dict[str, Any]]:
    def _consume_historical_lots(
        *,
        cost_payload: dict[str, Any],
        required_quantity: int,
    ) -> tuple[float, int, list[dict[str, Any]]]:
        lots = cost_payload.get("lots") or []
        if not isinstance(lots, list) or required_quantity <= 0:
            return 0.0, 0, []

        remaining_quantity = int(required_quantity)
        historical_cost_total = 0.0
        consumed_quantity = 0
        consumed_lots: list[dict[str, Any]] = []
        for raw_lot in lots:
            if remaining_quantity <= 0 or not isinstance(raw_lot, dict):
                break
            available_quantity = _safe_int(raw_lot.get("quantity")) or 0
            unit_cost = _safe_float(raw_lot.get("unit_cost"))
            if available_quantity <= 0 or unit_cost is None or unit_cost <= 0:
                continue
            take_quantity = min(int(remaining_quantity), int(available_quantity))
            if take_quantity <= 0:
                continue
            remaining_quantity -= int(take_quantity)
            consumed_quantity += int(take_quantity)
            historical_cost_total += float(unit_cost) * float(take_quantity)
            consumed_lots.append(
                {
                    "quantity": int(take_quantity),
                    "unit_cost": float(unit_cost),
                    "total_cost": float(unit_cost) * float(take_quantity),
                    "source": raw_lot.get("source"),
                    "reference_type": raw_lot.get("reference_type"),
                    "reference_id": raw_lot.get("reference_id"),
                    "history_id": raw_lot.get("history_id"),
                    "observed_at": raw_lot.get("observed_at"),
                }
            )
        return historical_cost_total, consumed_quantity, consumed_lots

    if not blueprint_type_id or not product_type_id:
        return None
    if runs <= 0:
        runs = 1

    bp = sde_session.query(Blueprints).filter_by(blueprintTypeID=int(blueprint_type_id)).first()
    if bp is None:
        return None

    mfg = _get_mfg_activity(getattr(bp, "activities", None))
    if not mfg:
        return None

    materials = mfg.get("materials")
    products = mfg.get("products")
    if not isinstance(materials, list) or not isinstance(products, list):
        return None

    output_qty_per_run = _output_quantity_per_run_for_job(
        sde_session=sde_session,
        blueprint_type_id=int(blueprint_type_id),
        product_type_id=int(product_type_id),
    )
    if not output_qty_per_run or output_qty_per_run <= 0:
        return None

    material_reduction = 0.0
    if blueprint_material_efficiency is not None:
        material_reduction = max(0.0, min(float(blueprint_material_efficiency) / 100.0, 0.99))

    material_cost_total = 0.0
    historical_material_cost_total = 0.0
    total_adjusted_quantity = 0
    historical_adjusted_quantity = 0
    input_cost_details: dict[str, Any] = {}
    for m in materials:
        if not isinstance(m, dict):
            continue
        mat_type_id = _safe_int(m.get("typeID"))
        qty = _safe_int(m.get("quantity"))
        if not mat_type_id or not qty or qty <= 0:
            continue
        cost_payload = (owned_input_unit_cost_by_type_id or {}).get(int(mat_type_id)) or {}
        base_total_quantity = int(qty) * int(runs)
        adjusted_total_quantity = _round_material_quantity(
            float(base_total_quantity) * max(0.0, 1.0 - material_reduction),
            minimum_quantity=(int(runs) if int(qty) > 0 else 0),
        )
        total_adjusted_quantity += int(adjusted_total_quantity)
        historical_cost_total, historical_quantity, consumed_lots = _consume_historical_lots(
            cost_payload=cost_payload,
            required_quantity=int(adjusted_total_quantity),
        )
        if historical_quantity > 0:
            historical_adjusted_quantity += int(historical_quantity)
            historical_material_cost_total += float(historical_cost_total)
            weighted_unit_cost = float(historical_cost_total) / float(historical_quantity)
            input_cost_details[str(int(mat_type_id))] = {
                "unit_cost": float(weighted_unit_cost),
                "quantity": int(historical_quantity),
                "source": "historical_asset_acquisition_cost",
                "reference_type": (consumed_lots[-1].get("reference_type") if consumed_lots else cost_payload.get("reference_type")),
                "reference_id": (consumed_lots[-1].get("reference_id") if consumed_lots else cost_payload.get("reference_id")),
                "history_id": (consumed_lots[-1].get("history_id") if consumed_lots else cost_payload.get("history_id")),
                "observed_at": (consumed_lots[-1].get("observed_at") if consumed_lots else cost_payload.get("observed_at")),
                "lots": consumed_lots,
            }
        material_cost_total += float(historical_cost_total)

        remaining_quantity = int(adjusted_total_quantity) - int(historical_quantity)
        if remaining_quantity <= 0:
            continue

        unit_price = market_price_map.get(int(mat_type_id))
        if unit_price is None:
            average_historical_unit_cost = _safe_float(cost_payload.get("unit_cost"))
            if average_historical_unit_cost is None or average_historical_unit_cost <= 0:
                continue
            unit_price = float(average_historical_unit_cost)
        material_cost_total += float(remaining_quantity) * float(unit_price)

    coverage_fraction = (
        float(historical_adjusted_quantity) / float(total_adjusted_quantity)
        if total_adjusted_quantity > 0
        else 0.0
    )
    if historical_adjusted_quantity > 0 and historical_adjusted_quantity == total_adjusted_quantity:
        material_cost_source = "historical_asset_acquisition_cost"
    elif historical_adjusted_quantity > 0:
        material_cost_source = "mixed_historical_and_market_estimate"
    else:
        material_cost_source = "market_snapshot_estimate"

    return {
        "materials_cost": float(material_cost_total),
        "output_quantity": int(output_qty_per_run) * int(runs),
        "historical_materials_cost": (float(historical_material_cost_total) if historical_adjusted_quantity > 0 else None),
        "historical_material_coverage_fraction": float(coverage_fraction),
        "historical_input_costs": input_cost_details,
        "material_cost_source": material_cost_source,
    }


def industry_job_material_type_ids(
    *,
    sde_session: Any,
    blueprint_type_id: int,
) -> list[int]:
    if not blueprint_type_id:
        return []
    bp = sde_session.query(Blueprints).filter_by(blueprintTypeID=int(blueprint_type_id)).first()
    if bp is None:
        return []
    mfg = _get_mfg_activity(getattr(bp, "activities", None))
    if not mfg:
        return []
    out: list[int] = []
    for material in mfg.get("materials") or []:
        if not isinstance(material, dict):
            continue
        type_id = _safe_int(material.get("typeID"))
        if type_id is not None:
            out.append(int(type_id))
    return out


#: Why there is no RealizedInventionInputs when no provider is registered.
NO_REALIZED_INVENTION_INPUTS_REASON = (
    "no inventor skills or decryptor choice available (the app has not registered the producer's invention inputs)"
)

#: The start of the reason a job's invention cost is unknown for want of the
#: producer's decryptor; the first overview of a process re-snapshots them
#: (resnapshot_jobs_without_decryptor_choice).
NO_DECRYPTOR_CHOICE_REASON = "no decryptor choice"

#: The ESI status of a finished invention job whose successful_runs is final.
_DELIVERED_STATUS = "delivered"


@dataclass(frozen=True)
class RealizedInventionInputs:
    """What the realized side needs to size an invention like the producer.

    `invention_context` carries the corp's inventors (skills by character)
    and the SDE encryption skills, the same InventionContext the producer's
    resolve_invention_chance reads. The decryptor is the producer's choice
    for the invented blueprint (`decryptor_by_blueprint_type_id`, from the
    last overview), or `default_decryptor` for every blueprint when the
    producer does not choose (decryptor selection off: NO_DECRYPTOR). None
    for both: no choice known, so no estimate.

    `decryptor_choices_published`: an overview has published its choices
    (in memory, or persisted before a restart, the publish marker), so a
    blueprint missing from them is one the overview does not plan, not one
    it has not reached yet; published and empty: it planned no invention."""

    invention_context: InventionContext
    decryptor_by_blueprint_type_id: Mapping[int, DecryptorOption]
    default_decryptor: DecryptorOption | None
    probability_floor: float
    decryptor_choices_published: bool = False

    def decryptor_for(self, blueprint_type_id: int) -> DecryptorOption | None:
        return self.decryptor_by_blueprint_type_id.get(int(blueprint_type_id), self.default_decryptor)


_REALIZED_INVENTION_INPUTS_PROVIDER: Callable[[], RealizedInventionInputs | None] | None = None


def set_realized_invention_inputs_provider(provider: Callable[[], RealizedInventionInputs | None] | None) -> None:
    """Register how the realized side gets the producer's invention inputs
    (flask bootstrap: IndustryService.realized_invention_inputs). None clears it."""
    global _REALIZED_INVENTION_INPUTS_PROVIDER
    _REALIZED_INVENTION_INPUTS_PROVIDER = provider


def current_realized_invention_inputs() -> tuple[RealizedInventionInputs | None, str | None]:
    """(inputs, None), or (None, why) when no provider is registered or it
    returns nothing."""
    provider = _REALIZED_INVENTION_INPUTS_PROVIDER
    try:
        inputs = provider() if provider is not None else None
    except Exception as exc:  # the snapshot goes on without an estimate, and says why
        logging.warning("Realized invention inputs could not be read: %s", exc)
        return None, f"the producer's invention inputs could not be read ({exc})"
    if inputs is None:
        return None, NO_REALIZED_INVENTION_INPUTS_REASON
    return inputs, None


@dataclass
class RealizedInventionRefresh:
    """One job refresh's view of invention: the producer's inputs read once
    (with why they are missing, passed down to every snapshot), the SDE
    invention activities memoized per invented blueprint, and the SDE
    decryptors (loaded on first use). Not shared across refreshes."""

    inputs: RealizedInventionInputs | None
    inputs_unknown_reason: str | None = None
    #: (NO_DECRYPTOR, the SDE's T2 decryptors); None: loaded on first use.
    decryptor_options: tuple[DecryptorOption, ...] | None = None
    source_activity_by_target: dict[int, tuple[int | None, Mapping[str, Any] | None]] = field(default_factory=dict)
    #: The SDE encryption skills (the producer's, when its inputs are read);
    #: None: loaded on first use.
    encryption_skill_type_ids: frozenset[int] | None = None

    @classmethod
    def read(cls) -> "RealizedInventionRefresh":
        inputs, reason = current_realized_invention_inputs()
        return cls(inputs=inputs, inputs_unknown_reason=reason)

    def decryptors(self, sde_session: Any) -> tuple[DecryptorOption, ...]:
        if self.decryptor_options is None:
            rows = get_t2_invention_decryptors(sde_session, language="en") if sde_session is not None else []
            self.decryptor_options = decryptor_options_from_rows(rows)
        return self.decryptor_options

    def encryption_skills(self, sde_session: Any) -> frozenset[int]:
        if self.encryption_skill_type_ids is None:
            known = self.inputs.invention_context.encryption_skill_type_ids if self.inputs is not None else None
            if known:
                self.encryption_skill_type_ids = frozenset(known)
            else:
                self.encryption_skill_type_ids = (
                    get_encryption_skill_type_ids(sde_session) if sde_session is not None else frozenset()
                )
        return self.encryption_skill_type_ids

    def overview_decryptor_for(self, blueprint_type_id: int) -> DecryptorOption | None:
        """The decryptor the published overview invents this blueprint with
        (in memory or persisted); None when it plans none for it. Not the
        "decryptor selection off" default: that is no per-blueprint choice."""
        if self.inputs is None:
            return None
        return self.inputs.decryptor_by_blueprint_type_id.get(int(blueprint_type_id))


def save_published_decryptor_choices(
    app_session: Any, choices: Mapping[int, tuple[DecryptorOption, str | None]], *, now: datetime,
) -> None:
    """Replace the persisted decryptor choices with `choices` ({invented
    blueprint: (decryptor, the producer's reason)}), the last published
    overview's, and the publish marker (also for no choices: an overview
    that planned no invention). Commits; a database error propagates."""
    app_session.query(RealizedInventionDecryptorChoiceModel).delete(synchronize_session=False)
    app_session.query(RealizedInventionDecryptorPublishModel).delete(synchronize_session=False)
    app_session.add(RealizedInventionDecryptorPublishModel(id=1, published_at=now, choice_count=len(choices)))
    for blueprint_type_id, (option, reason) in sorted(choices.items()):
        app_session.add(RealizedInventionDecryptorChoiceModel(
            blueprint_type_id=int(blueprint_type_id),
            decryptor_type_id=int(option.type_id) if option.type_id is not None else None,
            decryptor_type_name=str(option.type_name),
            probability_multiplier=float(option.probability_multiplier),
            me_modifier=int(option.me_modifier),
            te_modifier=int(option.te_modifier),
            max_run_modifier=int(option.max_run_modifier),
            choice_reason=reason,
            set_at=now,
        ))
    app_session.commit()


def load_published_decryptor_choices(app_session: Any) -> dict[int, DecryptorOption] | None:
    """The persisted decryptor choices: {} after a publish that planned no
    invention (the publish marker without rows), None when nothing was ever
    published (no marker and no rows; rows without a marker were stored
    before the marker existed and are a publish). A database error
    propagates."""
    rows = app_session.query(RealizedInventionDecryptorChoiceModel).all()
    if not rows:
        published = app_session.query(RealizedInventionDecryptorPublishModel.id).first() is not None
        return {} if published else None
    return {
        int(row.blueprint_type_id): (
            NO_DECRYPTOR if row.decryptor_type_id is None else DecryptorOption(
                type_id=int(row.decryptor_type_id),
                type_name=str(row.decryptor_type_name),
                probability_multiplier=float(row.probability_multiplier),
                me_modifier=int(row.me_modifier),
                te_modifier=int(row.te_modifier),
                max_run_modifier=int(row.max_run_modifier),
            )
        )
        for row in rows
    }


def resnapshot_jobs_without_decryptor_choice(
    *,
    app_session: Any,
    sde_session: Any,
    market_price_map: Mapping[int, float],
    invention_refresh: RealizedInventionRefresh,
) -> int:
    """Re-snapshot the stored manufacturing jobs whose invention cost was
    unknown for want of the producer's decryptor (NO_DECRYPTOR_CHOICE_REASON),
    with `invention_refresh`'s inputs: a job refresh that ran before the
    first overview of the process (the startup refresh) stored them so. The
    job's build cost and its historical acquisition are updated; the job
    keeps a reason when the estimate is still unknown (for a blueprint the
    overview does not plan: "this blueprint is not planned by the overview").
    The actual-invention source is re-evaluated too, from the owner's stored
    invention jobs with this publish's choices (they may now settle an
    ambiguous decryptor), so a reason naming both sources keeps both.
    Commits; returns the number of jobs re-snapshotted."""
    from eve_online_industry_tracker.application.characters.asset_history import (
        _owner_models,
        build_historical_input_cost_lookup,
    )

    count = 0
    for owner_kind, model, owner_field in (
        ("character", CharacterIndustryJobsModel, "character_id"),
        ("corporation", CorporationIndustryJobsModel, "corporation_id"),
    ):
        _, history_model, _, _ = _owner_models(owner_kind)
        invention_sources_by_owner: dict[int, dict[int, dict[str, Any]]] = {}

        def _invention_sources(owner_id: int) -> dict[int, dict[str, Any]]:
            """The owner's actual-invention sources from its stored invention
            jobs, as its job refresh reads them from ESI, with this
            publish's decryptor choices."""
            if owner_id not in invention_sources_by_owner:
                stored = (
                    app_session.query(model.raw)
                    .filter(getattr(model, owner_field) == owner_id)
                    .filter(model.activity_id == 8)
                    .all()
                )
                invention_sources_by_owner[owner_id] = build_invention_cost_per_run_by_blueprint_type(
                    jobs=[dict(raw) for (raw,) in stored if isinstance(raw, dict)],
                    sde_session=sde_session,
                    market_price_map=dict(market_price_map),
                    invention_refresh=invention_refresh,
                )
            return invention_sources_by_owner[owner_id]

        rows = (
            app_session.query(model)
            .filter(model.invention_cost_unknown_reason.like(f"%{NO_DECRYPTOR_CHOICE_REASON}%"))
            .all()
        )
        for row in rows:
            raw = row.raw if isinstance(row.raw, dict) else None
            if raw is None:
                logging.warning(
                    "Industry job %s: no stored ESI payload, so it cannot be re-snapshotted with the "
                    "overview's decryptor; it keeps its reason until the next job refresh", row.job_id,
                )
                continue
            owner_id = int(getattr(row, owner_field))
            completed_at = row.completed_date or row.end_date
            owned_costs = build_historical_input_cost_lookup(
                app_session=app_session, owner_kind=owner_kind, owner_id=owner_id, as_of=completed_at,
                type_ids=industry_job_material_type_ids(
                    sde_session=sde_session, blueprint_type_id=int(row.blueprint_type_id or 0),
                ),
            )
            snapshot = resolve_industry_job_cost_snapshot(
                job=type("IndustryJobPayload", (), dict(raw))(),
                sde_session=sde_session,
                market_price_map=dict(market_price_map),
                blueprint_provenance={
                    "item_id": row.blueprint_item_id,
                    "is_blueprint_copy": row.blueprint_is_blueprint_copy,
                    "blueprint_runs": row.blueprint_runs,
                    "blueprint_time_efficiency": row.blueprint_time_efficiency,
                    "blueprint_material_efficiency": row.blueprint_material_efficiency,
                    "source": row.blueprint_provenance_source,
                    "reference_id": row.blueprint_provenance_ref_id,
                },
                owned_input_unit_cost_by_type_id=owned_costs,
                invention_refresh=invention_refresh,
                **invention_cost_source_for(_invention_sources(owner_id), row.blueprint_type_id),
            )
            row.output_quantity = snapshot.get("output_quantity")
            row.materials_cost = snapshot.get("materials_cost")
            row.historical_materials_cost = snapshot.get("historical_materials_cost")
            row.historical_material_cost_source = snapshot.get("historical_material_cost_source")
            row.historical_material_coverage_fraction = snapshot.get("historical_material_coverage_fraction")
            row.historical_input_costs = snapshot.get("historical_input_costs")
            # The materials cost is replaced: a backfill's lower bound no longer applies.
            row.materials_cost_lower_bound_reason = None
            row.invention_cost = snapshot.get("invention_cost")
            row.invention_cost_unknown_reason = snapshot.get("invention_cost_unknown_reason")
            row.total_build_cost = snapshot.get("total_cost")
            row.unit_build_cost = snapshot.get("unit_cost")
            row.build_cost_source = snapshot.get("source")
            unit_cost = _safe_float(snapshot.get("unit_cost"))
            output_quantity = _safe_int(snapshot.get("output_quantity"))
            if unit_cost is not None and unit_cost > 0 and output_quantity and output_quantity > 0:
                (
                    app_session.query(history_model)
                    .filter(getattr(history_model, owner_field) == owner_id)
                    .filter(history_model.acquisition_reference_type == REFERENCE_TYPE_INDUSTRY_JOB)
                    .filter(history_model.acquisition_reference_id == int(row.job_id))
                    .update({
                        "acquisition_unit_cost": float(unit_cost),
                        "acquisition_total_cost": float(unit_cost) * float(output_quantity),
                    }, synchronize_session=False)
                )
            count += 1
    app_session.commit()
    if count:
        logging.info("Re-snapshotted %d industry job(s) stored without the overview's decryptor choice", count)
    return count


def _invention_target_product(invention: Mapping[str, Any], target_blueprint_type_id: int) -> Mapping[str, Any] | None:
    for product in invention.get("products") or []:
        if isinstance(product, Mapping) and _safe_int(product.get("type_id")) == int(target_blueprint_type_id):
            return product
    return None


def _invention_source_activity(
    sde_session: Any, target_blueprint_type_id: int,
    memo: dict[int, tuple[int | None, Mapping[str, Any] | None]] | None = None,
) -> tuple[int | None, Mapping[str, Any] | None]:
    """(source blueprint id, its SDE invention activity in the producer's
    shape), or (None, None) when nothing invents into the blueprint. The
    lowest source id wins, as in the producer. `memo` (one refresh's
    RealizedInventionRefresh.source_activity_by_target) reads each blueprint
    from the SDE once."""
    target = int(target_blueprint_type_id)
    if memo is not None and target in memo:
        return memo[target]
    source_by_target = get_invention_source_blueprint_ids(sde_session, [target])
    source_id = source_by_target.get(target)
    if source_id is None:
        result: tuple[int | None, Mapping[str, Any] | None] = (None, None)
    else:
        data = get_blueprint_manufacturing_data(sde_session, "en", [int(source_id)]).get(int(source_id)) or {}
        invention = data.get("invention") if isinstance(data, Mapping) else None
        result = (int(source_id), (invention if isinstance(invention, Mapping) else {}))
    if memo is not None:
        memo[target] = result
    return result


def _invented_runs_per_success(
    sde_session: Any, *, target_blueprint_type_id: int,
    memo: dict[int, tuple[int | None, Mapping[str, Any] | None]] | None = None,
) -> int | None:
    """Runs on one invented BPC: the SDE invention product quantity (not the
    T2 blueprint's maxProductionLimit; capital T2 modules invent 5 runs of a
    10-run limit). None when nothing invents into it or the SDE has no runs."""
    _, invention = _invention_source_activity(sde_session, int(target_blueprint_type_id), memo)
    product = _invention_target_product(invention or {}, int(target_blueprint_type_id))
    runs = _safe_int((product or {}).get("quantity"))
    return int(runs) if runs and runs > 0 else None


def _invention_attempt_inputs(
    invention: Mapping[str, Any] | None, decryptor: DecryptorOption,
) -> list[tuple[int, int]]:
    """[(type_id, quantity)] one invention attempt consumes: the SDE
    materials (datacores) and, with a decryptor, one decryptor."""
    inputs: list[tuple[int, int]] = []
    for material in (invention or {}).get("materials") or []:
        if not isinstance(material, Mapping):
            continue
        type_id = _safe_int(material.get("type_id"))
        quantity = _safe_int(material.get("quantity"))
        if type_id and quantity and quantity > 0:
            inputs.append((int(type_id), int(quantity)))
    if decryptor.type_id is not None:
        inputs.append((int(decryptor.type_id), 1))
    return inputs


def _price_invention_inputs(
    inputs: Iterable[tuple[int, int]], *,
    market_price_map: Mapping[int, float], owned_cost_map: Mapping[int, float] | None = None,
) -> tuple[float | None, list[int]]:
    """(the inputs' cost, []), or (None, the unpriced type ids): the owned
    unit cost when known, else the market price. Never a partial sum."""
    total = 0.0
    unpriced: list[int] = []
    for type_id, quantity in inputs:
        unit_price = (owned_cost_map or {}).get(type_id) or market_price_map.get(type_id)
        if unit_price is None or float(unit_price) <= 0:
            unpriced.append(int(type_id))
            continue
        total += float(quantity) * float(unit_price)
    if unpriced:
        return None, sorted(unpriced)
    return total, []


def estimate_invention_cost_per_run(
    *,
    sde_session: Any,
    target_blueprint_type_id: int,
    market_price_map: Mapping[int, float],
    owned_cost_map: Mapping[int, float] | None = None,
    invention_inputs: RealizedInventionInputs | None,
    inputs_unknown_reason: str = NO_REALIZED_INVENTION_INPUTS_REASON,
    memo: dict[int, tuple[int | None, Mapping[str, Any] | None]] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """The expected invention inputs one run of an invented blueprint costs,
    sized like the producer's _expected_invention_costs: one attempt's
    datacores (and the producer's decryptor, one per attempt) over the runs
    one invented BPC yields (SDE product quantity + the decryptor's runs)
    times EVE's chance for the corp's best qualifying inventor
    (resolve_invention_chance), after the admin floor.

    (estimate, None); (None, None) when nothing invents into the blueprint;
    (None, why) when an input is unknown: no inputs, unknown skills, no
    decryptor choice, no SDE runs or chance, an unpriced input. Never a
    constant or a partial sum."""
    target = int(target_blueprint_type_id)
    source_id, invention = _invention_source_activity(sde_session, target, memo)
    if source_id is None:
        return None, None
    product = _invention_target_product(invention or {}, target)
    if product is None:
        return None, f"the SDE invention of blueprint {source_id} lists no product {target}"
    base_runs = _safe_int(product.get("quantity"))
    if not base_runs or base_runs <= 0:
        return None, f"the SDE gives no runs per invented BPC for blueprint {target}"
    base_probability = _safe_float(product.get("probability"))
    if base_probability is None or base_probability <= 0:
        return None, f"the SDE gives no invention success chance for blueprint {target}"
    if invention_inputs is None:
        return None, inputs_unknown_reason
    decryptor = invention_inputs.decryptor_for(target)
    if decryptor is None:
        if invention_inputs.decryptor_choices_published and not invention_inputs.decryptor_by_blueprint_type_id:
            return None, (
                f"blueprint {target}: {NO_DECRYPTOR_CHOICE_REASON} (the last product overview planned no invention)"
            )
        if invention_inputs.decryptor_choices_published:
            return None, f"blueprint {target}: {NO_DECRYPTOR_CHOICE_REASON} (this blueprint is not planned by the overview)"
        return None, f"{NO_DECRYPTOR_CHOICE_REASON} for blueprint {target} (no product overview has planned it yet)"
    chance = resolve_invention_chance(
        invention_inputs.invention_context, blueprint_type_id=target,
        skill_entries=[e for e in ((invention or {}).get("skills") or []) if isinstance(e, Mapping)],
        base_probability=float(base_probability), decryptor=decryptor,
    )
    if chance.skills_unknown_reason is not None:
        return None, f"invention chance unknown: {chance.skills_unknown_reason}"
    probability = effective_invention_probability(chance.probability, invention_inputs.probability_floor)
    runs_per_bpc = invented_runs_per_blueprint(int(base_runs), decryptor)
    expected_attempts_per_run, _ = invention_attempts(1, runs_per_bpc, probability)
    if expected_attempts_per_run <= 0:
        return None, f"no positive invention success chance for blueprint {target}"

    inputs = _invention_attempt_inputs(invention, decryptor)
    if not inputs:
        return None, f"the SDE invention of blueprint {source_id} lists no inputs"
    material_cost, unpriced = _price_invention_inputs(
        inputs, market_price_map=market_price_map, owned_cost_map=owned_cost_map,
    )
    if material_cost is None:
        return None, f"invention input(s) {unpriced} of blueprint {target} have no price"
    return {
        "source_blueprint_type_id": source_id,
        "material_cost_per_attempt": float(material_cost),
        "probability": float(probability),
        "runs_per_invented_blueprint": int(runs_per_bpc),
        "decryptor_type_id": decryptor.type_id,
        "inventor_character_id": chance.inventor_character_id,
        "cost_per_run": float(material_cost) * float(expected_attempts_per_run),
    }, None


#: An ESI success chance is a float32: a decryptor fits when the skill factor
#: it implies is a whole-level factor within this relative tolerance (the
#: factors are 1/120 apart, about 0.6%).
_SUCCESS_CHANCE_RELATIVE_TOLERANCE = 1e-4


def _feasible_invention_skill_factors(
    invention: Mapping[str, Any] | None, encryption_skill_type_ids: frozenset[int],
) -> tuple[float, ...]:
    """Every skill factor 1 + (sci1 + sci2)/30 + enc/40 an installer of this
    invention can have: whole levels from the SDE's required level (at least
    1) to 5. When the SDE skills cannot be read as 2 science + 1 encryption,
    every level from 0 (wider: more candidates, never fewer)."""
    entries = [e for e in ((invention or {}).get("skills") or []) if isinstance(e, Mapping)]
    skill_set = invention_skill_set(entries, encryption_skill_type_ids)[0] if entries else None
    if skill_set is None:
        science_min, encryption_levels = 0, range(0, 6)
    else:
        required = required_invention_skill_levels(skill_set, entries)
        science_min = sum(int(required[type_id]) for type_id in skill_set.science_skill_type_ids)
        encryption_levels = (
            range(int(required[skill_set.encryption_skill_type_id]), 6)
            if skill_set.encryption_skill_type_id is not None else range(0, 1)
        )
    return tuple(sorted({
        1.0 + science / 30.0 + encryption / 40.0
        for science in range(min(science_min, 10), 11) for encryption in encryption_levels
    }))


def _infer_job_decryptor(
    *,
    probability: float | None,
    base_probability: float | None,
    skill_factors: tuple[float, ...],
    options: tuple[DecryptorOption, ...],
    overview_choice: DecryptorOption | None = None,
) -> tuple[DecryptorOption | None, str | None, bool]:
    """The decryptor an invention job used, read from its ESI success chance:
    SDE base x skill factor x decryptor multiplier, the skill factor one of
    `skill_factors` (whole levels). ESI's licensed_runs on an invention job is
    the source T1 copy's runs, so it is no evidence.

    (option, None, False) when exactly one option (no decryptor included)
    fits; (option, None, True) when several fit and the overview's published
    choice for the blueprint is one of them; else (None, "decryptor unknown:
    ...", False). Never guessed: no decryptor and Symmetry (both x1.0) only
    the overview's choice can tell apart."""
    if len(options) <= 1:
        return None, "decryptor unknown: the SDE gave no decryptor data", False
    if probability is None or probability <= 0:
        return None, "decryptor unknown: the job gives no success chance to read it from", False
    if probability >= 1.0 - 1e-9:
        return None, "decryptor unknown: the job's success chance is capped at 100%", False
    if base_probability is None or base_probability <= 0:
        return None, "decryptor unknown: the SDE gives no base success chance", False

    def _fits(option: DecryptorOption) -> bool:
        multiplier = float(option.probability_multiplier)
        if multiplier <= 0:
            return False
        implied = float(probability) / (float(base_probability) * multiplier)
        return any(abs(factor - implied) <= _SUCCESS_CHANCE_RELATIVE_TOLERANCE * factor for factor in skill_factors)

    matches = [o for o in options if _fits(o)]
    chance = f"the job's {float(probability):.4f} success chance over the SDE base {float(base_probability):g}"
    if len(matches) == 1:
        return matches[0], None, False
    if not matches:
        return None, f"decryptor unknown: {chance} matches no decryptor at any skill level", False
    if overview_choice is not None:
        for option in matches:
            if option.type_id == overview_choice.type_id:
                return option, None, True
    names = ", ".join(sorted(str(o.type_name) for o in matches))
    reason = f"decryptor unknown: candidates {names} ({chance} fits each)"
    if overview_choice is not None:
        reason += f"; the overview's choice ({overview_choice.type_name}) is not one of them"
    return None, reason, False


def build_invention_cost_per_run_by_blueprint_type(
    *,
    jobs: Iterable[dict[str, Any]] | None,
    sde_session: Any,
    market_price_map: dict[int, float],
    invention_refresh: RealizedInventionRefresh | None = None,
) -> dict[int, dict[str, Any]]:
    """{invented blueprint type_id: its invention cost per invented run from
    the corp's own delivered invention jobs}.

    cost per run = sum(runs x one attempt's inputs + job cost) /
    sum(successful_runs x licensed_runs): every attempt is paid for (failed
    ones and failed jobs included), only successes yield runs, each the SDE
    runs plus the job's decryptor's (not ESI's licensed_runs: that is the
    source T1 copy's runs). A job is left out when it is not delivered, has
    no successful_runs or runs, its decryptor cannot be read from its success
    chance (_infer_job_decryptor; several candidates are settled by the
    overview's published choice for the blueprint when it is one of them,
    listed in `decryptor_from_overview_choice_job_ids`) or an input has no
    price. A blueprint without a usable job, or with zero
    successes over its usable jobs, maps to {"unknown_reason": why} and no
    unit_cost_per_run: the caller then uses the expected estimate."""
    refresh = invention_refresh or RealizedInventionRefresh(inputs=None)
    memo = refresh.source_activity_by_target
    totals: dict[int, dict[str, Any]] = {}
    for job in jobs or []:
        if not isinstance(job, dict):
            continue
        raw = job.get("raw") if isinstance(job.get("raw"), dict) else job
        if _safe_int((raw or {}).get("activity_id")) != 8:
            continue
        target = _safe_int(job.get("product_type_id"))
        if not target or target <= 0:
            continue
        entry = totals.setdefault(int(target), {
            "cost": 0.0, "attempts": 0, "successful_runs": 0, "invented_runs": 0, "jobs": 0, "skipped": [],
            "overview_choice_job_ids": [],
        })
        job_id = job.get("job_id")

        def _skip(why: str) -> None:
            entry["skipped"].append(f"job {job_id}: {why}")
            logging.info("Invention job %s for blueprint %s skipped as a cost source: %s", job_id, target, why)

        if str((raw or {}).get("status") or "").lower() != _DELIVERED_STATUS:
            continue  # not finished: its outcome is not known yet (not a reason, just not yet)
        runs = _safe_int((raw or {}).get("runs"))
        successful_runs = _safe_int((raw or {}).get("successful_runs"))
        if successful_runs is None or successful_runs < 0:
            _skip("no successful_runs")
            continue
        if not runs or runs <= 0:
            _skip("no runs")
            continue
        source_id, invention = _invention_source_activity(sde_session, int(target), memo)
        product = _invention_target_product(invention or {}, int(target)) if source_id is not None else None
        base_runs = _safe_int((product or {}).get("quantity"))
        if not base_runs or base_runs <= 0:
            _skip("the SDE gives no runs per invented BPC")
            continue
        decryptor, decryptor_reason, by_overview_choice = _infer_job_decryptor(
            probability=_safe_float((raw or {}).get("probability")),
            base_probability=_safe_float((product or {}).get("probability")),
            skill_factors=_feasible_invention_skill_factors(invention, refresh.encryption_skills(sde_session)),
            options=refresh.decryptors(sde_session),
            overview_choice=refresh.overview_decryptor_for(int(target)),
        )
        if decryptor is None:
            _skip(str(decryptor_reason))
            continue
        per_attempt, unpriced = _price_invention_inputs(
            _invention_attempt_inputs(invention, decryptor), market_price_map=market_price_map,
        )
        if per_attempt is None:
            _skip(f"invention input(s) {unpriced} have no price")
            continue
        entry["cost"] += float(runs) * float(per_attempt) + float(_safe_float(job.get("cost")) or 0.0)
        entry["attempts"] += int(runs)
        entry["successful_runs"] += int(successful_runs)
        entry["invented_runs"] += int(successful_runs) * invented_runs_per_blueprint(int(base_runs), decryptor)
        entry["jobs"] += 1
        if by_overview_choice:
            entry["overview_choice_job_ids"].append(job_id)
            logging.info(
                "Invention job %s for blueprint %s: its success chance fits several decryptors; the overview's "
                "choice (%s) is one of them and is used", job_id, target, decryptor.type_name,
            )

    out: dict[int, dict[str, Any]] = {}
    for target, entry in totals.items():
        if entry["jobs"] == 0:
            if entry["skipped"]:
                out[target] = {"unknown_reason": "no usable invention job (" + "; ".join(entry["skipped"]) + ")"}
            continue  # only unfinished jobs: nothing to say yet
        if entry["invented_runs"] <= 0:
            out[target] = {"unknown_reason": (
                f"the corp's {entry['jobs']} delivered invention job(s) had no successful attempt "
                f"({entry['attempts']} attempts)"
            )}
            continue
        out[target] = {
            "total_cost": float(entry["cost"]),
            "unit_cost_per_run": float(entry["cost"]) / float(entry["invented_runs"]),
            "source": "actual_invention_job",
            "jobs": int(entry["jobs"]),
            "attempts": int(entry["attempts"]),
            "successful_runs": int(entry["successful_runs"]),
            "invented_runs": int(entry["invented_runs"]),
            "decryptor_from_overview_choice_job_ids": list(entry["overview_choice_job_ids"]),
        }
    return out


def invention_cost_source_for(
    invention_cost_by_blueprint_type: Mapping[int, Mapping[str, Any]], blueprint_type_id: Any,
) -> dict[str, Any]:
    """resolve_industry_job_cost_snapshot's actual-invention arguments for a
    manufacturing job of `blueprint_type_id`."""
    entry = invention_cost_by_blueprint_type.get(int(_safe_int(blueprint_type_id) or 0)) or {}
    unit = _safe_float(entry.get("unit_cost_per_run"))
    return {
        "invention_unit_cost_per_run": unit,
        "invention_cost_source": (str(entry.get("source") or "") or None) if unit is not None else None,
        "invention_source_unknown_reason": entry.get("unknown_reason") if unit is None else None,
    }


def resolve_industry_job_cost_snapshot(
    *,
    job: Any,
    sde_session: Any,
    market_price_map: dict[int, float] | None,
    invention_unit_cost_per_run: float | None = None,
    invention_cost_source: str | None = None,
    blueprint_provenance: dict[str, Any] | None = None,
    owned_input_unit_cost_by_type_id: dict[int, dict[str, Any]] | None = None,
    invention_inputs: RealizedInventionInputs | None = None,
    invention_refresh: RealizedInventionRefresh | None = None,
    invention_source_unknown_reason: str | None = None,
) -> dict[str, Any]:
    """The build cost snapshot of one industry job.

    For a manufacturing job of an invented blueprint the invention cost per
    run is the corp's actual invention jobs' (`invention_unit_cost_per_run`,
    build_invention_cost_per_run_by_blueprint_type) when known, else the
    expected estimate. `invention_refresh` carries the producer's inputs and
    the reason they are missing, read once per refresh (else they are read
    here, or `invention_inputs` is used). When neither source gives a cost,
    `invention_cost_unknown_reason` names both reasons
    (`invention_source_unknown_reason` is why the actual jobs gave none)."""
    blueprint_type_id = _safe_int(getattr(job, "blueprint_type_id", None))
    product_type_id = _safe_int(getattr(job, "product_type_id", None))
    raw = _industry_job_raw_payload(job)
    activity_id = _safe_int(raw.get("activity_id"))
    runs = _industry_job_runs(job)

    output_quantity = (
        _safe_int(getattr(job, "output_quantity", None))
        or _coalesce_int(raw, "output_quantity", "product_quantity", "produced")
    )
    materials_cost = _safe_float(getattr(job, "materials_cost", None))
    if materials_cost is None:
        materials_cost = _coalesce_float(raw, "materials_cost", "materials_cost_total")
    copy_cost = _safe_float(getattr(job, "copy_cost", None))
    if copy_cost is None:
        copy_cost = _coalesce_float(raw, "copy_cost", "copy_cost_total")
    invention_cost = _safe_float(getattr(job, "invention_cost", None))
    if invention_cost is None:
        invention_cost = _coalesce_float(raw, "invention_cost", "invention_cost_total")
    total_cost = _safe_float(getattr(job, "total_build_cost", None))
    if total_cost is None:
        total_cost = _coalesce_float(raw, "total_build_cost", "build_cost_total", "exact_total_cost", "total_cost")
    unit_cost = _safe_float(getattr(job, "unit_build_cost", None))
    if unit_cost is None:
        unit_cost = _coalesce_float(raw, "unit_build_cost", "build_cost_unit", "exact_unit_cost", "unit_cost")
    build_cost_source = str(
        getattr(job, "build_cost_source", None) or raw.get("build_cost_source") or raw.get("cost_source") or ""
    ).strip() or None
    blueprint_material_efficiency = _safe_int(
        getattr(job, "blueprint_material_efficiency", None)
        or raw.get("blueprint_material_efficiency")
        or raw.get("material_efficiency")
    )
    if blueprint_material_efficiency is None:
        blueprint_material_efficiency = _safe_int((blueprint_provenance or {}).get("blueprint_material_efficiency"))

    blueprint_time_efficiency = _safe_int(
        getattr(job, "blueprint_time_efficiency", None)
        or raw.get("blueprint_time_efficiency")
        or raw.get("time_efficiency")
    )
    if blueprint_time_efficiency is None:
        blueprint_time_efficiency = _safe_int((blueprint_provenance or {}).get("blueprint_time_efficiency"))

    blueprint_item_id = _safe_int(
        getattr(job, "blueprint_item_id", None)
        or raw.get("blueprint_id")
        or (blueprint_provenance or {}).get("item_id")
    )
    blueprint_is_copy = getattr(job, "blueprint_is_blueprint_copy", None)
    if blueprint_is_copy is None:
        blueprint_is_copy = raw.get("is_blueprint_copy")
    if blueprint_is_copy is None:
        blueprint_is_copy = (blueprint_provenance or {}).get("is_blueprint_copy")
    blueprint_runs = _safe_int(
        getattr(job, "blueprint_runs", None)
        or raw.get("licensed_runs")
        or raw.get("blueprint_runs")
        or (blueprint_provenance or {}).get("blueprint_runs")
    )
    blueprint_provenance_source = str((blueprint_provenance or {}).get("source") or "").strip() or None
    blueprint_provenance_ref_id = _safe_int((blueprint_provenance or {}).get("reference_id"))

    if output_quantity is None and blueprint_type_id and product_type_id:
        qpr = _output_quantity_per_run_for_job(
            sde_session=sde_session,
            blueprint_type_id=int(blueprint_type_id),
            product_type_id=int(product_type_id),
        )
        if qpr and qpr > 0:
            output_quantity = int(qpr) * int(runs)

    resolved_invention_source = invention_cost_source
    # A persisted snapshot keeps the reason it was stored with; it is not
    # re-estimated (its unit cost already holds what was known then).
    invention_cost_unknown_reason: str | None = (
        str(getattr(job, "invention_cost_unknown_reason", None) or raw.get("invention_cost_unknown_reason") or "")
        or None
    )
    has_persisted_cost = (unit_cost is not None and unit_cost > 0) or (total_cost is not None and total_cost > 0)
    if (
        invention_cost is None and not has_persisted_cost and activity_id in {None, 1}
        and blueprint_type_id and sde_session is not None
    ):
        if invention_unit_cost_per_run is not None and invention_unit_cost_per_run > 0:
            invention_cost = float(invention_unit_cost_per_run) * float(runs)
            resolved_invention_source = invention_cost_source or "actual_invention_job"
        elif blueprint_is_copy is False:
            pass  # built from an original: no invention to cost
        else:
            _invention_owned_cost_map: dict[int, float] | None = None
            if owned_input_unit_cost_by_type_id:
                _invention_owned_cost_map = {
                    int(tid): float(uc)
                    for tid, payload in owned_input_unit_cost_by_type_id.items()
                    if (uc := _safe_float((payload or {}).get("unit_cost"))) and uc > 0
                } or None
            inputs_unknown_reason: str | None = None
            if invention_inputs is None and invention_refresh is not None:
                invention_inputs = invention_refresh.inputs
                inputs_unknown_reason = invention_refresh.inputs_unknown_reason
            elif invention_inputs is None:
                invention_inputs, inputs_unknown_reason = current_realized_invention_inputs()
            estimate, unknown_reason = estimate_invention_cost_per_run(
                sde_session=sde_session,
                target_blueprint_type_id=int(blueprint_type_id),
                market_price_map=market_price_map or {},
                owned_cost_map=_invention_owned_cost_map,
                invention_inputs=invention_inputs,
                inputs_unknown_reason=inputs_unknown_reason or NO_REALIZED_INVENTION_INPUTS_REASON,
                memo=invention_refresh.source_activity_by_target if invention_refresh is not None else None,
            )
            if estimate is not None:
                invention_cost = float(estimate["cost_per_run"]) * float(runs)
                resolved_invention_source = "expected_invention_material_cost"
            elif unknown_reason is not None:
                invention_cost_unknown_reason = f"invention cost unknown: {unknown_reason}"
                if invention_source_unknown_reason:
                    invention_cost_unknown_reason += (
                        f"; the corp's own invention jobs give no cost either: {invention_source_unknown_reason}"
                    )
                logging.info(
                    "Industry job %s (blueprint %s): %s; its build cost is a lower bound and "
                    "feedback skips the cost update for sales of it",
                    getattr(job, "job_id", None), blueprint_type_id, invention_cost_unknown_reason,
                )

    if unit_cost is None and total_cost is not None and output_quantity and output_quantity > 0:
        unit_cost = float(total_cost) / float(output_quantity)
    if total_cost is None and unit_cost is not None and output_quantity and output_quantity > 0:
        total_cost = float(unit_cost) * float(output_quantity)

    if unit_cost is not None and unit_cost > 0 and output_quantity and output_quantity > 0:
        return {
            "output_quantity": int(output_quantity),
            "materials_cost": materials_cost,
            "historical_materials_cost": _safe_float(getattr(job, "historical_materials_cost", None)) or _coalesce_float(raw, "historical_materials_cost"),
            "historical_material_cost_source": getattr(job, "historical_material_cost_source", None) or raw.get("historical_material_cost_source"),
            "historical_material_coverage_fraction": _safe_float(getattr(job, "historical_material_coverage_fraction", None)) or _coalesce_float(raw, "historical_material_coverage_fraction"),
            "historical_input_costs": getattr(job, "historical_input_costs", None) or raw.get("historical_input_costs"),
            "copy_cost": copy_cost,
            "invention_cost": invention_cost,
            "invention_cost_unknown_reason": invention_cost_unknown_reason,
            "total_cost": float(total_cost or (float(unit_cost) * float(output_quantity))),
            "unit_cost": float(unit_cost),
            "source": build_cost_source or "persisted_job_cost_snapshot",
            "blueprint_item_id": blueprint_item_id,
            "blueprint_is_blueprint_copy": blueprint_is_copy,
            "blueprint_runs": blueprint_runs,
            "blueprint_time_efficiency": blueprint_time_efficiency,
            "blueprint_material_efficiency": blueprint_material_efficiency,
            "blueprint_provenance_source": blueprint_provenance_source,
            "blueprint_provenance_ref_id": blueprint_provenance_ref_id,
        }

    if blueprint_type_id and product_type_id and market_price_map:
        estimated = _estimate_industry_job_materials_cost_total(
            sde_session=sde_session,
            blueprint_type_id=int(blueprint_type_id),
            product_type_id=int(product_type_id),
            runs=int(_industry_job_runs(job)),
            market_price_map=market_price_map,
            blueprint_material_efficiency=blueprint_material_efficiency,
            owned_input_unit_cost_by_type_id=owned_input_unit_cost_by_type_id,
        )
        if estimated is not None:
            estimated_materials_cost = _safe_float(estimated.get("materials_cost")) or 0.0
            estimated_output_quantity = _safe_int(estimated.get("output_quantity")) or 0
            total_cost = (
                float(estimated_materials_cost)
                + float(_safe_float(getattr(job, "cost", None)) or 0.0)
                + float(copy_cost or 0.0)
                + float(invention_cost or 0.0)
            )
            source = build_cost_source or str(estimated.get("material_cost_source") or "market_snapshot_estimate")
            if resolved_invention_source:
                source = f"{source}+{resolved_invention_source}"
            return {
                "output_quantity": int(estimated_output_quantity),
                "materials_cost": float(estimated_materials_cost),
                "historical_materials_cost": _safe_float(estimated.get("historical_materials_cost")),
                "historical_material_cost_source": estimated.get("material_cost_source"),
                "historical_material_coverage_fraction": _safe_float(estimated.get("historical_material_coverage_fraction")),
                "historical_input_costs": estimated.get("historical_input_costs") or None,
                "copy_cost": copy_cost,
                "invention_cost": invention_cost,
                "invention_cost_unknown_reason": invention_cost_unknown_reason,
                "total_cost": float(total_cost),
                "unit_cost": float(total_cost) / float(estimated_output_quantity),
                "source": source,
                "blueprint_item_id": blueprint_item_id,
                "blueprint_is_blueprint_copy": blueprint_is_copy,
                "blueprint_runs": blueprint_runs,
                "blueprint_time_efficiency": blueprint_time_efficiency,
                "blueprint_material_efficiency": blueprint_material_efficiency,
                "blueprint_provenance_source": blueprint_provenance_source,
                "blueprint_provenance_ref_id": blueprint_provenance_ref_id,
            }

    return {
        "output_quantity": int(output_quantity) if output_quantity and output_quantity > 0 else None,
        "materials_cost": materials_cost,
        "historical_materials_cost": _safe_float(getattr(job, "historical_materials_cost", None)) or _coalesce_float(raw, "historical_materials_cost"),
        "historical_material_cost_source": getattr(job, "historical_material_cost_source", None) or raw.get("historical_material_cost_source"),
        "historical_material_coverage_fraction": _safe_float(getattr(job, "historical_material_coverage_fraction", None)) or _coalesce_float(raw, "historical_material_coverage_fraction"),
        "historical_input_costs": getattr(job, "historical_input_costs", None) or raw.get("historical_input_costs"),
        "copy_cost": copy_cost,
        "invention_cost": invention_cost,
        "invention_cost_unknown_reason": invention_cost_unknown_reason,
        "total_cost": total_cost,
        "unit_cost": unit_cost,
        "source": build_cost_source,
        "blueprint_item_id": blueprint_item_id,
        "blueprint_is_blueprint_copy": blueprint_is_copy,
        "blueprint_runs": blueprint_runs,
        "blueprint_time_efficiency": blueprint_time_efficiency,
        "blueprint_material_efficiency": blueprint_material_efficiency,
        "blueprint_provenance_source": blueprint_provenance_source,
        "blueprint_provenance_ref_id": blueprint_provenance_ref_id,
    }


def build_fifo_remaining_lots_by_type(
    *,
    wallet_transactions: Iterable[Any],
    industry_jobs: Iterable[Any] | None = None,
    sde_session: Any | None = None,
    market_prices: list[dict[str, Any]] | None = None,
    market_price_map_direct: dict[int, float] | None = None,
    on_hand_quantities_by_type: dict[int, int],
) -> dict[int, list[FifoLot]]:
    """Return remaining FIFO lots per type_id, aligned to current on-hand quantities.

    Reconstructs FIFO inventory lots from wallet transactions:
    - Buys add lots.
    - Sells consume lots FIFO (oldest first).

    Because transaction history can be incomplete, we align the resulting lots
    to the current on-hand quantity:
    - If transaction-derived remaining > on-hand, we drop excess from the *oldest*
      lots (FIFO-consistent: missing consumption removes oldest first).
    - If transaction-derived remaining < on-hand, we leave as-is (the remaining
      quantity has unknown cost basis).
    """

    market_price_map = market_price_map_direct if market_price_map_direct else _build_price_map(market_prices)

    tx_by_type: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for tx in wallet_transactions or []:
        tid = _safe_int(getattr(tx, "type_id", None))
        if not tid or tid <= 0:
            continue

        qty = _safe_int(getattr(tx, "quantity", None))
        if qty is None or qty <= 0:
            continue

        is_buy = getattr(tx, "is_buy", None)
        # If we can't tell, skip.
        if is_buy is None:
            continue

        unit_price = _safe_float(getattr(tx, "unit_price", None))
        # For sells, unit price is irrelevant; for buys, we need it.
        if is_buy is True and (unit_price is None or unit_price <= 0):
            continue

        date_s = getattr(tx, "date", None)
        dt = _parse_date(date_s)
        tx_id = _safe_int(getattr(tx, "transaction_id", None))

        tx_by_type[int(tid)].append(
            {
                "kind": ("buy" if bool(is_buy) else "sell"),
                "quantity": int(qty),
                "unit_price": float(unit_price or 0.0),
                "date": (str(date_s) if date_s is not None else None),
                "dt": dt,
                "sort_id": int(tx_id or 0),
                "reference_id": tx_id,
                "reference_type": REFERENCE_TYPE_WALLET_TRANSACTION,
                "source": ASSET_SOURCE_MARKET_BUY,
            }
        )

    # Add industry-job outputs as FIFO "buy" lots (best-effort).
    # This allows FIFO valuation for items you manufactured, not only bought.
    if industry_jobs is not None and sde_session is not None and market_price_map:
        completed_statuses = {"delivered", "ready", "completed"}
        qty_per_run_cache: dict[tuple[int, int], Optional[int]] = {}

        def _output_qty_per_run(blueprint_type_id: int, product_type_id: int) -> Optional[int]:
            key = (int(blueprint_type_id), int(product_type_id))
            if key in qty_per_run_cache:
                return qty_per_run_cache[key]
            bp = (
                sde_session.query(Blueprints)
                .filter_by(blueprintTypeID=int(blueprint_type_id))
                .first()
            )
            if bp is None:
                qty_per_run_cache[key] = None
                return None
            mfg = _get_mfg_activity(getattr(bp, "activities", None))
            if not mfg:
                qty_per_run_cache[key] = None
                return None
            products = mfg.get("products")
            if not isinstance(products, list) or not products:
                qty_per_run_cache[key] = None
                return None
            out_q: Optional[int] = None
            for p in products:
                if not isinstance(p, dict):
                    continue
                if _safe_int(p.get("typeID")) == int(product_type_id):
                    out_q = _safe_int(p.get("quantity"))
                    break
            if out_q is None and products:
                out_q = _safe_int(products[0].get("quantity")) if isinstance(products[0], dict) else None
            if not out_q or out_q <= 0:
                qty_per_run_cache[key] = None
                return None
            qty_per_run_cache[key] = int(out_q)
            return int(out_q)

        for job in industry_jobs or []:
            status = str(getattr(job, "status", "") or "").lower()
            completed_date = getattr(job, "completed_date", None) or getattr(job, "end_date", None)
            if status and status not in completed_statuses:
                continue
            if completed_date is None:
                continue

            product_type_id = _safe_int(getattr(job, "product_type_id", None))
            blueprint_type_id = _safe_int(getattr(job, "blueprint_type_id", None))
            if not product_type_id or not blueprint_type_id:
                continue

            snapshot = resolve_industry_job_cost_snapshot(
                job=job,
                sde_session=sde_session,
                market_price_map=market_price_map,
            )
            lot_qty = _safe_int(snapshot.get("output_quantity")) or 0
            unit_cost = _safe_float(snapshot.get("unit_cost"))
            if unit_cost is None or unit_cost <= 0:
                continue
            if lot_qty <= 0:
                qpr = _output_qty_per_run(int(blueprint_type_id), int(product_type_id))
                if qpr is None:
                    continue
                lot_qty = int(qpr) * int(_industry_job_runs(job))
                if lot_qty <= 0:
                    continue

            date_s = completed_date
            dt = _parse_date(date_s)
            job_id = _safe_int(getattr(job, "job_id", None))
            tx_by_type[int(product_type_id)].append(
                {
                    "kind": "buy",
                    "quantity": int(lot_qty),
                    "unit_price": float(unit_cost),
                    "date": (str(date_s) if date_s is not None else None),
                    "dt": dt,
                    "sort_id": int(job_id or 0),
                    "reference_id": job_id,
                    "reference_type": REFERENCE_TYPE_INDUSTRY_JOB,
                    "source": ASSET_SOURCE_INDUSTRY_BUILD,
                }
            )

    lots_by_type: dict[int, list[FifoLot]] = {}
    for tid, txs in tx_by_type.items():
        # Sort oldest -> newest.
        txs.sort(
            key=lambda r: (
                r.get("dt") or datetime.min,
                int(r.get("sort_id") or 0),
            )
        )

        lots: list[FifoLot] = []
        for r in txs:
            q = int(r.get("quantity") or 0)
            if q <= 0:
                continue
            if str(r.get("kind")) == "buy":
                up = float(r.get("unit_price") or 0.0)
                if up <= 0:
                    continue
                lots.append(
                    FifoLot(
                        quantity=q,
                        unit_price=up,
                        acquisition_date=r.get("date"),
                        reference_id=r.get("reference_id"),
                        reference_type=r.get("reference_type"),
                        source=r.get("source"),
                    )
                )
            else:
                sell = q
                while sell > 0 and lots:
                    head = lots[0]
                    take = min(sell, int(head.quantity))
                    sell -= int(take)
                    new_qty = int(head.quantity) - int(take)
                    if new_qty > 0:
                        lots[0] = FifoLot(
                            quantity=new_qty,
                            unit_price=float(head.unit_price),
                            acquisition_date=head.acquisition_date,
                            reference_id=head.reference_id,
                            reference_type=head.reference_type,
                            source=head.source,
                        )
                    else:
                        lots.pop(0)

        # Align to current on-hand.
        on_hand = int(on_hand_quantities_by_type.get(int(tid), 0) or 0)
        if on_hand <= 0:
            lots_by_type[int(tid)] = []
            continue

        remaining = sum(int(l.quantity) for l in lots)
        if remaining > on_hand:
            excess = int(remaining) - int(on_hand)
            # FIFO-consistent adjustment: missing consumption removes oldest lots first.
            while excess > 0 and lots:
                head = lots[0]
                take = min(excess, int(head.quantity))
                excess -= int(take)
                new_qty = int(head.quantity) - int(take)
                if new_qty > 0:
                    lots[0] = FifoLot(
                        quantity=new_qty,
                        unit_price=float(head.unit_price),
                        acquisition_date=head.acquisition_date,
                        reference_id=head.reference_id,
                        reference_type=head.reference_type,
                        source=head.source,
                    )
                else:
                    lots.pop(0)

        lots_by_type[int(tid)] = lots

    # Ensure types with on-hand but no history are present.
    for tid in (on_hand_quantities_by_type or {}).keys():
        try:
            tid_i = int(tid)
        except (TypeError, ValueError):
            continue
        lots_by_type.setdefault(tid_i, [])

    return lots_by_type


def fifo_allocate_cost(
    *,
    lots: list[FifoLot] | None,
    quantity: int,
) -> tuple[float, int]:
    """Return (total_cost, priced_quantity) for consuming `quantity` using FIFO lots."""
    q = int(quantity or 0)
    if q <= 0:
        return 0.0, 0
    if not lots:
        return 0.0, 0

    remaining = q
    total_cost = 0.0
    priced_qty = 0
    for lot in lots:
        if remaining <= 0:
            break
        lot_qty = int(lot.quantity)
        if lot_qty <= 0:
            continue
        take = min(remaining, lot_qty)
        if take <= 0:
            continue
        total_cost += float(take) * float(lot.unit_price)
        priced_qty += int(take)
        remaining -= int(take)

    return float(total_cost), int(priced_qty)


def fifo_allocate_cost_breakdown(
    *,
    lots: list[FifoLot] | None,
    quantity: int,
) -> dict[str, Any]:
    """Allocate FIFO lots and return a breakdown.

    Returns a dict with:
      - total_cost
      - priced_quantity
      - remaining_unpriced_quantity
      - by_source: {source: {"cost": float, "quantity": int}}
    """

    q = int(quantity or 0)
    if q <= 0:
        return {
            "total_cost": 0.0,
            "priced_quantity": 0,
            "remaining_unpriced_quantity": 0,
            "by_source": {},
        }
    if not lots:
        return {
            "total_cost": 0.0,
            "priced_quantity": 0,
            "remaining_unpriced_quantity": int(q),
            "by_source": {},
        }

    remaining = int(q)
    total_cost = 0.0
    priced_qty = 0
    by_source: dict[str, dict[str, Any]] = {}

    for lot in lots:
        if remaining <= 0:
            break
        lot_qty = int(getattr(lot, "quantity", 0) or 0)
        if lot_qty <= 0:
            continue
        take = min(remaining, lot_qty)
        if take <= 0:
            continue

        unit_price = float(getattr(lot, "unit_price", 0.0) or 0.0)
        chunk_cost = float(take) * float(unit_price)

        src = getattr(lot, "source", None) or "unknown"
        if not isinstance(src, str) or not src:
            src = "unknown"
        slot = by_source.get(src)
        if slot is None:
            slot = {"cost": 0.0, "quantity": 0}
            by_source[src] = slot
        slot["cost"] = float(slot.get("cost") or 0.0) + float(chunk_cost)
        slot["quantity"] = int(slot.get("quantity") or 0) + int(take)

        total_cost += float(chunk_cost)
        priced_qty += int(take)
        remaining -= int(take)

    return {
        "total_cost": float(total_cost),
        "priced_quantity": int(priced_qty),
        "remaining_unpriced_quantity": int(remaining),
        "by_source": by_source,
    }


def _build_price_map(market_prices: list[dict[str, Any]] | None) -> dict[int, float]:
    """Map type_id -> price using ESI /markets/prices/ payload.

    Prefers average_price, falls back to adjusted_price.
    """
    out: dict[int, float] = {}
    if not market_prices:
        return out
    for row in market_prices:
        if not isinstance(row, dict):
            continue
        raw_type_id = row.get("type_id")
        if raw_type_id is None:
            continue
        try:
            type_id = int(raw_type_id)
        except (TypeError, ValueError):
            continue
        if type_id <= 0:
            continue
        price = row.get("average_price")
        if price is None:
            price = row.get("adjusted_price")
        if price is None:
            continue
        try:
            price_f = float(price)
        except (TypeError, ValueError):
            continue
        if price_f <= 0:
            continue
        out[type_id] = price_f
    return out


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except (OverflowError, TypeError, ValueError):
        return None


def _get_mfg_activity(activities: Any) -> Optional[dict[str, Any]]:
    if not isinstance(activities, dict):
        return None
    mfg = activities.get("manufacturing")
    if isinstance(mfg, dict):
        return mfg
    return None


def estimate_industry_job_unit_cost(
    *,
    sde_session,
    blueprint_type_id: int,
    product_type_id: int,
    runs: int,
    job_cost: float | None,
    market_price_map: dict[int, float],
) -> Optional[float]:
    estimated = _estimate_industry_job_materials_cost_total(
        sde_session=sde_session,
        blueprint_type_id=int(blueprint_type_id),
        product_type_id=int(product_type_id),
        runs=int(runs),
        market_price_map=market_price_map,
    )
    if estimated is None:
        return None

    material_cost_total = _safe_float(estimated.get("materials_cost")) or 0.0
    total_output = _safe_int(estimated.get("output_quantity")) or 0
    if total_output <= 0:
        return None
    total_cost = float(material_cost_total) + float(job_cost or 0.0)
    return total_cost / float(total_output)


def build_cost_map_for_assets(
    *,
    app_session,
    sde_session,
    owner_kind: str,
    owner_id: int,
    asset_type_ids: Iterable[int],
    asset_quantities_by_type: dict[int, int] | None = None,
    wallet_tx_model,
    industry_job_model,
    market_prices: list[dict[str, Any]] | None,
) -> dict[int, CostInfo]:
    """Return type_id -> CostInfo for an owner.

    Strategy:
    - Prefer most recent completed industry job producing the type.
    - Else prefer most recent market buy transaction.
    - Else unknown.

    Notes:
    - This is a best-effort provenance inference; ESI assets do not include acquisition history.
    """
    type_ids = sorted({int(x) for x in asset_type_ids if isinstance(x, int) or str(x).isdigit()})
    if not type_ids:
        return {}

    market_price_map = _build_price_map(market_prices)

    # Pull wallet transactions (buy + sell), newest first.
    tx_rows = (
        app_session.query(wallet_tx_model)
        .filter(wallet_tx_model.type_id.in_(type_ids))
        .filter(getattr(wallet_tx_model, owner_kind) == int(owner_id))
        .order_by(desc(wallet_tx_model.date))
        .all()
    )
    tx_by_type: dict[int, list[Any]] = {}
    for r in tx_rows:
        tid = getattr(r, "type_id", None)
        if tid is None:
            continue
        try:
            tid_int = int(tid)
        except (TypeError, ValueError):
            continue
        tx_by_type.setdefault(tid_int, []).append(r)

    def _estimate_unit_cost_from_transactions(type_id: int, on_hand_qty: int) -> Optional[tuple[float, Any]]:
        """Estimate unit cost of current on-hand inventory using transaction history.

        We walk transactions backwards (newest -> oldest), adjusting the required
        quantity by sells, then allocating buys to cover current on-hand qty.

        This is quantity-aware and tends to value inventory using recent buys.
        Returns (unit_cost, reference_tx).
        """
        if on_hand_qty <= 0:
            return None
        txs = tx_by_type.get(type_id) or []
        if not txs:
            return None

        remaining = int(on_hand_qty)
        allocated_qty = 0
        allocated_cost = 0.0
        reference_tx = None

        for tx in txs:
            is_buy = getattr(tx, "is_buy", None)
            qty = _safe_int(getattr(tx, "quantity", None)) or 0
            if qty <= 0:
                continue

            # If this transaction is a sell, inventory was higher before the sell.
            if is_buy is False:
                remaining += qty
                continue

            # If this is a buy, allocate it towards the current on-hand inventory.
            if is_buy is True:
                unit_price = _safe_float(getattr(tx, "unit_price", None))
                if unit_price is None or unit_price <= 0:
                    continue

                take = min(remaining, qty)
                if take <= 0:
                    continue
                allocated_qty += take
                allocated_cost += float(take) * float(unit_price)
                remaining -= take
                if reference_tx is None:
                    reference_tx = tx
                if remaining <= 0:
                    break

        if allocated_qty <= 0:
            return None
        return (allocated_cost / float(allocated_qty), reference_tx)

    # Pull completed jobs (most recent per product type).
    # ESI job statuses vary; we treat delivered/ready/completed as "completed".
    completed_statuses = {"delivered", "ready", "completed"}
    job_rows = (
        app_session.query(industry_job_model)
        .filter(industry_job_model.product_type_id.in_(type_ids))
        .filter(getattr(industry_job_model, owner_kind) == int(owner_id))
        .order_by(desc(industry_job_model.end_date))
        .all()
    )
    last_job_by_type: dict[int, Any] = {}
    for r in job_rows:
        status = str(getattr(r, "status", "") or "").lower()
        if status and status not in completed_statuses:
            continue
        tid = getattr(r, "product_type_id", None)
        if tid is None:
            continue
        tid_int = int(tid)
        if tid_int not in last_job_by_type:
            last_job_by_type[tid_int] = r

    out: dict[int, CostInfo] = {}
    for tid in type_ids:
        job = last_job_by_type.get(tid)
        if job is not None:
            snapshot = resolve_industry_job_cost_snapshot(
                job=job,
                sde_session=sde_session,
                market_price_map=market_price_map,
            )
            unit_cost = _safe_float(snapshot.get("unit_cost"))
            total_cost = _safe_float(snapshot.get("total_cost"))

            out[tid] = CostInfo(
                source=ASSET_SOURCE_INDUSTRY_BUILD,
                unit_cost=unit_cost,
                total_cost=total_cost,
                reference_type=REFERENCE_TYPE_INDUSTRY_JOB,
                reference_id=_safe_int(getattr(job, "job_id", None)),
                acquisition_date=getattr(job, "end_date", None),
            )
            continue

        on_hand_qty = int((asset_quantities_by_type or {}).get(tid, 0) or 0)
        est = _estimate_unit_cost_from_transactions(tid, on_hand_qty)
        if est is not None:
            unit_cost, ref_tx = est
            out[tid] = CostInfo(
                source=ASSET_SOURCE_MARKET_BUY,
                unit_cost=unit_cost,
                total_cost=None,
                reference_type=REFERENCE_TYPE_WALLET_TRANSACTION,
                reference_id=_safe_int(getattr(ref_tx, "transaction_id", None)) if ref_tx is not None else None,
                acquisition_date=getattr(ref_tx, "date", None) if ref_tx is not None else None,
            )
            continue

        out[tid] = CostInfo(
            source=ASSET_SOURCE_UNKNOWN,
            unit_cost=None,
            total_cost=None,
            reference_type=None,
            reference_id=None,
            acquisition_date=None,
        )

    return out


def _get_or_fetch_market_price_on_date(
    *,
    type_id: int,
    target_date: str,
    app_session: Any,
    esi_service: Any,
    region_id: int = 10000002,
    unavailable: Optional[dict[int, str]] = None,
    transient: Optional[set[int]] = None,
) -> Optional[float]:
    """Return the closest daily average price on or before target_date for type_id.

    Checks the market_history DB first; fetches from ESI and caches if missing.
    None when no price is known; then `unavailable` (when given) gets why, and
    `transient` (when given) gets the type when that reason can clear on a
    retry: the ESI history fetch failed (market_history_unavailable_reason) or
    raised an ESI or DB error (with a WARNING). No history day on or before
    target_date is permanent. An unreadable ESI history row is skipped and
    counted, never stored as 0. A programming error propagates.
    """
    from sqlalchemy.exc import SQLAlchemyError

    from eve_online_industry_tracker.application.market_analysis.market_history_service import _history_row_values
    from eve_online_industry_tracker.infrastructure.esi_client import ESI_REQUEST_ERRORS

    def _record(reason: str, *, is_transient: bool) -> None:
        if unavailable is not None:
            unavailable[int(type_id)] = reason
        if is_transient and transient is not None:
            transient.add(int(type_id))

    def _stored_price() -> Optional[float]:
        row = (
            app_session.query(MarketHistoryModel)
            .filter(
                MarketHistoryModel.type_id == type_id,
                MarketHistoryModel.region_id == region_id,
                MarketHistoryModel.date <= target_date,
            )
            .order_by(MarketHistoryModel.date.desc())
            .first()
        )
        return float(row.close) if row and row.close and float(row.close) > 0 else None

    price = _stored_price()
    if price is not None:
        return price

    fetch_failure: Optional[str] = None
    try:
        history_rows = esi_service.get_market_history([type_id], region_id=region_id)
        if not isinstance(history_rows, dict) or int(type_id) not in history_rows:
            # ESIService leaves a type whose fetch failed out of the result.
            reader = getattr(esi_service, "market_history_unavailable_reason", None)
            fetch_failure = (reader(int(type_id), region_id=region_id) if reader is not None else None) or (
                f"ESI returned no market history for type_id={type_id}")
        items = (history_rows or {}).get(int(type_id), []) if isinstance(history_rows, dict) else []
        skipped = 0
        for item in items if isinstance(items, list) else []:
            values = _history_row_values(item)
            if values is None:
                skipped += 1
                continue
            date_str = values.pop("date")
            existing = (
                app_session.query(MarketHistoryModel)
                .filter(
                    MarketHistoryModel.type_id == type_id,
                    MarketHistoryModel.region_id == region_id,
                    MarketHistoryModel.date == date_str,
                )
                .first()
            )
            if existing:
                for field_name, value in values.items():
                    setattr(existing, field_name, value)
            else:
                app_session.add(MarketHistoryModel(type_id=type_id, region_id=region_id, date=date_str, **values))
        if skipped:
            logging.warning(
                "Market history for type_id=%s: skipped %d unreadable history row(s) "
                "(None, no date, or a price, volume or order count that is not a number)", type_id, skipped,
            )
        app_session.flush()
    except ESI_REQUEST_ERRORS + (SQLAlchemyError,) as e:
        logging.warning("Failed to fetch market history for type_id=%s: %s", type_id, e)
        _record(f"market history fetch failed: {e}", is_transient=True)
        return None

    price = _stored_price()
    if price is None:
        if fetch_failure is not None:
            _record(fetch_failure, is_transient=True)
        else:
            _record(f"no market history price on or before {target_date} for type_id={type_id}", is_transient=False)
    return price


def backfill_historical_market_costs(
    *,
    app_session: Any,
    sde_session: Any,
    esi_service: Any,
    job_model: Any,
    owner_filter: dict,
    region_id: int = 10000002,
) -> None:
    """Re-estimate material costs for jobs that used market_snapshot_estimate at 0% coverage.

    For each such job, fetches daily average prices from ESI market history for the
    job's start_date and stores a per-material breakdown in historical_input_costs.
    This makes cost estimates for jobs that completed while the tool was offline
    far more accurate than using current ESI prices.

    A material whose price failed transiently (its ESI history fetch failed)
    leaves the whole job as it was, with a WARNING: no partial lower-bound cost
    is written as final, and the next refresh retries it. A material with no
    history day on or before start_date (permanent) is in historical_input_costs
    with unit_cost None and its unknown_reason, never counted as 0 ISK, and
    materials_cost_lower_bound_reason says the materials cost is a lower bound.
    A job with no priced material is left as it was. ESI, data and DB errors
    skip the job with a WARNING; a programming error propagates.
    """
    from sqlalchemy.exc import SQLAlchemyError

    from eve_online_industry_tracker.infrastructure.esi_client import ESI_REQUEST_ERRORS

    jobs_to_fix = (
        app_session.query(job_model)
        .filter_by(**owner_filter)
        .filter(
            job_model.build_cost_source.like("market_snapshot_estimate%"),
            job_model.historical_material_coverage_fraction == 0.0,
            job_model.historical_input_costs.in_(["null", "{}", "", None]),
            job_model.blueprint_type_id != None,  # noqa: E711
        )
        .all()
    )

    if not jobs_to_fix:
        return

    for job in jobs_to_fix:
        try:
            start_date = str(getattr(job, "start_date", "") or "")[:10]
            if not start_date or len(start_date) < 10:
                continue

            blueprint_type_id = int(getattr(job, "blueprint_type_id", 0) or 0)
            product_type_id = int(getattr(job, "product_type_id", 0) or 0)
            runs = int(getattr(job, "runs", 1) or 1)
            me = int(getattr(job, "blueprint_material_efficiency", 0) or 0)
            output_quantity = int(getattr(job, "output_quantity", 0) or 0)

            if not blueprint_type_id or not product_type_id or output_quantity <= 0:
                continue

            material_type_ids = industry_job_material_type_ids(
                sde_session=sde_session,
                blueprint_type_id=blueprint_type_id,
            )
            if not material_type_ids:
                continue

            bp = sde_session.query(Blueprints).filter_by(blueprintTypeID=blueprint_type_id).first()
            if bp is None:
                continue
            mfg = _get_mfg_activity(getattr(bp, "activities", None))
            if not mfg:
                continue
            materials = mfg.get("materials") or []

            material_reduction = max(0.0, min(float(me) / 100.0, 0.99))
            total_materials_cost = 0.0
            input_cost_details: dict[str, Any] = {}
            priced_quantity = 0
            total_quantity = 0
            unpriced: dict[int, str] = {}
            transient: set[int] = set()

            for m in materials:
                if not isinstance(m, dict):
                    continue
                mat_type_id = _safe_int(m.get("typeID"))
                base_qty = _safe_int(m.get("quantity"))
                if not mat_type_id or not base_qty or base_qty <= 0:
                    continue

                adjusted_qty = _round_material_quantity(
                    float(base_qty) * float(runs) * max(0.0, 1.0 - material_reduction),
                    minimum_quantity=int(runs) if base_qty > 0 else 0,
                )
                total_quantity += adjusted_qty

                unit_price = _get_or_fetch_market_price_on_date(
                    type_id=mat_type_id,
                    target_date=start_date,
                    app_session=app_session,
                    esi_service=esi_service,
                    region_id=region_id,
                    unavailable=unpriced,
                    transient=transient,
                )
                if unit_price is None or unit_price <= 0:
                    # Unknown, never 0 ISK: the material is named with why.
                    input_cost_details[str(mat_type_id)] = {
                        "unit_cost": None,
                        "quantity": int(adjusted_qty),
                        "source": "historical_market_daily_average",
                        "reference_date": start_date,
                        "unknown_reason": unpriced.get(int(mat_type_id))
                        or f"no market history price on or before {start_date}",
                    }
                    continue

                line_cost = float(adjusted_qty) * float(unit_price)
                total_materials_cost += line_cost
                priced_quantity += adjusted_qty
                input_cost_details[str(mat_type_id)] = {
                    "unit_cost": float(unit_price),
                    "quantity": int(adjusted_qty),
                    "source": "historical_market_daily_average",
                    "reference_date": start_date,
                }

            unknown_details = [d for d in input_cost_details.values() if d.get("unit_cost") is None]
            unknown_reasons = sorted({str(d.get("unknown_reason")) for d in unknown_details})
            if transient:
                # A retry can price it: nothing is written, the job keeps its
                # estimate and the next refresh's backfill picks it up again.
                logging.warning(
                    "backfill_historical_market_costs: job_id=%s not backfilled, %s material(s) have no "
                    "historical price for now; the job keeps its cost estimate and is retried at the next "
                    "refresh (%s)",
                    getattr(job, "job_id", "?"), len(transient),
                    "; ".join(sorted({unpriced[t] for t in transient if t in unpriced})),
                )
                continue
            if priced_quantity <= 0:
                if unknown_reasons:
                    # No material priced: the job keeps its estimate.
                    logging.warning(
                        "backfill_historical_market_costs: job_id=%s has no historical material price, "
                        "its cost estimate is kept (%s)",
                        getattr(job, "job_id", "?"), "; ".join(unknown_reasons),
                    )
                continue
            lower_bound_reason: Optional[str] = None
            if unknown_details:
                lower_bound_reason = (
                    f"materials cost is a lower bound: {len(unknown_details)} material(s) unpriced "
                    f"({'; '.join(unknown_reasons)})"
                )
                logging.warning("backfill_historical_market_costs: job_id=%s %s",
                                getattr(job, "job_id", "?"), lower_bound_reason)

            coverage = float(priced_quantity) / float(total_quantity) if total_quantity > 0 else 0.0
            job_cost = float(getattr(job, "cost", 0) or 0)
            copy_cost = float(getattr(job, "copy_cost", 0) or 0)
            invention_cost = float(getattr(job, "invention_cost", 0) or 0)
            total_build_cost = total_materials_cost + job_cost + copy_cost + invention_cost

            job.materials_cost = total_materials_cost
            job.historical_materials_cost = total_materials_cost
            job.historical_material_cost_source = "historical_market_daily_average"
            job.historical_material_coverage_fraction = coverage
            job.historical_input_costs = input_cost_details
            job.materials_cost_lower_bound_reason = lower_bound_reason
            job.total_build_cost = total_build_cost
            job.unit_build_cost = total_build_cost / float(output_quantity)
            job.build_cost_source = "historical_market_price_on_start_date"

        except ESI_REQUEST_ERRORS + (SQLAlchemyError,) as e:
            logging.warning(
                "backfill_historical_market_costs: skipping job_id=%s: %s",
                getattr(job, "job_id", "?"),
                e,
            )
            continue

    try:
        app_session.commit()
    except SQLAlchemyError as e:
        logging.warning("backfill_historical_market_costs: commit failed: %s", e)
        app_session.rollback()
