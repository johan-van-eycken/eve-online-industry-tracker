from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
import logging
import math
from typing import Any, Iterable, Optional

from sqlalchemy import desc

from eve_online_industry_tracker.infrastructure.models import Blueprints, MarketHistoryModel


ASSET_SOURCE_INDUSTRY_BUILD = "industry_build"
ASSET_SOURCE_MARKET_BUY = "market_buy"
ASSET_SOURCE_UNKNOWN = "unknown"

REFERENCE_TYPE_INDUSTRY_JOB = "industry_job"
REFERENCE_TYPE_WALLET_TRANSACTION = "wallet_transaction"

_INVENTION_SOURCE_BLUEPRINT_CACHE: dict[int, dict[str, Any]] | None = None


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
    except Exception:
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


def _invention_probability(invention: dict[str, Any]) -> Optional[float]:
    probability = _safe_float(invention.get("probability"))
    if probability is not None and probability > 0:
        return probability
    products = invention.get("products") or []
    raw_probs: list[float] = []
    for product in products:
        if not isinstance(product, dict):
            continue
        prob = _safe_float(product.get("probability"))
        if prob is not None and prob > 0:
            raw_probs.append(float(prob))
    if not raw_probs:
        return None
    if len(raw_probs) == 1:
        return raw_probs[0]
    if max(raw_probs) - min(raw_probs) < 1e-9:
        return raw_probs[0]
    return None


def _invention_source_blueprint_by_target_type(sde_session: Any) -> dict[int, dict[str, Any]]:
    global _INVENTION_SOURCE_BLUEPRINT_CACHE
    if _INVENTION_SOURCE_BLUEPRINT_CACHE is not None:
        return _INVENTION_SOURCE_BLUEPRINT_CACHE

    mapping: dict[int, dict[str, Any]] = {}
    for blueprint in sde_session.query(Blueprints).all():
        activities = getattr(blueprint, "activities", None)
        if not isinstance(activities, dict):
            continue
        invention = activities.get("invention")
        if not isinstance(invention, dict):
            continue
        for product in invention.get("products") or []:
            if not isinstance(product, dict):
                continue
            target_blueprint_type_id = _safe_int(product.get("typeID"))
            if not target_blueprint_type_id or target_blueprint_type_id <= 0:
                continue
            if int(target_blueprint_type_id) in mapping:
                continue
            mapping[int(target_blueprint_type_id)] = {
                "source_blueprint_type_id": int(getattr(blueprint, "blueprintTypeID", 0) or 0),
                "invention": invention,
            }
    _INVENTION_SOURCE_BLUEPRINT_CACHE = mapping
    return mapping


def _target_blueprint_runs_per_success(sde_session: Any, *, target_blueprint_type_id: int) -> int:
    blueprint = sde_session.query(Blueprints).filter_by(blueprintTypeID=int(target_blueprint_type_id)).first()
    if blueprint is None:
        return 1
    runs = _safe_int(getattr(blueprint, "maxProductionLimit", None)) or 1
    return max(1, int(runs))


def _estimate_invention_material_cost(
    *,
    sde_session: Any,
    target_blueprint_type_id: int,
    market_price_map: dict[int, float],
    owned_cost_map: dict[int, float] | None = None,
) -> Optional[dict[str, Any]]:
    mapping = _invention_source_blueprint_by_target_type(sde_session)
    payload = mapping.get(int(target_blueprint_type_id))
    if not isinstance(payload, dict):
        return None
    invention = payload.get("invention")
    if not isinstance(invention, dict):
        return None
    material_cost = 0.0
    has_material_price = False
    for material in invention.get("materials") or []:
        if not isinstance(material, dict):
            continue
        material_type_id = _safe_int(material.get("typeID"))
        quantity = _safe_int(material.get("quantity"))
        if not material_type_id or not quantity or quantity <= 0:
            continue
        unit_price = (owned_cost_map or {}).get(int(material_type_id)) or market_price_map.get(int(material_type_id))
        if unit_price is None:
            continue
        has_material_price = True
        material_cost += float(quantity) * float(unit_price)
    if not has_material_price:
        return None
    return {
        "material_cost": float(material_cost),
        "probability": _invention_probability(invention),
        "target_runs_per_success": _target_blueprint_runs_per_success(
            sde_session,
            target_blueprint_type_id=int(target_blueprint_type_id),
        ),
    }


def build_invention_cost_per_run_by_blueprint_type(
    *,
    jobs: Iterable[dict[str, Any]] | None,
    sde_session: Any,
    market_price_map: dict[int, float],
) -> dict[int, dict[str, Any]]:
    rows_by_target_blueprint_type: dict[int, dict[str, Any]] = {}
    for job in jobs or []:
        if not isinstance(job, dict):
            continue
        raw = job.get("raw") if isinstance(job.get("raw"), dict) else job
        activity_id = _safe_int((raw or {}).get("activity_id"))
        if activity_id != 8:
            continue
        target_blueprint_type_id = _safe_int(job.get("product_type_id"))
        if not target_blueprint_type_id or target_blueprint_type_id <= 0:
            continue
        licensed_runs = _safe_int((raw or {}).get("licensed_runs")) or _target_blueprint_runs_per_success(
            sde_session,
            target_blueprint_type_id=int(target_blueprint_type_id),
        )
        licensed_runs = max(1, int(licensed_runs))
        estimate = _estimate_invention_material_cost(
            sde_session=sde_session,
            target_blueprint_type_id=int(target_blueprint_type_id),
            market_price_map=market_price_map,
        )
        invention_material_cost = float((estimate or {}).get("material_cost") or 0.0)
        total_cost = float(invention_material_cost) + float(_safe_float(job.get("cost")) or 0.0)
        completed_date = _parse_date(job.get("completed_date") or job.get("end_date"))
        current = rows_by_target_blueprint_type.get(int(target_blueprint_type_id))
        if current is not None:
            current_dt = _parse_date(current.get("completed_date") or current.get("end_date"))
            if current_dt is not None and completed_date is not None and current_dt >= completed_date:
                continue
        rows_by_target_blueprint_type[int(target_blueprint_type_id)] = {
            "total_cost": float(total_cost),
            "unit_cost_per_run": float(total_cost) / float(licensed_runs),
            "source": "actual_invention_job",
            "completed_date": job.get("completed_date") or job.get("end_date"),
        }

    return rows_by_target_blueprint_type


def resolve_industry_job_cost_snapshot(
    *,
    job: Any,
    sde_session: Any,
    market_price_map: dict[int, float] | None,
    invention_unit_cost_per_run: float | None = None,
    invention_cost_source: str | None = None,
    blueprint_provenance: dict[str, Any] | None = None,
    owned_input_unit_cost_by_type_id: dict[int, dict[str, Any]] | None = None,
) -> dict[str, Any]:
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
    if invention_cost is None and activity_id in {None, 1} and blueprint_type_id and market_price_map:
        if invention_unit_cost_per_run is not None and invention_unit_cost_per_run > 0:
            invention_cost = float(invention_unit_cost_per_run) * float(runs)
            resolved_invention_source = invention_cost_source or "actual_invention_job"
        else:
            _invention_owned_cost_map: dict[int, float] | None = None
            if owned_input_unit_cost_by_type_id:
                _invention_owned_cost_map = {
                    int(tid): float(uc)
                    for tid, payload in owned_input_unit_cost_by_type_id.items()
                    if (uc := _safe_float((payload or {}).get("unit_cost"))) and uc > 0
                } or None
            estimate = _estimate_invention_material_cost(
                sde_session=sde_session,
                target_blueprint_type_id=int(blueprint_type_id),
                market_price_map=market_price_map,
                owned_cost_map=_invention_owned_cost_map,
            )
            probability = _safe_float((estimate or {}).get("probability"))
            target_runs_per_success = _safe_int((estimate or {}).get("target_runs_per_success")) or 1
            material_cost = _safe_float((estimate or {}).get("material_cost"))
            if material_cost is not None and material_cost > 0 and probability is not None and probability > 0:
                invention_cost_per_run = (float(material_cost) / float(probability)) / float(max(1, target_runs_per_success))
                invention_cost = float(invention_cost_per_run) * float(runs)
                resolved_invention_source = "expected_invention_material_cost"

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
        except Exception:
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
        except Exception:
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
        except Exception:
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
    except Exception:
        return None


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
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
        except Exception:
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
) -> Optional[float]:
    """Return the closest daily average price on or before target_date for type_id.

    Checks the market_history DB first; fetches from ESI and caches if missing.
    """
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
    if row and row.close and float(row.close) > 0:
        return float(row.close)

    try:
        history_rows = esi_service.get_market_history([type_id], region_id=region_id)
        items = (history_rows or {}).get(int(type_id), [])
        for item in items:
            if not isinstance(item, dict):
                continue
            date_str = str(item.get("date", "")).strip()
            if not date_str:
                continue
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
                existing.close = float(item.get("average", item.get("close", 0)))
                existing.high = float(item.get("highest", 0))
                existing.low = float(item.get("lowest", 0))
                existing.volume = int(item.get("volume", 0))
                existing.order_count = int(item.get("order_count", 0))
            else:
                app_session.add(MarketHistoryModel(
                    type_id=type_id,
                    region_id=region_id,
                    date=date_str,
                    close=float(item.get("average", item.get("close", 0))),
                    high=float(item.get("highest", 0)),
                    low=float(item.get("lowest", 0)),
                    volume=int(item.get("volume", 0)),
                    order_count=int(item.get("order_count", 0)),
                ))
        app_session.flush()
    except Exception as e:
        logging.warning("Failed to fetch market history for type_id=%s: %s", type_id, e)
        return None

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
    """
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
                )
                if unit_price is None or unit_price <= 0:
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

            if not input_cost_details:
                continue

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
            job.total_build_cost = total_build_cost
            job.unit_build_cost = total_build_cost / float(output_quantity)
            job.build_cost_source = "historical_market_price_on_start_date"

        except Exception as e:
            logging.warning(
                "backfill_historical_market_costs: skipping job_id=%s: %s",
                getattr(job, "job_id", "?"),
                e,
            )
            continue

    try:
        app_session.commit()
    except Exception as e:
        logging.warning("backfill_historical_market_costs: commit failed: %s", e)
        app_session.rollback()
