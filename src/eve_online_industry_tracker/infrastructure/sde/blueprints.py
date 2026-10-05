from __future__ import annotations

import math
from typing import Any
from typing import Iterable

from sqlalchemy import bindparam, text

from eve_online_industry_tracker.db_models import Blueprints, Types

from eve_online_industry_tracker.infrastructure.sde.types import get_type_data


def _enrich_type_entry(
    type_data_map: dict[int, dict[str, Any]],
    raw_type_id: Any,
    *,
    quantity: Any | None = None,
    level: Any | None = None,
    probability: Any | None = None,
) -> dict[str, Any] | None:
    try:
        type_id = int(raw_type_id)
    except Exception:
        return None

    enriched: dict[str, Any] = dict(type_data_map.get(type_id, {}))
    if not enriched:
        enriched = {"type_id": type_id}

    if quantity is not None:
        enriched["quantity"] = quantity
    if level is not None:
        enriched["level"] = level
    if probability is not None:
        enriched["probability"] = probability

    return enriched


def get_blueprint_manufacturing_data(
    session,
    language: str,
    blueprint_type_ids: Iterable[int] | None = None,
) -> dict[int, dict]:
    """Return manufacturing materials/products/skills and research times for blueprints.

    If `blueprint_type_ids` is provided, only those blueprint typeIDs are loaded.
    """

    q = (
        session.query(Blueprints)
        .join(Types, Types.id == Blueprints.blueprintTypeID)
        .filter(Types.published.is_(True))
    )
    if blueprint_type_ids is not None:
        ids = list({int(i) for i in blueprint_type_ids if i is not None})
        if not ids:
            return {}
        q = q.filter(Blueprints.blueprintTypeID.in_(ids))

    blueprints = q.all()
    if not blueprints:
        return {}

    blueprint_type_id_set: set[int] = set()
    material_type_ids: set[int] = set()
    product_type_ids: set[int] = set()
    skill_type_ids: set[int] = set()

    for bp in blueprints:
        blueprint_type_id_set.add(bp.blueprintTypeID)
        activities = bp.activities if isinstance(bp.activities, dict) else {}
        manufacturing = activities.get("manufacturing", {})
        reaction = activities.get("reaction", {})

        invention = activities.get("invention", {})

        for mat in manufacturing.get("materials", []):
            try:
                material_type_ids.add(int(mat["typeID"]))
            except Exception:
                continue

        for prod in manufacturing.get("products", []):
            try:
                product_type_ids.add(int(prod["typeID"]))
            except Exception:
                continue

        for skill in manufacturing.get("skills", []):
            try:
                skill_type_ids.add(int(skill["typeID"]))
            except Exception:
                continue

        if isinstance(reaction, dict):
            for mat in reaction.get("materials", []):
                try:
                    material_type_ids.add(int(mat["typeID"]))
                except Exception:
                    continue

            for prod in reaction.get("products", []):
                try:
                    product_type_ids.add(int(prod["typeID"]))
                except Exception:
                    continue

            for skill in reaction.get("skills", []):
                try:
                    skill_type_ids.add(int(skill["typeID"]))
                except Exception:
                    continue

        # Invention activity (T2 invention from T1 BPCs)
        if isinstance(invention, dict):
            for mat in invention.get("materials", []):
                try:
                    material_type_ids.add(int(mat["typeID"]))
                except Exception:
                    continue

            for prod in invention.get("products", []):
                try:
                    product_type_ids.add(int(prod["typeID"]))
                except Exception:
                    continue

            for skill in invention.get("skills", []):
                try:
                    skill_type_ids.add(int(skill["typeID"]))
                except Exception:
                    continue

    all_type_ids = list(material_type_ids | product_type_ids | blueprint_type_id_set | skill_type_ids)
    type_data_map = get_type_data(session, language, all_type_ids)

    result: dict[int, dict] = {}

    for bp in blueprints:
        activities = bp.activities if isinstance(bp.activities, dict) else {}
        manufacturing = activities.get("manufacturing", {})
        reaction = activities.get("reaction", {}) if isinstance(activities.get("reaction", {}), dict) else {}

        invention = activities.get("invention", {}) if isinstance(activities.get("invention", {}), dict) else {}

        materials = []
        for mat in manufacturing.get("materials", []):
            enriched = _enrich_type_entry(
                type_data_map,
                mat.get("typeID"),
                quantity=mat.get("quantity"),
            )
            if enriched is not None:
                materials.append(enriched)

        products = []
        for prod in manufacturing.get("products", []):
            enriched = _enrich_type_entry(
                type_data_map,
                prod.get("typeID"),
                quantity=prod.get("quantity"),
            )
            if enriched is not None:
                products.append(enriched)

        skills = []
        for skill in manufacturing.get("skills", []):
            enriched = _enrich_type_entry(
                type_data_map,
                skill.get("typeID"),
                level=skill.get("level"),
            )
            if enriched is not None:
                skills.append(enriched)

        reaction_materials = []
        for mat in (reaction.get("materials", []) or []):
            if not isinstance(mat, dict):
                continue
            enriched = _enrich_type_entry(
                type_data_map,
                mat.get("typeID"),
                quantity=mat.get("quantity"),
            )
            if enriched is not None:
                reaction_materials.append(enriched)

        reaction_products = []
        for prod in (reaction.get("products", []) or []):
            if not isinstance(prod, dict):
                continue
            enriched = _enrich_type_entry(
                type_data_map,
                prod.get("typeID"),
                quantity=prod.get("quantity"),
            )
            if enriched is not None:
                reaction_products.append(enriched)

        reaction_skills = []
        for skill in (reaction.get("skills", []) or []):
            if not isinstance(skill, dict):
                continue
            enriched = _enrich_type_entry(
                type_data_map,
                skill.get("typeID"),
                level=skill.get("level"),
            )
            if enriched is not None:
                reaction_skills.append(enriched)

        invention_materials = []
        for mat in (invention.get("materials", []) or []):
            if not isinstance(mat, dict):
                continue
            enriched = _enrich_type_entry(
                type_data_map,
                mat.get("typeID"),
                quantity=mat.get("quantity"),
            )
            if enriched is not None:
                invention_materials.append(enriched)

        invention_products = []
        for prod in (invention.get("products", []) or []):
            if not isinstance(prod, dict):
                continue
            enriched = _enrich_type_entry(
                type_data_map,
                prod.get("typeID"),
                probability=prod.get("probability"),
                quantity=prod.get("quantity"),
            )
            if enriched is not None:
                invention_products.append(enriched)

        invention_probability = invention.get("probability", None)
        if invention_probability is None:
            # Some SDE exports store invention chance on the product entry instead of
            # as a top-level invention.probability.
            raw_probs: list[float] = []
            for prod in (invention.get("products", []) or []):
                if not isinstance(prod, dict):
                    continue
                p = prod.get("probability")
                if p is None:
                    continue
                try:
                    pf = float(p)
                except Exception:
                    continue
                if pf > 0:
                    raw_probs.append(pf)

            if len(raw_probs) == 1:
                invention_probability = raw_probs[0]
            elif len(raw_probs) > 1:
                try:
                    if max(raw_probs) - min(raw_probs) < 1e-9:
                        invention_probability = raw_probs[0]
                except Exception:
                    pass

        invention_skills = []
        for skill in (invention.get("skills", []) or []):
            if not isinstance(skill, dict):
                continue
            enriched = _enrich_type_entry(
                type_data_map,
                skill.get("typeID"),
                level=skill.get("level"),
            )
            if enriched is not None:
                invention_skills.append(enriched)

        type_id = bp.blueprintTypeID
        type_data = type_data_map.get(type_id, {})
        blueprint_type_payload = dict(type_data) if type_data else {"type_id": type_id}

        result[bp.blueprintTypeID] = {
            "blueprint": blueprint_type_payload,
            "type_id": type_id,
            "type_name": type_data.get("type_name", ""),
            "type_meta_group_id": type_data.get("meta_group_id"),
            "type_meta_group_name": type_data.get("meta_group_name", ""),
            "type_meta_group_color": type_data.get("meta_group_color"),
            "type_meta_group_icon_id": type_data.get("meta_group_icon_id"),
            "group_id": type_data.get("group_id"),
            "group_name": type_data.get("group_name", ""),
            "category_id": type_data.get("category_id"),
            "category_name": type_data.get("category_name", ""),
            "max_production_limit": int(getattr(bp, "maxProductionLimit", 0) or 0),
            "manufacturing": {
                "time": manufacturing.get("time", 0),
                "materials": materials,
                "products": products,
                "skills": skills,
            },
            "reaction": {
                "time": reaction.get("time", 0),
                "materials": reaction_materials,
                "products": reaction_products,
                "skills": reaction_skills,
            },
            "invention": {
                "time": invention.get("time", 0),
                "probability": invention_probability,
                "materials": invention_materials,
                "products": invention_products,
                "skills": invention_skills,
            },
            "research_time": {
                "time": activities.get("research_time", {}).get("time", 0),
            },
            "research_material": {
                "time": activities.get("research_material", {}).get("time", 0),
            },
            "copying": activities.get("copying", {}).get("time", 0),
        }

    return result


def get_invention_source_blueprint_ids(
    session,
    invented_blueprint_type_ids: Iterable[int],
) -> dict[int, int]:
    """{invented blueprint type_id: T1 source blueprint type_id} in one query.

    The invention activity lives on the source blueprint, with the blueprints
    it invents into as its products, so the source is found by searching the
    invention products. `activities` is a JSON text column; SQLite's
    json_each expands the products list so this is a single query instead of
    loading and scanning every blueprint in Python. Ids that nothing invents
    into are simply absent.
    The lowest source blueprint id wins when several blueprints invent into
    the same one.
    """
    ids = sorted({int(i) for i in invented_blueprint_type_ids if i is not None})
    if not ids:
        return {}
    rows = session.execute(
        text(
            "SELECT CAST(json_extract(p.value, '$.typeID') AS INTEGER) AS invented, "
            "b.blueprintTypeID AS source "
            "FROM blueprints b, "
            "json_each(json_extract(b.activities, '$.invention.products')) p "
            "WHERE CAST(json_extract(p.value, '$.typeID') AS INTEGER) IN :ids "
            "ORDER BY b.blueprintTypeID"
        ).bindparams(bindparam("ids", expanding=True)),
        {"ids": ids},
    ).all()
    result: dict[int, int] = {}
    for invented, source in rows:
        result.setdefault(int(invented), int(source))
    return result


def compute_optimal_me(blueprint_type_id: int, session, runs: int = 1) -> int:
    """Return the ME level where no material quantity further decreases.

    For each material in the blueprint's manufacturing activity, find the last
    ME level (1–10) that still reduces the required quantity. The optimal ME
    is the maximum of these per-material values. If no material ever benefits
    from ME research (e.g. qty=1 for all materials), returns 0.

    Algorithm:
        For ME in 1..10, compute me_adjusted_batch_quantity(qty, runs, ME) =
        max(runs, ceil(qty * runs * (1 - 0.01 * ME))) -- one ceiling over the
        whole batch, never fewer than one unit per run -- and compare it to the
        previous level. The last ME that yields a reduction is the material's
        optimal ME. optimal_ME = max across all materials.

    `runs` is the planned batch size (default 1, a single run).
    """
    blueprint = session.query(Blueprints).filter(
        Blueprints.blueprintTypeID == int(blueprint_type_id)
    ).first()
    if blueprint is None:
        return 0

    activities = blueprint.activities if isinstance(blueprint.activities, dict) else {}
    manufacturing = activities.get("manufacturing", {}) if isinstance(activities.get("manufacturing"), dict) else {}
    materials = manufacturing.get("materials", []) or []

    if not materials:
        return 0

    quantities: list[int] = []
    for mat in materials:
        try:
            quantities.append(int(mat.get("quantity", 0)))
        except (TypeError, ValueError):
            continue
    return optimal_me_for_quantities(quantities, runs=runs)


def _me_reduction(me: int) -> float:
    """ME level as the producer's combined material-reduction fraction.

    IndustryService._combine_reductions([me / 100.0]) for the ME term alone:
    1 - (1 - me/100), capped at 0.99. Mirrored, not simplified to me/100, so
    the float result is bit-for-bit the producer's.
    """
    fraction = int(me) / 100.0
    if fraction <= 0.0:
        return 0.0
    return max(0.0, min(1.0 - max(0.0, 1.0 - fraction), 0.99))


def me_adjusted_batch_quantity(base_qty_per_run: int, runs: int, me: int) -> int:
    """Units of one material for a whole batch at a blueprint ME level.

    Mirrors the producer's "Material adjustment" loop (application/industry/
    service.py, `adjusted_total = self._round_material_quantity(...)`): one
    ceiling over the whole batch, never per run, and never fewer than one
    unit per run:

        max(runs, ceil(base_qty_per_run * runs * (1 - reduction)))

    0 when base_qty_per_run or runs is not positive.
    """
    return reduced_batch_quantity(base_qty_per_run, runs, _me_reduction(me))


def reduced_batch_quantity(base_qty_per_run: int, runs: int, material_reduction: float) -> int:
    """me_adjusted_batch_quantity for an already-combined material reduction.

    `material_reduction` is the producer's combined fraction, i.e.
    IndustryService._combine_reductions([me / 100, structure, rig, ...]) --
    the caller combines, so the factors and their order stay the producer's.
    Same rounding as the producer's _round_material_quantity with a per-run
    floor: max(runs, ceil(base_qty_per_run * runs * (1 - reduction))).
    0 when base_qty_per_run or runs is not positive.
    """
    base = int(base_qty_per_run)
    runs = int(runs)
    if base <= 0 or runs <= 0:
        return 0
    raw = float(base * runs) * max(0.0, 1.0 - float(material_reduction))
    if raw <= 0:
        return 0
    return max(runs, int(math.ceil(raw)))


def me_adjusted_quantity(base_qty: int, me: int) -> int:
    """Per-run material quantity at a blueprint ME level (a one-run batch).

    qty(ME) = ceil(base_qty * (1 - 0.01 * ME)). For a batch of more than one
    run use me_adjusted_batch_quantity: the ceiling is applied to the batch
    total, so per-run rounding times runs overstates the quantity.
    """
    return me_adjusted_batch_quantity(base_qty, 1, me)


def optimal_me_for_quantities(quantities: Iterable[int], *, runs: int = 1) -> int:
    """Highest ME level (0-10) that still reduces any of these base quantities
    for a batch of `runs` runs.

    For each quantity, the last ME in 1..10 whose batch quantity is lower
    than the level below it; the maximum over quantities. 0 when none benefit.
    """
    per_material_optimal: list[int] = []
    for qty in quantities:
        if qty <= 0:
            continue
        last_useful_me = 0
        prev_qty = me_adjusted_batch_quantity(qty, runs, 0)
        for me in range(1, 11):
            curr_qty = me_adjusted_batch_quantity(qty, runs, me)
            if curr_qty < prev_qty:
                last_useful_me = me
            prev_qty = curr_qty
        per_material_optimal.append(last_useful_me)

    return max(per_material_optimal) if per_material_optimal else 0


def compute_optimal_te(
    blueprint_type_id: int,
    session,
    time_savings_threshold_pct: float = 1.0,
) -> int:
    """Return the TE level where per-level time savings fall below threshold_pct%.

    Iterates TE levels 1..10. When the time saved by going from TE-1 to TE
    falls below ``time_savings_threshold_pct``% of the base manufacturing time,
    returns TE-1 (the last level still worth researching). Returns 10 if the
    threshold is never reached within 1..10.

    time(TE) = ceil(base_time * (1 - 0.01 * TE))
    saved_pct(TE) = (time(TE-1) - time(TE)) / base_time * 100
    """
    blueprint = session.query(Blueprints).filter(
        Blueprints.blueprintTypeID == int(blueprint_type_id)
    ).first()
    if blueprint is None:
        return 0

    activities = blueprint.activities if isinstance(blueprint.activities, dict) else {}
    manufacturing = activities.get("manufacturing", {}) if isinstance(activities.get("manufacturing"), dict) else {}
    base_time = manufacturing.get("time", 0)

    try:
        base_time = int(base_time)
    except (TypeError, ValueError):
        return 0

    if base_time <= 0:
        return 0

    prev_time = base_time  # TE=0: ceil(base_time * 1.0) = base_time
    for te in range(1, 11):
        curr_time = math.ceil(base_time * (1.0 - 0.01 * te))
        saved_pct = (prev_time - curr_time) / base_time * 100.0
        if saved_pct < time_savings_threshold_pct:
            return te - 1  # last level worth researching is the previous one
        prev_time = curr_time

    return 10  # threshold never reached within TE 1–10
