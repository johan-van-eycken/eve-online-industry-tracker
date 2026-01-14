from __future__ import annotations

import math
import json
import datetime
from typing import Any, Dict, List

from sqlalchemy import bindparam, text

from classes.database_models import (
    CorporationStructuresModel,
    MapSolarSystems,
    NpcCorporations,
    NpcStations,
    PublicStructuresModel,
    StationOperations,
)

from flask_app.services.sde_localization import parse_localized


def _as_fraction(v: Any) -> float:
    """Normalize a stored bonus to a fraction.

    Some older rows stored percentages (e.g. 15.0 for 15%). We normalize here
    for safety in calculations.
    """

    try:
        f = float(v or 0.0)
    except Exception:
        return 0.0

    # Heuristic: values are expected as fractions (0.15 == 15%).
    # Legacy rows sometimes stored percentages (15.0 == 15%).
    # Treat 1.0 as percent too (1% is common; 100% bonuses are not).
    if f >= 1.0:
        f = f / 100.0
    return max(0.0, min(f, 1.0))


def _infer_rig_group_label_from_products(products: list[dict]) -> str:
    """Map blueprint outputs to the rig-group labels used by rig effects.

    This is a best-effort heuristic based on the manufactured product type.
    """

    if not products:
        return "All"

    p0 = next((p for p in products if isinstance(p, dict)), None) or {}
    cat = str(p0.get("category_name") or "").strip().lower()
    grp = str(p0.get("group_name") or "").strip().lower()

    # Reactions
    if "reaction" in grp or "reaction" in cat:
        if "biochemical" in grp or "biochemical" in cat:
            return "Biochemical Reactions"
        if "composite" in grp or "composite" in cat:
            return "Composite Reactions"
        if "hybrid" in grp or "hybrid" in cat:
            return "Hybrid Reactions"
        return "Biochemical Reactions"

    # Structures
    if cat == "structure" or "structure" in grp:
        return "Structures"

    # Ammo & charges
    if cat in {"charge", "charges"} or "ammo" in grp or "charge" in grp:
        return "Ammo & Charges"

    # Drones
    if cat == "drone" or "drone" in grp:
        return "Drones"

    # Modules
    if cat == "module" or "module" in grp:
        return "Modules"

    # Components
    if "component" in grp:
        if "capital" in grp:
            return "Capital Components"
        if "advanced" in grp or "adv" in grp:
            return "Advanced Components"
        return "Advanced Components"

    # Ships
    if cat == "ship" or "ship" in grp:
        advanced_tokens = [
            "assault",
            "interceptor",
            "interdictor",
            "covert",
            "strategic",
            "command",
            "marauder",
            "black ops",
            "logistics",
            "heavy assault",
            "recon",
            "electronic attack",
            "stealth bomber",
            "tactical destroyer",
        ]

        is_advanced = any(t in grp for t in advanced_tokens) or "t2" in grp or "t3" in grp

        if "capital" in grp or "supercarrier" in grp or "titan" in grp or "dread" in grp or "carrier" in grp:
            return "Capital Ships"

        # Size buckets (best-effort)
        small = ["frigate", "destroyer"]
        medium = ["cruiser", "battlecruiser"]
        large = ["battleship"]

        if any(x in grp for x in small):
            return "Advanced Small Ships" if is_advanced else "Basic Small Ships"
        if any(x in grp for x in medium):
            return "Advanced Medium Ships" if is_advanced else "Basic Medium Ships"
        if any(x in grp for x in large):
            return "Advanced Large Ships" if is_advanced else "Basic Large Ships"

        # Unknown ship group
        return "All Ships"

    return "All"


def _get_trained_skill_level(char_skills: list[dict], *, skill_name: str) -> int:
    if not char_skills or not skill_name:
        return 0
    wanted = str(skill_name).strip().lower()
    for s in char_skills:
        if not isinstance(s, dict):
            continue
        name = str(s.get("skill_name") or "").strip().lower()
        if name != wanted:
            continue
        try:
            return int(s.get("trained_skill_level") or 0)
        except Exception:
            return 0
    return 0


def _manufacturing_time_multiplier_from_skills(char_skills: list[dict]) -> float:
    """Return manufacturing time multiplier from character skills.

        Mirrors the client concept of "Skills and implants" for manufacturing duration.
        This function implements the skill portion:
      - Industry: -4% manufacturing time per level
      - Advanced Industry: -3% manufacturing time per level
    """

    industry_level = _get_trained_skill_level(char_skills, skill_name="Industry")
    advanced_industry_level = _get_trained_skill_level(char_skills, skill_name="Advanced Industry")

    industry_mult = 1.0 - (0.04 * float(max(0, min(industry_level, 5))))
    adv_mult = 1.0 - (0.03 * float(max(0, min(advanced_industry_level, 5))))

    mult = industry_mult * adv_mult
    return max(0.0, min(mult, 1.0))


_IMPLANT_ATTR_MANUFACTURING_TIME_BONUS = 440


def _safe_json_loads(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return None
    return value


def _manufacturing_time_multiplier_from_implants(
    sde_session,
    implant_type_ids: list[int] | None,
) -> tuple[float, list[dict]]:
    """Return (multiplier, details) for manufacturing time implants.

    Uses SDE dogma attribute `manufacturingTimeBonus` (id=440) which is stored
    as a negative percentage (e.g. -4.0 for -4% time).
    """

    if not implant_type_ids:
        return 1.0, []
    if sde_session is None:
        return 1.0, []

    ids = sorted({int(x) for x in implant_type_ids if x is not None and int(x) > 0})
    if not ids:
        return 1.0, []

    rows = (
        sde_session.execute(
            text("SELECT id, dogmaAttributes FROM typeDogma WHERE id IN :ids").bindparams(
                bindparam("ids", expanding=True)
            ),
            {"ids": ids},
        )
        .fetchall()
    )

    bonus_by_type_id: dict[int, float] = {}
    for type_id, attrs_raw in rows:
        attrs = _safe_json_loads(attrs_raw) or []
        if not isinstance(attrs, list):
            continue
        for a in attrs:
            if not isinstance(a, dict):
                continue
            aid = a.get("attributeID")
            if aid is None:
                continue
            try:
                if int(aid) != _IMPLANT_ATTR_MANUFACTURING_TIME_BONUS:
                    continue
            except Exception:
                continue
            val = a.get("value")
            if val is None:
                continue
            try:
                bonus_by_type_id[int(type_id)] = float(val)
            except Exception:
                continue

    mult = 1.0
    details: list[dict] = []
    for tid in ids:
        bonus_pct = bonus_by_type_id.get(int(tid))
        if bonus_pct is None:
            continue

        # Stored as negative % time (e.g. -4.0 means 4% faster).
        reduction = max(0.0, (-float(bonus_pct)) / 100.0)
        implant_mult = 1.0 - reduction
        implant_mult = max(0.0, min(implant_mult, 1.0))

        details.append(
            {
                "type_id": int(tid),
                "manufacturing_time_bonus_percent": float(bonus_pct),
                "time_reduction": float(reduction),
                "time_multiplier": float(implant_mult),
            }
        )
        mult *= implant_mult

    mult = max(0.0, min(float(mult), 1.0))
    return mult, details


def enrich_blueprints_for_character(
    blueprints: List[Dict[str, Any]],
    character,
    *,
    industry_profile: Any | None = None,
    manufacturing_system_cost_index: float | None = None,
    rig_payload: list[dict] | None = None,
    db_app_session: Any | None = None,
    db_sde_session: Any | None = None,
    language: str | None = None,
) -> List[Dict[str, Any]]:
    """Apply character-skill requirements and basic cost/value analysis.

    The blueprint payload includes a nested `manufacture_job` section for
    manufacturing-specific inputs and derived properties.
    """
    char_skills = (getattr(character, "skills", None) or {}).get("skills", []) or []
    mfg_skill_time_multiplier = _manufacturing_time_multiplier_from_skills(char_skills)
    mfg_skill_time_reduction = max(0.0, min(1.0, 1.0 - float(mfg_skill_time_multiplier)))

    # Implant-based time multiplier (best-effort).
    char_implants = getattr(character, "implants", None)
    if isinstance(char_implants, str):
        try:
            char_implants = json.loads(char_implants)
        except Exception:
            char_implants = None
    if not isinstance(char_implants, list):
        char_implants = []

    sde_session = db_sde_session
    if sde_session is None:
        try:
            sde_session = getattr(getattr(character, "_db_sde", None), "session", None)
        except Exception:
            sde_session = None

    lang = str(language or "en")

    def _build_location_maps(
        *,
        top_location_ids: list[int],
    ) -> tuple[
        dict[int, dict],
        dict[int, dict],
        dict[int, dict],
    ]:
        """Return (solar_system_map, npc_station_map, structure_map).

        Each map is keyed by the relevant ID.
        """

        if not top_location_ids:
            return {}, {}, {}

        solar_system_map: dict[int, dict] = {}
        npc_station_map: dict[int, dict] = {}
        structure_map: dict[int, dict] = {}

        # --- SDE lookups (solar systems + NPC stations) ---
        if sde_session is not None:
            try:
                ss_rows = (
                    sde_session.query(MapSolarSystems)
                    .filter(MapSolarSystems.id.in_(top_location_ids))
                    .all()
                )
                for ss in ss_rows or []:
                    solar_system_map[int(ss.id)] = {
                        "id": int(ss.id),
                        "name": parse_localized(getattr(ss, "name", None), lang) or str(ss.id),
                        "security_status": float(getattr(ss, "securityStatus", 0.0) or 0.0),
                    }
            except Exception:
                solar_system_map = {}

            try:
                station_rows = (
                    sde_session.query(NpcStations)
                    .filter(NpcStations.id.in_(top_location_ids))
                    .all()
                )
                owner_ids = {int(st.ownerID) for st in station_rows or [] if getattr(st, "ownerID", None) is not None}
                op_ids = {int(st.operationID) for st in station_rows or [] if getattr(st, "operationID", None) is not None}

                corp_map: dict[int, Any] = {}
                op_map: dict[int, Any] = {}
                if owner_ids:
                    corp_rows = sde_session.query(NpcCorporations).filter(NpcCorporations.id.in_(list(owner_ids))).all()
                    corp_map = {int(c.id): c for c in corp_rows or []}
                if op_ids:
                    op_rows = sde_session.query(StationOperations).filter(StationOperations.id.in_(list(op_ids))).all()
                    op_map = {int(o.id): o for o in op_rows or []}

                for st in station_rows or []:
                    owner = corp_map.get(int(st.ownerID)) if getattr(st, "ownerID", None) is not None else None
                    owner_name = parse_localized(getattr(owner, "name", None), lang) if owner else ""
                    station_name = owner_name

                    op = op_map.get(int(st.operationID)) if getattr(st, "operationID", None) is not None else None
                    if bool(getattr(st, "useOperationName", False)) and getattr(st, "operationID", None) is not None:
                        op_name = parse_localized(getattr(op, "operationName", None), lang) if op else ""
                        if op_name:
                            station_name = (station_name + " " + op_name).strip()

                    npc_station_map[int(st.id)] = {
                        "station_id": int(st.id),
                        "station_name": station_name or str(st.id),
                        "system_id": int(getattr(st, "solarSystemID", 0) or 0),
                        "owner_id": int(getattr(st, "ownerID", 0) or 0),
                        "owner_name": owner_name,
                    }
            except Exception:
                npc_station_map = {}

        # --- App DB lookups (structures) ---
        if db_app_session is not None:
            try:
                # Corporation structures (private) first.
                corp_rows = (
                    db_app_session.query(CorporationStructuresModel)
                    .filter(CorporationStructuresModel.structure_id.in_(top_location_ids))
                    .all()
                )
                for r in corp_rows or []:
                    sid = int(getattr(r, "structure_id"))
                    structure_map[sid] = {
                        "structure_id": sid,
                        "structure_name": getattr(r, "structure_name", None) or str(sid),
                        "system_id": (int(getattr(r, "system_id")) if getattr(r, "system_id", None) is not None else None),
                        "source": "corporation",
                    }

                pub_rows = (
                    db_app_session.query(PublicStructuresModel)
                    .filter(PublicStructuresModel.structure_id.in_(top_location_ids))
                    .all()
                )
                for r in pub_rows or []:
                    sid = int(getattr(r, "structure_id"))
                    # Prefer corporation structure naming if we already have it.
                    if sid in structure_map:
                        continue
                    structure_map[sid] = {
                        "structure_id": sid,
                        "structure_name": getattr(r, "structure_name", None) or str(sid),
                        "system_id": (int(getattr(r, "system_id")) if getattr(r, "system_id", None) is not None else None),
                        "source": "public",
                    }
            except Exception:
                structure_map = structure_map or {}

        # If we found stations/structures with a system_id, make sure their system is present in the map.
        system_ids: set[int] = set(solar_system_map.keys())
        system_ids.update(
            int(v.get("system_id") or 0)
            for v in npc_station_map.values()
            if v.get("system_id") is not None
        )
        system_ids.update(
            int(v.get("system_id") or 0)
            for v in structure_map.values()
            if v.get("system_id") is not None
        )
        system_ids.discard(0)

        if sde_session is not None and system_ids:
            missing = [sid for sid in system_ids if sid not in solar_system_map]
            if missing:
                try:
                    ss_rows = sde_session.query(MapSolarSystems).filter(MapSolarSystems.id.in_(missing)).all()
                    for ss in ss_rows or []:
                        solar_system_map[int(ss.id)] = {
                            "id": int(ss.id),
                            "name": parse_localized(getattr(ss, "name", None), lang) or str(ss.id),
                            "security_status": float(getattr(ss, "securityStatus", 0.0) or 0.0),
                        }
                except Exception:
                    pass

        return solar_system_map, npc_station_map, structure_map

    mfg_implant_time_multiplier, implant_details = _manufacturing_time_multiplier_from_implants(
        sde_session,
        [int(x) for x in char_implants if x is not None],
    )
    mfg_implant_time_reduction = max(0.0, min(1.0, 1.0 - float(mfg_implant_time_multiplier)))
    mfg_skill_implant_time_multiplier = float(mfg_skill_time_multiplier) * float(mfg_implant_time_multiplier)
    mfg_skill_implant_time_multiplier = max(0.0, min(mfg_skill_implant_time_multiplier, 1.0))
    mfg_skill_implant_time_reduction = max(0.0, min(1.0, 1.0 - float(mfg_skill_implant_time_multiplier)))

    def _round_payload(obj: Any, key: str | None = None) -> Any:
        if isinstance(obj, dict):
            return {k: _round_payload(v, k) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_round_payload(v, key) for v in obj]
        if isinstance(obj, float):
            k = (key or "").lower()
            # Heuristic rounding to reduce float noise while preserving meaning.
            if any(tok in k for tok in ["_isk", "price", "value", "cost", "tax", "fee"]):
                return round(obj, 2)
            if any(tok in k for tok in ["seconds", "_time", "run_time"]):
                return round(obj, 3)
            if any(tok in k for tok in ["index", "multiplier", "reduction", "fraction", "rate", "bonus"]):
                return round(obj, 6)
            return round(obj, 6)
        return obj

    def _group_flags(bp: dict) -> dict:
        flags: dict[str, Any] = {}
        for k in list(bp.keys()):
            if not k.startswith("is_"):
                continue
            flags[k] = bp.pop(k)
        if flags:
            bp["flags"] = flags
        return bp

    generated_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    schema_version = 2

    # Pre-build best-effort location maps (keyed by `top_location_id`).
    top_ids: list[int] = []
    for bp in blueprints:
        tid = bp.get("top_location_id")
        if tid is None:
            continue
        try:
            top_ids.append(int(tid))
        except Exception:
            continue
    top_ids = sorted(set(top_ids))
    solar_system_map, npc_station_map, structure_map = _build_location_maps(top_location_ids=top_ids)

    for bp in blueprints:
        required_skills = bp.get("required_skills", [])
        skill_requirements_met = True

        normalized_skills: list[dict] = []
        for skill in required_skills:
            skill_type_id = skill.get("type_id")
            required_level = skill.get("level", 0)
            char_skill = next((s for s in char_skills if s.get("skill_id") == skill_type_id), None)
            char_level = char_skill.get("trained_skill_level", 0) if char_skill else 0

            met = char_level >= required_level
            normalized_skills.append(
                {
                    "type_id": skill.get("type_id"),
                    "type_name": skill.get("type_name"),
                    "required_level": int(required_level or 0),
                    "character_level": int(char_level or 0),
                    "met": bool(met),
                    "category_id": skill.get("category_id"),
                    "category_name": skill.get("category_name"),
                    "group_id": skill.get("group_id"),
                    "group_name": skill.get("group_name"),
                }
            )

            if not met:
                skill_requirements_met = False

        bp["skill_requirements_met"] = skill_requirements_met
        bp["required_skills"] = normalized_skills

    for bp in blueprints:
        total_material_cost = 0.0
        total_product_value = 0.0
        missing_material_price_count = 0

        # Blueprint ME/TE levels
        # In the client, these are expressed as percentages (e.g. ME 10% => 0.90 multiplier).
        me_level = bp.get("blueprint_material_efficiency", 0) or 0
        te_level = bp.get("blueprint_time_efficiency", 0) or 0
        try:
            me_level_f = float(me_level)
        except Exception:
            me_level_f = 0.0
        try:
            te_level_f = float(te_level)
        except Exception:
            te_level_f = 0.0

        me_multiplier = 1.0 - (me_level_f * 0.01)
        te_multiplier = 1.0 - (te_level_f * 0.01)
        me_multiplier = max(0.0, min(me_multiplier, 1.0))
        te_multiplier = max(0.0, min(te_multiplier, 1.0))

        # Industry profile structure modifiers (fractions)
        profile_material_reduction = 0.0
        profile_time_reduction = 0.0
        profile_cost_reduction = 0.0
        profile_surcharge_rate = 0.0
        facility_tax = 0.0
        scc_surcharge = 0.0
        rig_group = "All"

        if industry_profile is not None:
            # facility bonuses
            profile_material_reduction = _as_fraction(getattr(industry_profile, "material_efficiency_bonus", None))
            profile_time_reduction = _as_fraction(getattr(industry_profile, "time_efficiency_bonus", None))
            profile_cost_reduction = _as_fraction(getattr(industry_profile, "facility_cost_bonus", None))

            facility_tax = _as_fraction(getattr(industry_profile, "facility_tax", None))
            scc_surcharge = _as_fraction(getattr(industry_profile, "scc_surcharge", None))
            profile_surcharge_rate = max(0.0, facility_tax + scc_surcharge)

            rig_group = _infer_rig_group_label_from_products(bp.get("products", []) or [])

        # Rig reductions are group- and activity-specific.
        rig_material_reduction = 0.0
        rig_time_reduction = 0.0
        rig_cost_reduction = 0.0
        if rig_payload:
            from flask_app.services.structure_rig_effects_service import compute_rig_reduction_for

            rig_material_reduction = compute_rig_reduction_for(
                rigs_payload=rig_payload, activity="manufacturing", group=rig_group, metric="material"
            )
            rig_time_reduction = compute_rig_reduction_for(
                rigs_payload=rig_payload, activity="manufacturing", group=rig_group, metric="time"
            )
            rig_cost_reduction = compute_rig_reduction_for(
                rigs_payload=rig_payload, activity="manufacturing", group=rig_group, metric="cost"
            )

        # Combine facility + rig reductions multiplicatively.
        effective_material_reduction = 1.0 - ((1.0 - profile_material_reduction) * (1.0 - rig_material_reduction))
        effective_time_reduction = 1.0 - ((1.0 - profile_time_reduction) * (1.0 - rig_time_reduction))
        effective_cost_reduction = 1.0 - ((1.0 - profile_cost_reduction) * (1.0 - rig_cost_reduction))

        # EIV (Estimated Item Value) uses adjusted prices (EVE client job-cost basis).
        # Per client behavior, it is based on the ME0 blueprint quantities (ignores ME/structure/rig material reductions).
        estimated_item_value = 0.0

        required_materials: list[dict] = []
        for mat in (bp.get("materials", []) or []):
            if not isinstance(mat, dict):
                continue
            base_qty_raw = mat.get("quantity", 0)
            try:
                base_qty = int(base_qty_raw or 0)
            except Exception:
                base_qty = 0

            # IMPORTANT: never truncate down to 0 for required inputs.
            # EVE material quantities effectively round up.
            raw_qty = float(base_qty) * float(me_multiplier) * (1.0 - float(effective_material_reduction))
            if base_qty > 0:
                adjusted_qty = max(1, int(math.ceil(max(0.0, raw_qty))))
            else:
                adjusted_qty = 0

            # Client shows "Total estimated price" using market price (best-effort: average first).
            unit_price = mat.get("average_price")
            if unit_price is None:
                unit_price = mat.get("unit_price")
            try:
                mat_price = float(unit_price) if unit_price is not None else None
            except Exception:
                mat_price = None

            if mat_price is None or mat_price <= 0:
                missing_material_price_count += 1
                mat_price_for_cost = 0.0
            else:
                mat_price_for_cost = float(mat_price)

            total_material_cost += adjusted_qty * mat_price_for_cost

            # EIV uses adjusted price multiplied by ME0 quantity (base_qty).
            try:
                adj_price = float(mat.get("adjusted_price")) if mat.get("adjusted_price") is not None else 0.0
            except Exception:
                adj_price = 0.0
            if adj_price > 0 and base_qty > 0:
                estimated_item_value += base_qty * adj_price

            required_materials.append(
                {
                    "type_id": mat.get("type_id"),
                    "type_name": mat.get("type_name"),
                    "category_id": mat.get("category_id"),
                    "category_name": mat.get("category_name"),
                    "group_id": mat.get("group_id"),
                    "group_name": mat.get("group_name"),
                    "quantity_me0": base_qty,
                    "quantity_after_efficiency": adjusted_qty,
                    "adjusted_price_isk": float(adj_price) if adj_price is not None else None,
                    "average_price_isk": (float(mat.get("average_price")) if mat.get("average_price") is not None else None),
                    "unit_price_isk": (float(mat_price) if mat_price is not None else None),
                    "total_cost_isk": float(adjusted_qty) * float(mat_price_for_cost),
                    "estimated_item_value_isk": float(base_qty) * float(adj_price or 0.0),
                }
            )

        bp["total_material_cost"] = total_material_cost
        bp["missing_material_price_count"] = missing_material_price_count
        bp["estimated_item_value_isk"] = estimated_item_value

        for prod in bp.get("products", []):
            prod_qty = prod.get("quantity", 0)
            prod_price = prod.get("average_price", 0.0) or 0.0
            total_product_value += prod_qty * prod_price

        bp["total_product_value"] = total_product_value
        bp["profit_margin"] = total_product_value - total_material_cost

        # Normalize base blueprint efficiency naming.
        bp["blueprint_material_efficiency_percent"] = int(me_level_f)
        bp["blueprint_time_efficiency_percent"] = int(te_level_f)
        bp.pop("blueprint_material_efficiency", None)
        bp.pop("blueprint_time_efficiency", None)

        # Manufacturing time estimate (seconds)
        # Normalize time units.
        bp["manufacturing_time_seconds"] = float(bp.get("manufacturing_time", 0) or 0.0)
        bp["copying_time_seconds"] = float(bp.get("copying_time", 0) or 0.0)
        bp["research_time_seconds"] = float(bp.get("research_time", 0) or 0.0)
        bp.pop("manufacturing_time", None)
        bp.pop("copying_time", None)
        bp.pop("research_time", None)

        base_time_s = float(bp.get("manufacturing_time_seconds", 0) or 0.0)
        est_time_s = (
            base_time_s
            * te_multiplier
            * (1.0 - effective_time_reduction)
            * mfg_skill_implant_time_multiplier
        )
        estimated_job_time_seconds = max(0.0, est_time_s)

        # Installation fee estimate (ISK)
        # Match client breakdown:
        # - Gross cost uses EIV * system_cost_index, then structure/rig cost reduction
        # - Taxes/surcharges (SCC + facility tax) apply to EIV directly
        ci = float(manufacturing_system_cost_index or 0.0)
        job_value = float(estimated_item_value)

        gross_cost = job_value * ci
        gross_cost_after_bonuses = gross_cost * (1.0 - effective_cost_reduction)
        taxes = job_value * profile_surcharge_rate
        total_job_cost_isk = max(0.0, gross_cost_after_bonuses + taxes)

        # Assemble the manufacturing job payload.
        bp["manufacture_job"] = {
            "required_skills": bp.get("required_skills", []) or [],
            "required_materials": required_materials,
            "properties": {
                "total_material_efficiency": {
                    "blueprint_material_efficiency_percent": int(me_level_f),
                    "blueprint_material_multiplier": float(me_multiplier),
                    "structure_material_reduction_fraction": float(effective_material_reduction),
                    "total_material_multiplier": float(me_multiplier) * (1.0 - float(effective_material_reduction)),
                },
                "total_time_efficiency": {
                    "base_run_time_seconds": float(base_time_s),
                    "blueprint_time_efficiency_percent": int(te_level_f),
                    "blueprint_time_multiplier": float(te_multiplier),
                    "structure_time_reduction_fraction": float(effective_time_reduction),
                    "skills_and_implants": {
                        "skill_time_multiplier": float(mfg_skill_time_multiplier),
                        "skill_time_reduction_fraction": float(mfg_skill_time_reduction),
                        "implant_time_multiplier": float(mfg_implant_time_multiplier),
                        "implant_time_reduction_fraction": float(mfg_implant_time_reduction),
                        "skills_and_implants_time_multiplier": float(mfg_skill_implant_time_multiplier),
                        "skills_and_implants_time_reduction_fraction": float(mfg_skill_implant_time_reduction),
                        "implant_details": implant_details,
                    },
                    "total_time_multiplier": float(te_multiplier) * (1.0 - float(effective_time_reduction)) * float(mfg_skill_implant_time_multiplier),
                    "estimated_job_time_seconds": float(estimated_job_time_seconds),
                },
                "job_cost": {
                    "system_cost_index": float(ci),
                    "scc_surcharge_fraction": float(scc_surcharge),
                    "facility_tax_fraction": float(facility_tax),
                    "surcharge_rate_total_fraction": float(profile_surcharge_rate),
                    "structure_cost_reduction_fraction": float(effective_cost_reduction),
                    "estimated_item_value_total_isk": float(estimated_item_value),
                    "gross_cost_isk": float(gross_cost),
                    "gross_cost_after_bonuses_isk": float(gross_cost_after_bonuses),
                    "taxes_isk": float(taxes),
                    "total_job_cost_isk": float(total_job_cost_isk),
                    "rig_group_label": rig_group,
                },
            },
        }

        # Attach schema metadata.
        bp["schema_version"] = schema_version
        bp["generated_at_utc"] = generated_at

        # Group location fields and enrich with SDE/app DB names.
        top_location_id = bp.pop("top_location_id", None)
        location_id = bp.pop("location_id", None)
        container_name = bp.pop("container_name", None)

        top_id_int: int | None
        try:
            top_id_int = int(top_location_id) if top_location_id is not None else None
        except Exception:
            top_id_int = None

        solar_system: dict | None = None
        npc_station: dict | None = None
        upwell_structure: dict | None = None
        top_location_type: str = "unknown"

        if top_id_int is not None:
            if top_id_int in solar_system_map:
                solar_system = solar_system_map.get(top_id_int)
                top_location_type = "solar_system"
            elif top_id_int in npc_station_map:
                npc_station = npc_station_map.get(top_id_int)
                top_location_type = "npc_station"
                sys_id = int((npc_station or {}).get("system_id") or 0)
                if sys_id and sys_id in solar_system_map:
                    solar_system = solar_system_map.get(sys_id)
            elif top_id_int in structure_map:
                upwell_structure = structure_map.get(top_id_int)
                top_location_type = "upwell_structure"
                sys_id = (upwell_structure or {}).get("system_id")
                try:
                    sys_id_int = int(sys_id) if sys_id is not None else 0
                except Exception:
                    sys_id_int = 0
                if sys_id_int and sys_id_int in solar_system_map:
                    solar_system = solar_system_map.get(sys_id_int)

        display_name = None
        system_name = (solar_system or {}).get("name") if isinstance(solar_system, dict) else None
        if top_location_type == "npc_station":
            station_name = (npc_station or {}).get("station_name") if isinstance(npc_station, dict) else None
            if system_name and station_name:
                display_name = f"{system_name} - {station_name}"
            else:
                display_name = station_name or system_name
        elif top_location_type == "upwell_structure":
            structure_name = (upwell_structure or {}).get("structure_name") if isinstance(upwell_structure, dict) else None
            if system_name and structure_name:
                display_name = f"{system_name} - {structure_name}"
            else:
                display_name = structure_name or system_name
        elif top_location_type == "solar_system":
            display_name = system_name

        bp["location"] = {
            "location_id": location_id,
            "top_location_id": top_location_id,
            "top_location_type": top_location_type,
            "container_name": container_name,
            "solar_system": solar_system,
            "npc_station": npc_station,
            "upwell_structure": upwell_structure,
            "display_name": display_name,
        }

        # Remove legacy noisy fields now represented in `manufacture_job`.
        for k in ["materials", "required_skills", "estimated_item_value_isk"]:
            bp.pop(k, None)

        # Group boolean flags under a dedicated section for readability.
        _group_flags(bp)

        # Round float noise for readability.
        rounded = _round_payload(bp)
        bp.clear()
        bp.update(rounded)

    return blueprints
