from __future__ import annotations

import copy
import math
import json
import datetime
from typing import Any, Dict, List

from sqlalchemy import bindparam, text
from sqlalchemy.sql import func

from classes.asset_provenance import build_fifo_remaining_lots_by_type, fifo_allocate_cost, fifo_allocate_cost_breakdown

from classes.database_models import (
    Blueprints,
    CharacterAssetsModel,
    CharacterIndustryJobsModel,
    CharacterWalletTransactionsModel,
    CorporationAssetsModel,
    CorporationIndustryJobsModel,
    CorporationStructuresModel,
    CorporationWalletTransactionsModel,
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


def _copying_time_multiplier_from_skills(char_skills: list[dict]) -> float:
    """Return blueprint copying time multiplier from character skills.

    In practice, blueprint copying speed is affected by the Science skill.
    """

    science_level = _get_trained_skill_level(char_skills, skill_name="Science")
    science_mult = 1.0 - (0.05 * float(max(0, min(science_level, 5))))
    return max(0.0, min(science_mult, 1.0))


_IMPLANT_ATTR_MANUFACTURING_TIME_BONUS = 440
_IMPLANT_ATTR_COPY_SPEED_BONUS = 452


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


def _copying_time_multiplier_from_implants(
    sde_session,
    implant_type_ids: list[int] | None,
) -> tuple[float, list[dict]]:
    """Return (multiplier, details) for blueprint copying speed implants.

    Uses SDE dogma attribute `copySpeedBonus` (id=452) which is stored
    as a negative percentage (e.g. -3.0 for -3% time).
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
                if int(aid) != _IMPLANT_ATTR_COPY_SPEED_BONUS:
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

        reduction = max(0.0, (-float(bonus_pct)) / 100.0)
        implant_mult = 1.0 - reduction
        implant_mult = max(0.0, min(implant_mult, 1.0))

        details.append(
            {
                "type_id": int(tid),
                "copy_speed_bonus_percent": float(bonus_pct),
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
    esi_service: Any | None = None,
    industry_profile: Any | None = None,
    manufacturing_system_cost_index: float | None = None,
    copying_system_cost_index: float | None = None,
    research_me_system_cost_index: float | None = None,
    research_te_system_cost_index: float | None = None,
    surcharge_rate_total_fraction: float | None = None,
    owned_blueprint_type_ids: set[int] | None = None,
    owned_blueprint_best_by_type_id: dict[int, dict] | None = None,
    include_submanufacturing: bool = False,
    submanufacturing_blueprint_type_id: int | None = None,
    progress_callback=None,
    maximize_blueprint_runs: bool = False,
    rig_payload: list[dict] | None = None,
    db_app_session: Any | None = None,
    db_sde_session: Any | None = None,
    language: str | None = None,
    use_fifo_inventory_costing: bool = True,
) -> List[Dict[str, Any]]:
    """Apply character-skill requirements and basic cost/value analysis.

    The blueprint payload includes a nested `manufacture_job` section for
    manufacturing-specific inputs and derived properties.
    """
    char_skills = (getattr(character, "skills", None) or {}).get("skills", []) or []
    mfg_skill_time_multiplier = _manufacturing_time_multiplier_from_skills(char_skills)
    mfg_skill_time_reduction = max(0.0, min(1.0, 1.0 - float(mfg_skill_time_multiplier)))

    copy_skill_time_multiplier = _copying_time_multiplier_from_skills(char_skills)
    copy_skill_time_reduction = max(0.0, min(1.0, 1.0 - float(copy_skill_time_multiplier)))

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

    # Optional: use the submanufacturing planner to compute effective input costs.
    try:
        from flask_app.services.submanufacturing_planner_service import plan_submanufacturing_tree
    except Exception:
        plan_submanufacturing_tree = None  # type: ignore[assignment]

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

    copy_implant_time_multiplier, copy_implant_details = _copying_time_multiplier_from_implants(
        sde_session,
        [int(x) for x in char_implants if x is not None],
    )
    copy_implant_time_reduction = max(0.0, min(1.0, 1.0 - float(copy_implant_time_multiplier)))
    copy_skill_implant_time_multiplier = float(copy_skill_time_multiplier) * float(copy_implant_time_multiplier)
    copy_skill_implant_time_multiplier = max(0.0, min(copy_skill_implant_time_multiplier, 1.0))
    copy_skill_implant_time_reduction = max(0.0, min(1.0, 1.0 - float(copy_skill_implant_time_multiplier)))

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

    # IMPORTANT: Do not mutate the input `blueprints` payload.
    # Upstream blueprint assets can be reused between requests; in-place mutation
    # causes run-scaled quantities/costs to compound (and can overflow UI rendering).
    blueprints_copy: list[dict] = []
    for bp_in in blueprints or []:
        if not isinstance(bp_in, dict):
            continue
        bp = dict(bp_in)

        mats_in = bp_in.get("materials", []) or []
        bp["materials"] = [dict(m) for m in mats_in if isinstance(m, dict)]

        prods_in = bp_in.get("products", []) or []
        bp["products"] = [dict(p) for p in prods_in if isinstance(p, dict)]

        skills_in = bp_in.get("required_skills", []) or []
        bp["required_skills"] = [dict(s) for s in skills_in if isinstance(s, dict)]

        flags_in = bp_in.get("flags")
        if isinstance(flags_in, dict):
            bp["flags"] = dict(flags_in)

        blueprints_copy.append(bp)

    # Flag reaction formulas via SDE activity data (not via name heuristics).
    # This is used by the Streamlit "Include Reactions" filter to match planner logic.
    reaction_by_blueprint_type_id: dict[int, bool] = {}
    if sde_session is not None:
        try:
            blueprint_type_ids = sorted(
                {
                    int(bp.get("type_id"))
                    for bp in blueprints_copy
                    if bp.get("type_id") is not None and int(bp.get("type_id")) > 0
                }
            )
        except Exception:
            blueprint_type_ids = []

        if blueprint_type_ids:
            try:
                rows = (
                    sde_session.query(Blueprints.blueprintTypeID, Blueprints.activities)
                    .filter(Blueprints.blueprintTypeID.in_(blueprint_type_ids))
                    .all()
                )
                for bp_tid, activities in rows or []:
                    has_reaction = (
                        isinstance(activities, dict)
                        and isinstance(activities.get("reaction"), dict)
                    )
                    reaction_by_blueprint_type_id[int(bp_tid)] = bool(has_reaction)
            except Exception:
                reaction_by_blueprint_type_id = {}

    for bp in blueprints_copy:
        tid = bp.get("type_id")
        try:
            tid_i = int(tid) if tid is not None else 0
        except Exception:
            tid_i = 0
        bp["is_reaction_blueprint"] = bool(reaction_by_blueprint_type_id.get(tid_i, False))

    # Pre-build best-effort location maps (keyed by `top_location_id`).
    top_ids: list[int] = []
    for bp in blueprints_copy:
        tid = bp.get("top_location_id")
        if tid is None:
            continue
        try:
            top_ids.append(int(tid))
        except Exception:
            continue
    top_ids = sorted(set(top_ids))
    solar_system_map, npc_station_map, structure_map = _build_location_maps(top_location_ids=top_ids)

    # --- Inventory FIFO lots (best-effort) ---
    # We precompute this once per request and reuse for all blueprint calculations.
    use_fifo_inventory_costing = bool(use_fifo_inventory_costing)
    inventory_on_hand_by_type: dict[int, int] = {}
    fifo_lots_by_type: dict[int, list] = {}

    # Only compute FIFO lots if we have an app session and at least one material type.
    if use_fifo_inventory_costing and db_app_session is not None:
        material_type_ids: set[int] = set()
        for bp in blueprints_copy:
            for m in (bp.get("materials", []) or []):
                if not isinstance(m, dict):
                    continue
                tid = m.get("type_id")
                if tid is None:
                    continue
                try:
                    tid_i = int(tid)
                except Exception:
                    continue
                if tid_i > 0:
                    material_type_ids.add(tid_i)

        if material_type_ids:
            # Aggregate on-hand quantities for character + corporation (if any).
            char_id = getattr(character, "character_id", None)
            corp_id = getattr(character, "corporation_id", None)

            try:
                if char_id is not None:
                    rows = (
                        db_app_session.query(CharacterAssetsModel.type_id, func.sum(CharacterAssetsModel.quantity))
                        .filter(CharacterAssetsModel.character_id == int(char_id))
                        .filter(CharacterAssetsModel.type_id.in_(sorted(material_type_ids)))
                        .group_by(CharacterAssetsModel.type_id)
                        .all()
                    )
                    for tid, qty_sum in rows or []:
                        if tid is None:
                            continue
                        try:
                            inventory_on_hand_by_type[int(tid)] = int(qty_sum or 0)
                        except Exception:
                            continue
            except Exception:
                inventory_on_hand_by_type = inventory_on_hand_by_type or {}

            try:
                if corp_id is not None:
                    rows = (
                        db_app_session.query(CorporationAssetsModel.type_id, func.sum(CorporationAssetsModel.quantity))
                        .filter(CorporationAssetsModel.corporation_id == int(corp_id))
                        .filter(CorporationAssetsModel.type_id.in_(sorted(material_type_ids)))
                        .group_by(CorporationAssetsModel.type_id)
                        .all()
                    )
                    for tid, qty_sum in rows or []:
                        if tid is None:
                            continue
                        try:
                            inventory_on_hand_by_type[int(tid)] = int(inventory_on_hand_by_type.get(int(tid), 0) or 0) + int(
                                qty_sum or 0
                            )
                        except Exception:
                            continue
            except Exception:
                pass

            # Load wallet transactions for these types (character + corp) to reconstruct FIFO lots.
            tx_rows: list[Any] = []
            try:
                if char_id is not None:
                    tx_rows.extend(
                        (
                            db_app_session.query(CharacterWalletTransactionsModel)
                            .filter(CharacterWalletTransactionsModel.character_id == int(char_id))
                            .filter(CharacterWalletTransactionsModel.type_id.in_(sorted(material_type_ids)))
                            .all()
                        )
                        or []
                    )
            except Exception:
                pass

            try:
                if corp_id is not None:
                    tx_rows.extend(
                        (
                            db_app_session.query(CorporationWalletTransactionsModel)
                            .filter(CorporationWalletTransactionsModel.corporation_id == int(corp_id))
                            .filter(CorporationWalletTransactionsModel.type_id.in_(sorted(material_type_ids)))
                            .all()
                        )
                        or []
                    )
            except Exception:
                pass

            try:
                fifo_lots_by_type = build_fifo_remaining_lots_by_type(
                    wallet_transactions=tx_rows,
                    industry_jobs=None,
                    sde_session=None,
                    market_prices=None,
                    on_hand_quantities_by_type=inventory_on_hand_by_type,
                )
            except Exception:
                fifo_lots_by_type = {}

            # Extend FIFO lots with completed industry jobs (built inventory), best-effort.
            # This requires SDE access and ESI market prices.
            if fifo_lots_by_type is not None:
                try:
                    job_rows: list[Any] = []
                    if char_id is not None:
                        job_rows.extend(
                            (
                                db_app_session.query(CharacterIndustryJobsModel)
                                .filter(CharacterIndustryJobsModel.character_id == int(char_id))
                                .filter(CharacterIndustryJobsModel.product_type_id.in_(sorted(material_type_ids)))
                                .all()
                            )
                            or []
                        )
                except Exception:
                    job_rows = []

                try:
                    if corp_id is not None:
                        job_rows.extend(
                            (
                                db_app_session.query(CorporationIndustryJobsModel)
                                .filter(CorporationIndustryJobsModel.corporation_id == int(corp_id))
                                .filter(CorporationIndustryJobsModel.product_type_id.in_(sorted(material_type_ids)))
                                .all()
                            )
                            or []
                        )
                except Exception:
                    pass

                try:
                    market_prices: list[dict[str, Any]] | None = None
                    if esi_service is not None:
                        market_prices = (esi_service.get_market_prices() or [])
                except Exception:
                    market_prices = None

                try:
                    fifo_lots_by_type = build_fifo_remaining_lots_by_type(
                        wallet_transactions=tx_rows,
                        industry_jobs=job_rows,
                        sde_session=sde_session,
                        market_prices=market_prices,
                        on_hand_quantities_by_type=inventory_on_hand_by_type,
                    )
                except Exception:
                    pass

    for bp in blueprints_copy:
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

    total_blueprints = len(blueprints_copy)

    # Memoize heavy per-blueprint computations within this request.
    # Many BPC stacks share identical stats (type_id + ME/TE + runs + bonuses),
    # so we can compute submanufacturing/manufacturing once and reuse.
    computed_blueprint_cache: dict[tuple, dict[str, Any]] = {}
    for idx_bp, bp in enumerate(blueprints_copy):
        if progress_callback is not None:
            try:
                progress_callback(int(idx_bp), int(total_blueprints), int(bp.get("type_id") or 0))
            except Exception:
                pass
        total_material_cost = 0.0
        total_product_value = 0.0
        missing_material_price_count = 0

        is_bpc = bool(bp.get("is_blueprint_copy"))
        remaining_runs: int | None = None
        try:
            rr = int(bp.get("blueprint_runs") or 0)
            if rr > 0:
                remaining_runs = rr
        except Exception:
            remaining_runs = None

        job_runs = 1
        if maximize_blueprint_runs and is_bpc and remaining_runs is not None:
            job_runs = max(1, int(remaining_runs))

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

        # Server-side submanufacturing integration (optional): replace market-priced material totals
        # with effective buy/build costs so blueprint totals are consistent end-to-end.
        should_compute_submfg = bool(include_submanufacturing)
        if should_compute_submfg and submanufacturing_blueprint_type_id is not None:
            try:
                should_compute_submfg = int(bp.get("type_id") or 0) == int(submanufacturing_blueprint_type_id)
            except Exception:
                should_compute_submfg = False

        # Cache key: include all parameters that affect derived costs/time and planner output.
        # Note: cache is request-local; we still include indices/bonuses for correctness.
        cache_key = (
            int(bp.get("type_id") or 0),
            bool(is_bpc),
            int(job_runs),
            int(me_level_f),
            int(te_level_f),
            round(float(effective_material_reduction or 0.0), 6),
            round(float(effective_time_reduction or 0.0), 6),
            round(float(effective_cost_reduction or 0.0), 6),
            round(float(profile_surcharge_rate or 0.0), 6),
            round(float(surcharge_rate_total_fraction or 0.0), 6),
            round(float(manufacturing_system_cost_index or 0.0), 6),
            round(float(copying_system_cost_index or 0.0), 6),
            round(float(research_me_system_cost_index or 0.0), 6),
            round(float(research_te_system_cost_index or 0.0), 6),
            round(float(mfg_skill_implant_time_multiplier or 0.0), 6),
            round(float(copy_skill_implant_time_multiplier or 0.0), 6),
            str(lang),
            bool(should_compute_submfg),
            int(submanufacturing_blueprint_type_id or 0),
            3,  # max_depth
        )

        cached_calc = computed_blueprint_cache.get(cache_key)
        if cached_calc is not None:
            bp.update(copy.deepcopy(cached_calc))
            # Ensure legacy/raw keys are removed even on cache hits.
            bp.pop("blueprint_material_efficiency", None)
            bp.pop("blueprint_time_efficiency", None)
            bp.pop("manufacturing_time", None)
            bp.pop("copying_time", None)
            bp.pop("research_time", None)
        else:
            # EIV (Estimated Item Value) uses adjusted prices (EVE client job-cost basis).
            # Per client behavior, it is based on the ME0 blueprint quantities (ignores ME/structure/rig material reductions).
            estimated_item_value_per_run = 0.0

            required_materials: list[dict] = []
            total_material_cost_inventory_fifo = 0.0
            for mat in (bp.get("materials", []) or []):
                if not isinstance(mat, dict):
                    continue
                base_qty_raw = mat.get("quantity", 0)
                try:
                    base_qty = int(base_qty_raw or 0)
                except Exception:
                    base_qty = 0

                base_qty_total = int(base_qty) * int(job_runs)

                # IMPORTANT: never truncate down to 0 for required inputs.
                # Material quantities in the client round up for the job total.
                raw_qty = float(base_qty_total) * float(me_multiplier) * (1.0 - float(effective_material_reduction))
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

                # FIFO inventory valuation (optional):
                # - Price up to on-hand qty using FIFO lots.
                # - Any unpriced inventory (missing history) and any shortfall are priced at market.
                inv_on_hand = int(inventory_on_hand_by_type.get(int(mat.get("type_id") or 0), 0) or 0)
                inv_used = min(int(adjusted_qty), max(0, inv_on_hand)) if use_fifo_inventory_costing else 0
                fifo_cost = 0.0
                fifo_priced_qty = 0
                fifo_breakdown_by_source: dict[str, Any] | None = None
                fifo_market_buy_cost = 0.0
                fifo_market_buy_qty = 0
                fifo_industry_build_cost = 0.0
                fifo_industry_build_qty = 0
                if use_fifo_inventory_costing and inv_used > 0:
                    lots = fifo_lots_by_type.get(int(mat.get("type_id") or 0))
                    bd = fifo_allocate_cost_breakdown(lots=lots, quantity=int(inv_used))
                    fifo_cost = float(bd.get("total_cost") or 0.0)
                    fifo_priced_qty = int(bd.get("priced_quantity") or 0)
                    fifo_breakdown_by_source = bd.get("by_source") if isinstance(bd.get("by_source"), dict) else None
                    if fifo_breakdown_by_source:
                        mb = fifo_breakdown_by_source.get("market_buy") if isinstance(fifo_breakdown_by_source.get("market_buy"), dict) else None
                        ib = fifo_breakdown_by_source.get("industry_build") if isinstance(fifo_breakdown_by_source.get("industry_build"), dict) else None
                        if mb:
                            fifo_market_buy_cost = float(mb.get("cost") or 0.0)
                            fifo_market_buy_qty = int(mb.get("quantity") or 0)
                        if ib:
                            fifo_industry_build_cost = float(ib.get("cost") or 0.0)
                            fifo_industry_build_qty = int(ib.get("quantity") or 0)
                unknown_inv_qty = max(0, int(inv_used) - int(fifo_priced_qty))
                buy_now_qty = max(0, int(adjusted_qty) - int(inv_used))
                fifo_total_cost = float(fifo_cost) + float(unknown_inv_qty + buy_now_qty) * float(mat_price_for_cost)
                total_material_cost_inventory_fifo += float(fifo_total_cost)

                # EIV per-run uses adjusted price multiplied by ME0 quantity (base_qty).
                try:
                    adj_price = float(mat.get("adjusted_price")) if mat.get("adjusted_price") is not None else 0.0
                except Exception:
                    adj_price = 0.0
                if adj_price > 0 and base_qty > 0:
                    estimated_item_value_per_run += base_qty * adj_price

                required_materials.append(
                    {
                        "type_id": mat.get("type_id"),
                        "type_name": mat.get("type_name"),
                        "category_id": mat.get("category_id"),
                        "category_name": mat.get("category_name"),
                        "group_id": mat.get("group_id"),
                        "group_name": mat.get("group_name"),
                        "quantity_me0": int(base_qty_total),
                        "quantity_after_efficiency": adjusted_qty,
                        "adjusted_price_isk": float(adj_price) if adj_price is not None else None,
                        "average_price_isk": (
                            float(mat.get("average_price")) if mat.get("average_price") is not None else None
                        ),
                        "unit_price_isk": (float(mat_price) if mat_price is not None else None),
                        "total_cost_isk": float(adjusted_qty) * float(mat_price_for_cost),
                        # FIFO inventory costing (best-effort)
                        "inventory_on_hand_qty": int(inv_on_hand) if use_fifo_inventory_costing else None,
                        "inventory_used_qty": int(inv_used) if use_fifo_inventory_costing else None,
                        "inventory_fifo_priced_qty": int(fifo_priced_qty) if use_fifo_inventory_costing else None,
                        "inventory_fifo_total_cost_isk": float(fifo_cost) if use_fifo_inventory_costing else None,
                        "inventory_fifo_breakdown_by_source": fifo_breakdown_by_source if use_fifo_inventory_costing else None,
                        "inventory_fifo_market_buy_qty": int(fifo_market_buy_qty) if use_fifo_inventory_costing else None,
                        "inventory_fifo_market_buy_cost_isk": float(fifo_market_buy_cost) if use_fifo_inventory_costing else None,
                        "inventory_fifo_industry_build_qty": int(fifo_industry_build_qty) if use_fifo_inventory_costing else None,
                        "inventory_fifo_industry_build_cost_isk": float(fifo_industry_build_cost) if use_fifo_inventory_costing else None,
                        "inventory_unknown_cost_qty": int(unknown_inv_qty) if use_fifo_inventory_costing else None,
                        "inventory_buy_now_qty": int(buy_now_qty) if use_fifo_inventory_costing else None,
                        "inventory_effective_total_cost_isk": float(fifo_total_cost) if use_fifo_inventory_costing else None,
                        "inventory_effective_unit_cost_isk": (
                            float(fifo_total_cost) / float(adjusted_qty) if use_fifo_inventory_costing and adjusted_qty > 0 else None
                        ),
                        # Filled by submanufacturing planner when available:
                        "effective_unit_cost_isk": None,
                        "effective_total_cost_isk": None,
                        "submanufacturing_recommendation": None,
                        "submanufacturing_buy_cost_isk": None,
                        "submanufacturing_build_cost_isk": None,
                        "estimated_item_value_isk": float(base_qty_total) * float(adj_price or 0.0),
                    }
                )

            submfg_plan_rows: list[dict] = []

            if (
                should_compute_submfg
                and plan_submanufacturing_tree is not None
                and sde_session is not None
                and isinstance(required_materials, list)
                and required_materials
            ):
                try:
                    planner_inputs: list[dict] = []
                    for rm in required_materials:
                        if not isinstance(rm, dict):
                            continue
                        tid = rm.get("type_id")
                        qty_eff = rm.get("quantity_after_efficiency")
                        if tid is None or qty_eff is None:
                            continue
                        planner_inputs.append(
                            {
                                "type_id": int(tid),
                                "type_name": rm.get("type_name"),
                                "quantity": int(qty_eff),
                            }
                        )

                    if planner_inputs:
                        submfg_plan_rows = plan_submanufacturing_tree(
                            sde_session=sde_session,
                            language=lang,
                            esi_service=esi_service,
                            materials=planner_inputs,
                            owned_blueprint_type_ids=owned_blueprint_type_ids,
                            owned_blueprint_best_by_type_id=owned_blueprint_best_by_type_id,
                            manufacturing_system_cost_index=float(manufacturing_system_cost_index or 0.0),
                            copying_system_cost_index=float(copying_system_cost_index or 0.0),
                            research_me_system_cost_index=float(research_me_system_cost_index or 0.0),
                            research_te_system_cost_index=float(research_te_system_cost_index or 0.0),
                            surcharge_rate_total_fraction=float(surcharge_rate_total_fraction or 0.0),
                            material_reduction_total_fraction=float(effective_material_reduction or 0.0),
                            time_reduction_total_fraction=float(effective_time_reduction or 0.0),
                            job_cost_reduction_total_fraction=float(effective_cost_reduction or 0.0),
                            inventory_on_hand_by_type=inventory_on_hand_by_type,
                            inventory_fifo_lots_by_type=fifo_lots_by_type,
                            use_fifo_inventory_costing=use_fifo_inventory_costing,
                            max_depth=3,
                        )

                        plan_by_type_id: dict[int, dict] = {}
                        for r in submfg_plan_rows or []:
                            if not isinstance(r, dict):
                                continue
                            tid = r.get("type_id")
                            if tid is None:
                                continue
                            try:
                                plan_by_type_id[int(tid)] = r
                            except Exception:
                                continue

                        effective_total_material_cost = 0.0
                        any_effective = False
                        for rm in required_materials:
                            if not isinstance(rm, dict):
                                continue
                            tid = rm.get("type_id")
                            if tid is None:
                                continue
                            plan = plan_by_type_id.get(int(tid))
                            if not isinstance(plan, dict):
                                continue

                            eff_cost = plan.get("effective_cost_isk")
                            rec = plan.get("recommendation")
                            buy_cost = plan.get("buy_cost_isk")
                            build = plan.get("build") if isinstance(plan.get("build"), dict) else None
                            build_cost = build.get("total_build_cost_isk") if isinstance(build, dict) else None

                            rm["submanufacturing_recommendation"] = rec
                            rm["submanufacturing_buy_cost_isk"] = float(buy_cost) if buy_cost is not None else None
                            rm["submanufacturing_build_cost_isk"] = float(build_cost) if build_cost is not None else None

                            if eff_cost is not None:
                                try:
                                    eff_cost_f = float(eff_cost)
                                except Exception:
                                    eff_cost_f = None
                                if eff_cost_f is not None:
                                    # If FIFO inventory costing is enabled and the planner recommends "buy" or "take",
                                    # override the effective cost with inventory FIFO cost (plus buy-now for any shortfall).
                                    if use_fifo_inventory_costing and str(rec or "").lower() in {"buy", "take"}:
                                        inv_eff = rm.get("inventory_effective_total_cost_isk")
                                        try:
                                            inv_eff_f = float(inv_eff) if inv_eff is not None else None
                                        except Exception:
                                            inv_eff_f = None
                                        rm["effective_total_cost_isk"] = inv_eff_f if inv_eff_f is not None else eff_cost_f
                                    else:
                                        rm["effective_total_cost_isk"] = eff_cost_f
                                    qty_eff = rm.get("quantity_after_efficiency")
                                    try:
                                        q = float(qty_eff or 0)
                                    except Exception:
                                        q = 0.0
                                    eff_total = rm.get("effective_total_cost_isk")
                                    try:
                                        eff_total_f = float(eff_total) if eff_total is not None else None
                                    except Exception:
                                        eff_total_f = None
                                    rm["effective_unit_cost_isk"] = (eff_total_f / q) if (eff_total_f is not None and q > 0) else None
                                    any_effective = True
                                    if eff_total_f is not None:
                                        effective_total_material_cost += eff_total_f

                        if any_effective:
                            bp["total_material_cost_effective"] = float(effective_total_material_cost)
                            bp["profit_margin_effective"] = float(total_product_value) - float(effective_total_material_cost)

                except Exception:
                    submfg_plan_rows = []

            bp["submanufacturing_plan"] = submfg_plan_rows if submfg_plan_rows else []

            bp["total_material_cost"] = total_material_cost
            if use_fifo_inventory_costing:
                bp["total_material_cost_inventory_fifo"] = float(total_material_cost_inventory_fifo)
                bp["profit_margin_inventory_fifo"] = float(total_product_value) - float(total_material_cost_inventory_fifo)
            bp["missing_material_price_count"] = missing_material_price_count
            estimated_item_value_total = float(estimated_item_value_per_run) * float(job_runs)
            bp["estimated_item_value_isk"] = estimated_item_value_total

            for prod in bp.get("products", []):
                if not isinstance(prod, dict):
                    continue
                prod_qty = prod.get("quantity_per_run")
                if prod_qty is None:
                    prod_qty = prod.get("quantity", 0)
                prod_price = prod.get("average_price", 0.0) or 0.0
                try:
                    prod_qty_i = int(prod_qty or 0)
                except Exception:
                    prod_qty_i = 0

                prod_qty_total = int(prod_qty_i) * int(job_runs)
                prod["quantity_per_run"] = int(prod_qty_i)
                prod["quantity_total"] = int(prod_qty_total)

                total_product_value += float(prod_qty_total) * float(prod_price)

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
                * float(job_runs)
            )
            estimated_job_time_seconds = max(0.0, est_time_s)

            # Installation fee estimate (ISK)
            # Match client breakdown:
            # - Gross cost uses EIV * system_cost_index, then structure/rig cost reduction
            # - Taxes/surcharges (SCC + facility tax) apply to EIV directly
            ci = float(manufacturing_system_cost_index or 0.0)
            job_value = float(estimated_item_value_total)

            gross_cost = job_value * ci
            gross_cost_after_bonuses = gross_cost * (1.0 - effective_cost_reduction)
            taxes = job_value * profile_surcharge_rate
            total_job_cost_isk = max(0.0, gross_cost_after_bonuses + taxes)

            # Optional: include blueprint copying overhead when manufacturing is based on a BPC.
            # This estimates the copy job that would have produced the current BPC runs.
            # Notes:
            # - Copy time scales linearly with runs (using max production limit as the "max-run copy" reference).
            # - Copy installation fee is estimated using the copying system cost index, using manufacturing EIV per-run
            #   scaled by BPC runs. This keeps the cost basis consistent with the manufacturing EIV definition.
            copy_job: dict | None = None
            effective_total_job_time_seconds = float(estimated_job_time_seconds)
            effective_total_job_cost_isk = float(total_job_cost_isk)

            if is_bpc:
                # Copy overhead should match the amount we manufacture, bounded by remaining runs.
                bpc_runs = int(job_runs)
                if remaining_runs is not None:
                    bpc_runs = max(1, min(int(bpc_runs), int(remaining_runs)))

                try:
                    max_runs = int(bp.get("max_production_limit") or 0)
                except Exception:
                    max_runs = 0

                if bpc_runs > 0:
                    run_ratio = 1.0
                    if max_runs > 0:
                        run_ratio = float(bpc_runs) / float(max_runs)
                        run_ratio = max(0.0, min(run_ratio, 1.0))

                    # Copying rig reductions are activity-specific.
                    copy_rig_time_reduction = 0.0
                    copy_rig_cost_reduction = 0.0
                    if rig_payload:
                        from flask_app.services.structure_rig_effects_service import compute_rig_reduction_for

                        copy_rig_time_reduction = compute_rig_reduction_for(
                            rigs_payload=rig_payload, activity="copying", group=rig_group, metric="time"
                        )
                        copy_rig_cost_reduction = compute_rig_reduction_for(
                            rigs_payload=rig_payload, activity="copying", group=rig_group, metric="cost"
                        )

                    copy_effective_time_reduction = 1.0 - (
                        (1.0 - float(profile_time_reduction)) * (1.0 - float(copy_rig_time_reduction))
                    )
                    copy_effective_cost_reduction = 1.0 - (
                        (1.0 - float(profile_cost_reduction)) * (1.0 - float(copy_rig_cost_reduction))
                    )

                    base_copy_time_s = float(bp.get("copying_time_seconds", 0) or 0.0)
                    copy_base_time_for_runs_s = base_copy_time_s * float(run_ratio)
                    copy_estimated_time_seconds = max(
                        0.0,
                        copy_base_time_for_runs_s
                        * (1.0 - float(copy_effective_time_reduction))
                        * float(copy_skill_implant_time_multiplier),
                    )

                    copy_ci = float(copying_system_cost_index or 0.0)
                    copy_job_value = float(estimated_item_value_per_run) * float(bpc_runs)
                    copy_gross_cost = copy_job_value * copy_ci
                    copy_gross_cost_after_bonuses = copy_gross_cost * (1.0 - float(copy_effective_cost_reduction))
                    copy_taxes = copy_job_value * float(profile_surcharge_rate)
                    copy_total_job_cost_isk = max(0.0, copy_gross_cost_after_bonuses + copy_taxes)

                    copy_job = {
                        "runs": int(bpc_runs),
                        "remaining_runs": int(remaining_runs) if remaining_runs is not None else None,
                        "max_runs": int(max_runs) if max_runs > 0 else None,
                        "run_ratio": float(run_ratio),
                        "time": {
                            "base_copy_time_seconds_max_runs": float(base_copy_time_s),
                            "base_copy_time_seconds_for_runs": float(copy_base_time_for_runs_s),
                            "structure_time_reduction_fraction": float(copy_effective_time_reduction),
                            "skills_and_implants": {
                                "skill_time_multiplier": float(copy_skill_time_multiplier),
                                "skill_time_reduction_fraction": float(copy_skill_time_reduction),
                                "implant_time_multiplier": float(copy_implant_time_multiplier),
                                "implant_time_reduction_fraction": float(copy_implant_time_reduction),
                                "skills_and_implants_time_multiplier": float(copy_skill_implant_time_multiplier),
                                "skills_and_implants_time_reduction_fraction": float(copy_skill_implant_time_reduction),
                                "implant_details": copy_implant_details,
                            },
                            "estimated_copy_time_seconds": float(copy_estimated_time_seconds),
                        },
                        "job_cost": {
                            "system_cost_index": float(copy_ci),
                            "scc_surcharge_fraction": float(scc_surcharge),
                            "facility_tax_fraction": float(facility_tax),
                            "surcharge_rate_total_fraction": float(profile_surcharge_rate),
                            "structure_cost_reduction_fraction": float(copy_effective_cost_reduction),
                            "estimated_item_value_total_isk": float(copy_job_value),
                            "gross_cost_isk": float(copy_gross_cost),
                            "gross_cost_after_bonuses_isk": float(copy_gross_cost_after_bonuses),
                            "taxes_isk": float(copy_taxes),
                            "total_job_cost_isk": float(copy_total_job_cost_isk),
                            "rig_group_label": rig_group,
                        },
                    }

                    effective_total_job_time_seconds += float(copy_estimated_time_seconds)
                    effective_total_job_cost_isk += float(copy_total_job_cost_isk)

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
                        "total_time_multiplier": float(te_multiplier)
                        * (1.0 - float(effective_time_reduction))
                        * float(mfg_skill_implant_time_multiplier),
                        "estimated_job_time_seconds": float(estimated_job_time_seconds),
                    },
                    "job_cost": {
                        "system_cost_index": float(ci),
                        "scc_surcharge_fraction": float(scc_surcharge),
                        "facility_tax_fraction": float(facility_tax),
                        "surcharge_rate_total_fraction": float(profile_surcharge_rate),
                        "structure_cost_reduction_fraction": float(effective_cost_reduction),
                        "estimated_item_value_total_isk": float(estimated_item_value_total),
                        "gross_cost_isk": float(gross_cost),
                        "gross_cost_after_bonuses_isk": float(gross_cost_after_bonuses),
                        "taxes_isk": float(taxes),
                        "total_job_cost_isk": float(total_job_cost_isk),
                        "rig_group_label": rig_group,
                    },
                    "copy_job": copy_job,
                    "effective_totals": {
                        "estimated_total_time_seconds": float(effective_total_job_time_seconds),
                        "estimated_total_job_cost_isk": float(effective_total_job_cost_isk),
                    },
                    "job_runs": int(job_runs),
                },
            }

            # Cache the derived fields (exclude per-asset identifiers/location fields).
            keys_to_cache = [
                "submanufacturing_plan",
                "total_material_cost",
                "total_material_cost_inventory_fifo",
                "total_material_cost_effective",
                "missing_material_price_count",
                "total_product_value",
                "profit_margin",
                "profit_margin_effective",
                "profit_margin_inventory_fifo",
                "blueprint_material_efficiency_percent",
                "blueprint_time_efficiency_percent",
                "manufacturing_time_seconds",
                "copying_time_seconds",
                "research_time_seconds",
                "manufacture_job",
                "products",
            ]
            cached_fields: dict[str, Any] = {}
            for k in keys_to_cache:
                if k in bp:
                    cached_fields[k] = bp.get(k)
            computed_blueprint_cache[cache_key] = copy.deepcopy(cached_fields)

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

    if progress_callback is not None:
        try:
            progress_callback(int(total_blueprints), int(total_blueprints), 0)
        except Exception:
            pass

    return blueprints_copy
