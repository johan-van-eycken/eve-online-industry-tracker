from __future__ import annotations

from flask import Blueprint, request
import json
import time
from sqlalchemy import bindparam, text

from flask_app.bootstrap import require_ready
from flask_app.state import state
from flask_app.http import ok, error

from flask_app.db import get_db_app_session, get_db_sde_session
from flask_app.persistence import corporation_structures_repo, industry_profiles_repo
from flask_app.services import blueprints_service
from flask_app.services.sde_context import ensure_sde_ready, get_language
from flask_app.services.sde_locations_service import get_solar_systems, get_npc_stations
from flask_app.services.sde_types_service import get_type_data
from flask_app.services.industry_builder_service import enrich_blueprints_for_character
from flask_app.services.structure_rig_effects_service import get_rig_effects_for_type_ids
from flask_app.settings import public_structures_cache_ttl_seconds
from flask_app.services.public_structures_cache_service import (
    get_cached_public_structures,
    trigger_refresh_public_structures_for_system,
)


industry_bp = Blueprint("industry", __name__)


_STRUCTURE_RIG_MFG_CACHE_TTL_SECONDS = 24 * 3600
_STRUCTURE_RIGS_CACHE_VERSION = 3
_RIG_ATTR_TIME_REDUCTION = 2593
_RIG_ATTR_MATERIAL_REDUCTION = 2594
_RIG_ATTR_COST_REDUCTION = 2595


def _camel_to_words(s: str) -> str:
    out = []
    buf = ""
    for ch in s:
        if buf and ch.isupper() and (not buf[-1].isupper()):
            out.append(buf)
            buf = ch
        else:
            buf += ch
    if buf:
        out.append(buf)
    return " ".join(out).strip()


def _parse_rig_effect(effect_name: str) -> tuple[str | None, str | None, str | None]:
    """Return (activity, group_token, metric) for known rig effects.

    Examples:
      rigAmmoManufactureMaterialBonus -> (manufacturing, Ammo, material)
      rigInventionTimeBonus -> (invention, None, time)
    """

    if not effect_name or not effect_name.startswith("rig"):
        return None, None, None

    metric: str | None
    if ("Material" in effect_name) or ("MatBonus" in effect_name):
        metric = "material"
    elif "Time" in effect_name:
        metric = "time"
    elif "Cost" in effect_name:
        metric = "cost"
    else:
        metric = None

    activity: str | None = None
    group_token: str | None = None

    rest = effect_name[3:]

    # Research ME/TE rigs encode the "activity" in the prefix (rigME* / rigTE*).
    if rest.startswith("ME") and "Research" in rest:
        return "research_me", None, metric
    if rest.startswith("TE") and "Research" in rest:
        return "research_te", None, metric

    # Reaction rigs are named like rigReactionBioMatBonus / rigReactionBioTimeBonus
    if rest.startswith("Reaction"):
        # group token includes Reaction + subtype (Bio/Comp/Hyb)
        group_part = rest
        for suffix in ["MatBonus", "TimeBonus", "CostBonus", "MaterialBonus", "TimeBonus", "CostBonus"]:
            if suffix in group_part:
                group_part = group_part.split(suffix, 1)[0]
                break
        return "manufacturing", group_part or None, metric
    # Prefer more specific tokens first.
    for token, activity_key in [
        ("Manufacture", "manufacturing"),
        ("Invention", "invention"),
        ("Research", "research"),
        ("Copying", "copying"),
        ("Copy", "copying"),
        ("Reaction", "manufacturing"),
    ]:
        idx = rest.find(token)
        if idx >= 0:
            activity = activity_key
            if idx > 0:
                group_token = rest[:idx]
            break

    return activity, group_token, metric


_RIG_GROUP_TOKEN_LABELS: dict[str, str] = {
    # Manufacturing
    "Equipment": "Modules",
    "Ammo": "Ammo & Charges",
    "Drone": "Drones",
    "Smallship": "Basic Small Ships",
    "Mediumship": "Basic Medium Ships",
    "Mediumships": "Basic Medium Ships",
    "Largeship": "Basic Large Ships",
    "AdvComponent": "Advanced Components",
    "AdvSmship": "Advanced Small Ships",
    "AdvMedShip": "Advanced Medium Ships",
    "AdvLarShip": "Advanced Large Ships",
    "BasCapComp": "Capital Components",
    "CapShip": "Capital Ships",
    "AdvCapComponent": "Advanced Capital Components",
    "Structure": "Structures",
    # Reactions
    "ReactionBio": "Biochemical Reactions",
    "ReactionComp": "Composite Reactions",
    "ReactionHyb": "Hybrid Reactions",
    # Some variants that may show up in SDE
    "ThukkerAdvCapComp": "Advanced Capital Components",
    "ThukkerBasCapComp": "Capital Components",
    "AllShip": "All Ships",
}


def _extract_structure_industry_bonuses_from_type_bonus(row: dict, *, language: str) -> dict:
    """Extract base structure industry bonuses from a typeBonus row.

    This currently targets engineering complexes/citadels entries like:
      - reduction in material requirements for manufacturing jobs
      - reduction in ISK requirements for manufacturing and science jobs
      - reduction in time requirements for manufacturing and science jobs

    Returns values as fractions (e.g. 0.15 == 15% reduction).
    """

    role_raw = row.get("roleBonuses")
    try:
        role = json.loads(role_raw) if isinstance(role_raw, str) else (role_raw or [])
    except Exception:
        role = []

    material_pct = 0.0
    time_pct = 0.0
    cost_pct = 0.0

    for b in role or []:
        if not isinstance(b, dict):
            continue
        bonus = b.get("bonus")
        if bonus is None:
            continue
        try:
            # SDE sometimes stores reductions as negative numbers.
            bonus_val = abs(float(bonus))
        except Exception:
            continue

        txt_obj = b.get("bonusText") or {}
        if isinstance(txt_obj, dict):
            txt = txt_obj.get(language) or txt_obj.get("en") or ""
        else:
            txt = ""
        t = str(txt).lower()

        if "material requirements" in t:
            material_pct = max(material_pct, bonus_val)
        elif "isk requirements" in t:
            cost_pct = max(cost_pct, bonus_val)
        elif "time requirements" in t:
            time_pct = max(time_pct, bonus_val)

    return {
        "material_reduction": max(0.0, material_pct) / 100.0,
        "time_reduction": max(0.0, time_pct) / 100.0,
        "cost_reduction": max(0.0, cost_pct) / 100.0,
    }


@industry_bp.get("/structure_type_bonuses/<int:type_id>")
def structure_type_bonuses(type_id: int):
    """Return base industry bonuses for a given structure type.

    Sourced from SDE `typeBonus.roleBonuses` when available.
    """
    if not type_id:
        return error(message="Type ID is required.", status_code=400)

    try:
        require_ready()
        ensure_sde_ready()
        session = get_db_sde_session()
        language = get_language()

        row = session.execute(
            text("SELECT id, roleBonuses, miscBonuses, types FROM typeBonus WHERE id = :id"),
            {"id": int(type_id)},
        ).mappings().fetchone()

        if not row:
            return ok(data={"type_id": int(type_id), "bonuses": {"material_reduction": 0.0, "time_reduction": 0.0, "cost_reduction": 0.0}})

        bonuses = _extract_structure_industry_bonuses_from_type_bonus(dict(row), language=language)
        return ok(data={"type_id": int(type_id), "bonuses": bonuses})
    except Exception as e:
        return error(message=f"Error in GET Method `/structure_type_bonuses/{type_id}`: " + str(e))


@industry_bp.get("/industry_builder_data/<int:character_id>")
def industry_builder(character_id: int):
    try:
        require_ready()
        character = state.char_manager.get_character_by_id(character_id)
        if not character:
            return error(
                message=f"Error in GET Method `/industry_builder_data/{character_id}`: Character ID {character_id} not found",
                status_code=400,
            )

        session = get_db_app_session()
        language = get_language()

        sde_session = None
        try:
            ensure_sde_ready()
            sde_session = get_db_sde_session()
        except Exception:
            sde_session = None

        # Optional industry profile selection (defaults to the character's default profile).
        profile_id = request.args.get("profile_id", default=None, type=int)
        selected_profile = None
        if profile_id:
            selected_profile = industry_profiles_repo.get_by_id(session, int(profile_id))
            if selected_profile and int(selected_profile.character_id) != int(character_id):
                return error(message="Industry profile does not belong to this character.", status_code=400)
        else:
            selected_profile = industry_profiles_repo.get_default_for_character_id(session, int(character_id))

        # Live manufacturing system cost index (from ESI), if we have a profile+system.
        manufacturing_ci = 0.0
        if selected_profile is not None and getattr(selected_profile, "system_id", None) is not None:
            if state.esi_service is not None:
                try:
                    systems = state.esi_service.get_industry_systems()
                    sid = int(getattr(selected_profile, "system_id"))
                    row = next((s for s in systems if s.get("solar_system_id") == sid), None)
                    if row:
                        for entry in (row.get("cost_indices") or []):
                            if entry.get("activity") == "manufacturing":
                                manufacturing_ci = float(entry.get("cost_index") or 0.0)
                                break
                except Exception:
                    manufacturing_ci = 0.0

        # Rig effects for the selected profile (SDE-driven).
        rig_payload: list[dict] = []
        try:
            if selected_profile is not None:
                rig_ids = [
                    getattr(selected_profile, "rig_slot0_type_id", None),
                    getattr(selected_profile, "rig_slot1_type_id", None),
                    getattr(selected_profile, "rig_slot2_type_id", None),
                ]
                rig_ids = [int(x) for x in rig_ids if x is not None and int(x) != 0]
                if rig_ids:
                    ensure_sde_ready()
                    sde_session = get_db_sde_session()
                    rig_payload = get_rig_effects_for_type_ids(sde_session, rig_ids)
        except Exception:
            rig_payload = []
        all_blueprints = blueprints_service.get_blueprint_assets(session, state.esi_service)
        if character_id:
            all_blueprints = [bp for bp in all_blueprints if bp.get("owner_id") == character_id]

        data = enrich_blueprints_for_character(
            all_blueprints,
            character,
            industry_profile=selected_profile,
            manufacturing_system_cost_index=manufacturing_ci,
            rig_payload=rig_payload,
            db_app_session=session,
            db_sde_session=sde_session,
            language=language,
        )
        return ok(
            data=data,
            meta={
                "profile_id": (int(getattr(selected_profile, "id")) if selected_profile is not None else None),
                "profile_name": (getattr(selected_profile, "profile_name", None) if selected_profile is not None else None),
            },
        )
    except Exception as e:
        return error(message=f"Error in GET Method `/industry_builder_data/{character_id}`: " + str(e))


@industry_bp.get("/solar_systems")
def solar_systems():
    try:
        require_ready()
        ensure_sde_ready()
        session = get_db_sde_session()
        language = get_language()
        solar_systems_data = get_solar_systems(session, language)
        return ok(data=solar_systems_data)
    except Exception as e:
        return error(message="Error in GET Method `/solar_systems`: " + str(e))


@industry_bp.get("/npc_stations/<int:system_id>")
def npc_stations(system_id: int):
    if not system_id:
        return error(message="System ID is required to fetch NPC stations.", status_code=400)

    try:
        require_ready()
        ensure_sde_ready()
        session = get_db_sde_session()
        language = get_language()
        npc_stations_data = get_npc_stations(session, language, system_id)
        return ok(data=npc_stations_data)
    except ValueError as ve:
        return error(message=f"Error in GET Method `/npc_stations/{system_id}`: " + str(ve), status_code=404)
    except Exception as e:
        return error(message=f"Error in GET Method `/npc_stations/{system_id}`: " + str(e))


@industry_bp.get("/public_structures/<int:system_id>")
def structures(system_id: int):
    if not system_id:
        return error(message="System ID is required to fetch public structures.", status_code=400)

    try:
        require_ready()
        if state.esi_service is None:
            return error(
                message=(
                    f"Error in GET Method `/public_structures/{system_id}`: "
                    "ESI service is not initialized (application not fully ready for ESI-backed endpoints)"
                ),
                status_code=503,
            )

        ttl_seconds = public_structures_cache_ttl_seconds()
        public_structures, is_fresh = get_cached_public_structures(system_id, ttl_seconds=ttl_seconds)
        refreshing = False
        if not is_fresh:
            refreshing = trigger_refresh_public_structures_for_system(system_id)

        type_ids = list({s["type_id"] for s in public_structures if s.get("type_id") is not None})
        owner_ids = list({int(s["owner_id"]) for s in public_structures if s.get("owner_id") is not None})
        ensure_sde_ready()
        session = get_db_sde_session()
        language = get_language()
        type_map = get_type_data(session, language, type_ids)

        owner_name_map: dict[int, str] = {}
        try:
            if owner_ids:
                resolved = state.esi_service.get_universe_names(owner_ids)
                owner_name_map = {int(k): (v or {}).get("name") for k, v in resolved.items()}
        except Exception:
            owner_name_map = {}

        enriched_structures = []
        for s in public_structures:
            type_id = s.get("type_id")
            extra = type_map.get(int(type_id), {}) if type_id is not None else {}
            owner_id = s.get("owner_id")
            owner_name = owner_name_map.get(int(owner_id)) if owner_id is not None else None
            enriched_structures.append({**s, **extra, "owner_name": owner_name})
        return ok(data=enriched_structures, meta={"refreshing": refreshing, "cache_fresh": is_fresh})
    except ValueError as ve:
        return error(message=f"Error in GET Method `/public_structures/{system_id}`: " + str(ve), status_code=404)
    except Exception as e:
        return error(message=f"Error in GET Method `/public_structures/{system_id}`: " + str(e))


@industry_bp.get("/corporation_structures/<int:character_id>")
def corporation_structures(character_id: int):
    if not character_id:
        return error(message="Character ID is required to fetch corporation structures.", status_code=400)

    try:
        require_ready()
        character = state.char_manager.get_character_by_id(character_id)
        if not character:
            return error(
                message=f"Error in GET Method `/corporation_structures/{character_id}`: Character ID {character_id} not found",
                status_code=400,
            )
        if not character.corporation_id:
            return error(
                message=f"Error in GET Method `/corporation_structures/{character_id}`: Character has no corporation_id",
                status_code=400,
            )

        session = get_db_app_session()
        structures = corporation_structures_repo.list_by_corporation_id(session, int(character.corporation_id))

        out = [s.to_dict() for s in structures]
        # Best-effort: enrich with SDE type names (used by UI for rig filtering).
        try:
            type_ids = list({int(s.get("type_id")) for s in out if s.get("type_id") is not None})
            if type_ids:
                ensure_sde_ready()
                sde_session = get_db_sde_session()
                language = get_language()
                type_map = get_type_data(sde_session, language, type_ids)
                for s in out:
                    tid = s.get("type_id")
                    extra = type_map.get(int(tid), {}) if tid is not None else {}
                    # Keep original keys; just add enrichment fields.
                    s.update(extra)
        except Exception:
            pass

        # Best-effort: resolve corporation names for owner display.
        try:
            corp_ids = list({int(s.get("corporation_id")) for s in out if s.get("corporation_id") is not None})
            if corp_ids and state.esi_service is not None:
                resolved = state.esi_service.get_universe_names(corp_ids)
                corp_name_map = {int(k): (v or {}).get("name") for k, v in resolved.items()}
                for s in out:
                    cid = s.get("corporation_id")
                    if cid is None:
                        continue
                    s["owner_name"] = corp_name_map.get(int(cid))
        except Exception:
            pass

        return ok(data=out)
    except Exception as e:
        return error(message=f"Error in GET Method `/corporation_structures/{character_id}`: " + str(e))


@industry_bp.get("/industry_profiles/<int:character_id>")
def industry_profiles(character_id: int):
    try:
        require_ready()
        session = get_db_app_session()
        profiles = industry_profiles_repo.list_by_character_id(session, character_id)
        return ok(data=[p.to_dict() for p in profiles])
    except Exception as e:
        return error(message=f"Error in GET Method `/industry_profiles/{character_id}`: " + str(e))


@industry_bp.get("/industry_system_cost_index/<int:system_id>")
def industry_system_cost_index(system_id: int):
    if not system_id:
        return error(message="System ID is required.", status_code=400)

    try:
        require_ready()
        if state.esi_service is None:
            return error(message="ESI service not initialized.", status_code=503)

        systems = state.esi_service.get_industry_systems()
        row = next((s for s in systems if s.get("solar_system_id") == system_id), None)
        if not row:
            return ok(data={"solar_system_id": system_id, "cost_indices": []})
        return ok(data=row)
    except Exception as e:
        return error(message=f"Error in GET Method `/industry_system_cost_index/{system_id}`: " + str(e))


@industry_bp.get("/industry_facility/<int:facility_id>")
def industry_facility(facility_id: int):
    if not facility_id:
        return error(message="Facility ID is required.", status_code=400)

    try:
        require_ready()
        if state.esi_service is None:
            return error(message="ESI service not initialized.", status_code=503)

        facilities = state.esi_service.get_industry_facilities()
        row = next((f for f in facilities if f.get("facility_id") == facility_id), None)
        if not row:
            # Not all facilities are returned (depends on ESI). Return minimal shape.
            return ok(data={"facility_id": facility_id, "tax": None})
        # ESI no longer guarantees a `tax` field (as of 2026-01).
        # Keep response shape stable for the UI.
        out = dict(row) if isinstance(row, dict) else {"facility_id": facility_id}
        out.setdefault("tax", None)
        return ok(data=out)
    except Exception as e:
        return error(message=f"Error in GET Method `/industry_facility/{facility_id}`: " + str(e))


@industry_bp.get("/structure_rigs")
def structure_rigs():
    """Return a list of structure manufacturing rigs with their reduction bonuses.

    Uses the local SDE for type IDs/names and ESI /universe/types/{type_id}/ dogma
    attributes for the actual bonus values.
    """
    try:
        require_ready()
        cache = getattr(state, "_structure_rigs_cache", None)
        now = time.time()
        # Cache shape is (timestamp, version, data)
        if (
            cache
            and isinstance(cache, tuple)
            and len(cache) == 3
            and cache[1] == _STRUCTURE_RIGS_CACHE_VERSION
            and (now - cache[0] < _STRUCTURE_RIG_MFG_CACHE_TTL_SECONDS)
        ):
            return ok(data=cache[2])

        ensure_sde_ready()
        session = get_db_sde_session()
        language = get_language()

        # Find published Standup manufacturing rigs in the SDE.
        # `types.name` is stored as a JSON blob; LIKE searches are against that JSON.
        rows = session.execute(
            text(
                """
                SELECT id, name
                FROM types
                WHERE published = 1
                  AND name LIKE '%Standup%'
                                    AND (
                                                name LIKE '%Manufacturing%'
                                         OR name LIKE '%Research%'
                                         OR name LIKE '%Invention%'
                                    )
                                    AND name NOT LIKE '%Blueprint%'
                ORDER BY id ASC
                """
            )
        ).fetchall()

        type_ids = [int(r[0]) for r in rows]

        # Load dogma in batch.
        dogma_by_type_id: dict[int, dict] = {}
        if type_ids:
            dogma_rows = session.execute(
                text("SELECT id, dogmaAttributes, dogmaEffects FROM typeDogma WHERE id IN :ids").bindparams(
                    bindparam("ids", expanding=True)
                ),
                {"ids": type_ids},
            ).fetchall()

            for tid, attrs_raw, effects_raw in dogma_rows:
                attr_map: dict[int, float] = {}
                effect_ids: list[int] = []
                try:
                    attrs = json.loads(attrs_raw) if isinstance(attrs_raw, str) else (attrs_raw or [])
                    for a in attrs or []:
                        if not isinstance(a, dict):
                            continue
                        aid = a.get("attributeID")
                        val = a.get("value")
                        if aid is None or val is None:
                            continue
                        attr_map[int(aid)] = float(val)
                except Exception:
                    attr_map = {}

                try:
                    effs = json.loads(effects_raw) if isinstance(effects_raw, str) else (effects_raw or [])
                    for e in effs or []:
                        if not isinstance(e, dict):
                            continue
                        eid = e.get("effectID")
                        if eid is None:
                            continue
                        effect_ids.append(int(eid))
                except Exception:
                    effect_ids = []

                dogma_by_type_id[int(tid)] = {"attr_map": attr_map, "effect_ids": effect_ids}

        # Resolve effect names (batch).
        all_effect_ids = sorted(
            {eid for d in dogma_by_type_id.values() for eid in (d.get("effect_ids") or []) if eid is not None}
        )
        effect_name_by_id: dict[int, str] = {}
        if all_effect_ids:
            eff_rows = session.execute(
                text("SELECT id, name FROM dogmaEffects WHERE id IN :ids").bindparams(bindparam("ids", expanding=True)),
                {"ids": all_effect_ids},
            ).fetchall()
            effect_name_by_id = {int(r[0]): (r[1] or "") for r in eff_rows}

        rigs_out: list[dict] = []
        for type_id, name_json in rows:
            try:
                name_obj = json.loads(name_json) if isinstance(name_json, str) else {}
                rig_name = name_obj.get(language) or name_obj.get("en") or str(type_id)
            except Exception:
                rig_name = str(type_id)

            d = dogma_by_type_id.get(int(type_id)) or {}
            attr_map = d.get("attr_map") or {}

            # ESI encodes these as percent values (often negative for reductions).
            time_val = attr_map.get(_RIG_ATTR_TIME_REDUCTION, 0.0)
            mat_val = attr_map.get(_RIG_ATTR_MATERIAL_REDUCTION, 0.0)
            cost_val = attr_map.get(_RIG_ATTR_COST_REDUCTION, 0.0)

            time_reduction = max(0.0, (-float(time_val)) / 100.0)
            material_reduction = max(0.0, (-float(mat_val)) / 100.0)
            cost_reduction = max(0.0, (-float(cost_val)) / 100.0)

            effects_out: list[dict] = []
            for eid in d.get("effect_ids") or []:
                ename = effect_name_by_id.get(int(eid)) or ""
                # Filter noise (slot/security/etc). Keep only rig*Bonus effects.
                if ename in {"rigSlot"}:
                    continue
                if not ename.startswith("rig"):
                    continue
                if "Bonus" not in ename:
                    continue

                activity, group_token, metric = _parse_rig_effect(ename)
                if activity is None or metric is None:
                    continue

                if metric == "material":
                    val = material_reduction
                elif metric == "time":
                    val = time_reduction
                else:
                    val = cost_reduction

                if val <= 0:
                    continue

                if not group_token:
                    group = "All"
                else:
                    group = _RIG_GROUP_TOKEN_LABELS.get(group_token) or _camel_to_words(group_token)
                effects_out.append(
                    {
                        "effect_id": int(eid),
                        "effect_name": ename,
                        "activity": activity,
                        "group": group,
                        "metric": metric,
                        "value": float(val),
                    }
                )

            rigs_out.append(
                {
                    "type_id": int(type_id),
                    "name": str(rig_name),
                    "time_reduction": time_reduction,
                    "material_reduction": material_reduction,
                    "cost_reduction": cost_reduction,
                    "effects": effects_out,
                }
            )

        # Cache and return.
        rigs_out.sort(key=lambda r: r["name"])
        state._structure_rigs_cache = (now, _STRUCTURE_RIGS_CACHE_VERSION, rigs_out)
        return ok(data=rigs_out)
    except Exception as e:
        return error(message="Error in GET Method `/structure_rigs`: " + str(e))


@industry_bp.post("/industry_profiles")
def create_industry_profile():
    try:
        require_ready()
        data = request.get_json()
        character_id = data.get("character_id")

        if not character_id:
            return error(message="Character ID is required to create an industry profile.", status_code=400)

        session = get_db_app_session()
        profile_id = industry_profiles_repo.create(session, data)
        return ok(data={"id": profile_id}, status_code=201)
    except Exception as e:
        return error(message="Error in POST Method `/industry_profiles`: " + str(e))


@industry_bp.put("/industry_profiles/<int:profile_id>")
def update_industry_profile(profile_id: int):
    try:
        require_ready()
        data = request.json
        session = get_db_app_session()
        industry_profiles_repo.update(session, profile_id, data)
        return ok(message="Industry profile updated successfully.")
    except Exception as e:
        return error(message=f"Error in PUT Method `/industry_profiles/{profile_id}`: " + str(e))


@industry_bp.delete("/industry_profiles/<int:profile_id>")
def delete_industry_profile(profile_id: int):
    try:
        require_ready()
        session = get_db_app_session()
        industry_profiles_repo.delete(session, profile_id)
        return ok(message="Industry profile deleted successfully.")
    except ValueError as ve:
        return error(message=f"Error in DELETE Method `/industry_profiles/{profile_id}`: " + str(ve), status_code=404)
    except Exception as e:
        return error(message=f"Error in DELETE Method `/industry_profiles/{profile_id}`: " + str(e))
