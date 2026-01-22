from __future__ import annotations

from flask import Blueprint, request
import json
import time
import threading
import uuid
from datetime import datetime
from sqlalchemy import bindparam, text
from sqlalchemy.sql import func

from classes.asset_provenance import build_fifo_remaining_lots_by_type
from classes.database_models import (
    CharacterAssetsModel,
    CharacterIndustryJobsModel,
    CharacterWalletTransactionsModel,
    CorporationAssetsModel,
    CorporationIndustryJobsModel,
    CorporationWalletTransactionsModel,
)

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
from flask_app.services.submanufacturing_planner_service import plan_submanufacturing_tree
from flask_app.services.structure_rig_effects_service import get_rig_effects_for_type_ids
from flask_app.settings import public_structures_cache_ttl_seconds
from flask_app.services.public_structures_cache_service import (
    get_cached_public_structures,
    trigger_refresh_public_structures_for_system,
)


industry_bp = Blueprint("industry", __name__)


_INDUSTRY_BUILDER_JOB_TTL_SECONDS = 6 * 3600


def _industry_builder_job_key(*, character_id: int, profile_id: int | None, maximize_runs: bool) -> str:
    pid = int(profile_id or 0)
    return f"{int(character_id)}:{pid}:{1 if maximize_runs else 0}"


def _cleanup_old_industry_builder_jobs() -> None:
    now = datetime.utcnow()
    with state.industry_builder_jobs_lock:
        to_delete: list[str] = []
        for job_id, job in list(state.industry_builder_jobs.items()):
            if not isinstance(job, dict):
                to_delete.append(str(job_id))
                continue
            created_at = job.get("created_at")
            if not isinstance(created_at, datetime):
                continue
            age_s = (now - created_at).total_seconds()
            if age_s > _INDUSTRY_BUILDER_JOB_TTL_SECONDS:
                to_delete.append(str(job_id))

        for job_id in to_delete:
            state.industry_builder_jobs.pop(job_id, None)
            # Also remove from the key map (best-effort)
            for k, v in list(state.industry_builder_jobs_by_key.items()):
                if v == job_id:
                    state.industry_builder_jobs_by_key.pop(k, None)


def _run_industry_builder_update_job(
    *,
    job_id: str,
    character_id: int,
    profile_id: int | None,
    maximize_runs: bool,
) -> None:
    """Compute full Industry Builder dataset (incl. submanufacturing) in a background thread."""
    try:
        require_ready()
        character = state.char_manager.get_character_by_id(int(character_id))
        if not character:
            raise ValueError(f"Character ID {character_id} not found")

        session = get_db_app_session()
        language = get_language()

        sde_session = None
        try:
            ensure_sde_ready()
            sde_session = get_db_sde_session()
        except Exception:
            sde_session = None

        # Resolve profile (same logic as GET /industry_builder_data)
        selected_profile = None
        if profile_id:
            selected_profile = industry_profiles_repo.get_by_id(session, int(profile_id))
            if selected_profile and int(getattr(selected_profile, "character_id", 0) or 0) != int(character_id):
                raise ValueError("Industry profile does not belong to this character")
        else:
            selected_profile = industry_profiles_repo.get_default_for_character_id(session, int(character_id))

        # Live cost indices
        manufacturing_ci = 0.0
        copying_ci = 0.0
        research_me_ci = 0.0
        research_te_ci = 0.0
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
                            elif entry.get("activity") == "copying":
                                copying_ci = float(entry.get("cost_index") or 0.0)
                            elif entry.get("activity") == "researching_material_efficiency":
                                research_me_ci = float(entry.get("cost_index") or 0.0)
                            elif entry.get("activity") == "researching_time_efficiency":
                                research_te_ci = float(entry.get("cost_index") or 0.0)
                except Exception:
                    manufacturing_ci = 0.0
                    copying_ci = 0.0
                    research_me_ci = 0.0
                    research_te_ci = 0.0

        # Surcharge rate (fraction)
        surcharge_rate = 0.0
        if selected_profile is not None:
            try:
                facility_tax = float(getattr(selected_profile, "facility_tax", 0.0) or 0.0)
            except Exception:
                facility_tax = 0.0
            try:
                scc_surcharge = float(getattr(selected_profile, "scc_surcharge", 0.0) or 0.0)
            except Exception:
                scc_surcharge = 0.0
            if facility_tax >= 1.0:
                facility_tax = facility_tax / 100.0
            if scc_surcharge >= 1.0:
                scc_surcharge = scc_surcharge / 100.0
            surcharge_rate = max(0.0, facility_tax + scc_surcharge)

        # Owned blueprint maps (character + corporation)
        owned_bp_type_ids: set[int] = set()
        owned_bp_best_by_type_id: dict[int, dict] = {}

        def _consider_owned_bp(
            *,
            blueprint_type_id: int,
            is_blueprint_copy: bool,
            me_percent: int | None,
            te_percent: int | None,
            runs: int | None,
        ) -> None:
            tid = int(blueprint_type_id)
            if tid <= 0:
                return
            owned_bp_type_ids.add(tid)

            try:
                me_i = int(me_percent or 0)
            except Exception:
                me_i = 0
            try:
                te_i = int(te_percent or 0)
            except Exception:
                te_i = 0

            rec = {
                "is_blueprint_copy": bool(is_blueprint_copy),
                "me_percent": int(me_i),
                "te_percent": int(te_i),
                "runs": (int(runs) if runs is not None else None),
            }

            cur = owned_bp_best_by_type_id.get(tid)
            if not isinstance(cur, dict):
                owned_bp_best_by_type_id[tid] = rec
                return

            cur_is_bpc = bool(cur.get("is_blueprint_copy"))
            new_is_bpc = bool(rec.get("is_blueprint_copy"))
            if new_is_bpc and not cur_is_bpc:
                owned_bp_best_by_type_id[tid] = rec
                return
            if new_is_bpc == cur_is_bpc:
                cur_me = int(cur.get("me_percent") or 0)
                cur_te = int(cur.get("te_percent") or 0)
                new_me = int(rec.get("me_percent") or 0)
                new_te = int(rec.get("te_percent") or 0)
                if (new_me > cur_me) or (new_me == cur_me and new_te > cur_te):
                    owned_bp_best_by_type_id[tid] = rec

        try:
            rows = (
                session.query(
                    CharacterAssetsModel.type_id,
                    CharacterAssetsModel.is_blueprint_copy,
                    CharacterAssetsModel.blueprint_material_efficiency,
                    CharacterAssetsModel.blueprint_time_efficiency,
                    CharacterAssetsModel.blueprint_runs,
                )
                .filter(
                    CharacterAssetsModel.type_category_name == "Blueprint",
                    CharacterAssetsModel.character_id == int(character_id),
                )
                .all()
            )
            for tid, is_bpc, me, te, runs in rows or []:
                if tid is None:
                    continue
                _consider_owned_bp(
                    blueprint_type_id=int(tid),
                    is_blueprint_copy=bool(is_bpc),
                    me_percent=(int(me) if me is not None else None),
                    te_percent=(int(te) if te is not None else None),
                    runs=(int(runs) if runs is not None else None),
                )
        except Exception:
            pass

        try:
            corp_id = getattr(character, "corporation_id", None)
            if corp_id is not None:
                rows = (
                    session.query(
                        CorporationAssetsModel.type_id,
                        CorporationAssetsModel.is_blueprint_copy,
                        CorporationAssetsModel.blueprint_material_efficiency,
                        CorporationAssetsModel.blueprint_time_efficiency,
                        CorporationAssetsModel.blueprint_runs,
                    )
                    .filter(
                        CorporationAssetsModel.type_category_name == "Blueprint",
                        CorporationAssetsModel.corporation_id == int(corp_id),
                    )
                    .all()
                )
                for tid, is_bpc, me, te, runs in rows or []:
                    if tid is None:
                        continue
                    _consider_owned_bp(
                        blueprint_type_id=int(tid),
                        is_blueprint_copy=bool(is_bpc),
                        me_percent=(int(me) if me is not None else None),
                        te_percent=(int(te) if te is not None else None),
                        runs=(int(runs) if runs is not None else None),
                    )
        except Exception:
            pass

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

        # Load owned blueprints only (fast path)
        all_blueprints = blueprints_service.get_blueprint_assets(session, state.esi_service, include_unowned=False)
        all_blueprints = [bp for bp in all_blueprints if bp.get("owner_id") == int(character_id)]

        def _progress(done: int, total: int, _bp_type_id: int) -> None:
            with state.industry_builder_jobs_lock:
                job = state.industry_builder_jobs.get(job_id)
                if not isinstance(job, dict):
                    return
                job["progress_done"] = int(done)
                job["progress_total"] = int(total)
                job["updated_at"] = datetime.utcnow()

        data = enrich_blueprints_for_character(
            all_blueprints,
            character,
            esi_service=state.esi_service,
            industry_profile=selected_profile,
            manufacturing_system_cost_index=manufacturing_ci,
            copying_system_cost_index=copying_ci,
            research_me_system_cost_index=research_me_ci,
            research_te_system_cost_index=research_te_ci,
            surcharge_rate_total_fraction=surcharge_rate,
            owned_blueprint_type_ids=owned_bp_type_ids,
            owned_blueprint_best_by_type_id=owned_bp_best_by_type_id,
            include_submanufacturing=True,
            submanufacturing_blueprint_type_id=None,
            maximize_blueprint_runs=bool(maximize_runs),
            rig_payload=rig_payload,
            db_app_session=session,
            db_sde_session=sde_session,
            language=language,
            progress_callback=_progress,
        )

        meta = {
            "profile_id": (int(getattr(selected_profile, "id")) if selected_profile is not None else None),
            "profile_name": (getattr(selected_profile, "profile_name", None) if selected_profile is not None else None),
        }

        with state.industry_builder_jobs_lock:
            job = state.industry_builder_jobs.get(job_id)
            if isinstance(job, dict):
                job["status"] = "done"
                job["data"] = data
                job["meta"] = meta
                job["updated_at"] = datetime.utcnow()

    except Exception as e:
        with state.industry_builder_jobs_lock:
            job = state.industry_builder_jobs.get(job_id)
            if isinstance(job, dict):
                job["status"] = "error"
                job["error"] = str(e)
                job["updated_at"] = datetime.utcnow()


@industry_bp.post("/industry_builder_update/<int:character_id>")
def industry_builder_update(character_id: int):
    """Kick off a background computation of Industry Builder data (incl. submanufacturing)."""
    try:
        require_ready()
        _cleanup_old_industry_builder_jobs()

        character = state.char_manager.get_character_by_id(int(character_id))
        if not character:
            return error(message=f"Character ID {character_id} not found", status_code=400)

        payload = request.get_json(silent=True) or {}
        profile_id = payload.get("profile_id")
        try:
            profile_id_i = int(profile_id) if profile_id is not None else None
        except Exception:
            profile_id_i = None
        maximize_runs = bool(payload.get("maximize_runs", False))

        key = _industry_builder_job_key(character_id=int(character_id), profile_id=profile_id_i, maximize_runs=maximize_runs)

        with state.industry_builder_jobs_lock:
            existing_job_id = state.industry_builder_jobs_by_key.get(key)
            if existing_job_id:
                existing = state.industry_builder_jobs.get(existing_job_id)
                if isinstance(existing, dict) and existing.get("status") in {"running", "done"}:
                    return ok(data={"job_id": str(existing_job_id)})

            job_id = uuid.uuid4().hex
            state.industry_builder_jobs_by_key[key] = job_id
            state.industry_builder_jobs[job_id] = {
                "status": "running",
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "character_id": int(character_id),
                "profile_id": (int(profile_id_i) if profile_id_i is not None else None),
                "maximize_runs": bool(maximize_runs),
                "progress_done": 0,
                "progress_total": None,
                "error": None,
                "meta": None,
                "data": None,
            }

        t = threading.Thread(
            target=_run_industry_builder_update_job,
            kwargs={
                "job_id": job_id,
                "character_id": int(character_id),
                "profile_id": profile_id_i,
                "maximize_runs": bool(maximize_runs),
            },
            daemon=True,
        )
        t.start()

        return ok(data={"job_id": str(job_id)})

    except Exception as e:
        return error(message=f"Error starting Industry Builder update: {e}")


@industry_bp.get("/industry_builder_update_status/<job_id>")
def industry_builder_update_status(job_id: str):
    try:
        require_ready()
        with state.industry_builder_jobs_lock:
            job = state.industry_builder_jobs.get(str(job_id))
            if not isinstance(job, dict):
                return error(message="Job not found", status_code=404)

            return ok(
                data={
                    "job_id": str(job_id),
                    "status": job.get("status"),
                    "progress_done": job.get("progress_done"),
                    "progress_total": job.get("progress_total"),
                    "error": job.get("error"),
                }
            )
    except Exception as e:
        return error(message=f"Error reading job status: {e}")


@industry_bp.get("/industry_builder_update_result/<job_id>")
def industry_builder_update_result(job_id: str):
    try:
        require_ready()
        with state.industry_builder_jobs_lock:
            job = state.industry_builder_jobs.get(str(job_id))
            if not isinstance(job, dict):
                return error(message="Job not found", status_code=404)
            if job.get("status") == "error":
                return error(message=str(job.get("error") or "Job failed"), status_code=500)
            if job.get("status") != "done":
                return error(message="Job not finished", status_code=409)

            data = job.get("data")
            meta = job.get("meta")

        return ok(data=data, meta=meta)
    except Exception as e:
        return error(message=f"Error reading job result: {e}")

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
        maximize_runs = bool(request.args.get("maximize_runs", default=0, type=int))
        include_submanufacturing = bool(request.args.get("include_submanufacturing", default=0, type=int))
        submanufacturing_blueprint_type_id = request.args.get("blueprint_type_id", default=None, type=int)
        selected_profile = None
        if profile_id:
            selected_profile = industry_profiles_repo.get_by_id(session, int(profile_id))
            if selected_profile and int(selected_profile.character_id) != int(character_id):
                return error(message="Industry profile does not belong to this character.", status_code=400)
        else:
            selected_profile = industry_profiles_repo.get_default_for_character_id(session, int(character_id))

        # Live manufacturing system cost index (from ESI), if we have a profile+system.
        manufacturing_ci = 0.0
        copying_ci = 0.0
        research_me_ci = 0.0
        research_te_ci = 0.0
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
                            elif entry.get("activity") == "copying":
                                copying_ci = float(entry.get("cost_index") or 0.0)
                            elif entry.get("activity") == "researching_material_efficiency":
                                research_me_ci = float(entry.get("cost_index") or 0.0)
                            elif entry.get("activity") == "researching_time_efficiency":
                                research_te_ci = float(entry.get("cost_index") or 0.0)
                except Exception:
                    manufacturing_ci = 0.0
                    copying_ci = 0.0
                    research_me_ci = 0.0
                    research_te_ci = 0.0

        # Surcharge rate used in job fee estimate (fraction)
        surcharge_rate = 0.0
        if selected_profile is not None:
            try:
                facility_tax = float(getattr(selected_profile, "facility_tax", 0.0) or 0.0)
            except Exception:
                facility_tax = 0.0
            try:
                scc_surcharge = float(getattr(selected_profile, "scc_surcharge", 0.0) or 0.0)
            except Exception:
                scc_surcharge = 0.0
            if facility_tax >= 1.0:
                facility_tax = facility_tax / 100.0
            if scc_surcharge >= 1.0:
                scc_surcharge = scc_surcharge / 100.0
            surcharge_rate = max(0.0, facility_tax + scc_surcharge)

        # Best-effort: determine owned blueprint type_ids + best ME/TE per blueprint (character + corporation).
        owned_bp_type_ids: set[int] = set()
        owned_bp_best_by_type_id: dict[int, dict] = {}

        def _consider_owned_bp(
            *,
            blueprint_type_id: int,
            is_blueprint_copy: bool,
            me_percent: int | None,
            te_percent: int | None,
            runs: int | None,
        ) -> None:
            tid = int(blueprint_type_id)
            if tid <= 0:
                return
            owned_bp_type_ids.add(tid)

            me_i = 0
            te_i = 0
            try:
                me_i = int(me_percent or 0)
            except Exception:
                me_i = 0
            try:
                te_i = int(te_percent or 0)
            except Exception:
                te_i = 0

            rec = {
                "is_blueprint_copy": bool(is_blueprint_copy),
                "me_percent": int(me_i),
                "te_percent": int(te_i),
                "runs": (int(runs) if runs is not None else None),
            }

            cur = owned_bp_best_by_type_id.get(tid)
            if not isinstance(cur, dict):
                owned_bp_best_by_type_id[tid] = rec
                return

            cur_is_bpc = bool(cur.get("is_blueprint_copy"))
            new_is_bpc = bool(rec.get("is_blueprint_copy"))
            if new_is_bpc and not cur_is_bpc:
                owned_bp_best_by_type_id[tid] = rec
                return
            if new_is_bpc == cur_is_bpc:
                cur_me = int(cur.get("me_percent") or 0)
                cur_te = int(cur.get("te_percent") or 0)
                new_me = int(rec.get("me_percent") or 0)
                new_te = int(rec.get("te_percent") or 0)
                if (new_me > cur_me) or (new_me == cur_me and new_te > cur_te):
                    owned_bp_best_by_type_id[tid] = rec

        try:
            rows = (
                session.query(
                    CharacterAssetsModel.type_id,
                    CharacterAssetsModel.is_blueprint_copy,
                    CharacterAssetsModel.blueprint_material_efficiency,
                    CharacterAssetsModel.blueprint_time_efficiency,
                    CharacterAssetsModel.blueprint_runs,
                )
                .filter(
                    CharacterAssetsModel.type_category_name == "Blueprint",
                    CharacterAssetsModel.character_id == int(character_id),
                )
                .all()
            )
            for tid, is_bpc, me, te, runs in rows or []:
                if tid is None:
                    continue
                _consider_owned_bp(
                    blueprint_type_id=int(tid),
                    is_blueprint_copy=bool(is_bpc),
                    me_percent=(int(me) if me is not None else None),
                    te_percent=(int(te) if te is not None else None),
                    runs=(int(runs) if runs is not None else None),
                )
        except Exception:
            pass

        try:
            corp_id = getattr(character, "corporation_id", None)
            if corp_id is not None:
                rows = (
                    session.query(
                        CorporationAssetsModel.type_id,
                        CorporationAssetsModel.is_blueprint_copy,
                        CorporationAssetsModel.blueprint_material_efficiency,
                        CorporationAssetsModel.blueprint_time_efficiency,
                        CorporationAssetsModel.blueprint_runs,
                    )
                    .filter(
                        CorporationAssetsModel.type_category_name == "Blueprint",
                        CorporationAssetsModel.corporation_id == int(corp_id),
                    )
                    .all()
                )
                for tid, is_bpc, me, te, runs in rows or []:
                    if tid is None:
                        continue
                    _consider_owned_bp(
                        blueprint_type_id=int(tid),
                        is_blueprint_copy=bool(is_bpc),
                        me_percent=(int(me) if me is not None else None),
                        te_percent=(int(te) if te is not None else None),
                        runs=(int(runs) if runs is not None else None),
                    )
        except Exception:
            pass

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
        all_blueprints = blueprints_service.get_blueprint_assets(session, state.esi_service, include_unowned=False)
        if character_id:
            all_blueprints = [bp for bp in all_blueprints if bp.get("owner_id") == character_id]

        data = enrich_blueprints_for_character(
            all_blueprints,
            character,
            esi_service=state.esi_service,
            industry_profile=selected_profile,
            manufacturing_system_cost_index=manufacturing_ci,
            copying_system_cost_index=copying_ci,
            research_me_system_cost_index=research_me_ci,
            research_te_system_cost_index=research_te_ci,
            surcharge_rate_total_fraction=surcharge_rate,
            owned_blueprint_type_ids=owned_bp_type_ids,
            owned_blueprint_best_by_type_id=owned_bp_best_by_type_id,
            include_submanufacturing=include_submanufacturing,
            submanufacturing_blueprint_type_id=(int(submanufacturing_blueprint_type_id) if submanufacturing_blueprint_type_id else None),
            maximize_blueprint_runs=maximize_runs,
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


@industry_bp.post("/industry_submanufacturing_plan/<int:character_id>")
def industry_submanufacturing_plan(character_id: int):
    """Return build-vs-buy suggestions for required materials.

        Request JSON body:
            {
                "materials": [{"type_id": 123, "type_name": "Tritanium", "quantity": 1000}, ...],
                "profile_id": 1,     # optional
                "max_depth": 10      # optional (default 25)
            }

        Notes:
        - Recursive planner (depth-limited for safety).
        - Reaction-formula outputs are treated as Buy (no reaction planning).
        - Buy decisions use average price; job fees use adjusted price (EIV basis).
    """

    if not character_id:
        return error(message="Character ID is required.", status_code=400)

    try:
        require_ready()
        character = state.char_manager.get_character_by_id(character_id)
        if not character:
            return error(
                message=f"Error in POST Method `/industry_submanufacturing_plan/{character_id}`: Character ID {character_id} not found",
                status_code=400,
            )

        payload = request.get_json(silent=True) or {}
        materials = payload.get("materials") or []
        if not isinstance(materials, list) or not materials:
            return error(message="Request must include a non-empty 'materials' list.", status_code=400)

        profile_id = payload.get("profile_id")
        try:
            profile_id_i = int(profile_id) if profile_id is not None else None
        except Exception:
            profile_id_i = None

        max_depth = payload.get("max_depth")
        try:
            max_depth_i = int(max_depth) if max_depth is not None else 3
        except Exception:
            max_depth_i = 3
        max_depth_i = max(1, min(max_depth_i, 50))

        session = get_db_app_session()
        language = get_language()

        # Optional industry profile selection (defaults to the character's default profile).
        selected_profile = None
        if profile_id_i:
            selected_profile = industry_profiles_repo.get_by_id(session, int(profile_id_i))
            if selected_profile and int(getattr(selected_profile, "character_id", 0) or 0) != int(character_id):
                return error(message="Industry profile does not belong to this character.", status_code=400)
        else:
            selected_profile = industry_profiles_repo.get_default_for_character_id(session, int(character_id))

        # Live manufacturing system cost index (from ESI), if we have a profile+system.
        manufacturing_ci = 0.0
        copying_ci = 0.0
        research_me_ci = 0.0
        research_te_ci = 0.0
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
                            elif entry.get("activity") == "copying":
                                copying_ci = float(entry.get("cost_index") or 0.0)
                            elif entry.get("activity") == "researching_material_efficiency":
                                research_me_ci = float(entry.get("cost_index") or 0.0)
                            elif entry.get("activity") == "researching_time_efficiency":
                                research_te_ci = float(entry.get("cost_index") or 0.0)
                except Exception:
                    manufacturing_ci = 0.0
                    copying_ci = 0.0
                    research_me_ci = 0.0
                    research_te_ci = 0.0

        # Surcharge rate used in job fee estimate (fraction)
        surcharge_rate = 0.0
        if selected_profile is not None:
            try:
                facility_tax = float(getattr(selected_profile, "facility_tax", 0.0) or 0.0)
            except Exception:
                facility_tax = 0.0
            try:
                scc_surcharge = float(getattr(selected_profile, "scc_surcharge", 0.0) or 0.0)
            except Exception:
                scc_surcharge = 0.0
            # These are stored as fractions in newer rows, but legacy rows sometimes store percentages.
            # Keep parity with industry_builder_service._as_fraction without importing it.
            if facility_tax >= 1.0:
                facility_tax = facility_tax / 100.0
            if scc_surcharge >= 1.0:
                scc_surcharge = scc_surcharge / 100.0
            surcharge_rate = max(0.0, facility_tax + scc_surcharge)

        ensure_sde_ready()
        sde_session = get_db_sde_session()

        # Best-effort: determine owned blueprint type_ids + best ME/TE per blueprint (character + corporation).
        owned_bp_type_ids: set[int] = set()
        owned_bp_best_by_type_id: dict[int, dict] = {}

        def _consider_owned_bp(
            *,
            blueprint_type_id: int,
            is_blueprint_copy: bool,
            me_percent: int | None,
            te_percent: int | None,
            runs: int | None,
        ) -> None:
            tid = int(blueprint_type_id)
            if tid <= 0:
                return
            owned_bp_type_ids.add(tid)

            me_i = 0
            te_i = 0
            try:
                me_i = int(me_percent or 0)
            except Exception:
                me_i = 0
            try:
                te_i = int(te_percent or 0)
            except Exception:
                te_i = 0

            rec = {
                "is_blueprint_copy": bool(is_blueprint_copy),
                "me_percent": int(me_i),
                "te_percent": int(te_i),
                "runs": (int(runs) if runs is not None else None),
            }

            cur = owned_bp_best_by_type_id.get(tid)
            if not isinstance(cur, dict):
                owned_bp_best_by_type_id[tid] = rec
                return

            # Prefer an owned BPC over a BPO (matches planner semantics and avoids assumptions).
            cur_is_bpc = bool(cur.get("is_blueprint_copy"))
            new_is_bpc = bool(rec.get("is_blueprint_copy"))
            if new_is_bpc and not cur_is_bpc:
                owned_bp_best_by_type_id[tid] = rec
                return
            if new_is_bpc == cur_is_bpc:
                # Same kind: pick the best efficiency.
                cur_me = int(cur.get("me_percent") or 0)
                cur_te = int(cur.get("te_percent") or 0)
                new_me = int(rec.get("me_percent") or 0)
                new_te = int(rec.get("te_percent") or 0)
                if (new_me > cur_me) or (new_me == cur_me and new_te > cur_te):
                    owned_bp_best_by_type_id[tid] = rec

        try:
            rows = (
                session.query(
                    CharacterAssetsModel.type_id,
                    CharacterAssetsModel.is_blueprint_copy,
                    CharacterAssetsModel.blueprint_material_efficiency,
                    CharacterAssetsModel.blueprint_time_efficiency,
                    CharacterAssetsModel.blueprint_runs,
                )
                .filter(
                    CharacterAssetsModel.type_category_name == "Blueprint",
                    CharacterAssetsModel.character_id == int(character_id),
                )
                .all()
            )
            for tid, is_bpc, me, te, runs in rows or []:
                if tid is None:
                    continue
                _consider_owned_bp(
                    blueprint_type_id=int(tid),
                    is_blueprint_copy=bool(is_bpc),
                    me_percent=(int(me) if me is not None else None),
                    te_percent=(int(te) if te is not None else None),
                    runs=(int(runs) if runs is not None else None),
                )
        except Exception:
            owned_bp_type_ids = owned_bp_type_ids or set()

        try:
            corp_id = getattr(character, "corporation_id", None)
            if corp_id is not None:
                rows = (
                    session.query(
                        CorporationAssetsModel.type_id,
                        CorporationAssetsModel.is_blueprint_copy,
                        CorporationAssetsModel.blueprint_material_efficiency,
                        CorporationAssetsModel.blueprint_time_efficiency,
                        CorporationAssetsModel.blueprint_runs,
                    )
                    .filter(
                        CorporationAssetsModel.type_category_name == "Blueprint",
                        CorporationAssetsModel.corporation_id == int(corp_id),
                    )
                    .all()
                )
                for tid, is_bpc, me, te, runs in rows or []:
                    if tid is None:
                        continue
                    _consider_owned_bp(
                        blueprint_type_id=int(tid),
                        is_blueprint_copy=bool(is_bpc),
                        me_percent=(int(me) if me is not None else None),
                        te_percent=(int(te) if te is not None else None),
                        runs=(int(runs) if runs is not None else None),
                    )
        except Exception:
            pass

        # --- FIFO inventory costing inputs (best-effort) ---
        # Compute on-hand quantities for the requested material types (character + corporation)
        # and reconstruct FIFO lots from wallet transactions.
        material_type_ids: set[int] = set()
        for m in materials or []:
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

        inventory_on_hand_by_type: dict[int, int] = {}
        fifo_lots_by_type: dict[int, list] = {}

        if material_type_ids:
            try:
                rows = (
                    session.query(CharacterAssetsModel.type_id, func.sum(CharacterAssetsModel.quantity))
                    .filter(CharacterAssetsModel.character_id == int(character_id))
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
                corp_id = getattr(character, "corporation_id", None)
                if corp_id is not None:
                    rows = (
                        session.query(CorporationAssetsModel.type_id, func.sum(CorporationAssetsModel.quantity))
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

            tx_rows: list[object] = []
            try:
                tx_rows.extend(
                    (
                        session.query(CharacterWalletTransactionsModel)
                        .filter(CharacterWalletTransactionsModel.character_id == int(character_id))
                        .filter(CharacterWalletTransactionsModel.type_id.in_(sorted(material_type_ids)))
                        .all()
                    )
                    or []
                )
            except Exception:
                pass

            try:
                corp_id = getattr(character, "corporation_id", None)
                if corp_id is not None:
                    tx_rows.extend(
                        (
                            session.query(CorporationWalletTransactionsModel)
                            .filter(CorporationWalletTransactionsModel.corporation_id == int(corp_id))
                            .filter(CorporationWalletTransactionsModel.type_id.in_(sorted(material_type_ids)))
                            .all()
                        )
                        or []
                    )
            except Exception:
                pass

            try:
                job_rows: list[object] = []

                try:
                    job_rows.extend(
                        (
                            session.query(CharacterIndustryJobsModel)
                            .filter(CharacterIndustryJobsModel.character_id == int(character_id))
                            .filter(CharacterIndustryJobsModel.product_type_id.in_(sorted(material_type_ids)))
                            .all()
                        )
                        or []
                    )
                except Exception:
                    job_rows = job_rows or []

                try:
                    corp_id = getattr(character, "corporation_id", None)
                    if corp_id is not None:
                        job_rows.extend(
                            (
                                session.query(CorporationIndustryJobsModel)
                                .filter(CorporationIndustryJobsModel.corporation_id == int(corp_id))
                                .filter(CorporationIndustryJobsModel.product_type_id.in_(sorted(material_type_ids)))
                                .all()
                            )
                            or []
                        )
                except Exception:
                    pass

                market_prices: list[dict] | None = None
                try:
                    if state.esi_service is not None:
                        market_prices = (state.esi_service.get_market_prices() or [])
                except Exception:
                    market_prices = None

                fifo_lots_by_type = build_fifo_remaining_lots_by_type(
                    wallet_transactions=tx_rows,
                    industry_jobs=job_rows,
                    sde_session=sde_session,
                    market_prices=market_prices,
                    on_hand_quantities_by_type=inventory_on_hand_by_type,
                )
            except Exception:
                fifo_lots_by_type = {}

        plan = plan_submanufacturing_tree(
            sde_session=sde_session,
            language=language,
            esi_service=state.esi_service,
            materials=materials,
            owned_blueprint_type_ids=owned_bp_type_ids,
            owned_blueprint_best_by_type_id=owned_bp_best_by_type_id,
            manufacturing_system_cost_index=manufacturing_ci,
            copying_system_cost_index=copying_ci,
            research_me_system_cost_index=research_me_ci,
            research_te_system_cost_index=research_te_ci,
            surcharge_rate_total_fraction=surcharge_rate,
            inventory_on_hand_by_type=inventory_on_hand_by_type,
            inventory_fifo_lots_by_type=fifo_lots_by_type,
            use_fifo_inventory_costing=True,
            max_depth=max_depth_i,
        )

        return ok(
            data=plan,
            meta={
                "profile_id": (int(getattr(selected_profile, "id")) if selected_profile is not None else None),
                "profile_name": (getattr(selected_profile, "profile_name", None) if selected_profile is not None else None),
                "manufacturing_system_cost_index": float(manufacturing_ci),
                "copying_system_cost_index": float(copying_ci),
                "research_me_system_cost_index": float(research_me_ci),
                "research_te_system_cost_index": float(research_te_ci),
                "surcharge_rate_total_fraction": float(surcharge_rate),
                "max_depth": int(max_depth_i),
            },
        )
    except Exception as e:
        return error(message=f"Error in POST Method `/industry_submanufacturing_plan/{character_id}`: " + str(e))


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
