from __future__ import annotations

from flask import Blueprint, request

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


industry_bp = Blueprint("industry", __name__)


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
        all_blueprints = blueprints_service.get_blueprint_assets(session, state.esi_service)
        if character_id:
            all_blueprints = [bp for bp in all_blueprints if bp.get("owner_id") == character_id]

        return ok(data=enrich_blueprints_for_character(all_blueprints, character))
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
        public_structures = state.esi_service.get_public_structures(system_id=system_id, filter="manufacturing_basic")
        type_ids = list({s["type_id"] for s in public_structures if "type_id" in s})
        ensure_sde_ready()
        session = get_db_sde_session()
        language = get_language()
        type_map = get_type_data(session, language, type_ids)
        enriched_structures = [{**s, **type_map.get(s["type_id"], {})} for s in public_structures]
        return ok(data=enriched_structures)
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
        return ok(data=[s.to_dict() for s in structures])
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
