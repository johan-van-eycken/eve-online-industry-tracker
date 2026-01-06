from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from classes.database_models import IndustryProfilesModel

from flask_app.domain.industry_profile import IndustryProfile


def list_by_character_id(session, character_id: int) -> List[IndustryProfile]:
    rows = (
        session.query(IndustryProfilesModel)
        .filter(IndustryProfilesModel.character_id == character_id)
        .all()
    )
    return [IndustryProfile.from_model(r) for r in rows]


def create(session, data: Dict[str, Any]) -> int:
    profile = IndustryProfilesModel(
        character_id=data["character_id"],
        profile_name=data["profile_name"],
        is_default=data.get("is_default", False),
        region_id=data.get("region_id"),
        system_id=data.get("system_id"),
        facility_id=data.get("facility_id"),
        facility_type=data.get("facility_type"),
        facility_tax=data.get("facility_tax"),
        material_efficiency_bonus=data.get("material_efficiency_bonus"),
        time_efficiency_bonus=data.get("time_efficiency_bonus"),
        rig_slot0_type_id=data.get("rig_slot0_type_id"),
        rig_slot1_type_id=data.get("rig_slot1_type_id"),
        rig_slot2_type_id=data.get("rig_slot2_type_id"),
    )
    session.add(profile)
    session.commit()
    return int(profile.id)


def update(session, profile_id: int, data: Dict[str, Any]) -> None:
    profile = (
        session.query(IndustryProfilesModel)
        .filter(IndustryProfilesModel.id == profile_id)
        .first()
    )
    if not profile:
        raise ValueError(f"Industry profile with id {profile_id} not found.")

    if data.get("is_default", False):
        session.query(IndustryProfilesModel).filter(
            IndustryProfilesModel.character_id == profile.character_id,
            IndustryProfilesModel.id != profile_id,
        ).update({"is_default": False})

    for field in [
        "profile_name",
        "is_default",
        "region_id",
        "system_id",
        "facility_id",
        "facility_type",
        "facility_tax",
        "material_efficiency_bonus",
        "time_efficiency_bonus",
        "rig_slot0_type_id",
        "rig_slot1_type_id",
        "rig_slot2_type_id",
    ]:
        if field in data:
            setattr(profile, field, data[field])

    profile.updated_at = datetime.now()
    session.commit()


def delete(session, profile_id: int) -> None:
    profile = (
        session.query(IndustryProfilesModel)
        .filter(IndustryProfilesModel.id == profile_id)
        .first()
    )
    if not profile:
        raise ValueError(f"Industry profile with id {profile_id} not found.")

    session.delete(profile)
    session.commit()
