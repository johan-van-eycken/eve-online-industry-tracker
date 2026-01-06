from __future__ import annotations

from typing import Dict, Iterable, List

from classes.database_models import (
    CharacterAssetsModel,
    CharacterModel,
    CorporationAssetsModel,
    CorporationModel,
)


def get_character_blueprints(session) -> List[CharacterAssetsModel]:
    return (
        session.query(CharacterAssetsModel)
        .filter(CharacterAssetsModel.type_category_name == "Blueprint")
        .all()
    )


def get_corporation_blueprints(session) -> List[CorporationAssetsModel]:
    return (
        session.query(CorporationAssetsModel)
        .filter(CorporationAssetsModel.type_category_name == "Blueprint")
        .all()
    )


def get_character_name_map(session, character_ids: Iterable[int]) -> Dict[int, str]:
    ids = list({int(i) for i in character_ids if i is not None})
    if not ids:
        return {}
    rows = session.query(CharacterModel).filter(CharacterModel.character_id.in_(ids)).all()
    return {int(r.character_id): r.character_name for r in rows}


def get_corporation_name_map(session, corporation_ids: Iterable[int]) -> Dict[int, str]:
    ids = list({int(i) for i in corporation_ids if i is not None})
    if not ids:
        return {}
    rows = session.query(CorporationModel).filter(CorporationModel.corporation_id.in_(ids)).all()
    return {int(r.corporation_id): r.corporation_name for r in rows}
