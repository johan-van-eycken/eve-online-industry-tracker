from __future__ import annotations

from typing import Iterable

from eve_online_industry_tracker.db_models import (
    CharacterAssetsModel,
    CharacterModel,
    CorporationAssetsModel,
    CorporationModel,
)


def get_character_blueprints(session) -> list[CharacterAssetsModel]:
    return session.query(CharacterAssetsModel).filter(CharacterAssetsModel.type_category_name == "Blueprint").all()


def get_character_blueprint_type_ids(session, character_id: int) -> list[int]:
    rows = (
        session.query(CharacterAssetsModel.type_id)
        .filter(
            CharacterAssetsModel.character_id == int(character_id),
            CharacterAssetsModel.type_category_name == "Blueprint",
        )
        .distinct()
        .all()
    )
    return sorted({int(row[0]) for row in rows if row and row[0] is not None})


def get_character_blueprint_assets(session, character_id: int) -> list[CharacterAssetsModel]:
    return (
        session.query(CharacterAssetsModel)
        .filter(
            CharacterAssetsModel.character_id == int(character_id),
            CharacterAssetsModel.type_category_name == "Blueprint",
        )
        .all()
    )

def get_character_blueprint_assets_for_ids(session, character_ids: Iterable[int]) -> list[CharacterAssetsModel]:
    ids = list({int(i) for i in character_ids if i is not None})
    if not ids:
        return []
    return (
        session.query(CharacterAssetsModel)
        .filter(
            CharacterAssetsModel.character_id.in_(ids),
            CharacterAssetsModel.type_category_name == "Blueprint",
        )
        .all()
    )


def get_corporation_blueprints(session) -> list[CorporationAssetsModel]:
    return session.query(CorporationAssetsModel).filter(CorporationAssetsModel.type_category_name == "Blueprint").all()

def get_corporation_blueprint_assets_for_ids(session, corporation_ids: Iterable[int]) -> list[CorporationAssetsModel]:
    ids = list({int(i) for i in corporation_ids if i is not None})
    if not ids:
        return []
    return (
        session.query(CorporationAssetsModel)
        .filter(
            CorporationAssetsModel.corporation_id.in_(ids),
            CorporationAssetsModel.type_category_name == "Blueprint",
        )
        .all()
    )


def get_character_name_map(session, character_ids: Iterable[int]) -> dict[int, str]:
    ids = list({int(i) for i in character_ids if i is not None})
    if not ids:
        return {}
    rows = session.query(CharacterModel).filter(CharacterModel.character_id.in_(ids)).all()
    return {int(r.character_id): r.character_name for r in rows}


def get_corporation_name_map(session, corporation_ids: Iterable[int]) -> dict[int, str]:
    ids = list({int(i) for i in corporation_ids if i is not None})
    if not ids:
        return {}
    rows = session.query(CorporationModel).filter(CorporationModel.corporation_id.in_(ids)).all()
    return {int(r.corporation_id): r.corporation_name for r in rows}
