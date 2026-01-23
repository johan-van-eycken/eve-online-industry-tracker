from __future__ import annotations

from typing import Iterable

from eve_online_industry_tracker.db_models import Categories, Groups, TypeMaterials, Types


def get_ore_groups(session) -> list[Groups]:
    # categoryID == 25 is "Asteroid" category in SDE
    return session.query(Groups).filter(Groups.published == 1, Groups.categoryID == 25).all()


def get_published_types_by_group_ids(session, group_ids: Iterable[int]) -> list[Types]:
    ids = list({int(i) for i in group_ids if i is not None})
    if not ids:
        return []
    return session.query(Types).filter(Types.published == 1, Types.groupID.in_(ids)).all()


def get_groups_by_ids(session, group_ids: Iterable[int]) -> list[Groups]:
    ids = list({int(i) for i in group_ids if i is not None})
    if not ids:
        return []
    return session.query(Groups).filter(Groups.id.in_(ids)).all()


def get_categories_by_ids(session, category_ids: Iterable[int]) -> list[Categories]:
    ids = list({int(i) for i in category_ids if i is not None})
    if not ids:
        return []
    return session.query(Categories).filter(Categories.id.in_(ids)).all()


def get_type_materials_by_type_ids(session, type_ids: Iterable[int]) -> list[TypeMaterials]:
    ids = list({int(i) for i in type_ids if i is not None})
    if not ids:
        return []
    return session.query(TypeMaterials).filter(TypeMaterials.id.in_(ids)).all()


def get_types_by_ids(session, type_ids: Iterable[int]) -> list[Types]:
    ids = list({int(i) for i in type_ids if i is not None})
    if not ids:
        return []
    return session.query(Types).filter(Types.id.in_(ids)).all()


def get_base_material_types(session) -> list[Types]:
    # Base materials: groupID == 18, metaGroupID is NULL
    return (
        session.query(Types)
        .filter(Types.published == 1, Types.groupID == 18, Types.metaGroupID == None)
        .all()
    )
