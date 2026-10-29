from __future__ import annotations

from typing import Iterable

from sqlalchemy import select

from eve_online_industry_tracker.db_models import PublicStructuresModel


def list_by_system_id(session, system_id: int) -> list[PublicStructuresModel]:
    stmt = select(PublicStructuresModel).where(PublicStructuresModel.system_id == system_id)
    stmt = stmt.order_by(PublicStructuresModel.structure_name.asc())
    return list(session.execute(stmt).scalars().all())


def upsert_many(session, structures: Iterable[dict]) -> int:
    """Upsert a list of public structure dicts into db_app.

    Expected keys per item:
      - structure_id (int)
      - system_id (int)
      - owner_id (int|None)
      - type_id (int|None)
      - structure_name (str|None)
      - services (list|None)
    """

    count = 0
    for item in structures:
        structure_id = item.get("structure_id")
        system_id = item.get("system_id")
        if not structure_id or not system_id:
            continue

        model = PublicStructuresModel(
            structure_id=int(structure_id),
            system_id=int(system_id),
            owner_id=(int(item["owner_id"]) if item.get("owner_id") is not None else None),
            type_id=(int(item["type_id"]) if item.get("type_id") is not None else None),
            structure_name=item.get("structure_name"),
            services=item.get("services"),
        )
        session.merge(model)
        count += 1

    session.commit()
    return count

