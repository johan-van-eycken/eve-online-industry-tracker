from __future__ import annotations

from typing import List

from classes.database_models import CorporationStructuresModel

from flask_app.domain.corporation_structure import CorporationStructure


def list_by_corporation_id(session, corporation_id: int) -> List[CorporationStructure]:
    rows = (
        session.query(CorporationStructuresModel)
        .filter(CorporationStructuresModel.corporation_id == corporation_id)
        .all()
    )
    return [CorporationStructure.from_model(r) for r in rows]
