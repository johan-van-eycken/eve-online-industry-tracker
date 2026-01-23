from __future__ import annotations

from eve_online_industry_tracker.db_models import CorporationStructuresModel

from eve_online_industry_tracker.domain.corporation_structure import CorporationStructure


def list_by_corporation_id(session, corporation_id: int) -> list[CorporationStructure]:
    rows = (
        session.query(CorporationStructuresModel)
        .filter(CorporationStructuresModel.corporation_id == corporation_id)
        .all()
    )
    return [CorporationStructure.from_model(r) for r in rows]
