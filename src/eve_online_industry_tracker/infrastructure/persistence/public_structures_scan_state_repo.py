from __future__ import annotations

from datetime import datetime

from sqlalchemy import select

from eve_online_industry_tracker.db_models import PublicStructuresScanStateModel


_STATE_ID = 1


def get_state(session) -> PublicStructuresScanStateModel:
    row = session.execute(select(PublicStructuresScanStateModel).where(PublicStructuresScanStateModel.id == _STATE_ID)).scalar_one_or_none()
    if row is None:
        row = PublicStructuresScanStateModel(id=_STATE_ID, cursor=0, last_completed_at=None)
        session.add(row)
        session.commit()
    return row


def get_cursor(session) -> int:
    return int(get_state(session).cursor or 0)


def set_cursor(session, cursor: int) -> None:
    st = get_state(session)
    st.cursor = int(max(0, cursor))
    session.commit()


def mark_completed(session) -> None:
    st = get_state(session)
    st.last_completed_at = datetime.utcnow()
    st.cursor = 0
    session.commit()


def get_last_completed_at(session) -> datetime | None:
    return get_state(session).last_completed_at
