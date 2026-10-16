from __future__ import annotations

from typing import Any

from sqlalchemy import text

from eve_online_industry_tracker.infrastructure.sde.localization import parse_localized

#: SDE groups.id "Science", the group of every invention skill (the same value
#: as invention_model.SCIENCE_SKILL_GROUP_ID; infrastructure does not import
#: the application layer).
_SCIENCE_SKILL_GROUP_ID = 270


def get_encryption_skill_type_ids(sde_session: Any) -> frozenset[int]:
    """Type ids of the invention encryption skills: published skills of the
    SDE "Science" group whose English name ends in "Encryption Methods" (7 in
    the 2026 SDE). The English name is read on purpose, whatever the display
    language, so the rule does not depend on a translation."""
    rows = sde_session.execute(
        text("SELECT id, name FROM types WHERE groupID = :gid AND published = 1"),
        {"gid": _SCIENCE_SKILL_GROUP_ID},
    ).fetchall()
    return frozenset(
        int(type_id) for type_id, raw_name in rows
        if type_id is not None and parse_localized(raw_name, "en").endswith("Encryption Methods")
    )
