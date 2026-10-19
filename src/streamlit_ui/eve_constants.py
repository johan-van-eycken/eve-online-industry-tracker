from __future__ import annotations

# EVE Online industry activity ids (game constants). Which activities exist
# and their labels come from the SDE's industryActivities through the API
# payloads' activity_name (infrastructure.sde.activities).
from eve_online_industry_tracker.domain.industry_activities import (  # noqa: F401
    ACTIVITY_MANUFACTURING,
    LAB_ACTIVITY_IDS as ACTIVITY_LAB_IDS,  # TE Research, ME Research, Copying, Invention
    REACTION_ACTIVITY_IDS as ACTIVITY_REACTION_IDS,
)
