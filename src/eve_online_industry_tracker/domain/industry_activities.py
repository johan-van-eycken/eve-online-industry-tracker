"""EVE industry activities: the ids the app acts on and the labels it shows.

The ids are game constants (the activity_id of an ESI industry job, the
activityID of an SDE assembly line). Which activities exist and what they
are called comes from the SDE's industryActivities
(infrastructure.sde.activities); this module only keeps the app's own labels
where they differ from the SDE's name, so the pages that key on them
(industry jobs: icons, colours, slot groups) keep working.
"""
from __future__ import annotations

from typing import Mapping

ACTIVITY_MANUFACTURING = 1
ACTIVITY_TE_RESEARCH = 3
ACTIVITY_ME_RESEARCH = 4
ACTIVITY_COPYING = 5
ACTIVITY_INVENTION = 8
ACTIVITY_REACTION = 9

LAB_ACTIVITY_IDS: frozenset[int] = frozenset({ACTIVITY_TE_RESEARCH, ACTIVITY_ME_RESEARCH, ACTIVITY_COPYING,
                                              ACTIVITY_INVENTION})
REACTION_ACTIVITY_IDS: frozenset[int] = frozenset({ACTIVITY_REACTION})

# The app's label where it differs from the SDE's name (build 3607705):
# 3 "Time Efficiency Research", 4 "Material Efficiency Research", 9 "Reactions".
DISPLAY_LABEL_OVERRIDES: Mapping[int, str] = {
    ACTIVITY_TE_RESEARCH: "TE Research",
    ACTIVITY_ME_RESEARCH: "ME Research",
    ACTIVITY_REACTION: "Reaction",
}


def activity_labels(sde_names: Mapping[int, str]) -> dict[int, str]:
    """{activity id: label} for every activity the SDE has: the app's label
    where DISPLAY_LABEL_OVERRIDES has one, else the SDE's name. An override
    for an id the SDE does not have is not a label."""
    return {int(a): DISPLAY_LABEL_OVERRIDES.get(int(a), name) for a, name in sde_names.items()}


def unknown_activity_label(activity_id: object) -> str:
    return f"Unknown ({activity_id})"
