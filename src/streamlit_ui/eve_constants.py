from __future__ import annotations

# EVE Online industry activity IDs (same as IndustryService._ACTIVITY_ID_TO_NAME)
ACTIVITY_MANUFACTURING = 1
ACTIVITY_LAB_IDS: frozenset[int] = frozenset({3, 4, 5, 8})  # TE Research, ME Research, Copying, Invention
ACTIVITY_REACTION_IDS: frozenset[int] = frozenset({9})

ACTIVITY_ID_TO_NAME: dict[int, str] = {
    1: "Manufacturing",
    3: "TE Research",
    4: "ME Research",
    5: "Copying",
    8: "Invention",
    9: "Reaction",
}
