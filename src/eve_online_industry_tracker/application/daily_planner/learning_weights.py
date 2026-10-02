"""The learned per-type weights' band, enforced on write AND on read.

accuracy_ema, velocity_multiplier and cost_multiplier each multiply a
product's adjusted_score directly, and they persist in plan_learning_weights.
FeedbackProcessor clamps every EMA write, so no new out-of-band value can be
stored. But a hand-edited or pre-clamp row would still be read raw, so every
reader goes through read_weight() as well.
"""
from __future__ import annotations

import logging
import math
from typing import Any

logger = logging.getLogger(__name__)

# The admin settings schema defines no bounds for learning weights, hence
# named constants: a factor of 4 either way from neutral.
LEARNING_WEIGHT_MIN = 0.25
LEARNING_WEIGHT_MAX = 4.0
#: The starting value of every weight (the columns' DEFAULT 1.0): no adjustment.
NEUTRAL_WEIGHT = 1.0


def clamp_weight(value: float) -> float:
    """`value` limited to [LEARNING_WEIGHT_MIN, LEARNING_WEIGHT_MAX]."""
    return max(LEARNING_WEIGHT_MIN, min(LEARNING_WEIGHT_MAX, value))


def read_weight(weights: Any | None, name: str) -> float:
    """The stored weight `name` of a plan_learning_weights row, clamped to the band.

    `weights is None` means the type has no feedback yet. Its defined value
    is NEUTRAL_WEIGHT, not a fallback. A row whose value is not a finite real
    number (NaN, text kept by SQLite's REAL affinity, None, a bool) is corrupt.
    It reads as neutral with a WARNING, so the item is still scored instead
    of being dropped by float() raising inside a per-item handler.
    """
    if weights is None:
        return NEUTRAL_WEIGHT
    raw = getattr(weights, name, None)
    if isinstance(raw, bool) or not isinstance(raw, (int, float)) or math.isnan(raw):
        logger.warning(
            "Daily planner: learned weight %s=%r for type_id=%s is not a number; "
            "reading it as neutral %.1f",
            name, raw, getattr(weights, "type_id", "?"), NEUTRAL_WEIGHT,
        )
        return NEUTRAL_WEIGHT
    return clamp_weight(float(raw))
