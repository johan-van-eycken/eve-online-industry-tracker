"""The learned per-type weights' band, enforced on write AND on read.

accuracy_ema, velocity_multiplier and cost_multiplier each multiply a
product's adjusted_score directly, and they persist in plan_learning_weights.
FeedbackProcessor clamps every EMA write, so no new out-of-band value can be
stored. But a hand-edited or pre-clamp row would still be read raw, so every
reader goes through read_weight() as well.
"""
from __future__ import annotations

import contextvars
import logging
import math
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

logger = logging.getLogger(__name__)

# The admin settings schema defines no bounds for learning weights, hence
# named constants: a factor of 4 either way from neutral.
LEARNING_WEIGHT_MIN = 0.25
LEARNING_WEIGHT_MAX = 4.0
#: The starting value of every weight (the columns' DEFAULT 1.0): no adjustment.
NEUTRAL_WEIGHT = 1.0


#: The stored weights of a plan_learning_weights row.
WEIGHT_NAMES = ("accuracy_ema", "velocity_multiplier", "cost_multiplier")

#: The type_ids of the rows already warned about in this compute, or None
#: outside a compute (then every corrupt read warns).
_WARNED_THIS_COMPUTE: contextvars.ContextVar[set[Any] | None] = contextvars.ContextVar(
    "learning_weights_warned_this_compute", default=None,
)


@contextmanager
def weight_warnings_once_per_compute() -> Iterator[None]:
    """Within this block a row with corrupt stored weights warns once, naming
    every corrupt field, however many readers (scorer, pipeline analyzer,
    feedback processor) read it (FU-04)."""
    token = _WARNED_THIS_COMPUTE.set(set())
    try:
        yield
    finally:
        _WARNED_THIS_COMPUTE.reset(token)


def clamp_weight(value: float) -> float:
    """`value` limited to [LEARNING_WEIGHT_MIN, LEARNING_WEIGHT_MAX]."""
    return max(LEARNING_WEIGHT_MIN, min(LEARNING_WEIGHT_MAX, value))


def read_weight(weights: Any | None, name: str) -> float:
    """The stored weight `name` of a plan_learning_weights row, clamped to the band.

    `weights is None` means the type has no feedback yet. Its defined value
    is NEUTRAL_WEIGHT, not a fallback. A row whose value is not a finite real
    number (NaN, +/-inf, text kept by SQLite's REAL affinity, None, a bool) is corrupt.
    It reads as neutral with a WARNING, so the item is still scored instead
    of being dropped by float() raising inside a per-item handler. The
    WARNING names every corrupt weight of the row; inside
    weight_warnings_once_per_compute() it is logged once per row (type_id).
    """
    if weights is None:
        return NEUTRAL_WEIGHT
    raw = getattr(weights, name, None)
    if not _is_corrupt(raw):
        return clamp_weight(float(raw))
    warned = _WARNED_THIS_COMPUTE.get()
    type_id = getattr(weights, "type_id", None)
    if warned is not None:
        if type_id in warned:
            return NEUTRAL_WEIGHT
        warned.add(type_id)
    corrupt = {
        field: getattr(weights, field)
        for field in dict.fromkeys((*WEIGHT_NAMES, name))
        if hasattr(weights, field) and _is_corrupt(getattr(weights, field))
    }
    logger.warning(
        "Daily planner: learned weight(s) %s for type_id=%s not a finite number; "
        "reading them as neutral %.1f",
        ", ".join(f"{field}={value!r}" for field, value in corrupt.items()),
        type_id if type_id is not None else "?", NEUTRAL_WEIGHT,
    )
    return NEUTRAL_WEIGHT


def _is_corrupt(raw: Any) -> bool:
    return isinstance(raw, bool) or not isinstance(raw, (int, float)) or not math.isfinite(raw)
