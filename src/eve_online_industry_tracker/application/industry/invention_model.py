"""EVE invention rules shared by the overview producer and the daily planner.

Game rules, not settings. An invented BPC without a decryptor is ME 2 /
TE 4, with the SDE's runs per invented BPC. A decryptor adds its ME, TE and
max-run modifiers and multiplies the success chance. Decryptor values come
from the SDE: dogma attributes 1112 (inventionPropabilityMultiplier), 1113,
1114 and 1124 on group 1304 "Generic Decryptor", read by
infrastructure/sde/decryptors.py:get_t2_invention_decryptors.
"""
from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Iterator, Mapping

logger = logging.getLogger(__name__)

INVENTED_BASE_ME = 2
INVENTED_BASE_TE = 4
#: EVE's research ceilings; a blueprint's ME/TE never leaves 0..10 / 0..20.
MAX_BLUEPRINT_ME = 10
MAX_BLUEPRINT_TE = 20


@dataclass(frozen=True)
class DecryptorOption:
    """One way to invent: no decryptor (type_id None) or one SDE decryptor."""

    type_id: int | None
    type_name: str
    probability_multiplier: float = 1.0
    me_modifier: int = 0
    te_modifier: int = 0
    max_run_modifier: int = 0


NO_DECRYPTOR = DecryptorOption(type_id=None, type_name="No decryptor")


def invented_blueprint_efficiency(decryptor: DecryptorOption) -> tuple[int, int]:
    """(ME, TE) of a BPC invented with `decryptor`: 2 / 4 plus its modifiers."""
    me = min(MAX_BLUEPRINT_ME, max(0, INVENTED_BASE_ME + int(decryptor.me_modifier)))
    te = min(MAX_BLUEPRINT_TE, max(0, INVENTED_BASE_TE + int(decryptor.te_modifier)))
    return me, te


def invented_runs_per_blueprint(base_runs: int, decryptor: DecryptorOption) -> int:
    """Runs on one invented BPC: the SDE's runs plus the decryptor's, at least 1."""
    return max(1, int(base_runs) + int(decryptor.max_run_modifier))


def decryptor_options_from_rows(rows: Iterable[Mapping[str, Any]]) -> tuple[DecryptorOption, ...]:
    """NO_DECRYPTOR first, then every usable SDE decryptor by type_id.

    `rows` have the get_t2_invention_decryptors shape. A row without the four
    modifiers, or with a non-positive id or success multiplier, is skipped
    with a WARNING: it cannot be costed, and a guessed modifier would change
    the ME, the runs or the odds.
    """
    options: list[DecryptorOption] = []
    for row in rows:
        try:
            option = DecryptorOption(
                type_id=int(row["type_id"]),
                type_name=str(row.get("type_name") or row["type_id"]),
                probability_multiplier=float(row["invention_probability_multiplier"]),
                me_modifier=int(row["invention_me_modifier"]),
                te_modifier=int(row["invention_te_modifier"]),
                max_run_modifier=int(row["invention_max_run_modifier"]),
            )
        except (KeyError, TypeError, ValueError):
            logger.warning("Invention: skipping malformed SDE decryptor row %r", dict(row))
            continue
        if int(option.type_id or 0) <= 0 or option.probability_multiplier <= 0:
            logger.warning(
                "Invention: skipping decryptor %s with a non-positive id or success multiplier",
                option.type_name,
            )
            continue
        options.append(option)
    return (NO_DECRYPTOR, *sorted(options, key=lambda o: int(o.type_id or 0)))


_quiet_state = threading.local()


@contextmanager
def quiet_warnings() -> Iterator[None]:
    """Drop WARNING records on this thread from loggers carrying
    QuietWarningsFilter. This is used while the producer evaluates plans it
    throws away (decryptor candidates, the extra-batch plan), so a missing
    input is logged once, by the plan that is kept. ERROR and above pass."""
    _quiet_state.depth = getattr(_quiet_state, "depth", 0) + 1
    try:
        yield
    finally:
        _quiet_state.depth -= 1


def warnings_are_quiet() -> bool:
    return getattr(_quiet_state, "depth", 0) > 0


class QuietWarningsFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return not (record.levelno == logging.WARNING and warnings_are_quiet())


logger.addFilter(QuietWarningsFilter())


def _no_decryptor_for(_blueprint_type_id: int) -> DecryptorOption:
    return NO_DECRYPTOR


@dataclass
class InventionContext:
    """Per-refresh invention inputs. One object is shared by the top-level row
    and every nested sub-build of one overview refresh."""

    decryptor_for_blueprint: Callable[[int], DecryptorOption] = _no_decryptor_for
