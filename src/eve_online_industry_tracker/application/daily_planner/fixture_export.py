# src/eve_online_industry_tracker/application/daily_planner/fixture_export.py
"""Sanitise real product-overview rows into a committable test fixture.

This repository is public. A raw overview dump exposes the corporation's
holdings, production mix and margins, so nothing raw may be written to disk.
What the tests need is the row *shape* — which keys exist and how they nest —
not the values, so keys and structure are preserved while magnitudes are
replaced with deterministic synthetic numbers and identity fields are dropped.
"""
from __future__ import annotations

import random
from typing import Any

# Any key containing one of these substrings is removed, at any nesting depth.
IDENTITY_KEY_SUBSTRINGS: tuple[str, ...] = (
    "character",
    "corporation",
    "corp_id",
    "owner",
    "wallet",
    "balance",
    "station",
    "structure",
    "facility",
    "location",
    "order_id",
    "job_id",
    "installer",
    "token",
    "secret",
    # Additions beyond the brief, found while scanning the real overview-row
    # builder (`IndustryService._build_single_product_row` and friends) for
    # string-valued fields that would otherwise pass through unscrambled:
    "system",  # catches solar_system_id — which system the corp builds/holds
               # assets in is opsec-sensitive, and as a bare int it could
               # survive the generic magnitude scramble still looking like a
               # plausible real system id, so it is dropped outright instead.
    "container",  # catches container_name — a player-chosen label on an
                  # asset container, i.e. free-text the owner wrote, so it is
                  # treated the same as any other identifying string field.
)

# Keys whose values are public static data and must survive verbatim.
_PUBLIC_KEYS: frozenset[str] = frozenset({"type_id", "type_name", "blueprint_type_id"})


def _is_identity_key(key: str) -> bool:
    if key in _PUBLIC_KEYS:
        return False
    lowered = key.lower()
    return any(part in lowered for part in IDENTITY_KEY_SUBSTRINGS)


def _scramble_number(value: Any, rng: random.Random) -> Any:
    """Replace a magnitude with a plausible one, preserving type and sign."""
    factor = rng.uniform(0.5, 1.5)
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        scrambled = int(value * factor)
        if value != 0 and scrambled == 0:
            scrambled = 1 if value > 0 else -1
        return scrambled
    return round(float(value) * factor, 2)


def _sanitise_value(key: str, value: Any, rng: random.Random) -> Any:
    if isinstance(value, dict):
        return _sanitise_mapping(value, rng)
    if isinstance(value, list):
        return [_sanitise_value(key, item, rng) for item in value]
    if key in _PUBLIC_KEYS or isinstance(value, bool) or value is None:
        return value
    if isinstance(value, (int, float)):
        return _scramble_number(value, rng)
    return value


def _sanitise_mapping(row: dict[str, Any], rng: random.Random) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in row.items():
        if _is_identity_key(str(key)):
            continue
        out[str(key)] = _sanitise_value(str(key), value, rng)
    return out


def sanitise_overview_rows(
    rows: list[dict[str, Any]], *, seed: int = 20260917
) -> list[dict[str, Any]]:
    """Return sanitised copies of `rows`, safe to commit to a public repository."""
    rng = random.Random(seed)
    return [_sanitise_mapping(row, rng) for row in rows if isinstance(row, dict)]
