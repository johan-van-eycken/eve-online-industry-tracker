# src/eve_online_industry_tracker/application/daily_planner/fixture_export.py
"""Sanitise real product-overview rows into a committable test fixture.

This repository is public. A raw overview dump exposes the corporation's
holdings, production mix and margins, so nothing raw may be written to disk.
What the tests need is the row *shape* — which keys exist and how they nest —
not the values, so keys and structure are preserved while magnitudes are
replaced with deterministic synthetic numbers and identity information is
removed.

Fix round 1 (2026-09-18): a substring key deny-list was defeated twice by
free-text string fields that no key-name pattern predicted — `profile_name`
(owner-authored, nested under `manufacturing_job["industry_profile"]`) and
`ship_name` (EVE's default ship name is literally `"<Character Name>'s
<Type>"`, nested under `blueprint_copy`/`blueprint_original`). The row shape
grows with every enrichment pass in `IndustryService`, so a deny-list can
never be trusted to enumerate every future identity-bearing string key.

The policy is inverted as a result: strings are guilty until proven
innocent. Every string value is replaced with a redaction placeholder unless
its *key* is on a small, justified allow-list of public EVE SDE data
(`type_name`, `meta_group_name` — read verbatim by the contract layer in
`application/industry/overview_row.py` and by `PlannerInputRow`). Numbers are
still scrambled in place (not redacted) because the numeric *shape* — sign,
rough magnitude, type — is what tests assert on. The key-name deny-list is
kept as a second, independent layer: a denied key is dropped entirely
regardless of its value's type, which is still how numeric identity fields
(`structure_id`, `corporation_id`, ...) are removed rather than merely
scrambled into another number that happens to look plausible.

Known limitation: magnitudes are scrambled independently per field, so
cross-field arithmetic invariants in a row do not survive sanitisation
(profit_amount is not revenue minus cost; material_cost is not the sum of
the scrambled `materials` entries). This is intentional, not an oversight —
preserving those ratios would preserve profit margins, which is exactly the
information this sanitiser exists to hide. Do not write fixture tests that
recompute or check row-internal arithmetic; they will not hold.
"""
from __future__ import annotations

import random
from typing import Any

# Any key containing one of these substrings is removed, at any nesting
# depth, regardless of its value's type. This is a second, independent layer
# on top of the string allow-list below — it is how numeric identity fields
# (which are scrambled, not redacted, by the type-based path) get removed
# instead of merely turned into a different but still plausible-looking
# number.
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
    # Additions beyond the original brief, found while scanning the real
    # overview-row builder (`IndustryService._build_single_product_row` and
    # friends) for numeric identity fields the generic scramble would not
    # reliably destroy:
    "system",  # solar_system_id — which system the corp builds/holds assets
               # in is opsec-sensitive, and a scrambled int could still land
               # in the range of a real (if wrong) system id.
    "container",  # container_name — a player-chosen label on an asset
                  # container. Now also caught by the string allow-list
                  # inversion below (its value is free text), but kept here
                  # too as defence in depth.
)

# Keys whose *string* values are public EVE SDE data and may survive
# verbatim. Every other string value is replaced with `_REDACTED`. This is
# deliberately small: `type_name` is required by `PlannerInputRow`, and
# `meta_group_name` is read verbatim by `get_meta_group_name` in
# `application/industry/overview_row.py`. Numeric public ids (`type_id`,
# `blueprint_type_id`) do not need to be on this list — numbers are never
# redacted, only scrambled, and scrambling a type id would break the
# fixture's usefulness for testing the type-id-keyed code paths, so they are
# exempted from scrambling separately in `_sanitise_value`.
_PUBLIC_STRING_KEYS: frozenset[str] = frozenset({"type_name", "meta_group_name"})

# Numeric keys that are public SDE identifiers and must survive unscrambled.
_PUBLIC_NUMERIC_KEYS: frozenset[str] = frozenset({"type_id", "blueprint_type_id"})

_REDACTED = "<redacted>"


def _is_identity_key(key: str) -> bool:
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
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if key in _PUBLIC_NUMERIC_KEYS:
            return value
        return _scramble_number(value, rng)
    if isinstance(value, str):
        if key in _PUBLIC_STRING_KEYS:
            return value
        return _REDACTED
    # Unknown/exotic value type: redact rather than risk leaking it verbatim.
    return _REDACTED


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
