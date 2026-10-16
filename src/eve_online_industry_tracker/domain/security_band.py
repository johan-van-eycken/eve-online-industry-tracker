"""The security band of a structure's system, and what it does to a rig.

EVE scales an engineering or reactor rig's bonus by the security of the
system the structure is in. Each rig carries the multipliers as SDE dogma
attributes: hiSecModifier (2355), lowSecModifier (2356) and nullSecModifier
(2357); the rig's security effect (e.g. 6842
structureEngineeringRigSecurityModification) multiplies its bonus
attributes by securityModifier (2358), which the server sets to the
multiplier of the system's band. Wormhole space uses the nullsec multiplier.
A rig with disallowInHighSec (1970), e.g. a reactor rig, cannot be fitted in
highsec at all. infrastructure/sde/rig_effects reads all of this per rig
(rig["security_multipliers"]: band -> multiplier, None where the rig
cannot be fitted).

Bands, from the SDE solar system: security status >= 0.45 highsec,
> 0.0 lowsec, <= 0.0 nullsec (the SDE's own high/low split: its lowest
highsec system is 0.450192, its highest lowsec 0.449944), and wormhole for a
system in a J-space region (region ids 11000000-11999999; every one of
them has a wormhole class in the SDE).
"""
from __future__ import annotations

from typing import Any

HIGH, LOW, NULL, WORMHOLE = "high", "low", "null", "wormhole"
BANDS: tuple[str, ...] = (HIGH, LOW, NULL, WORMHOLE)
EMPIRE_BANDS = frozenset({HIGH, LOW})

HIGHSEC_MIN_SECURITY = 0.45
_J_SPACE_REGION_IDS = range(11_000_000, 12_000_000)



def _float(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int | None:
    try:
        return None if value is None else int(value)
    except (TypeError, ValueError):
        return None


def security_band(security_status: Any, region_id: Any = None) -> str | None:
    """The band of a system from its SDE security status and region; None
    when neither says."""
    region = _int(region_id)
    if region is not None and region in _J_SPACE_REGION_IDS:
        return WORMHOLE
    status = _float(security_status)
    if status is None:
        return None
    if status >= HIGHSEC_MIN_SECURITY:
        return HIGH
    if status > 0.0:
        return LOW
    return NULL


def profile_security_band(profile: Any) -> tuple[str | None, str | None]:
    """(band, None) for an industry profile, or (None, why) when its system's
    band is unknown.

    Reads system_security_band when the profile carries one, else
    system_security_status / system_region_id (set by
    IndustryService._resolve_industry_profile_context from the SDE)."""
    if not isinstance(profile, dict):
        return None, "no industry profile"
    explicit = str(profile.get("system_security_band") or "").strip().lower()
    if explicit in BANDS:
        return explicit, None
    system = profile.get("industry_system") if isinstance(profile.get("industry_system"), dict) else {}

    def read(key: str, system_key: str) -> Any:
        value = profile.get(key)
        return value if value is not None else system.get(system_key)

    status = read("system_security_status", "security_status")
    region_id = read("system_region_id", "region_id")
    band = security_band(status, region_id)
    if band is not None:
        return band, None
    system_id = profile.get("system_id") or system.get("solar_system_id")
    if not system_id:
        return None, "the industry profile has no solar system"
    return None, f"the SDE gives no security status for solar system {system_id}"


def rig_security_multiplier(rig: Any, band: str | None) -> tuple[float, str | None]:
    """(multiplier, reason) for one rig in a band.

    - Known band: the rig's multiplier there, or 0.0 with a reason when the
      rig cannot be fitted in that band (no bonus: an over-buy, never an
      under-buy).
    - Unknown band (None): the smallest multiplier the rig can have in any
      band it fits, with a reason, so the bonus is never over-stated.
    - A rig with no security_multipliers: 0.0 with a reason. Only a
      security-scaled effect asks, so the multiplier is missing data, and no
      bonus over-buys rather than under-buys."""
    multipliers = rig.get("security_multipliers") if isinstance(rig, dict) else None
    type_id = rig.get("type_id") if isinstance(rig, dict) else None
    if not isinstance(multipliers, dict) or not multipliers:
        return 0.0, f"rig {type_id} has a security-scaled bonus but no security multiplier, so its bonus is not applied"
    if band is not None:
        value = _float(multipliers.get(band))
        if value is None:
            return 0.0, f"rig {type_id} cannot be fitted in {band} space (SDE), so its bonus is not applied"
        return value, None
    fitted = [v for v in (_float(x) for x in multipliers.values()) if v is not None]
    if not fitted:
        return 0.0, f"rig {type_id} has no security multiplier in the SDE, so its bonus is not applied"
    return min(fitted), f"rig {type_id}: the system's security band is unknown, so the smallest multiplier {min(fitted):g} applies"


def scaled_effect_value(rig: Any, effect: Any, band: str | None) -> float:
    """An effect's value with its rig's multiplier for the band (the smallest
    one when the band is unknown): what the producer applies, for display."""
    try:
        value = float((effect or {}).get("value") or 0.0)
    except (TypeError, ValueError, AttributeError):
        return 0.0
    if not isinstance(effect, dict) or not effect.get("security_scaled"):
        return value
    multiplier, _ = rig_security_multiplier(rig, band)
    return value * multiplier
