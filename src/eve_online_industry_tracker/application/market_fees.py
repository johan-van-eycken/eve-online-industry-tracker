"""The NPC-station market fees, from the admin settings industry/*.

    sales tax  = base_sales_tax_rate x (1 - accounting_skill_reduction x Accounting)
    broker fee = clamp(broker_fee_base
                       - broker_relations_reduction_per_level x Broker Relations
                       - faction_standing_reduction_per_point x the owner faction's standing
                       - corp_standing_reduction_per_point x the owner corporation's standing,
                       broker_fee_min, broker_fee_base)

The one copy of the formula: the producer (IndustryService's market fee
context), the character's stored market fees (Character.refresh_market_fees)
and the price suggestion use it.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA

_KEYS = ("base_sales_tax_rate", "accounting_skill_reduction", "broker_fee_base", "broker_fee_min",
         "broker_relations_reduction_per_level", "faction_standing_reduction_per_point",
         "corp_standing_reduction_per_point")


@dataclass(frozen=True)
class MarketFeeSettings:
    base_sales_tax_rate: float
    accounting_skill_reduction: float
    broker_fee_base: float
    broker_fee_min: float
    broker_relations_reduction_per_level: float
    faction_standing_reduction_per_point: float
    corp_standing_reduction_per_point: float

    @classmethod
    def from_admin(cls, admin_settings: Any) -> "MarketFeeSettings":
        """The industry/* settings, read now (a change applies to the next
        computation); the schema defaults without a settings store. A stored
        invalid value raises AdminSettingInvalid naming it."""
        specs = ADMIN_SETTINGS_SCHEMA["industry"]["settings"]
        values = {key: float(specs[key]["default"] if admin_settings is None else admin_settings.get("industry", key))
                  for key in _KEYS}
        return cls(**values)


def sales_tax_fraction(settings: MarketFeeSettings, *, accounting_level: int) -> float:
    rate = settings.base_sales_tax_rate * (1.0 - settings.accounting_skill_reduction * float(accounting_level))
    return max(0.0, min(1.0, rate))


def broker_fee_fraction(settings: MarketFeeSettings, *, broker_relations_level: int, faction_standing: float,
                        corp_standing: float) -> float:
    fee = (settings.broker_fee_base
           - settings.broker_relations_reduction_per_level * float(broker_relations_level)
           - settings.faction_standing_reduction_per_point * float(faction_standing)
           - settings.corp_standing_reduction_per_point * float(corp_standing))
    return max(settings.broker_fee_min, min(settings.broker_fee_base, fee))


def _assumed(settings: MarketFeeSettings, why: str) -> tuple[float, float, str]:
    tax = sales_tax_fraction(settings, accounting_level=0)
    broker = broker_fee_fraction(settings, broker_relations_level=0, faction_standing=0.0, corp_standing=0.0)
    return tax, broker, (f"fees assumed at {tax:.2%} sales tax and {broker:.2%} broker fee (no Accounting, Broker "
                         f"Relations or standings): {why}")


def fees_from_stored_market_fees(raw: Any, settings: MarketFeeSettings) -> tuple[float, float, str | None]:
    """(sales tax, broker fee, None) from a character's stored market fees
    (Character.refresh_market_fees: its skills and standings at Jita 4-4)
    with the current settings. Without them: the fees of an untrained
    character without standings (the highest, so a price is not set too low)
    and the reason they are assumed."""
    payload = raw
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            return _assumed(settings, "the character's stored market fees are unreadable")
    if not payload:
        return _assumed(settings, "no stored market fees for the character (refresh it)")
    jita = payload.get("jita_4_4") if isinstance(payload, dict) else None
    if not isinstance(jita, dict):
        return _assumed(settings, "the character's stored market fees are unreadable")
    if jita.get("unknown_reason"):
        return _assumed(settings, str(jita["unknown_reason"]))
    skills, standings = jita.get("skills") or {}, jita.get("standings") or {}
    try:
        accounting = int(skills["accounting_level"])
        broker_relations = int(skills["broker_relations_level"])
        faction = float(standings.get("faction_standing") or 0.0)
        corp = float(standings.get("corp_standing") or 0.0)
    except (KeyError, TypeError, ValueError):
        return _assumed(settings, "the character's stored market fees lack its skills")
    return (sales_tax_fraction(settings, accounting_level=accounting),
            broker_fee_fraction(settings, broker_relations_level=broker_relations, faction_standing=faction,
                                corp_standing=corp),
            None)
