"""Admin settings schema and persistence manager.

Settings are stored in a JSON file and can be edited at runtime via the
Streamlit admin page.  Consuming services read values through the manager
instance on ``AppState``.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from threading import Lock
from typing import Any


# ---------------------------------------------------------------------------
# Schema: defines every admin-configurable setting with type, default, label,
# and optional min/max constraints.  The Streamlit admin page auto-generates
# form controls from this schema.
# ---------------------------------------------------------------------------

ADMIN_SETTINGS_SCHEMA: dict[str, dict[str, Any]] = {
    "performance": {
        "label": "Performance",
        "settings": {
            "market_order_max_workers": {
                "type": "int",
                "default": 10,
                "min": 1,
                "max": 20,
                "label": "Market order fetch threads",
                "help": "Number of parallel threads for ESI market order fetches.",
            },
            "market_history_max_workers": {
                "type": "int",
                "default": 10,
                "min": 1,
                "max": 20,
                "label": "Market history fetch threads",
                "help": "Number of parallel threads for ESI market history fetches.",
            },
            "esi_pagination_sleep_seconds": {
                "type": "float",
                "default": 0.1,
                "min": 0.0,
                "max": 1.0,
                "step": 0.05,
                "label": "ESI pagination delay (seconds)",
                "help": "Sleep between paginated ESI pages (public endpoints).",
            },
            "esi_auth_pagination_sleep_seconds": {
                "type": "float",
                "default": 0.1,
                "min": 0.0,
                "max": 1.0,
                "step": 0.05,
                "label": "ESI auth pagination delay (seconds)",
                "help": "Sleep between paginated ESI pages (authenticated endpoints).",
            },
            "product_row_build_max_workers": {
                "type": "int",
                "default": 8,
                "min": 1,
                "max": 16,
                "label": "Product row build threads",
                "help": "Number of parallel threads for building product rows during overview refresh.",
            },
        },
    },
    "cache_ttl": {
        "label": "Cache Lifetimes",
        "settings": {
            "type_orders_cache_ttl_seconds": {
                "type": "int",
                "default": 300,
                "min": 60,
                "max": 7200,
                "label": "Type orders in-memory cache (seconds)",
                "help": "In-memory cache lifetime for per-type ESI order book fetches.",
            },
            "market_prices_cache_ttl_seconds": {
                "type": "int",
                "default": 3600,
                "min": 300,
                "max": 86400,
                "label": "Adjusted market prices cache (seconds)",
                "help": "Cache lifetime for /markets/prices/ (CCP adjusted prices).",
            },
            "market_history_cache_ttl_seconds": {
                "type": "int",
                "default": 21600,
                "min": 3600,
                "max": 86400,
                "label": "Market history cache (seconds)",
                "help": "In-memory cache lifetime for per-type ESI market history.",
            },
            "industry_facilities_cache_ttl_seconds": {
                "type": "int",
                "default": 21600,
                "min": 3600,
                "max": 86400,
                "label": "Industry facilities cache (seconds)",
                "help": "In-memory cache lifetime for /industry/facilities/.",
            },
            "material_price_cache_ttl_seconds": {
                "type": "int",
                "default": 3600,
                "min": 300,
                "max": 86400,
                "label": "Material price DB cache (seconds)",
                "help": "Database-backed cache lifetime for material orderbook views.",
            },
            "structure_rig_cache_ttl_seconds": {
                "type": "int",
                "default": 86400,
                "min": 3600,
                "max": 604800,
                "label": "Structure rig bonus cache (seconds)",
                "help": "Cache lifetime for structure rig manufacturing bonuses.",
            },
            "region_volume_cache_ttl_seconds": {
                "type": "int",
                "default": 21600,
                "min": 3600,
                "max": 86400,
                "label": "Region volume cache (seconds)",
                "help": "In-memory cache lifetime for aggregated region trade volumes.",
            },
        },
    },
    "market_defaults": {
        "label": "Market Defaults",
        "settings": {
            "default_sales_tax": {
                "type": "float",
                "default": 0.03375,
                "min": 0.0,
                "max": 0.20,
                "step": 0.00125,
                "format": "%.4f",
                "label": "Default sales tax rate",
                "help": "Applied when computing sale proceeds.",
            },
            "default_broker_fee": {
                "type": "float",
                "default": 0.03,
                "min": 0.0,
                "max": 0.20,
                "step": 0.005,
                "format": "%.4f",
                "label": "Default broker fee rate",
                "help": "Applied when computing sale proceeds.",
            },
            "orderbook_depth": {
                "type": "int",
                "default": 5,
                "min": 1,
                "max": 50,
                "label": "Orderbook smoothing depth",
                "help": "Number of best orders used for volume-weighted price calculation.",
            },
            "default_region_id": {
                "type": "int",
                "default": 10000002,
                "min": 10000000,
                "max": 19999999,
                "label": "Default region ID",
                "help": "Default trade region (10000002 = The Forge / Jita).",
            },
            "default_station_id": {
                "type": "int",
                "default": 60003760,
                "min": 60000000,
                "max": 69999999,
                "label": "Default station ID",
                "help": "Default trade station (60003760 = Jita 4-4 CNAP).",
            },
        },
    },
    "industry": {
        "label": "Industry",
        "settings": {
            "npc_station_facility_tax": {
                "type": "float",
                "default": 0.0025,
                "min": 0.0,
                "max": 0.10,
                "step": 0.0005,
                "format": "%.4f",
                "label": "NPC station facility tax",
                "help": "Tax rate applied at NPC manufacturing stations.",
            },
            "scc_surcharge": {
                "type": "float",
                "default": 0.04,
                "min": 0.0,
                "max": 0.20,
                "step": 0.005,
                "format": "%.4f",
                "label": "SCC surcharge",
                "help": "CONCORD surcharge on the job value of manufacturing, reaction, copying and invention jobs (4%). Manufacturing, copying and invention are fitted on real ESI jobs; reactions use EVE's documented formula and are not validated (no reaction jobs to fit).",
            },
            "research_scc_surcharge": {
                "type": "float",
                "default": 0.02,
                "min": 0.0,
                "max": 0.20,
                "step": 0.005,
                "format": "%.4f",
                "label": "SCC surcharge (ME/TE research)",
                "help": "CONCORD surcharge on the job value of ME/TE research jobs (2%, fitted on real ESI jobs).",
            },
            "base_sales_tax_rate": {
                "type": "float",
                "default": 0.075,
                "min": 0.0,
                "max": 0.20,
                "step": 0.005,
                "format": "%.4f",
                "label": "Base sales tax rate",
                "help": "Base sales tax rate before Accounting skill reduction (7.5%).",
            },
            "accounting_skill_reduction": {
                "type": "float",
                "default": 0.11,
                "min": 0.0,
                "max": 0.25,
                "step": 0.01,
                "format": "%.2f",
                "label": "Accounting skill reduction per level",
                "help": "Fractional reduction per Accounting skill level (0.11 = 11%).",
            },
            "broker_fee_base": {
                "type": "float",
                "default": 0.03,
                "min": 0.0,
                "max": 0.10,
                "step": 0.005,
                "format": "%.4f",
                "label": "Base broker fee",
                "help": "Maximum broker fee rate before skill/standing reductions.",
            },
            "broker_fee_min": {
                "type": "float",
                "default": 0.01,
                "min": 0.0,
                "max": 0.10,
                "step": 0.005,
                "format": "%.4f",
                "label": "Minimum broker fee",
                "help": "Floor broker fee that cannot be reduced below.",
            },
            "broker_relations_reduction_per_level": {
                "type": "float",
                "default": 0.003,
                "min": 0.0,
                "max": 0.01,
                "step": 0.0005,
                "format": "%.4f",
                "label": "Broker Relations reduction per level",
                "help": "Broker fee reduction per Broker Relations skill level.",
            },
            "faction_standing_reduction_per_point": {
                "type": "float",
                "default": 0.0003,
                "min": 0.0,
                "max": 0.001,
                "step": 0.0001,
                "format": "%.4f",
                "label": "Faction standing reduction per point",
                "help": "Broker fee reduction per point of faction standing.",
            },
            "corp_standing_reduction_per_point": {
                "type": "float",
                "default": 0.0002,
                "min": 0.0,
                "max": 0.001,
                "step": 0.0001,
                "format": "%.4f",
                "label": "Corp standing reduction per point",
                "help": "Broker fee reduction per point of NPC corporation standing.",
            },
            "copy_cost_eiv_multiplier": {
                "type": "float",
                "default": 0.02,
                "min": 0.0,
                "max": 0.10,
                "step": 0.005,
                "format": "%.4f",
                "label": "Copy cost EIV multiplier",
                "help": "Fraction of material EIV used for blueprint copy job cost (2%).",
            },
            "invention_cost_eiv_multiplier": {
                "type": "float",
                "default": 0.02,
                "min": 0.0,
                "max": 0.10,
                "step": 0.005,
                "format": "%.4f",
                "label": "Invention cost EIV multiplier",
                "help": "Invention job value per attempt = this × the invented blueprint's per-run material EIV (2%).",
            },
            "research_cost_eiv_multiplier": {
                "type": "float",
                "default": 0.02,
                "min": 0.0,
                "max": 0.10,
                "step": 0.005,
                "format": "%.5f",
                "label": "Research cost EIV multiplier",
                "help": "Research job value = material EIV × this × EVE's level multiplier (1 at level 1 … 2438.1 at level 10, the same for every blueprint rank) (2%).",
            },
            "max_blueprint_material_efficiency": {
                "type": "int",
                "default": 10,
                "min": 1,
                "max": 20,
                "label": "Max blueprint ME",
                "help": "Maximum material efficiency level for blueprints.",
            },
            "max_blueprint_time_efficiency": {
                "type": "int",
                "default": 20,
                "min": 0,
                "max": 20,
                "step": 2,
                "label": "Max blueprint TE (%)",
                "help": (
                    "Maximum blueprint time efficiency, as EVE's percentage (0-20 %, 2 % per research "
                    "level, so 20 % = level 10). The daily planner researches every owned BPO below "
                    "it up to this TE; blueprints without an owned source are costed at it."
                ),
            },
            "invention_probability_floor": {
                "type": "float",
                "default": 0.01,
                "min": 0.001,
                "max": 0.10,
                "step": 0.005,
                "format": "%.3f",
                "label": "Invention probability floor",
                "help": "Minimum invention success probability used in attempt calculations.",
            },
            "invention_decryptor_selection_enabled": {
                "type": "bool",
                "default": True,
                "label": "Pick the best decryptor per invented item",
                "help": "Evaluate no decryptor and every T2 decryptor for each invented item and use the one "
                        "with the highest profit per invention cycle (the lowest cost per unit when the sell "
                        "price is unknown). Off: every invention is planned without a decryptor.",
            },
            "portfolio_liquidity_threshold": {
                "type": "int",
                "default": 100,
                "min": 1,
                "max": 10000,
                "label": "Portfolio liquidity threshold (units)",
                "help": "Minimum hub volume (units) for a product to score 'healthy liquidity'.",
            },
            "portfolio_order_count_threshold": {
                "type": "int",
                "default": 5,
                "min": 1,
                "max": 100,
                "label": "Portfolio order count threshold",
                "help": "Minimum number of hub orders for a product to score 'healthy liquidity'.",
            },
            "industry_hangar_flag": {
                "type": "select",
                "default": None,
                "options": [
                    {"value": None, "label": "All corp hangar divisions (no filter)"},
                    {"value": "CorpDeliveries", "label": "Corp Deliveries"},
                    {"value": "CorpSAG1", "label": "Division 1 (CorpSAG1)"},
                    {"value": "CorpSAG2", "label": "Division 2 (CorpSAG2)"},
                    {"value": "CorpSAG3", "label": "Division 3 (CorpSAG3)"},
                    {"value": "CorpSAG4", "label": "Division 4 (CorpSAG4)"},
                    {"value": "CorpSAG5", "label": "Division 5 (CorpSAG5)"},
                    {"value": "CorpSAG6", "label": "Division 6 (CorpSAG6)"},
                    {"value": "CorpSAG7", "label": "Division 7 (CorpSAG7)"},
                ],
                "label": "Industry hangar division",
                "help": "Corp office hangar division used as the material sourcing pool. "
                        "Only assets in this division are counted as available inputs. "
                        "Set to 'All' to use all corp assets (legacy behaviour).",
            },
        },
    },
    "daily_planner": {
        "label": "Daily Planner",
        "settings": {
            "planner_ema_alpha": {
                "type": "float",
                "default": 0.2,
                "min": 0.01,
                "max": 0.5,
                "step": 0.01,
                "format": "%.2f",
                "label": "EMA alpha",
                "help": "EMA smoothing factor for plan learning weights (accuracy, velocity, cost).",
            },
            "planner_min_isk_per_hour": {
                "type": "int",
                "default": 5_000_000,
                "min": 100_000,
                "max": 100_000_000,
                "label": "Minimum ISK/hour for build decision",
                "help": "Items scoring below this threshold are skipped instead of built.",
            },
            "planner_price_drift_threshold_pct": {
                "type": "float",
                "default": 5.0,
                "min": 0.5,
                "max": 25.0,
                "step": 0.5,
                "format": "%.1f",
                "label": "Price drift threshold (%)",
                "help": "Percentage price change that counts as drifted for freshness score.",
            },
            "planner_freshness_warning_level": {
                "type": "float",
                "default": 0.9,
                "min": 0.5,
                "max": 1.0,
                "step": 0.05,
                "format": "%.2f",
                "label": "Freshness warning level",
                "help": "freshness_score below this value triggers the staleness warning in the UI.",
            },
            "planner_industry_data_max_age_hours": {
                "type": "int",
                "default": 12,
                "min": 1,
                "max": 72,
                "label": "Max IndustryService data age (hours)",
                "help": "Age threshold above which the planner warns that Industry Builder data may be stale.",
            },
            "planner_bpo_strong_buy_days": {
                "type": "int",
                "default": 30,
                "min": 1,
                "max": 365,
                "label": "BPO Strong Buy break-even (days)",
                "help": "Break-even days threshold for a BPO to be recommended as a Strong Buy.",
            },
            "planner_bpo_consider_days": {
                "type": "int",
                "default": 90,
                "min": 1,
                "max": 730,
                "label": "BPO Consider break-even (days)",
                "help": "Break-even days threshold for a BPO to be flagged as an investment opportunity.",
            },
            "planner_slow_mover_timeout_days": {
                "type": "int",
                "default": 60,
                "min": 7,
                "max": 365,
                "label": "Slow mover timeout (days)",
                "help": "Days before an unsold batch is written as a slow_mover outcome.",
            },
            "planner_shopping_future_stock_days": {
                "type": "int",
                "default": 7,
                "min": 1,
                "max": 30,
                "label": "Future stock pre-buy window (days)",
                "help": "Look-ahead window for pre-buying future_stock materials.",
            },
            "planner_analytics_batch_window": {
                "type": "int",
                "default": 20,
                "min": 5,
                "max": 100,
                "label": "Analytics batch window",
                "help": "Number of completed batches shown in Tab 4 accuracy/velocity view.",
            },
            "planner_history_days": {
                "type": "int",
                "default": 90,
                "min": 7,
                "max": 365,
                "label": "Plan history depth (days)",
                "help": "Rolling window of archived plans shown in the Tab 4 history timeline.",
            },
            "planner_market_hub": {
                "type": "str",
                "default": "jita",
                "label": "Market hub",
                "help": "Primary market hub for shopping list price lookups and BPO analysis.",
            },
            "planner_vwap_days": {
                "type": "int",
                "default": 5,
                "min": 1,
                "max": 30,
                "label": "VWAP trading days",
                "help": "Number of trading days used for VWAP calculation; falls back to spot if fewer days available.",
            },
            "planner_market_depth_threads": {
                "type": "int",
                "default": 10,
                "min": 1,
                "max": 20,
                "label": "Market depth fetch threads",
                "help": "Max parallel ESI calls for market depth collection in MarketIntelligenceJob.",
            },
            "planner_invention_min_attempts": {
                "type": "int",
                "default": 10,
                "min": 1,
                "max": 100,
                "label": "Invention min attempts",
                "help": "Minimum invention attempts before actual success rate replaces the theoretical rate.",
            },
            "planner_competition_index_gate": {
                "type": "float",
                "default": 4.0,
                "min": 0.5,
                "max": 20.0,
                "step": 0.5,
                "format": "%.1f",
                "label": "Competition index gate",
                "help": "competition_index above this value forces an item to 'watch' regardless of score.",
            },
            "planner_mineral_correlation_threshold": {
                "type": "float",
                "default": -0.6,
                "min": -1.0,
                "max": 0.0,
                "step": 0.05,
                "format": "%.2f",
                "label": "Mineral correlation threshold",
                "help": "Pearson correlation below this value flags an item as mineral-squeeze sensitive.",
            },
            "planner_mineral_squeeze_factor": {
                "type": "float",
                "default": 0.85,
                "min": 0.5,
                "max": 1.0,
                "step": 0.01,
                "format": "%.2f",
                "label": "Mineral squeeze score multiplier",
                "help": "Score multiplier applied to mineral-squeeze-sensitive items when Tritanium 7d trend > +5%.",
            },
            "planner_momentum_pause_threshold": {
                "type": "float",
                "default": -6.0,
                "min": -20.0,
                "max": 0.0,
                "step": 0.5,
                "format": "%.1f",
                "label": "Momentum pause threshold",
                "help": "momentum_signal below this value triggers a pause decision (in addition to the -8% trend gate).",
            },
            "planner_mineral_correlation_min_days": {
                "type": "int",
                "default": 30,
                "min": 5,
                "max": 180,
                "label": "Mineral correlation min data points",
                "help": "Minimum trading days required before computing margin–mineral Pearson correlation.",
            },
            "planner_market_refresh_interval_hours": {
                "type": "int",
                "default": 3,
                "min": 1,
                "max": 24,
                "label": "Market intelligence refresh interval (hours)",
                "help": "How often MarketIntelligenceJob refreshes market depth and VWAP data.",
            },
            "planner_max_plan_age_hours": {
                "type": "int",
                "default": 36,
                "min": 6,
                "max": 168,
                "label": "Max plan age before expired mode (hours)",
                "help": "Plan age above which the UI switches to expired mode and hides Tab 1 actions.",
            },
            "planner_min_profit_per_batch": {
                "type": "int",
                "default": 20_000_000,
                "min": 1_000_000,
                "max": 500_000_000,
                "label": "Minimum profit per batch (ISK)",
                "help": "Minimum absolute ISK profit per batch for a build decision.",
            },
            "planner_price_outlier_sigma": {
                "type": "float",
                "default": 3.0,
                "min": 1.0,
                "max": 10.0,
                "step": 0.5,
                "format": "%.1f",
                "label": "Price outlier sigma",
                "help": "Standard deviations from avg_30d beyond which a sell order is excluded as a price outlier.",
            },
        },
    },
    "reprocessing": {
        "label": "Reprocessing",
        "settings": {
            "facility_type": {
                "type": "select",
                "strict": True,
                "default": "npc_station",
                "options": [
                    {"value": "npc_station", "label": "NPC station"},
                    {"value": "athanor", "label": "Athanor"},
                    {"value": "tatara", "label": "Tatara"},
                ],
                "label": "Ore calculator: reprocessing facility",
                "help": "Where the ore calculator reprocesses. An Athanor adds 2% and a Tatara 5.5% "
                        "(the SDE's strRefiningYieldBonus); an NPC station has a 50% base yield and no rigs.",
            },
            "rig": {
                "type": "select",
                "strict": True,
                "default": "none",
                "options": [
                    {"value": "none", "label": "No reprocessing rig"},
                    {"value": "t1", "label": "T1 rig (51% base)"},
                    {"value": "t2", "label": "T2 rig (53% base)"},
                ],
                "label": "Ore calculator: reprocessing rig",
                "help": "The structure's reprocessing rig (M-Set on an Athanor, L-Set on a Tatara), assumed to "
                        "cover every ore reprocessed. None for an NPC station.",
            },
            "security_band": {
                "type": "select",
                "strict": True,
                "default": "high",
                "options": [
                    {"value": "high", "label": "Highsec (x1.00)"},
                    {"value": "low", "label": "Lowsec (x1.06)"},
                    {"value": "null", "label": "Nullsec / wormhole (x1.12)"},
                ],
                "label": "Ore calculator: security band",
                "help": "The facility's security band; the rig's security modifier only applies with a rig.",
            },
            "npc_station_id": {
                "type": "int",
                "optional": True,
                "default": None,
                # No min/max: the int coercion would clamp a wrong id into another station's; the SDE
                # lookup rejects an id it does not have instead.
                "label": "Ore calculator: NPC station id",
                "help": "The NPC station (its id, e.g. 60003760 for Jita IV - Moon 4) whose SDE reprocessing "
                        "yield and base tax the ore calculator uses. Required for an NPC station.",
            },
            "tax": {
                "type": "float",
                "optional": True,
                "strict": True,  # outside min/max is an error, not clamped
                "default": None,
                "min": 0.0,
                "max": 0.5,
                "step": 0.0005,
                "format": "%.4f",
                "label": "Ore calculator: reprocessing tax (effective rate)",
                "help": "The effective reprocessing tax (a fraction), charged on the estimated value of the "
                        "minerals produced. At an NPC station, empty uses the station's SDE base tax; your "
                        "standings lower it, so set the rate you actually pay here (after standings). At a player "
                        "structure the owner sets it, so it is required.",
            },
        },
    },
    "esi_resilience": {
        "label": "ESI Resilience",
        "settings": {
            "esi_max_retries": {
                "type": "int",
                "default": 4,
                "min": 1,
                "max": 10,
                "label": "ESI max retries",
                "help": "Maximum retry attempts for failed ESI requests.",
            },
            "esi_request_timeout_seconds": {
                "type": "float",
                "default": 15.0,
                "min": 5.0,
                "max": 60.0,
                "step": 1.0,
                "label": "ESI request timeout (seconds)",
                "help": "Timeout per individual ESI HTTP request.",
            },
            "esi_error_budget_low_watermark": {
                "type": "int",
                "default": 5,
                "min": 1,
                "max": 50,
                "label": "ESI error budget low watermark",
                "help": "Start pacing requests when error budget remaining drops below this.",
            },
            "esi_error_budget_max_sleep_seconds": {
                "type": "int",
                "default": 60,
                "min": 10,
                "max": 300,
                "label": "ESI error budget max sleep (seconds)",
                "help": "Maximum sleep duration when ESI error budget is exhausted.",
            },
            "esi_error_budget_pacing_sleep_seconds": {
                "type": "float",
                "default": 0.2,
                "min": 0.05,
                "max": 2.0,
                "step": 0.05,
                "format": "%.2f",
                "label": "ESI error budget pacing sleep (seconds)",
                "help": "Sleep duration when error budget is low but not exhausted.",
            },
        },
    },
}


# ---------------------------------------------------------------------------
# AdminSettingsManager
# ---------------------------------------------------------------------------

_FALSE_STRINGS = frozenset({"false", "0", "no", "off", "n", ""})
_TRUE_STRINGS = frozenset({"true", "1", "yes", "on", "y"})


class AdminSettingsManager:
    """Loads, validates and persists admin settings from/to a JSON file."""

    def __init__(self, file_path: str | Path) -> None:
        self._path = Path(file_path)
        self._lock = Lock()
        self._data: dict[str, dict[str, Any]] = {}
        self._load()

    # -- public API ---------------------------------------------------------

    def get(self, category: str, key: str) -> Any:
        """Return the current value for *category*/*key*, falling back to the schema default."""
        spec = self._spec(category, key)
        with self._lock:
            return self._data.get(category, {}).get(key, spec["default"])

    def get_all(self) -> dict[str, dict[str, Any]]:
        """Return all current settings (filled with defaults for missing keys)."""
        result: dict[str, dict[str, Any]] = {}
        for cat_key, cat_schema in ADMIN_SETTINGS_SCHEMA.items():
            cat_values: dict[str, Any] = {}
            for key, spec in cat_schema["settings"].items():
                with self._lock:
                    cat_values[key] = self._data.get(cat_key, {}).get(key, spec["default"])
            result[cat_key] = cat_values
        return result

    def set(self, category: str, key: str, value: Any) -> None:
        """Update a single setting and persist to disk."""
        spec = self._spec(category, key)
        coerced = self._coerce_named(category, key, value, spec)
        with self._lock:
            self._data.setdefault(category, {})[key] = coerced
            self._persist()

    def set_bulk(self, updates: dict[str, dict[str, Any]]) -> None:
        """Update multiple settings at once and persist."""
        for category, values in updates.items():
            for key, value in values.items():
                spec = self._spec(category, key)
                coerced = self._coerce_named(category, key, value, spec)
                self._data.setdefault(category, {})[key] = coerced
        with self._lock:
            self._persist()

    def reset_to_defaults(self) -> None:
        """Reset all settings to schema defaults and persist."""
        with self._lock:
            self._data = {}
            self._persist()

    # -- internals ----------------------------------------------------------

    @staticmethod
    def _spec(category: str, key: str) -> dict[str, Any]:
        cat = ADMIN_SETTINGS_SCHEMA.get(category)
        if cat is None:
            raise KeyError(f"Unknown settings category: {category!r}")
        spec = cat["settings"].get(key)
        if spec is None:
            raise KeyError(f"Unknown setting: {category!r}/{key!r}")
        return spec

    @classmethod
    def _coerce_named(cls, category: str, key: str, value: Any, spec: dict[str, Any]) -> Any:
        """_coerce, with the setting named in a ValueError (the 400 the settings route returns)."""
        try:
            return cls._coerce(value, spec)
        except ValueError as exc:
            raise ValueError(f"{category}/{key}: {exc}") from exc

    @staticmethod
    def _coerce(value: Any, spec: dict[str, Any]) -> Any:
        typ = spec["type"]
        if spec.get("optional") and (value is None or (isinstance(value, str) and not value.strip())):
            return None  # an optional setting that is not set
        if typ == "int":
            if isinstance(value, str):
                value = value.strip()
            coerced = int(value)
            if "step" in spec:
                # Snap to the nearest value on the step grid from min (TE 15 -> 16).
                origin, step = int(spec.get("min", 0)), int(spec["step"])
                coerced = origin + ((coerced - origin) * 2 + step) // (2 * step) * step
            if "min" in spec:
                coerced = max(spec["min"], coerced)
            if "max" in spec:
                coerced = min(spec["max"], coerced)
            return coerced
        if typ == "float":
            coerced = float(value)
            if spec.get("strict") and not (spec.get("min", coerced) <= coerced <= spec.get("max", coerced)):
                raise ValueError(f"{coerced!r} is outside {spec.get('min')}-{spec.get('max')}")
            if "min" in spec:
                coerced = max(spec["min"], coerced)
            if "max" in spec:
                coerced = min(spec["max"], coerced)
            return coerced
        if typ == "bool":
            if isinstance(value, str):
                text = value.strip().lower()
                if text in _FALSE_STRINGS:
                    return False
                if text in _TRUE_STRINGS:
                    return True
                raise ValueError(f"not a boolean: {value!r}")
            return bool(value)
        if typ == "str":
            return str(value)
        if typ == "select" and spec.get("strict"):
            # A strict select has no "no filter" option: any other value is an error.
            allowed_values = [opt["value"] for opt in spec.get("options", [])]
            if value not in allowed_values:
                raise ValueError(f"{value!r} is not one of {', '.join(map(str, allowed_values))}")
            return value
        if typ == "select":
            # Treat empty string and the string "null" / "None" as None (no filter)
            if value is None or value == "" or str(value).lower() in ("null", "none"):
                return None
            allowed = {opt["value"] for opt in spec.get("options", []) if opt["value"] is not None}
            return str(value) if str(value) in allowed else None
        return value

    def _load(self) -> None:
        if not self._path.exists():
            self._data = {}
            return
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception:
            logging.warning("Failed to load admin settings from %s; using defaults", self._path)
            self._data = {}
            return
        self._data = self._coerce_stored(payload if isinstance(payload, dict) else {})

    @classmethod
    def _coerce_stored(cls, payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """The stored settings, each coerced against its spec (type, min/max)
        as set() would. A value that changes logs a WARNING with the key and
        the old and new value; one that cannot be coerced falls back to the
        default (dropped, so get() returns the default). A key the schema no
        longer has (e.g. the removed planner_optimal_te_threshold_pct) is
        ignored with an INFO log."""
        data: dict[str, dict[str, Any]] = {}
        for category, values in payload.items():
            cat_schema = ADMIN_SETTINGS_SCHEMA.get(category)
            if cat_schema is None or not isinstance(values, dict):
                logging.info("Admin settings: ignoring stored category %r, not in the schema", category)
                continue
            for key, raw in values.items():
                spec = cat_schema["settings"].get(key)
                if spec is None:
                    logging.info("Admin settings: ignoring stored setting %s/%s=%r, not in the schema", category, key, raw)
                    continue
                try:
                    coerced = cls._coerce(raw, spec)
                except (TypeError, ValueError, OverflowError) as exc:
                    if spec.get("strict"):
                        # Kept as stored, so its consumer fails loudly naming it instead of a default.
                        logging.error("Admin settings: stored %s/%s=%r is invalid (%s); fix it on the Admin "
                                      "Settings page", category, key, raw, exc)
                        data.setdefault(category, {})[key] = raw
                        continue
                    logging.warning(
                        "Admin settings: stored %s/%s=%r is not a valid %s; using the default %r",
                        category, key, raw, spec["type"], spec["default"],
                    )
                    continue
                widened = isinstance(raw, int) and not isinstance(raw, bool) and isinstance(coerced, float)
                if coerced != raw or (type(coerced) is not type(raw) and not widened):
                    logging.warning(
                        "Admin settings: stored %s/%s=%r is outside its spec; using %r",
                        category, key, raw, coerced,
                    )
                data.setdefault(category, {})[key] = coerced
        return data

    def _persist(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.write_text(json.dumps(self._data, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self._path)
