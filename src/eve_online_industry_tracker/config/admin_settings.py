"""Admin settings schema and persistence manager.

Settings are stored in a JSON file and can be edited at runtime via the
Streamlit admin page.  Consuming services read values through the manager
instance on ``AppState``.
"""
from __future__ import annotations

import json
import logging
import math
from pathlib import Path
from threading import Lock
from typing import Any

from eve_online_industry_tracker.application.errors import ServiceError


# ---------------------------------------------------------------------------
# Schema: defines every admin-configurable setting with type, default, label,
# and optional min/max constraints.  The Streamlit admin page auto-generates
# form controls from this schema.  Every int and float is strict (min/max, and
# an int's step, are checked, never clamped or snapped; a float's step is only
# the settings page's input granularity); "startup_critical" marks a number
# read on every ESI request or by a daemon thread, whose stored invalid value
# reads as the default, with a WARNING once per process (see
# AdminSettingsManager).
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
                "startup_critical": True,  # read on every ESI request / by a daemon: invalid reads as the default
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
                "startup_critical": True,  # read on every ESI request / by a daemon: invalid reads as the default
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
                "label": "Ore calculator: Tatara reprocessing rig",
                "help": "A Tatara's Standup L-Set Reprocessing Monitor, which covers asteroid ore, moon ore and "
                        "ice (the SDE's rig effects). Not used for an Athanor (its rigs are set per ore class "
                        "below); none for an NPC station.",
            },
            **{f"athanor_rig_{ore_class}": {
                "type": "select",
                "strict": True,
                "default": "none",
                "options": [
                    {"value": "none", "label": "No rig"},
                    {"value": "t1", "label": f"{rig_name} I (51% base)"},
                    {"value": "t2", "label": f"{rig_name} II (53% base)"},
                ],
                "label": f"Ore calculator: Athanor {ore_label} rig",
                "help": f"An Athanor's Standup M-Set {rig_name}: it lifts only {ore_label}. Without it "
                        f"{ore_label} gets the unrigged 50% base and no security modifier. Not used for a Tatara "
                        "or an NPC station.",
            } for ore_class, ore_label, rig_name in [
                ("asteroid", "asteroid ore", "Asteroid Ore Grading Processor"),
                ("moon", "moon ore", "Moon Ore Grading Processor"),
                ("ice", "ice", "Ice Grading Processor"),
            ]},
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
                "help": "The facility's security band; a rig's security modifier only applies to the ore classes "
                        "it covers.",
            },
            "npc_station_id": {
                "type": "int",
                "optional": True,
                "default": None,
                # No min/max: the SDE lookup rejects an id it does not have instead.
                "label": "Ore calculator: NPC station id",
                "help": "The NPC station (its id, e.g. 60003760 for Jita IV - Moon 4 - Caldari Navy Assembly "
                        "Plant) whose SDE reprocessing yield and base tax the ore calculator uses. Required for an "
                        "NPC station. The configured station's full name is shown below the input.",
            },
            "tax": {
                "type": "float",
                "optional": True,
                "default": None,
                "min": 0.0,
                "max": 0.5,
                "step": 0.0005,
                "format": "%.4f",
                "label": "Ore calculator: reprocessing tax (effective rate)",
                "help": "The effective reprocessing tax (a fraction), charged on the minerals produced at their "
                        "ESI adjusted price (per community testing). At an NPC station, empty uses the station's SDE base tax; your "
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
                "startup_critical": True,  # read on every ESI request / by a daemon: invalid reads as the default
                "type": "int",
                "default": 5,
                "min": 1,
                "max": 50,
                "label": "ESI error budget low watermark",
                "help": "Start pacing requests when error budget remaining drops below this.",
            },
            "esi_error_budget_max_sleep_seconds": {
                "startup_critical": True,  # read on every ESI request / by a daemon: invalid reads as the default
                "type": "int",
                "default": 60,
                "min": 10,
                "max": 300,
                "label": "ESI error budget max sleep (seconds)",
                "help": "Maximum sleep duration when ESI error budget is exhausted.",
            },
            "esi_error_budget_pacing_sleep_seconds": {
                "startup_critical": True,  # read on every ESI request / by a daemon: invalid reads as the default
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


# (category, key) of the startup-critical settings whose default-in-use WARNING was logged.
_DEFAULT_IN_USE_WARNED: set[tuple[str, str]] = set()
_DEFAULT_IN_USE_LOCK = Lock()


def _warn_default_in_use(category: str, key: str, value: Any, problem: str, default: Any) -> None:
    """A WARNING, once per process, the first time a startup-critical
    setting's stored invalid value is read as its default."""
    with _DEFAULT_IN_USE_LOCK:
        if (category, key) in _DEFAULT_IN_USE_WARNED:
            return
        _DEFAULT_IN_USE_WARNED.add((category, key))
    logging.warning("Admin settings: %s/%s=%r is invalid (%s); using the default %r until it is fixed on the "
                    "Admin Settings page", category, key, value, problem, default)


class AdminSettingInvalid(ServiceError):
    """A stored admin setting is not a valid value: raised where it is read.

    A ServiceError (500), not a KeyError, AttributeError or ValueError, so the
    readers that fall back when there is no settings store do not swallow it."""

    def __init__(self, category: str, key: str, value: Any, problem: str):
        self.category, self.key, self.value, self.problem = category, key, value, problem
        super().__init__(f"admin setting {category}/{key}={value!r} is invalid ({problem}); fix it on the Admin "
                         "Settings page", status_code=500)


def _range_text(spec: dict[str, Any]) -> str:
    low, high = spec.get("min"), spec.get("max")
    if low is not None and high is not None:
        return f"{low!r} to {high!r}"
    return f"at least {low!r}" if low is not None else f"at most {high!r}"


def _coerce_number(value: Any, spec: dict[str, Any]) -> int | float:
    """An int or float setting's value, or ValueError. Strict: a bool, a
    non-numeric string, a fraction for an int, NaN/inf, a value outside
    min/max or (an int) off its step grid are refused, never coerced or
    clamped. A float's step is the input granularity of the settings page,
    not a validity rule: a float is checked for type, finiteness and min/max
    only. A numeric string
    ("45", "6.0") and an integral float for an int (6.0) are read as the
    number."""
    is_int = spec["type"] == "int"
    kind = "an integer" if is_int else "a number"
    if isinstance(value, bool):
        raise ValueError(f"{value!r} is a boolean, not {kind}")
    if isinstance(value, str):
        try:
            number: int | float = float(value.strip())   # "6.0" for an int reads like 6.0
        except ValueError:
            raise ValueError(f"{value!r} is not {kind}") from None
    elif isinstance(value, (int, float)):
        number = value
    else:
        raise ValueError(f"{value!r} is not {kind}")
    if not math.isfinite(number):
        raise ValueError(f"{value!r} is not {kind}")
    if is_int:
        if isinstance(number, float):
            if not number.is_integer():
                raise ValueError(f"{value!r} is not an integer")
            number = int(number)
    else:
        number = float(number)
    if ("min" in spec and number < spec["min"]) or ("max" in spec and number > spec["max"]):
        raise ValueError(f"{number!r} is outside {_range_text(spec)}")
    if is_int and "step" in spec:
        origin, step = int(spec.get("min", 0)), int(spec["step"])
        if (number - origin) % step:
            raise ValueError(f"{number!r} is not a step of {step} from {origin} ({origin}, {origin + step}, "
                             f"{origin + 2 * step}, ...; {_range_text(spec)})")
    return number


def _is_strict(spec: dict[str, Any]) -> bool:
    """A setting whose stored invalid value is kept and reported, never
    replaced: every number, and a strict select."""
    return spec["type"] in ("int", "float") or bool(spec.get("strict"))


class AdminSettingsManager:
    """Loads, validates and persists admin settings from/to a JSON file.

    Every number and strict select is strict: an invalid value is refused on
    save (ValueError naming category/key and the range), and a stored invalid
    value is kept as stored, logged as an ERROR at load and listed by
    invalid()/problems() (the Admin Settings page, /health). Reading it with
    get() raises AdminSettingInvalid naming it, except for a setting marked
    startup_critical (ESI pacing and the market refresh loop, read on every
    ESI request or by a daemon thread): that one reads as its schema default,
    named in the ERROR, the page and /health."""

    def __init__(self, file_path: str | Path) -> None:
        self._path = Path(file_path)
        self._lock = Lock()
        self._data: dict[str, dict[str, Any]] = {}
        self._invalid: dict[tuple[str, str], str] = {}
        self._notices: list[str] = []
        self._load()

    # -- public API ---------------------------------------------------------

    def get(self, category: str, key: str) -> Any:
        """Return the current value for *category*/*key*, falling back to the
        schema default when it is not stored. A stored invalid value raises
        AdminSettingInvalid (a startup_critical one gives the default)."""
        spec = self._spec(category, key)
        with self._lock:
            value = self._data.get(category, {}).get(key, spec["default"])
            problem = self._invalid.get((category, key))
        if problem is None:
            return value
        if spec.get("startup_critical"):
            _warn_default_in_use(category, key, value, problem, spec["default"])
            return spec["default"]
        raise AdminSettingInvalid(category, key, value, problem)

    def get_all(self) -> dict[str, dict[str, Any]]:
        """Return all current settings (filled with defaults for missing
        keys). A stored invalid value is returned as stored: invalid() says
        which, so the settings page shows it instead of another value."""
        result: dict[str, dict[str, Any]] = {}
        for cat_key, cat_schema in ADMIN_SETTINGS_SCHEMA.items():
            cat_values: dict[str, Any] = {}
            for key, spec in cat_schema["settings"].items():
                with self._lock:
                    cat_values[key] = self._data.get(cat_key, {}).get(key, spec["default"])
            result[cat_key] = cat_values
        return result

    def invalid(self) -> dict[str, dict[str, dict[str, Any]]]:
        """{category: {key: {"value", "problem", "startup_critical"}}} for each
        stored invalid value."""
        out: dict[str, dict[str, dict[str, Any]]] = {}
        with self._lock:
            for (category, key), problem in sorted(self._invalid.items()):
                spec = ADMIN_SETTINGS_SCHEMA[category]["settings"][key]
                out.setdefault(category, {})[key] = {
                    "value": self._data.get(category, {}).get(key),
                    "problem": problem,
                    "startup_critical": bool(spec.get("startup_critical")),
                }
        return out

    def notices(self) -> list[str]:
        """What the load changed in memory (the single-rig migration), until
        the next save writes it to the file."""
        with self._lock:
            return list(self._notices)

    def problems(self) -> list[str]:
        """One line per stored invalid value, then the notices (the /health
        warnings)."""
        lines = []
        for category, values in self.invalid().items():
            for key, entry in values.items():
                spec = ADMIN_SETTINGS_SCHEMA[category]["settings"][key]
                lines.append(f"admin setting {category}/{key}={entry['value']!r} is invalid ({entry['problem']}): "
                             f"{_invalid_consequence(spec)}; fix it on the Admin Settings page")
        return lines + self.notices()

    def set(self, category: str, key: str, value: Any) -> None:
        """Update a single setting and persist to disk."""
        spec = self._spec(category, key)
        coerced = self._coerce_named(category, key, value, spec)
        with self._lock:
            self._data.setdefault(category, {})[key] = coerced
            self._invalid.pop((category, key), None)
            self._notices = []   # the migrated values are in the file now
            self._persist()

    def set_bulk(self, updates: dict[str, dict[str, Any]]) -> None:
        """Update multiple settings at once and persist. Every value is
        checked first: one invalid value saves none of them."""
        checked: list[tuple[str, str, Any]] = []
        for category, values in updates.items():
            if not isinstance(values, dict):
                raise ValueError(f"{category}: expected an object of settings, got {values!r}")
            for key, value in values.items():
                spec = self._spec(category, key)
                checked.append((category, key, self._coerce_named(category, key, value, spec)))
        with self._lock:
            for category, key, coerced in checked:
                self._data.setdefault(category, {})[key] = coerced
                self._invalid.pop((category, key), None)
            self._notices = []   # the migrated values are in the file now
            self._persist()

    def reset_to_defaults(self) -> None:
        """Reset all settings to schema defaults and persist."""
        with self._lock:
            self._data = {}
            self._invalid = {}
            self._notices = []
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
        except (ValueError, TypeError) as exc:
            raise ValueError(f"{category}/{key}: {exc}") from exc

    @staticmethod
    def _coerce(value: Any, spec: dict[str, Any]) -> Any:
        typ = spec["type"]
        if spec.get("optional") and (value is None or (isinstance(value, str) and not value.strip())):
            return None  # an optional setting that is not set
        if typ in ("int", "float"):
            return _coerce_number(value, spec)
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
        self._data, self._invalid = self._coerce_stored(payload if isinstance(payload, dict) else {})
        notice = _migrate_single_athanor_rig(self._data)
        self._notices = [notice] if notice else []

    @classmethod
    def _coerce_stored(cls, payload: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], dict[tuple[str, str], str]]:
        """(the stored settings, {(category, key): problem}), each value
        checked against its spec as set() would. An invalid number or strict
        select is kept as stored with an ERROR naming it (get() then raises,
        or gives the default for a startup_critical one); any other invalid
        value falls back to the default with a WARNING. A value read as
        another type ("45" for an int) logs a WARNING. A key the schema no
        longer has (e.g. the removed planner_optimal_te_threshold_pct) is
        ignored with an INFO log."""
        data: dict[str, dict[str, Any]] = {}
        invalid: dict[tuple[str, str], str] = {}
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
                    if _is_strict(spec):
                        # Kept as stored, so it is shown and fails loudly naming it instead of a default.
                        logging.error("Admin settings: stored %s/%s=%r is invalid (%s): %s; fix it on the Admin "
                                      "Settings page", category, key, raw, exc, _invalid_consequence(spec))
                        data.setdefault(category, {})[key] = raw
                        invalid[(category, key)] = str(exc)
                        continue
                    logging.warning(
                        "Admin settings: stored %s/%s=%r is not a valid %s; using the default %r",
                        category, key, raw, spec["type"], spec["default"],
                    )
                    continue
                widened = isinstance(raw, int) and not isinstance(raw, bool) and isinstance(coerced, float)
                if coerced != raw or (type(coerced) is not type(raw) and not widened):
                    logging.warning(
                        "Admin settings: stored %s/%s=%r is not stored as its type; read as %r",
                        category, key, raw, coerced,
                    )
                data.setdefault(category, {})[key] = coerced
        return data, invalid

    def _persist(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.write_text(json.dumps(self._data, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self._path)


def _invalid_consequence(spec: dict[str, Any]) -> str:
    if spec.get("startup_critical"):
        return f"the app uses the default {spec['default']!r} until it is fixed"
    return "the features that read it fail with an error until it is fixed"


_ATHANOR_RIG_KEYS = ("athanor_rig_asteroid", "athanor_rig_moon", "athanor_rig_ice")


def _migrate_single_athanor_rig(data: dict[str, dict[str, Any]]) -> str | None:
    """The migration from PR #14's one reprocessing/rig to a rig per ore class.

    The rule: on an Athanor with a stored rig (t1/t2) and none of the
    athanor_rig_* settings stored, the rig is read as athanor_rig_asteroid,
    the rig the single setting priced (the M-Set Asteroid Ore Grading
    Processor); moon ore and ice stay unrigged until their rig is set, which
    may lower their yield from what the single setting gave. In memory only,
    with a WARNING: the file is rewritten on the next save (which then keeps
    athanor_rig_asteroid, so the rule no longer applies). The notice it
    returns is shown on /health and the Admin Settings page until then.

    reprocessing/rig itself is left as stored: it is now the Tatara's L-Set
    Reprocessing Monitor. After a switch to a Tatara the old value is read
    as the monitor, which is its original meaning (the structure's one
    reprocessing rig, covering every ore). An Athanor does not read it, so
    an invalid stored rig does not fail an Athanor (it is still listed as
    invalid)."""
    reprocessing = data.get("reprocessing")
    if not reprocessing or reprocessing.get("facility_type") != "athanor":
        return None
    rig = reprocessing.get("rig")
    if rig not in ("t1", "t2") or any(key in reprocessing for key in _ATHANOR_RIG_KEYS):
        return None
    reprocessing["athanor_rig_asteroid"] = rig
    notice = (f"admin setting reprocessing/rig={rig!r} on an Athanor is read as reprocessing/athanor_rig_asteroid "
              "(the M-Set Asteroid Ore Grading Processor it priced); moon ore and ice are unrigged until "
              "athanor_rig_moon / athanor_rig_ice are set. Save the settings on the Admin Settings page to keep it")
    logging.warning("Admin settings: %s", notice)
    return notice
