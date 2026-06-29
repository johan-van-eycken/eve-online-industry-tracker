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
                "help": "CONCORD surcharge applied to all industry jobs.",
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
            "research_cost_eiv_multiplier": {
                "type": "float",
                "default": 0.02105,
                "min": 0.0,
                "max": 0.10,
                "step": 0.005,
                "format": "%.5f",
                "label": "Research cost EIV multiplier",
                "help": "Fraction of material EIV × duration used for research job cost.",
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
                "min": 1,
                "max": 30,
                "label": "Max blueprint TE",
                "help": "Maximum time efficiency level for blueprints.",
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
        coerced = self._coerce(value, spec)
        with self._lock:
            self._data.setdefault(category, {})[key] = coerced
            self._persist()

    def set_bulk(self, updates: dict[str, dict[str, Any]]) -> None:
        """Update multiple settings at once and persist."""
        for category, values in updates.items():
            for key, value in values.items():
                spec = self._spec(category, key)
                coerced = self._coerce(value, spec)
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

    @staticmethod
    def _coerce(value: Any, spec: dict[str, Any]) -> Any:
        typ = spec["type"]
        if typ == "int":
            coerced = int(value)
            if "min" in spec:
                coerced = max(spec["min"], coerced)
            if "max" in spec:
                coerced = min(spec["max"], coerced)
            return coerced
        if typ == "float":
            coerced = float(value)
            if "min" in spec:
                coerced = max(spec["min"], coerced)
            if "max" in spec:
                coerced = min(spec["max"], coerced)
            return coerced
        if typ == "bool":
            return bool(value)
        if typ == "str":
            return str(value)
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
            self._data = payload if isinstance(payload, dict) else {}
        except Exception:
            logging.warning("Failed to load admin settings from %s; using defaults", self._path)
            self._data = {}

    def _persist(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.write_text(json.dumps(self._data, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self._path)
