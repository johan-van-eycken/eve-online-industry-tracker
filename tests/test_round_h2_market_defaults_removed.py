"""Round H2.7: market_defaults/default_region_id and default_station_id had
no effect (every market hub carries its own region and station, and the
ESI service's defaults took precedence over them anyway), so they are
removed with their strictness. A value still stored in the file is ignored
with an INFO, not reported as invalid. Fake values only.
"""
from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.application.market_pricing.service import MarketPricingService
from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA, AdminSettingsManager


def test_the_settings_are_gone_from_the_schema():
    settings = ADMIN_SETTINGS_SCHEMA["market_defaults"]["settings"]
    assert "default_region_id" not in settings
    assert "default_station_id" not in settings


def test_a_stored_value_is_ignored_not_invalid(tmp_path, caplog):
    path = tmp_path / "admin_settings.json"
    path.write_text(json.dumps({"market_defaults": {"default_region_id": "not a region", "default_station_id": 1}}),
                    encoding="utf-8")
    with caplog.at_level(logging.INFO):
        mgr = AdminSettingsManager(path)
    assert mgr.invalid() == {}
    assert mgr.problems() == []
    assert "default_region_id" in caplog.text


@pytest.mark.parametrize("hub", sorted(MarketPricingService._MARKET_HUBS))
def test_each_hub_is_priced_in_its_own_region_and_station(hub):
    class _Admin:
        def get(self, category, key):
            raise AssertionError(f"read {category}/{key}")

    svc = MarketPricingService(state=SimpleNamespace(admin_settings=_Admin(),
                                                     esi_service=SimpleNamespace(_region_id=1, _station_id=2)))
    context = svc._market_hub_context(hub)
    configured = MarketPricingService._MARKET_HUBS[hub]
    assert (context["region_id"], context["station_id"]) == (configured["region_id"], configured["station_id"])
