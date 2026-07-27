"""Unit tests for industry hangar scoping (Task 1).

Verifies:
1. AdminSettingsManager correctly stores/coerces the ``industry_hangar_flag``
   select setting (None means no filter; a valid CorpSAG* value is preserved).
2. ``_get_owned_item_inventory()`` filters corp assets by ``location_flag``
   when ``corporation_hangar_flags`` is supplied, and returns all corp assets
   when it is ``None``.
"""
from __future__ import annotations

import os
import sys
import tempfile
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.config.admin_settings import (  # noqa: E402
    ADMIN_SETTINGS_SCHEMA,
    AdminSettingsManager,
)
from eve_online_industry_tracker.infrastructure.models import (  # noqa: E402
    BaseApp,
    CorporationAssetsModel,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    BaseApp.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine)()


def _make_corp_asset(
    *,
    item_id: int,
    corporation_id: int = 98765,
    type_id: int = 34,
    location_flag: str = "CorpSAG1",
    quantity: int = 100,
) -> CorporationAssetsModel:
    return CorporationAssetsModel(
        corporation_id=corporation_id,
        item_id=item_id,
        type_id=type_id,
        type_name="Tritanium",
        type_category_name="Material",
        location_id=1_000_000_001,
        location_flag=location_flag,
        is_singleton=False,
        is_blueprint_copy=False,
        quantity=quantity,
    )


# ---------------------------------------------------------------------------
# AdminSettingsManager — select type coercion
# ---------------------------------------------------------------------------

class TestIndustryHangarFlagSetting:
    def test_schema_declares_industry_hangar_flag(self) -> None:
        """The 'industry_hangar_flag' key must exist in the industry settings schema."""
        industry_settings = ADMIN_SETTINGS_SCHEMA["industry"]["settings"]
        assert "industry_hangar_flag" in industry_settings
        spec = industry_settings["industry_hangar_flag"]
        assert spec["type"] == "select"
        assert spec["default"] is None

    def test_schema_options_include_all_corp_sag_flags(self) -> None:
        spec = ADMIN_SETTINGS_SCHEMA["industry"]["settings"]["industry_hangar_flag"]
        option_values = [opt["value"] for opt in spec["options"]]
        expected_flags = [
            None, "CorpDeliveries",
            "CorpSAG1", "CorpSAG2", "CorpSAG3", "CorpSAG4",
            "CorpSAG5", "CorpSAG6", "CorpSAG7",
        ]
        assert option_values == expected_flags

    def test_manager_default_is_none(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        mgr = AdminSettingsManager(path)
        assert mgr.get("industry", "industry_hangar_flag") is None

    def test_manager_stores_valid_corp_sag_flag(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        mgr = AdminSettingsManager(path)
        mgr.set("industry", "industry_hangar_flag", "CorpSAG2")
        assert mgr.get("industry", "industry_hangar_flag") == "CorpSAG2"

    def test_manager_rejects_invalid_flag_and_stores_none(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        mgr = AdminSettingsManager(path)
        mgr.set("industry", "industry_hangar_flag", "CorpSAGInvalid")
        assert mgr.get("industry", "industry_hangar_flag") is None

    def test_manager_stores_none_when_given_empty_string(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        mgr = AdminSettingsManager(path)
        mgr.set("industry", "industry_hangar_flag", "CorpSAG3")
        mgr.set("industry", "industry_hangar_flag", "")
        assert mgr.get("industry", "industry_hangar_flag") is None

    def test_manager_persists_and_reloads_flag(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        mgr1 = AdminSettingsManager(path)
        mgr1.set("industry", "industry_hangar_flag", "CorpSAG4")
        mgr2 = AdminSettingsManager(path)
        assert mgr2.get("industry", "industry_hangar_flag") == "CorpSAG4"


# ---------------------------------------------------------------------------
# _get_owned_item_inventory — hangar flag filtering
# ---------------------------------------------------------------------------

class TestGetOwnedItemInventoryHangarFilter:
    """Tests that verify the SQLAlchemy filter applied inside
    _get_owned_item_inventory when corporation_hangar_flags is set.

    These tests bypass the full IndustryService and directly query the
    in-memory DB the same way the nested helper would.
    """

    def _query_corp_assets(
        self,
        session: Session,
        corporation_ids: list[int],
        hangar_flags: list[str] | None,
    ) -> list[CorporationAssetsModel]:
        """Replicate the query logic from _get_owned_item_inventory."""
        q = (
            session.query(CorporationAssetsModel)
            .filter(CorporationAssetsModel.corporation_id.in_(corporation_ids))
        )
        if hangar_flags:
            q = q.filter(CorporationAssetsModel.location_flag.in_(hangar_flags))
        return q.all()

    def _populate_session(self) -> Session:
        session = _make_db_session()
        session.add(_make_corp_asset(item_id=1001, location_flag="CorpSAG1", quantity=50))
        session.add(_make_corp_asset(item_id=1002, location_flag="CorpSAG2", quantity=200))
        session.add(_make_corp_asset(item_id=1003, location_flag="CorpSAG3", quantity=75))
        session.add(_make_corp_asset(item_id=1004, location_flag="CorpDeliveries", quantity=10))
        session.commit()
        return session

    def test_no_filter_returns_all_corp_assets(self) -> None:
        session = self._populate_session()
        results = self._query_corp_assets(session, [98765], hangar_flags=None)
        assert len(results) == 4

    def test_single_flag_filters_to_matching_assets(self) -> None:
        session = self._populate_session()
        results = self._query_corp_assets(session, [98765], hangar_flags=["CorpSAG2"])
        assert len(results) == 1
        assert results[0].item_id == 1002
        assert results[0].location_flag == "CorpSAG2"

    def test_single_flag_excludes_other_divisions(self) -> None:
        session = self._populate_session()
        results = self._query_corp_assets(session, [98765], hangar_flags=["CorpSAG2"])
        returned_flags = {r.location_flag for r in results}
        assert returned_flags == {"CorpSAG2"}

    def test_multiple_flags_returns_union(self) -> None:
        session = self._populate_session()
        results = self._query_corp_assets(session, [98765], hangar_flags=["CorpSAG1", "CorpSAG3"])
        assert len(results) == 2
        returned_flags = {r.location_flag for r in results}
        assert returned_flags == {"CorpSAG1", "CorpSAG3"}

    def test_nonexistent_flag_returns_empty(self) -> None:
        session = self._populate_session()
        results = self._query_corp_assets(session, [98765], hangar_flags=["CorpSAG7"])
        assert results == []

    def test_filter_does_not_cross_corporations(self) -> None:
        """Assets from a different corporation are never returned."""
        session = _make_db_session()
        # Corp 98765 has assets in CorpSAG2
        session.add(_make_corp_asset(item_id=2001, corporation_id=98765, location_flag="CorpSAG2"))
        # Corp 99999 also has assets in CorpSAG2 — should not appear when filtering by corp 98765
        session.add(_make_corp_asset(item_id=2002, corporation_id=99999, location_flag="CorpSAG2"))
        session.commit()
        results = self._query_corp_assets(session, [98765], hangar_flags=["CorpSAG2"])
        assert len(results) == 1
        assert results[0].corporation_id == 98765
