from __future__ import annotations

from types import SimpleNamespace


from eve_online_industry_tracker.application.industry.service import IndustryService  # noqa: E402
from eve_online_industry_tracker.db_models import (
    Blueprints,
    CharacterAssetHistoryModel,
    CharacterAssetsModel,
    CharacterIndustryJobsModel,
)
from fakes import _CorpManager, _CharManager


def _build_service(*, sessions, characters, corporations) -> IndustryService:
    service = IndustryService(
        state=SimpleNamespace(
            esi_service=None,
            char_manager=_CharManager(characters),
            corp_manager=_CorpManager(corporations),
        ),
        sessions=sessions,
    )
    return service


def test_get_owned_blueprint_assets_falls_back_to_historical_rows(session_provider) -> None:

    app_session = session_provider.app_session()
    app_session.add(
        CharacterAssetHistoryModel(
            character_id=1,
            item_id=9001,
            observed_at="2026-01-02T00:00:00Z",
            snapshot_source="asset_refresh",
            type_id=5001,
            type_name="Historical Test Blueprint",
            location_id=6001,
            location_type="station",
            location_flag="Hangar",
            is_singleton=True,
            quantity=1,
            is_blueprint_copy=True,
            blueprint_runs=4,
            blueprint_time_efficiency=20,
            blueprint_material_efficiency=10,
            acquisition_source="wallet_transaction",
            acquisition_total_cost=88.0,
        )
    )
    app_session.add(
        CharacterIndustryJobsModel(
            character_id=1,
            job_id=1,
            status="active",
            blueprint_item_id=9001,
        )
    )
    app_session.commit()
    app_session.close()

    sde_session = session_provider.sde_session()
    sde_session.add(Blueprints(blueprintTypeID=5001, maxProductionLimit=1, activities={}))
    sde_session.commit()
    sde_session.close()

    service = _build_service(
        sessions=session_provider,
        characters=[{"character_id": 1, "character_name": "Builder", "corporation_id": 10}],
        corporations=[],
    )

    character_assets, corporation_assets, character_name_by_id, _, _ = service._get_owned_blueprint_assets(
        owned_blueprints_scope="character:1"
    )

    assert corporation_assets == []
    assert character_name_by_id[1] == "Builder"
    assert len(character_assets) == 1
    asset = character_assets[0]
    assert int(asset.item_id) == 9001
    assert int(asset.type_id) == 5001
    assert bool(asset.is_blueprint_copy) is True
    assert int(asset.blueprint_runs or 0) == 4
    assert str(asset.type_category_name) == "Blueprint"
    assert float(asset.acquisition_total_cost or 0.0) == 88.0


def test_get_owned_item_inventory_prefers_historical_cost_over_market_fallback(session_provider) -> None:

    app_session = session_provider.app_session()
    app_session.add(
        CharacterAssetsModel(
            character_id=1,
            item_id=1001,
            type_id=34,
            type_name="Tritanium",
            type_category_name="Material",
            location_id=6001,
            is_singleton=False,
            is_blueprint_copy=False,
            quantity=5,
            type_average_price=12.0,
            is_container=False,
            is_asset_safety_wrap=False,
            is_ship=False,
            is_office_folder=False,
        )
    )
    app_session.add_all(
        [
            CharacterAssetHistoryModel(
                character_id=1,
                item_id=2001,
                observed_at="2026-01-01T00:00:00Z",
                snapshot_source="asset_refresh",
                type_id=34,
                type_name="Tritanium",
                quantity=5,
                acquisition_unit_cost=6.0,
            ),
            CharacterAssetHistoryModel(
                character_id=1,
                item_id=2002,
                observed_at="2026-01-03T00:00:00Z",
                snapshot_source="asset_refresh",
                type_id=34,
                type_name="Tritanium",
                quantity=5,
                acquisition_unit_cost=7.5,
            ),
        ]
    )
    app_session.commit()
    app_session.close()

    service = _build_service(
        sessions=session_provider,
        characters=[{"character_id": 1, "character_name": "Builder", "corporation_id": 10}],
        corporations=[],
    )

    quantity_by_type_id, unit_cost_by_type_id = service._get_owned_item_inventory(
        owned_blueprints_scope="character:1"
    )

    assert quantity_by_type_id[34] == 5
    assert unit_cost_by_type_id[34] == 7.5
