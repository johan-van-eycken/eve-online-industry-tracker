from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from classes.database_models import (
    BaseApp,
    CharacterAssetsModel,
    CharacterIndustryJobsModel,
    CharacterWalletTransactionsModel,
    CorporationAssetsModel,
    CorporationIndustryJobsModel,
    CorporationWalletTransactionsModel,
)
from eve_online_industry_tracker.application.industry.service import IndustryService
from webpages.industry_builder_ci_mapping import map_invention_materials_breakdown_to_rows


@dataclass
class _FakeCharacter:
    character_id: int
    corporation_id: int | None
    skills: dict[str, Any] | None = None


class _FakeCharManager:
    def __init__(self, character: _FakeCharacter):
        self._character = character

    def get_character_by_id(self, character_id: int) -> _FakeCharacter | None:
        if int(character_id) == int(self._character.character_id):
            return self._character
        return None


class _FakeEsiService:
    def __init__(self, market_prices: list[dict[str, Any]]):
        self._market_prices = market_prices

    def get_market_prices(self) -> list[dict[str, Any]]:
        return list(self._market_prices)


class _FakeSessions:
    def __init__(self, *, app_session_factory: Callable[[], Any], sde_session_factory: Callable[[], Any]):
        self._app_session_factory = app_session_factory
        self._sde_session_factory = sde_session_factory

    def app_session(self) -> Any:
        return self._app_session_factory()

    def sde_session(self) -> Any:
        return self._sde_session_factory()

    def oauth_session(self) -> Any:
        raise RuntimeError("oauth_session not needed for these tests")


class _FakeState:
    def __init__(self, *, character: _FakeCharacter, market_prices: list[dict[str, Any]]):
        self.char_manager = _FakeCharManager(character)
        self.esi_service = _FakeEsiService(market_prices)
        # The service uses this for language fallback.
        self.db_sde = type("_FakeDbSde", (), {"language": "en"})()


def _create_app_session():
    engine = create_engine("sqlite+pysqlite:///:memory:")

    # Only create the tables IndustryService.industry_invention_options queries.
    BaseApp.metadata.create_all(
        engine,
        tables=[
            CharacterAssetsModel.__table__,
            CorporationAssetsModel.__table__,
            CharacterWalletTransactionsModel.__table__,
            CorporationWalletTransactionsModel.__table__,
            CharacterIndustryJobsModel.__table__,
            CorporationIndustryJobsModel.__table__,
        ],
    )

    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def _insert_character_asset(
    session,
    *,
    character_id: int,
    item_id: int,
    type_id: int,
    quantity: int,
    type_name: str | None = None,
) -> None:
    session.add(
        CharacterAssetsModel(
            character_id=int(character_id),
            item_id=int(item_id),
            type_id=int(type_id),
            type_name=type_name,
            location_id=1,
            is_singleton=False,
            is_blueprint_copy=False,
            quantity=int(quantity),
            is_container=False,
            is_asset_safety_wrap=False,
            is_ship=False,
            is_office_folder=False,
        )
    )


def _insert_corp_asset(
    session,
    *,
    corporation_id: int,
    item_id: int,
    type_id: int,
    quantity: int,
    type_name: str | None = None,
) -> None:
    session.add(
        CorporationAssetsModel(
            corporation_id=int(corporation_id),
            item_id=int(item_id),
            type_id=int(type_id),
            type_name=type_name,
            location_id=1,
            is_singleton=False,
            is_blueprint_copy=False,
            quantity=int(quantity),
            is_container=False,
            is_asset_safety_wrap=False,
            is_ship=False,
            is_office_folder=False,
        )
    )


def _fake_blueprints_by_id() -> dict[int, dict[str, Any]]:
    # Blueprint IDs are synthetic in tests; only the structure matters.
    input_bp_type_id = 1001
    output_bp_type_id = 2002
    product_type_id = 3003
    mfg_material_type_id = 4004
    datacore_type_id = 20424

    return {
        input_bp_type_id: {
            "type_id": input_bp_type_id,
            "type_name": "Test T1 BP",
            "invention": {
                "time": 123.0,
                "probability": 0.5,
                "materials": [
                    {
                        "type_id": datacore_type_id,
                        "type_name": "Datacore - Test",
                        "quantity": 2,
                        "group_name": "Datacores",
                        "category_name": "Datacore",
                    }
                ],
                "products": [
                    {
                        "type_id": output_bp_type_id,
                        "quantity": 1,
                        "probability": 0.5,
                    }
                ],
                "skills": [],
            },
        },
        output_bp_type_id: {
            "type_id": output_bp_type_id,
            "type_name": "Test T2 BP",
            "manufacturing": {
                "time": 10.0,
                "materials": [
                    {
                        "type_id": mfg_material_type_id,
                        "type_name": "Mfg Mat",
                        "quantity": 1,
                        "group_name": "Materials",
                        "category_name": "Material",
                    }
                ],
                "products": [
                    {
                        "type_id": product_type_id,
                        "type_name": "Test Product",
                        "quantity": 1,
                        "group_name": "Products",
                        "category_name": "Product",
                    }
                ],
            },
        },
    }


def _fake_get_blueprint_manufacturing_data(_sde_session: Any, _language: str, blueprint_type_ids: list[int]) -> dict[int, dict]:
    bp_map = _fake_blueprints_by_id()
    out: dict[int, dict] = {}
    for tid in blueprint_type_ids or []:
        if int(tid) in bp_map:
            out[int(tid)] = bp_map[int(tid)]
    return out


def _fake_get_t2_invention_decryptors(_sde_session: Any, *, language: str) -> list[dict[str, Any]]:
    # One decryptor is enough to validate decryptor inventory valuation.
    return [
        {
            "type_id": 5005,
            "type_name": "Test Decryptor",
            "invention_probability_multiplier": 1.0,
            "invention_me_modifier": 0,
            "invention_te_modifier": 0,
            "invention_max_run_modifier": 0,
        }
    ]


def _market_prices() -> list[dict[str, Any]]:
    return [
        {"type_id": 20424, "average_price": 100.0, "adjusted_price": 100.0},
        {"type_id": 5005, "average_price": 1000.0, "adjusted_price": 1000.0},
        {"type_id": 4004, "average_price": 50.0, "adjusted_price": 50.0},
        {"type_id": 3003, "average_price": 500.0, "adjusted_price": 500.0},
    ]


@pytest.mark.parametrize(
    "on_hand_qty, expected_action",
    [
        (10, "take"),
        (1, "take+buy"),
    ],
)
def test_invention_inventory_cost_pipeline_character_assets(monkeypatch, on_hand_qty: int, expected_action: str) -> None:
    # Patch SDE accessors used by both the service and invention_options_service.
    monkeypatch.setattr(
        "eve_online_industry_tracker.application.industry.service.get_blueprint_manufacturing_data",
        _fake_get_blueprint_manufacturing_data,
    )
    monkeypatch.setattr(
        "eve_online_industry_tracker.infrastructure.invention_options_service.get_blueprint_manufacturing_data",
        _fake_get_blueprint_manufacturing_data,
    )
    monkeypatch.setattr(
        "eve_online_industry_tracker.infrastructure.invention_options_service.get_t2_invention_decryptors",
        _fake_get_t2_invention_decryptors,
    )
    monkeypatch.setattr(
        "eve_online_industry_tracker.infrastructure.sde.decryptors.get_t2_invention_decryptors",
        _fake_get_t2_invention_decryptors,
    )

    app_session = _create_app_session()

    # Character inventory contains datacores + decryptors.
    _insert_character_asset(app_session, character_id=123, item_id=1, type_id=20424, quantity=int(on_hand_qty), type_name="Datacore")
    _insert_character_asset(app_session, character_id=123, item_id=2, type_id=5005, quantity=10, type_name="Decryptor")
    app_session.commit()

    character = _FakeCharacter(character_id=123, corporation_id=None, skills={"skills": []})
    state = _FakeState(character=character, market_prices=_market_prices())

    sessions = _FakeSessions(app_session_factory=lambda: app_session, sde_session_factory=lambda: object())
    svc = IndustryService(state=state, sessions=sessions)

    data, _meta = svc.industry_invention_options(character_id=123, blueprint_type_id=1001, payload={"prefer_inventory_consumption": False})

    mats_bd = ((data or {}).get("invention") or {}).get("materials_breakdown")
    assert isinstance(mats_bd, list) and mats_bd, "Expected invention materials_breakdown"
    dc = next((r for r in mats_bd if int(r.get("type_id") or 0) == 20424), None)
    assert isinstance(dc, dict)

    assert int(dc.get("required_quantity") or 0) == 2
    assert int(dc.get("inventory_on_hand_qty") or 0) == int(on_hand_qty)

    inv_used = int(dc.get("inventory_used_qty") or 0)
    buy_now = int(dc.get("buy_now_qty") or 0)

    if expected_action == "take":
        assert inv_used == 2
        assert buy_now == 0
        assert dc.get("inventory_cost_isk") == pytest.approx(200.0)
    else:
        assert inv_used == 1
        assert buy_now == 1
        assert dc.get("inventory_cost_isk") == pytest.approx(100.0)

    # UI mapping: attempt scale is 1/probability = 2.0 for our fake blueprint.
    ui_rows = map_invention_materials_breakdown_to_rows(mats_bd, attempts_scale=2.0)
    ui_dc = next((r for r in ui_rows if str(r.get("Material")) == "Datacore - Test"), None)
    assert isinstance(ui_dc, dict)
    assert ui_dc.get("Action") == expected_action
    assert ui_dc.get("Job Runs") is None
    assert ui_dc.get("Qty") == pytest.approx(4.0)
    inv_cost_scaled = ui_dc.get("Inventory Cost")
    if expected_action == "take":
        assert inv_cost_scaled == pytest.approx(400.0)
    else:
        assert inv_cost_scaled == pytest.approx(200.0)

    # Decryptor inventory valuation is included on decryptor option rows.
    opts = (data or {}).get("options")
    dec_opt = next((o for o in (opts or []) if isinstance(o, dict) and o.get("decryptor_type_id") == 5005), None)
    assert isinstance(dec_opt, dict)
    assert int(dec_opt.get("decryptor_inventory_on_hand_qty") or 0) == 10
    assert int(dec_opt.get("decryptor_inventory_used_qty") or 0) == 1
    assert float(dec_opt.get("decryptor_inventory_cost_isk") or 0.0) == pytest.approx(1000.0)


def test_invention_inventory_cost_pipeline_corporation_assets(monkeypatch) -> None:
    monkeypatch.setattr(
        "eve_online_industry_tracker.application.industry.service.get_blueprint_manufacturing_data",
        _fake_get_blueprint_manufacturing_data,
    )
    monkeypatch.setattr(
        "eve_online_industry_tracker.infrastructure.invention_options_service.get_blueprint_manufacturing_data",
        _fake_get_blueprint_manufacturing_data,
    )
    monkeypatch.setattr(
        "eve_online_industry_tracker.infrastructure.invention_options_service.get_t2_invention_decryptors",
        _fake_get_t2_invention_decryptors,
    )
    monkeypatch.setattr(
        "eve_online_industry_tracker.infrastructure.sde.decryptors.get_t2_invention_decryptors",
        _fake_get_t2_invention_decryptors,
    )

    app_session = _create_app_session()
    _insert_corp_asset(app_session, corporation_id=999, item_id=10, type_id=20424, quantity=5, type_name="Datacore")
    app_session.commit()

    character = _FakeCharacter(character_id=123, corporation_id=999, skills={"skills": []})
    state = _FakeState(character=character, market_prices=_market_prices())
    sessions = _FakeSessions(app_session_factory=lambda: app_session, sde_session_factory=lambda: object())
    svc = IndustryService(state=state, sessions=sessions)

    data, _meta = svc.industry_invention_options(character_id=123, blueprint_type_id=1001, payload={"prefer_inventory_consumption": False})
    mats_bd = ((data or {}).get("invention") or {}).get("materials_breakdown")
    assert isinstance(mats_bd, list) and mats_bd
    dc = next((r for r in mats_bd if int(r.get("type_id") or 0) == 20424), None)
    assert isinstance(dc, dict)

    # Corporation inventory should be counted as on-hand and consumed first.
    assert int(dc.get("inventory_on_hand_qty") or 0) == 5
    assert int(dc.get("inventory_used_qty") or 0) == 2
    assert int(dc.get("buy_now_qty") or 0) == 0
    assert dc.get("inventory_cost_isk") == pytest.approx(200.0)
