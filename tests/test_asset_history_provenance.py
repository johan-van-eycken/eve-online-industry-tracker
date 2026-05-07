from __future__ import annotations

import os
import sys
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from classes.asset_history import (  # noqa: E402
    backfill_wallet_buy_acquisitions,
    build_historical_input_cost_lookup,
    clear_historical_backfill,
    lookup_historical_blueprint_provenance,
    record_historical_acquisition,
    sync_asset_history,
)
import classes.character as character_module  # noqa: E402
from classes.character import Character  # noqa: E402
from classes.asset_provenance import resolve_industry_job_cost_snapshot  # noqa: E402
from classes.database_models import (  # noqa: E402
    BaseApp,
    BaseSde,
    Blueprints,
    CharacterAssetEventModel,
    CharacterAssetHistoryModel,
    CharacterAssetsModel,
    CharacterIndustryJobsModel,
    CharacterWalletTransactionsModel,
)


def _make_sessions() -> tuple[Session, Session]:
    app_engine = create_engine("sqlite:///:memory:")
    sde_engine = create_engine("sqlite:///:memory:")
    BaseApp.metadata.create_all(bind=app_engine)
    BaseSde.metadata.create_all(bind=sde_engine)
    return sessionmaker(bind=app_engine)(), sessionmaker(bind=sde_engine)()


class _FakeDbManager:
    def __init__(self, session: Session) -> None:
        self.session = session


class _FakeEsiClient:
    def __init__(self, jobs: list[dict[str, object]]) -> None:
        self._jobs = jobs

    def esi_get(self, endpoint: str, params=None, paginate: bool = False):
        if endpoint.endswith("/industry/jobs/"):
            return list(self._jobs)
        if endpoint == "/markets/prices/":
            return []
        raise AssertionError(endpoint)


def _make_character_for_refresh(*, app_session: Session, sde_session: Session, jobs: list[dict[str, object]]) -> Character:
    character = object.__new__(Character)
    character.character_id = 1
    character.character_name = "Builder"
    character._db_app = _FakeDbManager(app_session)
    character._db_sde = _FakeDbManager(sde_session)
    character._esi_client = _FakeEsiClient(jobs)
    character.ensure_esi = lambda: None
    return character


def _stub_refresh_dependencies(monkeypatch, *, snapshot_by_job_id: dict[int, dict[str, object]]) -> None:
    monkeypatch.setattr(character_module, "build_market_price_map", lambda rows: {})
    monkeypatch.setattr(character_module, "build_invention_cost_per_run_by_blueprint_type", lambda **kwargs: {})
    monkeypatch.setattr(character_module, "industry_job_material_type_ids", lambda **kwargs: [])
    monkeypatch.setattr(character_module, "lookup_historical_blueprint_provenance", lambda **kwargs: None)
    monkeypatch.setattr(character_module, "build_historical_input_cost_lookup", lambda **kwargs: {})

    def _resolve_snapshot(**kwargs):
        job = kwargs["job"]
        payload = snapshot_by_job_id[int(job.job_id)]
        return {
            "blueprint_item_id": None,
            "blueprint_is_blueprint_copy": None,
            "blueprint_runs": None,
            "blueprint_time_efficiency": None,
            "blueprint_material_efficiency": None,
            "blueprint_provenance_source": None,
            "blueprint_provenance_ref_id": None,
            "materials_cost": payload.get("materials_cost", 0.0),
            "historical_materials_cost": None,
            "historical_material_cost_source": None,
            "historical_material_coverage_fraction": None,
            "historical_input_costs": None,
            "copy_cost": 0.0,
            "invention_cost": 0.0,
            "output_quantity": payload["output_quantity"],
            "total_cost": payload["total_cost"],
            "unit_cost": payload["unit_cost"],
            "source": payload.get("source", "test_snapshot"),
        }

    monkeypatch.setattr(character_module, "resolve_industry_job_cost_snapshot", _resolve_snapshot)


def test_sync_asset_history_records_asset_lifecycle_events() -> None:
    app_session, _ = _make_sessions()

    app_session.add_all(
        [
            CharacterAssetsModel(
                character_id=1,
                item_id=1001,
                type_id=34,
                quantity=5,
                location_id=6001,
                is_singleton=False,
                is_blueprint_copy=False,
                is_container=False,
                is_asset_safety_wrap=False,
                is_ship=False,
                is_office_folder=False,
            ),
            CharacterAssetsModel(
                character_id=1,
                item_id=1002,
                type_id=35,
                quantity=2,
                location_id=6002,
                is_singleton=False,
                is_blueprint_copy=False,
                is_container=False,
                is_asset_safety_wrap=False,
                is_ship=False,
                is_office_folder=False,
            ),
        ]
    )
    app_session.commit()

    sync_asset_history(
        app_session=app_session,
        owner_kind="character",
        owner_id=1,
        observed_at="2026-01-01T00:00:00Z",
        asset_rows=[
            {"item_id": 1001, "type_id": 34, "quantity": 3, "location_id": 7001},
            {"item_id": 1003, "type_id": 36, "quantity": 7, "location_id": 7002},
        ],
    )
    app_session.commit()

    events = app_session.query(CharacterAssetEventModel).order_by(CharacterAssetEventModel.item_id).all()
    assert len(events) == 3
    assert [(event.item_id, event.event_kind, event.quantity_delta) for event in events] == [
        (1001, "quantity_changed", -2),
        (1002, "disappeared", -2),
        (1003, "appeared", 7),
    ]

    history_rows = app_session.query(CharacterAssetHistoryModel).order_by(CharacterAssetHistoryModel.item_id).all()
    assert len(history_rows) == 2
    assert [row.item_id for row in history_rows] == [1001, 1003]


def test_lookup_historical_blueprint_provenance_resolves_exact_vanished_bpc() -> None:
    app_session, _ = _make_sessions()

    app_session.add(
        CharacterAssetHistoryModel(
            character_id=1,
            item_id=9001,
            observed_at="2026-01-01T00:00:00Z",
            snapshot_source="asset_refresh",
            type_id=5000,
            quantity=1,
            is_blueprint_copy=True,
            blueprint_runs=2,
            blueprint_time_efficiency=20,
            blueprint_material_efficiency=10,
        )
    )
    app_session.commit()

    provenance = lookup_historical_blueprint_provenance(
        app_session=app_session,
        owner_kind="character",
        owner_id=1,
        blueprint_item_id=9001,
        blueprint_type_id=5000,
        as_of="2026-01-02T00:00:00Z",
    )

    assert provenance is not None
    assert provenance["item_id"] == 9001
    assert provenance["is_blueprint_copy"] is True
    assert provenance["blueprint_runs"] == 2
    assert provenance["blueprint_material_efficiency"] == 10
    assert provenance["blueprint_time_efficiency"] == 20
    assert provenance["source"] == "historical_blueprint_exact_item"


def test_lookup_historical_blueprint_provenance_forward_fills_consistent_future_state() -> None:
    app_session, _ = _make_sessions()

    app_session.add_all(
        [
            CharacterAssetHistoryModel(
                character_id=1,
                item_id=-2001,
                observed_at="2026-03-01T00:00:00Z",
                snapshot_source="historical_backfill",
                type_id=5000,
                quantity=5,
            ),
            CharacterAssetHistoryModel(
                character_id=1,
                item_id=9101,
                observed_at="2026-04-27T00:00:00Z",
                snapshot_source="asset_refresh",
                type_id=5000,
                quantity=1,
                is_blueprint_copy=True,
                blueprint_runs=10,
                blueprint_time_efficiency=20,
                blueprint_material_efficiency=10,
            ),
            CharacterAssetHistoryModel(
                character_id=1,
                item_id=9102,
                observed_at="2026-05-01T00:00:00Z",
                snapshot_source="asset_refresh",
                type_id=5000,
                quantity=1,
                is_blueprint_copy=True,
                blueprint_runs=10,
                blueprint_time_efficiency=20,
                blueprint_material_efficiency=10,
            ),
        ]
    )
    app_session.commit()

    provenance = lookup_historical_blueprint_provenance(
        app_session=app_session,
        owner_kind="character",
        owner_id=1,
        blueprint_item_id=9001,
        blueprint_type_id=5000,
        as_of="2026-04-03T00:00:00Z",
    )

    assert provenance is not None
    assert provenance["item_id"] == 9101
    assert provenance["is_blueprint_copy"] is True
    assert provenance["blueprint_runs"] == 10
    assert provenance["blueprint_material_efficiency"] == 10
    assert provenance["blueprint_time_efficiency"] == 20
    assert provenance["source"] == "historical_blueprint_type_forward_fill"


def test_lookup_historical_blueprint_provenance_does_not_forward_fill_mixed_future_state() -> None:
    app_session, _ = _make_sessions()

    app_session.add_all(
        [
            CharacterAssetHistoryModel(
                character_id=1,
                item_id=9101,
                observed_at="2026-04-27T00:00:00Z",
                snapshot_source="asset_refresh",
                type_id=5000,
                quantity=1,
                is_blueprint_copy=True,
                blueprint_runs=10,
                blueprint_time_efficiency=20,
                blueprint_material_efficiency=10,
            ),
            CharacterAssetHistoryModel(
                character_id=1,
                item_id=9102,
                observed_at="2026-05-01T00:00:00Z",
                snapshot_source="asset_refresh",
                type_id=5000,
                quantity=1,
                is_blueprint_copy=True,
                blueprint_runs=5,
                blueprint_time_efficiency=20,
                blueprint_material_efficiency=6,
            ),
        ]
    )
    app_session.commit()

    provenance = lookup_historical_blueprint_provenance(
        app_session=app_session,
        owner_kind="character",
        owner_id=1,
        blueprint_item_id=9001,
        blueprint_type_id=5000,
        as_of="2026-04-03T00:00:00Z",
    )

    assert provenance is None


def test_build_historical_input_cost_lookup_returns_historical_lots_before_job() -> None:
    app_session, _ = _make_sessions()

    app_session.add_all(
        [
            CharacterAssetHistoryModel(
                character_id=1,
                item_id=2001,
                observed_at="2026-01-01T00:00:00Z",
                snapshot_source="asset_refresh",
                type_id=34,
                quantity=100,
                acquisition_source="buy_tx",
                acquisition_unit_cost=5.0,
                acquisition_reference_id=1,
            ),
            CharacterAssetHistoryModel(
                character_id=1,
                item_id=2002,
                observed_at="2026-01-03T00:00:00Z",
                snapshot_source="asset_refresh",
                type_id=34,
                quantity=50,
                acquisition_source="buy_tx",
                acquisition_unit_cost=7.5,
                acquisition_reference_id=2,
            ),
            CharacterAssetHistoryModel(
                character_id=1,
                item_id=2003,
                observed_at="2026-01-05T00:00:00Z",
                snapshot_source="asset_refresh",
                type_id=34,
                quantity=50,
                acquisition_source="buy_tx",
                acquisition_unit_cost=9.0,
                acquisition_reference_id=3,
            ),
        ]
    )
    app_session.commit()

    lookup = build_historical_input_cost_lookup(
        app_session=app_session,
        owner_kind="character",
        owner_id=1,
        as_of="2026-01-04T00:00:00Z",
        type_ids=[34],
    )

    assert lookup[34]["unit_cost"] == ((100 * 5.0) + (50 * 7.5)) / 150
    assert lookup[34]["source"] == "buy_tx"
    assert lookup[34]["reference_id"] == 2
    assert [lot["quantity"] for lot in lookup[34]["lots"]] == [100, 50]
    assert [lot["unit_cost"] for lot in lookup[34]["lots"]] == [5.0, 7.5]


def test_backfill_wallet_buy_acquisitions_populates_historical_lookup() -> None:
    app_session, _ = _make_sessions()

    app_session.add(
        CharacterWalletTransactionsModel(
            character_id=1,
            transaction_id=501,
            date="2026-01-02T00:00:00Z",
            is_buy=True,
            quantity=25,
            type_id=34,
            type_name="Tritanium",
            unit_price=6.5,
            total_price=162.5,
        )
    )
    app_session.commit()

    wallet_transactions = app_session.query(CharacterWalletTransactionsModel).filter_by(character_id=1).all()
    backfill_wallet_buy_acquisitions(
        app_session=app_session,
        owner_kind="character",
        owner_id=1,
        wallet_transactions=wallet_transactions,
    )
    app_session.commit()

    lookup = build_historical_input_cost_lookup(
        app_session=app_session,
        owner_kind="character",
        owner_id=1,
        as_of="2026-01-03T00:00:00Z",
        type_ids=[34],
    )

    assert lookup[34]["unit_cost"] == 6.5
    assert lookup[34]["source"] == "wallet_transaction"
    assert lookup[34]["reference_type"] == "wallet_transaction"
    assert lookup[34]["reference_id"] == 501


def test_record_historical_industry_output_populates_later_input_lookup() -> None:
    app_session, _ = _make_sessions()

    record_historical_acquisition(
        app_session=app_session,
        owner_kind="character",
        owner_id=1,
        observed_at="2026-01-01T00:00:00Z",
        type_id=34,
        type_name="Tritanium",
        quantity=100,
        acquisition_source="industry_build",
        acquisition_unit_cost=7.25,
        acquisition_total_cost=725.0,
        acquisition_reference_type="industry_job",
        acquisition_reference_id=9001,
    )
    app_session.commit()

    lookup = build_historical_input_cost_lookup(
        app_session=app_session,
        owner_kind="character",
        owner_id=1,
        as_of="2026-01-02T00:00:00Z",
        type_ids=[34],
    )

    assert lookup[34]["unit_cost"] == 7.25
    assert lookup[34]["source"] == "industry_build"
    assert lookup[34]["reference_type"] == "industry_job"
    assert lookup[34]["reference_id"] == 9001

    clear_historical_backfill(
        app_session=app_session,
        owner_kind="character",
        owner_id=1,
    )
    app_session.commit()

    cleared_lookup = build_historical_input_cost_lookup(
        app_session=app_session,
        owner_kind="character",
        owner_id=1,
        as_of="2026-01-02T00:00:00Z",
        type_ids=[34],
    )
    assert 34 not in cleared_lookup


def test_resolve_industry_job_cost_snapshot_prefers_historical_owned_inputs() -> None:
    _, sde_session = _make_sessions()

    sde_session.add(
        Blueprints(
            blueprintTypeID=5002,
            maxProductionLimit=1,
            activities={
                "manufacturing": {
                    "materials": [{"typeID": 34, "quantity": 10}],
                    "products": [{"typeID": 102, "quantity": 2}],
                }
            },
        )
    )
    sde_session.commit()

    job = type(
        "IndustryJobPayload",
        (),
        {
            "blueprint_type_id": 5002,
            "product_type_id": 102,
            "successful_runs": 2,
            "runs": 2,
            "cost": 50.0,
            "raw": {"activity_id": 1},
        },
    )()

    snapshot = resolve_industry_job_cost_snapshot(
        job=job,
        sde_session=sde_session,
        market_price_map={34: 19.0},
        blueprint_provenance={
            "item_id": 9001,
            "is_blueprint_copy": True,
            "blueprint_runs": 2,
            "blueprint_material_efficiency": 10,
            "blueprint_time_efficiency": 20,
            "source": "historical_blueprint_exact_item",
            "reference_id": 44,
        },
        owned_input_unit_cost_by_type_id={
            34: {
                "unit_cost": 7.0,
                "source": "buy_tx",
                "reference_type": "wallet_transaction",
                "reference_id": 88,
                "history_id": 99,
                "observed_at": "2026-01-01T00:00:00Z",
                "lots": [
                    {
                        "unit_cost": 7.0,
                        "quantity": 18,
                        "source": "buy_tx",
                        "reference_type": "wallet_transaction",
                        "reference_id": 88,
                        "history_id": 99,
                        "observed_at": "2026-01-01T00:00:00Z",
                    }
                ],
            }
        },
    )

    assert snapshot["output_quantity"] == 4
    assert snapshot["materials_cost"] == 126.0
    assert snapshot["historical_materials_cost"] == 126.0
    assert snapshot["historical_material_cost_source"] == "historical_asset_acquisition_cost"
    assert snapshot["historical_material_coverage_fraction"] == 1.0
    assert snapshot["historical_input_costs"]["34"]["unit_cost"] == 7.0
    assert snapshot["total_cost"] == 176.0
    assert snapshot["unit_cost"] == 44.0
    assert snapshot["source"] == "historical_asset_acquisition_cost"
    assert snapshot["blueprint_item_id"] == 9001
    assert snapshot["blueprint_is_blueprint_copy"] is True
    assert snapshot["blueprint_material_efficiency"] == 10
    assert snapshot["blueprint_provenance_source"] == "historical_blueprint_exact_item"
    assert snapshot["blueprint_provenance_ref_id"] == 44


def test_resolve_industry_job_cost_snapshot_consumes_historical_lots_fifo() -> None:
    _, sde_session = _make_sessions()

    sde_session.add(
        Blueprints(
            blueprintTypeID=5003,
            maxProductionLimit=1,
            activities={
                "manufacturing": {
                    "materials": [{"typeID": 34, "quantity": 10}],
                    "products": [{"typeID": 103, "quantity": 1}],
                }
            },
        )
    )
    sde_session.commit()

    job = type(
        "IndustryJobPayload",
        (),
        {
            "blueprint_type_id": 5003,
            "product_type_id": 103,
            "successful_runs": 2,
            "runs": 2,
            "cost": 50.0,
            "raw": {"activity_id": 1},
        },
    )()

    snapshot = resolve_industry_job_cost_snapshot(
        job=job,
        sde_session=sde_session,
        market_price_map={34: 19.0},
        owned_input_unit_cost_by_type_id={
            34: {
                "unit_cost": 6.25,
                "source": "buy_tx",
                "reference_type": "wallet_transaction",
                "reference_id": 102,
                "history_id": 202,
                "observed_at": "2026-01-02T00:00:00Z",
                "lots": [
                    {
                        "unit_cost": 5.0,
                        "quantity": 12,
                        "source": "buy_tx",
                        "reference_type": "wallet_transaction",
                        "reference_id": 101,
                        "history_id": 201,
                        "observed_at": "2026-01-01T00:00:00Z",
                    },
                    {
                        "unit_cost": 8.5,
                        "quantity": 8,
                        "source": "buy_tx",
                        "reference_type": "wallet_transaction",
                        "reference_id": 102,
                        "history_id": 202,
                        "observed_at": "2026-01-02T00:00:00Z",
                    },
                ],
            }
        },
    )

    assert snapshot["materials_cost"] == 128.0
    assert snapshot["historical_materials_cost"] == 128.0
    assert snapshot["historical_input_costs"]["34"]["quantity"] == 20
    assert snapshot["historical_input_costs"]["34"]["unit_cost"] == 6.4
    assert snapshot["historical_input_costs"]["34"]["lots"] == [
        {
            "quantity": 12,
            "unit_cost": 5.0,
            "total_cost": 60.0,
            "source": "buy_tx",
            "reference_type": "wallet_transaction",
            "reference_id": 101,
            "history_id": 201,
            "observed_at": "2026-01-01T00:00:00Z",
        },
        {
            "quantity": 8,
            "unit_cost": 8.5,
            "total_cost": 68.0,
            "source": "buy_tx",
            "reference_type": "wallet_transaction",
            "reference_id": 102,
            "history_id": 202,
            "observed_at": "2026-01-02T00:00:00Z",
        },
    ]
    assert snapshot["total_cost"] == 178.0
    assert snapshot["unit_cost"] == 89.0


def test_refresh_industry_jobs_preserves_existing_jobs_for_backfill(monkeypatch) -> None:
    app_session, sde_session = _make_sessions()
    app_session.add(
        CharacterIndustryJobsModel(
            character_id=1,
            job_id=9001,
            status="delivered",
            completed_date="2026-01-01T00:00:00Z",
            product_type_id=34,
            output_quantity=2,
            unit_build_cost=10.0,
            total_build_cost=20.0,
            build_cost_source="persisted",
            raw={},
        )
    )
    app_session.commit()

    fetched_jobs = [
        {
            "job_id": 9002,
            "status": "delivered",
            "completed_date": "2026-01-02T00:00:00Z",
            "blueprint_type_id": 5002,
            "product_type_id": 35,
            "runs": 1,
            "successful_runs": 1,
            "cost": 15.0,
        }
    ]
    _stub_refresh_dependencies(
        monkeypatch,
        snapshot_by_job_id={
            9002: {"output_quantity": 3, "unit_cost": 11.0, "total_cost": 33.0},
        },
    )
    character = _make_character_for_refresh(app_session=app_session, sde_session=sde_session, jobs=fetched_jobs)

    character.refresh_industry_jobs()

    job_ids = {
        row.job_id
        for row in app_session.query(CharacterIndustryJobsModel)
        .filter_by(character_id=1)
        .all()
    }
    assert job_ids == {9001, 9002}

    history_rows = (
        app_session.query(CharacterAssetHistoryModel)
        .filter_by(character_id=1, snapshot_source="historical_backfill")
        .order_by(CharacterAssetHistoryModel.acquisition_reference_id)
        .all()
    )
    assert [row.acquisition_reference_id for row in history_rows] == [9001, 9002]


def test_refresh_industry_jobs_skips_active_job_output_backfill(monkeypatch) -> None:
    app_session, sde_session = _make_sessions()
    fetched_jobs = [
        {
            "job_id": 9010,
            "status": "active",
            "end_date": "2026-01-03T00:00:00Z",
            "blueprint_type_id": 5002,
            "product_type_id": 36,
            "runs": 1,
            "successful_runs": 1,
            "cost": 22.0,
        }
    ]
    _stub_refresh_dependencies(
        monkeypatch,
        snapshot_by_job_id={
            9010: {"output_quantity": 4, "unit_cost": 12.0, "total_cost": 48.0},
        },
    )
    character = _make_character_for_refresh(app_session=app_session, sde_session=sde_session, jobs=fetched_jobs)

    character.refresh_industry_jobs()

    history_rows = (
        app_session.query(CharacterAssetHistoryModel)
        .filter_by(character_id=1, snapshot_source="historical_backfill", acquisition_reference_type="industry_job")
        .all()
    )
    assert history_rows == []
