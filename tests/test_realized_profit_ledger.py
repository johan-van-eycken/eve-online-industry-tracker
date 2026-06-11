from __future__ import annotations

import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.infrastructure.models import (  # noqa: E402
    BaseApp,
    BaseSde,
    Blueprints,
    CharacterModel,
    CharacterIndustryJobsModel,
    CharacterRealizedSalesLedgerModel,
    CharacterWalletJournalModel,
    CharacterWalletTransactionsModel,
    CorporationIndustryJobsModel,
    CorporationRealizedSalesLedgerModel,
    CorporationWalletTransactionsModel,
)
from eve_online_industry_tracker.application.characters.asset_provenance import build_market_price_map, resolve_industry_job_cost_snapshot  # noqa: E402
from eve_online_industry_tracker.application.characters.realized_profit import (  # noqa: E402
    CharacterRealizedProfitLedgerService,
    CorporationRealizedProfitLedgerService,
)


def _make_sessions() -> tuple[Session, Session]:
    app_engine = create_engine("sqlite:///:memory:")
    sde_engine = create_engine("sqlite:///:memory:")
    BaseApp.metadata.create_all(bind=app_engine)
    BaseSde.metadata.create_all(bind=sde_engine)
    return sessionmaker(bind=app_engine)(), sessionmaker(bind=sde_engine)()


def test_realized_profit_ledger_builds_from_completed_industry_jobs() -> None:
    app_session, sde_session = _make_sessions()

    app_session.add(
        CharacterModel(
            character_id=1,
            character_name="Test Character",
            market_fees='{"jita_4_4": {"rates": {"sales_tax_fraction": 0.02, "broker_fee_fraction": 0.01}}}',
        )
    )

    sde_session.add(
        Blueprints(
            blueprintTypeID=5000,
            maxProductionLimit=1,
            activities={
                "manufacturing": {
                    "materials": [{"typeID": 34, "quantity": 2}],
                    "products": [{"typeID": 100, "quantity": 5}],
                }
            },
        )
    )
    sde_session.commit()

    app_session.add(
        CharacterIndustryJobsModel(
            character_id=1,
            job_id=1000,
            status="delivered",
            end_date="2026-01-01T00:00:00Z",
            completed_date="2026-01-01T00:00:00Z",
            blueprint_type_id=5000,
            product_type_id=100,
            successful_runs=2,
            runs=2,
            cost=100.0,
        )
    )
    app_session.add(
        CharacterWalletJournalModel(
            character_id=1,
            wallet_journal_id=300,
            amount=960.0,
            balance=0.0,
            date="2026-01-02T00:00:00Z",
            description="Sale",
            ref_type="market_transaction",
            tax=20.0,
        )
    )
    app_session.add(
        CharacterWalletTransactionsModel(
            character_id=1,
            transaction_id=200,
            client_name="Buyer",
            date="2026-01-02T00:00:00Z",
            is_buy=False,
            is_personal=True,
            journal_ref_id=300,
            quantity=10,
            type_id=100,
            type_name="Test Product",
            type_group_name="Ships",
            type_category_name="Ship",
            unit_price=100.0,
            total_price=1000.0,
        )
    )
    app_session.commit()

    service = CharacterRealizedProfitLedgerService(
        app_session=app_session,
        sde_session=sde_session,
        market_prices=[{"type_id": 34, "average_price": 20.0}],
    )

    rows = service.rebuild(character_id=1)

    assert len(rows) == 1
    row = rows[0]
    assert row["transaction_id"] == 200
    assert row["priced_quantity"] == 10
    assert row["unpriced_quantity"] == 0
    assert row["confidence"] == "Medium"
    assert row["source_mix"]["industry_build"]["quantity"] == 10
    assert row["allocated_cost"] == 180.0
    assert row["other_fees_amount"] == 30.0
    assert row["net_revenue"] == 950.0
    assert row["realized_profit"] == 770.0
    assert row["fee_capture_mode"] == "journal_plus_estimated_broker_fee"

    persisted = app_session.query(CharacterRealizedSalesLedgerModel).filter_by(character_id=1).all()
    assert len(persisted) == 1


def test_realized_profit_ledger_prefers_persisted_job_cost_snapshot() -> None:
    app_session, sde_session = _make_sessions()

    app_session.add(
        CharacterModel(
            character_id=1,
            character_name="Snapshot Character",
            market_fees='{"jita_4_4": {"rates": {"sales_tax_fraction": 0.02, "broker_fee_fraction": 0.01}}}',
        )
    )

    sde_session.add(
        Blueprints(
            blueprintTypeID=5001,
            maxProductionLimit=1,
            activities={
                "manufacturing": {
                    "materials": [{"typeID": 34, "quantity": 2}],
                    "products": [{"typeID": 101, "quantity": 5}],
                }
            },
        )
    )
    sde_session.commit()

    app_session.add(
        CharacterIndustryJobsModel(
            character_id=1,
            job_id=1001,
            status="delivered",
            end_date="2026-01-01T00:00:00Z",
            completed_date="2026-01-01T00:00:00Z",
            blueprint_type_id=5001,
            product_type_id=101,
            successful_runs=2,
            runs=2,
            cost=100.0,
            output_quantity=10,
            materials_cost=80.0,
            invention_cost=40.0,
            total_build_cost=220.0,
            unit_build_cost=22.0,
            build_cost_source="persisted_job_cost_snapshot",
        )
    )
    app_session.add(
        CharacterWalletJournalModel(
            character_id=1,
            wallet_journal_id=301,
            amount=960.0,
            balance=0.0,
            date="2026-01-02T00:00:00Z",
            description="Sale",
            ref_type="market_transaction",
            tax=20.0,
        )
    )
    app_session.add(
        CharacterWalletTransactionsModel(
            character_id=1,
            transaction_id=201,
            client_name="Buyer",
            date="2026-01-02T00:00:00Z",
            is_buy=False,
            is_personal=True,
            journal_ref_id=301,
            quantity=10,
            type_id=101,
            type_name="Snapshot Product",
            type_group_name="Ships",
            type_category_name="Ship",
            unit_price=100.0,
            total_price=1000.0,
        )
    )
    app_session.commit()

    service = CharacterRealizedProfitLedgerService(
        app_session=app_session,
        sde_session=sde_session,
        market_prices=[{"type_id": 34, "average_price": 20.0}],
    )

    rows = service.rebuild(character_id=1)

    assert len(rows) == 1
    row = rows[0]
    assert row["allocated_cost"] == 220.0


def test_build_market_price_map_prefers_lower_available_price() -> None:
    price_map = build_market_price_map(
        [
            {"type_id": 34, "average_price": 20.0, "adjusted_price": 15.0},
            {"type_id": 35, "average_price": None, "adjusted_price": 12.0},
            {"type_id": 36, "average_price": 8.0, "adjusted_price": None},
        ]
    )

    assert price_map[34] == 15.0
    assert price_map[35] == 12.0
    assert price_map[36] == 8.0


def test_resolve_industry_job_cost_snapshot_applies_blueprint_material_efficiency() -> None:
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
            "raw": {"activity_id": 1, "blueprint_material_efficiency": 10},
        },
    )()

    snapshot = resolve_industry_job_cost_snapshot(
        job=job,
        sde_session=sde_session,
        market_price_map={34: 19.0},
    )

    assert snapshot["output_quantity"] == 4
    assert snapshot["materials_cost"] == 342.0
    assert snapshot["total_cost"] == 392.0
    assert snapshot["unit_cost"] == 98.0
    assert snapshot["source"] == "market_snapshot_estimate"


def test_realized_profit_ledger_marks_unpriced_sales_low_confidence() -> None:
    app_session, sde_session = _make_sessions()

    app_session.add(
        CharacterModel(
            character_id=1,
            character_name="Test Character",
        )
    )

    app_session.add(
        CharacterWalletTransactionsModel(
            character_id=1,
            transaction_id=201,
            client_name="Buyer",
            date="2026-01-03T00:00:00Z",
            is_buy=False,
            is_personal=True,
            journal_ref_id=None,
            quantity=5,
            type_id=101,
            type_name="Unknown Source Item",
            type_group_name="Ammo",
            type_category_name="Charge",
            unit_price=50.0,
            total_price=250.0,
        )
    )
    app_session.commit()

    service = CharacterRealizedProfitLedgerService(
        app_session=app_session,
        sde_session=sde_session,
        market_prices=[],
    )

    rows = service.rebuild(character_id=1)

    assert len(rows) == 1
    row = rows[0]
    assert row["priced_quantity"] == 0
    assert row["unpriced_quantity"] == 5
    assert row["confidence"] == "Low"
    assert row["realized_profit"] == 250.0
    assert row["realized_margin_fraction"] == 1.0
    assert row["source_mix"]["untracked_inventory"]["quantity"] == 5
    assert any("could not be matched" in note.lower() for note in row["notes"])


def test_realized_profit_ledger_seeds_opening_inventory_from_first_known_build_cost() -> None:
    app_session, sde_session = _make_sessions()

    app_session.add(
        CharacterModel(
            character_id=1,
            character_name="Opening Inventory Character",
            market_fees='{"jita_4_4": {"rates": {"sales_tax_fraction": 0.02, "broker_fee_fraction": 0.01}}}',
        )
    )

    sde_session.add(
        Blueprints(
            blueprintTypeID=5100,
            maxProductionLimit=1,
            activities={
                "manufacturing": {
                    "materials": [{"typeID": 34, "quantity": 2}],
                    "products": [{"typeID": 110, "quantity": 5}],
                }
            },
        )
    )
    sde_session.commit()

    app_session.add(
        CharacterIndustryJobsModel(
            character_id=1,
            job_id=1100,
            status="delivered",
            end_date="2026-01-02T00:00:00Z",
            completed_date="2026-01-02T00:00:00Z",
            blueprint_type_id=5100,
            product_type_id=110,
            successful_runs=2,
            runs=2,
            cost=100.0,
            output_quantity=10,
            total_build_cost=220.0,
            unit_build_cost=22.0,
            build_cost_source="persisted_job_cost_snapshot",
        )
    )
    app_session.add_all(
        [
            CharacterWalletTransactionsModel(
                character_id=1,
                transaction_id=210,
                client_name="Buyer A",
                date="2026-01-01T00:00:00Z",
                is_buy=False,
                is_personal=True,
                journal_ref_id=None,
                quantity=5,
                type_id=110,
                type_name="Opening Inventory Product",
                type_group_name="Ships",
                type_category_name="Ship",
                unit_price=50.0,
                total_price=250.0,
            ),
            CharacterWalletTransactionsModel(
                character_id=1,
                transaction_id=211,
                client_name="Buyer B",
                date="2026-01-03T00:00:00Z",
                is_buy=False,
                is_personal=True,
                journal_ref_id=None,
                quantity=5,
                type_id=110,
                type_name="Opening Inventory Product",
                type_group_name="Ships",
                type_category_name="Ship",
                unit_price=50.0,
                total_price=250.0,
            ),
        ]
    )
    app_session.commit()

    service = CharacterRealizedProfitLedgerService(
        app_session=app_session,
        sde_session=sde_session,
        market_prices=[{"type_id": 34, "average_price": 20.0}],
    )

    rows = service.rebuild(character_id=1)

    assert len(rows) == 2
    first_row = next(row for row in rows if row["transaction_id"] == 210)
    second_row = next(row for row in rows if row["transaction_id"] == 211)

    assert first_row["priced_quantity"] == 5
    assert first_row["unpriced_quantity"] == 0
    assert first_row["allocated_cost"] == 110.0
    assert first_row["source_mix"]["opening_inventory"]["quantity"] == 5
    assert any("opening inventory" in note.lower() for note in first_row["notes"])

    assert second_row["priced_quantity"] == 5
    assert second_row["unpriced_quantity"] == 0
    assert second_row["allocated_cost"] == 110.0
    assert second_row["source_mix"]["industry_build"]["quantity"] == 5


def test_character_realized_profit_includes_non_personal_sales() -> None:
    app_session, sde_session = _make_sessions()

    app_session.add(
        CharacterModel(
            character_id=1,
            character_name="Test Character",
        )
    )

    app_session.add(
        CharacterWalletTransactionsModel(
            character_id=1,
            transaction_id=301,
            client_name="Corp Buyer",
            date="2026-01-05T00:00:00Z",
            is_buy=False,
            is_personal=False,
            journal_ref_id=None,
            quantity=5,
            type_id=101,
            type_name="Corp Sale",
            type_group_name="Ammo",
            type_category_name="Charge",
            unit_price=50.0,
            total_price=250.0,
        )
    )
    app_session.commit()

    service = CharacterRealizedProfitLedgerService(
        app_session=app_session,
        sde_session=sde_session,
        market_prices=[],
    )

    rows = service.rebuild(character_id=1)

    assert len(rows) == 1
    row = rows[0]
    assert row["transaction_id"] == 301
    assert row["priced_quantity"] == 0
    assert row["unpriced_quantity"] == 5
    persisted = app_session.query(CharacterRealizedSalesLedgerModel).filter_by(character_id=1).all()
    assert len(persisted) == 1


def test_character_realized_profit_estimates_market_fees_without_journal() -> None:
    app_session, sde_session = _make_sessions()

    app_session.add(
        CharacterModel(
            character_id=1,
            character_name="Trader",
            market_fees='{"jita_4_4": {"rates": {"sales_tax_fraction": 0.02, "broker_fee_fraction": 0.01}}}',
        )
    )
    app_session.add(
        CharacterWalletTransactionsModel(
            character_id=1,
            transaction_id=401,
            client_name="Buyer",
            date="2026-01-06T00:00:00Z",
            is_buy=False,
            is_personal=True,
            journal_ref_id=None,
            quantity=2,
            type_id=101,
            type_name="Untracked Sold Item",
            type_group_name="Ammo",
            type_category_name="Charge",
            unit_price=100.0,
            total_price=200.0,
        )
    )
    app_session.commit()

    service = CharacterRealizedProfitLedgerService(
        app_session=app_session,
        sde_session=sde_session,
        market_prices=[],
    )

    rows = service.rebuild(character_id=1)

    assert len(rows) == 1
    row = rows[0]
    assert row["sales_tax_amount"] == 4.0
    assert row["other_fees_amount"] == 2.0
    assert row["net_revenue"] == 194.0
    assert row["realized_profit"] == 194.0
    assert row["fee_capture_mode"] == "estimated_market_fees"


def test_corporation_realized_profit_ledger_uses_gross_only_fee_capture() -> None:
    app_session, sde_session = _make_sessions()

    sde_session.add(
        Blueprints(
            blueprintTypeID=7000,
            maxProductionLimit=1,
            activities={
                "manufacturing": {
                    "materials": [{"typeID": 34, "quantity": 4}],
                    "products": [{"typeID": 200, "quantity": 2}],
                }
            },
        )
    )
    sde_session.commit()

    app_session.add(
        CorporationIndustryJobsModel(
            corporation_id=10,
            job_id=5000,
            status="delivered",
            end_date="2026-02-01T00:00:00Z",
            completed_date="2026-02-01T00:00:00Z",
            blueprint_type_id=7000,
            product_type_id=200,
            successful_runs=2,
            runs=2,
            cost=40.0,
        )
    )
    app_session.add(
        CorporationWalletTransactionsModel(
            corporation_id=10,
            division=1,
            transaction_id=9000,
            client_name="Buyer",
            date="2026-02-02T00:00:00Z",
            is_buy=False,
            quantity=4,
            type_id=200,
            type_name="Corp Product",
            type_group_name="Modules",
            type_category_name="Module",
            unit_price=50.0,
            total_price=200.0,
        )
    )
    app_session.commit()

    service = CorporationRealizedProfitLedgerService(
        app_session=app_session,
        sde_session=sde_session,
        market_prices=[{"type_id": 34, "average_price": 5.0}],
    )

    rows = service.rebuild(corporation_id=10)

    assert len(rows) == 1
    row = rows[0]
    assert row["corporation_id"] == 10
    assert row["priced_quantity"] == 4
    assert row["unpriced_quantity"] == 0
    assert row["fee_capture_mode"] == "gross_only"
    assert row["confidence"] == "Medium"
    assert row["source_mix"]["industry_build"]["quantity"] == 4
    assert row["net_revenue"] == 200.0
    assert row["allocated_cost"] == 80.0
    assert row["realized_profit"] == 120.0

    persisted = app_session.query(CorporationRealizedSalesLedgerModel).filter_by(corporation_id=10).all()
    assert len(persisted) == 1