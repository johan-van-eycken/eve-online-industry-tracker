"""Realized-profit fee estimates are computed at use (each ledger rebuild)
from the character's stored skills and standings with the current admin
settings industry/*, through application.market_fees like the price
suggestion. Not from the stored rates (market_fees.jita_4_4.rates), which
lag a settings change until the next character refresh.

Without known skills the fees are the highest (an untrained character
without standings), with the reason in the row's notes and a distinct fee
capture mode, and the realized-profit page shows it. Fake values throughout.
"""
from __future__ import annotations

import json
import os
import sys
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.application.characters.realized_profit import (  # noqa: E402
    CharacterRealizedProfitLedgerService,
)
from eve_online_industry_tracker.config.admin_settings import (  # noqa: E402
    AdminSettingInvalid,
    AdminSettingsManager,
)
from eve_online_industry_tracker.infrastructure.models import (  # noqa: E402
    BaseApp,
    BaseSde,
    CharacterModel,
    CharacterWalletJournalModel,
    CharacterWalletTransactionsModel,
)



#: Every character id is one the character manager has (J2.4 checks it).
_KNOWN_CHARACTERS = SimpleNamespace(get_character_by_id=lambda character_id: object())

def _sessions():
    app_engine, sde_engine = create_engine("sqlite:///:memory:"), create_engine("sqlite:///:memory:")
    BaseApp.metadata.create_all(bind=app_engine)
    BaseSde.metadata.create_all(bind=sde_engine)
    return sessionmaker(bind=app_engine)(), sessionmaker(bind=sde_engine)()


def _stored_fees(accounting=4, broker_relations=5, faction=2.0, corp=1.0) -> str:
    # Stale stored rates (0.5): never used.
    return json.dumps({"jita_4_4": {
        "skills": {"accounting_level": accounting, "broker_relations_level": broker_relations},
        "standings": {"faction_standing": faction, "corp_standing": corp},
        "rates": {"sales_tax_fraction": 0.5, "broker_fee_fraction": 0.5}}})


def _sale(app_session, *, market_fees, journal=None):
    app_session.add(CharacterModel(character_id=1, character_name="Fake Trader", market_fees=market_fees))
    if journal is not None:
        app_session.add(CharacterWalletJournalModel(
            character_id=1, wallet_journal_id=300, amount=journal[0], balance=0.0, date="2026-01-02T00:00:00Z",
            description="Sale", ref_type="market_transaction", tax=journal[1]))
    app_session.add(CharacterWalletTransactionsModel(
        character_id=1, transaction_id=401, client_name="Fake Buyer", date="2026-01-02T00:00:00Z", is_buy=False,
        is_personal=True, journal_ref_id=300 if journal is not None else None, quantity=10, type_id=101,
        type_name="Fake Item", type_group_name="Ammo", type_category_name="Charge", unit_price=100.0,
        total_price=1000.0))
    app_session.commit()


def _rebuild(market_fees, *, admin_settings=None, journal=None):
    app_session, sde_session = _sessions()
    _sale(app_session, market_fees=market_fees, journal=journal)
    service = CharacterRealizedProfitLedgerService(app_session=app_session, sde_session=sde_session,
                                                   market_prices=[], admin_settings=admin_settings)
    rows = service.rebuild(character_id=1)
    assert len(rows) == 1
    return rows[0]


def _manager(tmp_path, industry=None) -> AdminSettingsManager:
    mgr = AdminSettingsManager(tmp_path / "admin_settings.json")
    if industry:
        mgr.set_bulk({"industry": industry})
    return mgr


def test_the_fees_come_from_the_stored_skills_and_the_current_settings(tmp_path):
    mgr = _manager(tmp_path, {"base_sales_tax_rate": 0.05, "broker_fee_base": 0.04})
    row = _rebuild(_stored_fees(), admin_settings=mgr)
    tax = 0.05 * (1 - 0.11 * 4)
    broker = 0.04 - 0.003 * 5 - 0.0003 * 2.0 - 0.0002 * 1.0
    assert row["sales_tax_amount"] == pytest.approx(1000.0 * tax)
    assert row["other_fees_amount"] == pytest.approx(1000.0 * broker)
    assert row["net_revenue"] == pytest.approx(1000.0 * (1 - tax - broker))
    assert row["fee_capture_mode"] == "estimated_market_fees"
    assert not any("assumed" in note.lower() for note in row["notes"])


def test_a_settings_change_applies_at_the_next_rebuild(tmp_path):
    mgr = _manager(tmp_path)
    first = _rebuild(_stored_fees(), admin_settings=mgr)
    mgr.set("industry", "base_sales_tax_rate", 0.02)
    second = _rebuild(_stored_fees(), admin_settings=mgr)
    assert first["sales_tax_amount"] == pytest.approx(1000.0 * 0.075 * 0.56)
    assert second["sales_tax_amount"] == pytest.approx(1000.0 * 0.02 * 0.56)


def test_without_a_settings_store_the_schema_defaults_are_used():
    row = _rebuild(_stored_fees())
    assert row["sales_tax_amount"] == pytest.approx(1000.0 * 0.075 * 0.56)


@pytest.mark.parametrize("market_fees, why", [
    (json.dumps({"jita_4_4": {"rates": {"sales_tax_fraction": None, "broker_fee_fraction": None},
                              "unknown_reason": "the character's skills are not loaded"}}), "skills are not loaded"),
    (None, "no stored market fees"),
    (json.dumps({"jita_4_4": {"rates": {"sales_tax_fraction": 0.02, "broker_fee_fraction": 0.01}}}), "lack its skills"),
    ("{not json", "unreadable"),
])
def test_unknown_skills_give_the_highest_fees_with_the_reason(market_fees, why):
    row = _rebuild(market_fees)
    assert row["sales_tax_amount"] == pytest.approx(75.0)      # 7.5%: no Accounting
    assert row["other_fees_amount"] == pytest.approx(30.0)     # 3%: no Broker Relations, no standings
    assert row["fee_capture_mode"] == "assumed_market_fees"
    reasons = [note for note in row["notes"] if note.startswith("Fees assumed")]
    assert len(reasons) == 1 and why in reasons[0] and "7.50%" in reasons[0] and "3.00%" in reasons[0]


def test_with_a_journal_only_the_broker_fee_is_assumed():
    row = _rebuild(None, journal=(960.0, 20.0))
    assert row["sales_tax_amount"] == pytest.approx(20.0)       # the journal's tax
    assert row["other_fees_amount"] == pytest.approx(20.0 + 30.0)
    assert row["fee_capture_mode"] == "journal_plus_assumed_broker_fee"
    assert any(note.startswith("Fees assumed") and "no stored market fees" in note for note in row["notes"])


def test_an_invalid_fee_setting_fails_the_rebuild_naming_it(tmp_path):
    path = tmp_path / "admin_settings.json"
    path.write_text(json.dumps({"industry": {"base_sales_tax_rate": 0.9}}), encoding="utf-8")
    with pytest.raises(AdminSettingInvalid, match="industry/base_sales_tax_rate"):
        _rebuild(_stored_fees(), admin_settings=AdminSettingsManager(path))


def test_the_character_service_passes_the_settings(tmp_path):
    from eve_online_industry_tracker.application.characters.service import CharactersService

    app_session, sde_session = _sessions()
    _sale(app_session, market_fees=_stored_fees())
    state = SimpleNamespace(
        char_manager=_KNOWN_CHARACTERS,
        admin_settings=_manager(tmp_path, {"base_sales_tax_rate": 0.05}),
        db_app=SimpleNamespace(session=app_session), db_sde=SimpleNamespace(session=sde_session),
        esi_service=SimpleNamespace(get_market_prices=lambda: []),
    )
    rows = CharactersService(state=state).get_realized_profit_ledger(character_id=1)["rows"]
    assert rows[0]["sales_tax_amount"] == pytest.approx(1000.0 * 0.05 * 0.56)


# --- the realized-profit page -------------------------------------------------------------------

def test_the_page_names_the_assumed_fees_per_character():
    from streamlit_ui.pages.realized_profit import _assumed_fee_warnings

    note = "Fees assumed at 7.50% sales tax and 3.00% broker fee (no Accounting, ...): skills not loaded"
    rows = [
        {"character_id": 1, "fee_capture_mode": "assumed_market_fees", "notes": ["x", note]},
        {"character_id": 1, "fee_capture_mode": "journal_plus_assumed_broker_fee", "notes": [note]},
        {"character_id": 2, "fee_capture_mode": "estimated_market_fees", "notes": ["Sales tax was estimated"]},
        {"corporation_id": 9, "fee_capture_mode": "journal_matched", "notes": []},
    ]
    warnings = _assumed_fee_warnings(rows, {1: "Fake Trader", 2: "Other"})
    assert warnings == [f"Fake Trader: 2 sale(s) with assumed fees. {note}"]
    assert _assumed_fee_warnings(rows[2:], {}) == []


# --- review round 1: M4 (assumed fees are Low confidence) and I2 (stale persisted fees) ----------

def test_assumed_fees_are_low_confidence():
    from eve_online_industry_tracker.application.characters.realized_profit import _confidence

    assert _rebuild(None)["confidence"] == "Low"
    # A fully priced sale: Medium with the character's own fee estimate, Low with assumed fees.
    assert _confidence(priced_quantity=10, unpriced_quantity=0, fee_capture_mode="estimated_market_fees") == "Medium"
    for mode in ("assumed_market_fees", "journal_plus_assumed_broker_fee"):
        assert _confidence(priced_quantity=10, unpriced_quantity=0, fee_capture_mode=mode) == "Low"


def _ledger(app_session, sde_session, admin_settings=None):
    return CharacterRealizedProfitLedgerService(app_session=app_session, sde_session=sde_session,
                                                market_prices=[], admin_settings=admin_settings)


def _persisted_sale(tmp_path, market_fees=None, industry=None):
    app_session, sde_session = _sessions()
    _sale(app_session, market_fees=_stored_fees() if market_fees is None else market_fees)
    mgr = _manager(tmp_path, industry)
    _ledger(app_session, sde_session, mgr).rebuild(character_id=1)
    return app_session, sde_session, mgr


def test_each_row_carries_its_fee_inputs_and_is_fresh_while_they_hold(tmp_path):
    app_session, sde_session, mgr = _persisted_sale(tmp_path)
    ledger = _ledger(app_session, sde_session, mgr)
    rows = ledger.list_rows(character_id=1)
    assert rows[0]["fee_inputs"]
    assert ledger.stale_character_ids(rows) == set()


def test_a_settings_change_makes_the_rows_stale(tmp_path):
    app_session, sde_session, mgr = _persisted_sale(tmp_path)
    mgr.set("industry", "broker_fee_base", 0.05)
    ledger = _ledger(app_session, sde_session, mgr)
    assert ledger.stale_character_ids(ledger.list_rows(character_id=1)) == {1}


def test_a_skills_or_standings_change_makes_the_rows_stale(tmp_path):
    app_session, sde_session, mgr = _persisted_sale(tmp_path)
    character = app_session.query(CharacterModel).filter_by(character_id=1).one()
    character.market_fees = _stored_fees(accounting=5)
    app_session.commit()
    ledger = _ledger(app_session, sde_session, mgr)
    assert ledger.stale_character_ids(ledger.list_rows(character_id=1)) == {1}


def test_a_row_without_fee_inputs_is_stale(tmp_path):
    app_session, sde_session, mgr = _persisted_sale(tmp_path)
    from eve_online_industry_tracker.infrastructure.models import CharacterRealizedSalesLedgerModel

    app_session.query(CharacterRealizedSalesLedgerModel).update({"fee_inputs": None})
    app_session.commit()
    ledger = _ledger(app_session, sde_session, mgr)
    assert ledger.stale_character_ids(ledger.list_rows(character_id=1)) == {1}


def test_reading_the_ledger_rebuilds_a_stale_character(tmp_path):
    from eve_online_industry_tracker.application.characters.service import CharactersService

    app_session, sde_session, mgr = _persisted_sale(tmp_path)
    state = SimpleNamespace(char_manager=_KNOWN_CHARACTERS, admin_settings=mgr, db_app=SimpleNamespace(session=app_session),
                            db_sde=SimpleNamespace(session=sde_session),
                            esi_service=SimpleNamespace(get_market_prices=lambda: []))
    before = CharactersService(state=state).get_realized_profit_ledger(character_id=1)["rows"]
    assert before[0]["sales_tax_amount"] == pytest.approx(1000.0 * 0.075 * 0.56)
    mgr.set("industry", "base_sales_tax_rate", 0.05)
    after = CharactersService(state=state).get_realized_profit_ledger(character_id=1)["rows"]
    assert after[0]["sales_tax_amount"] == pytest.approx(1000.0 * 0.05 * 0.56)
    ledger = _ledger(app_session, sde_session, mgr)
    assert ledger.stale_character_ids(ledger.list_rows(character_id=1)) == set()


def test_the_fee_inputs_column_is_migrated():
    import inspect

    from eve_online_industry_tracker.infrastructure import schema_migrations

    source = inspect.getsource(schema_migrations.ensure_app_schema)
    assert 'table="character_realized_sales_ledger", column="fee_inputs"' in source


def test_concurrent_reads_rebuild_a_stale_character_once(monkeypatch):
    """Two requests that both find character 1 stale must not both rebuild
    it: two rebuilds race on UNIQUE(character_id, transaction_id). The
    rebuild runs under a per-character lock and re-checks staleness inside."""
    import threading
    import time
    from types import SimpleNamespace

    from eve_online_industry_tracker.application.characters import service as characters_service

    barrier = threading.Barrier(2, timeout=5)
    calls = {"stale": 0}
    rebuilt: list[int] = []
    stale = {1}
    guard = threading.Lock()

    class _Ledger:
        def __init__(self, **_):
            pass

        def list_rows(self, character_id=None):
            return [{"character_id": 1}]

        def stale_character_ids(self, rows):
            with guard:
                calls["stale"] += 1
                first_two = calls["stale"] <= 2
            if first_two:
                barrier.wait()   # both requests see the character stale
            return set(stale)

        def stale_resolved_cost_owner_ids(self, rows):
            return set()

        def rebuild(self, character_id=None):
            time.sleep(0.05)
            rebuilt.append(character_id)
            stale.discard(character_id)
            return []

    monkeypatch.setattr(characters_service, "CharacterRealizedProfitLedgerService", _Ledger)
    monkeypatch.setattr(characters_service, "summarize_realized_profit_rows", lambda rows: {})
    state = SimpleNamespace(
        char_manager=_KNOWN_CHARACTERS,
        esi_service=SimpleNamespace(get_market_prices=lambda: []),
        db_app=SimpleNamespace(session=None), db_sde=SimpleNamespace(session=None),
        admin_settings=None,
    )
    svc = characters_service.CharactersService(state=state)
    errors: list[BaseException] = []

    def _read():
        try:
            svc.get_realized_profit_ledger(character_id=1)
        except BaseException as exc:  # surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=_read) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert not errors
    assert rebuilt == [1]
