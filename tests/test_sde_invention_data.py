"""SDE invention data: the T2 decryptors and the encryption skills.

The in-memory tests build only the rows they need. The pin test reads the
local SDE (public game data, read-only) and skips when it is not present."""
from __future__ import annotations

import json
import logging
import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from eve_online_industry_tracker.application.industry.invention_model import NO_DECRYPTOR, decryptor_options_from_rows
from eve_online_industry_tracker.infrastructure.models import Groups, Types
from eve_online_industry_tracker.infrastructure.sde.decryptors import get_t2_invention_decryptors

_LOCAL_SDE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "database", "eve_sde.db"))


_ACCELERANT_DOGMA = {1112: 1.2, 1113: 2.0, 1114: 10.0, 1124: 1.0}


def _dogma(attrs):
    """typeDogma.dogmaAttributes JSON for {attributeID: value}."""
    return json.dumps([{"attributeID": a, "value": v} for a, v in attrs.items()])


def _seed(sde_engine, *, dogma_by_type_id, types):
    """typeDogma rows ({type_id: raw JSON text}) and group-1304 types ({type_id: name})."""
    with sde_engine.begin() as conn:
        conn.execute(text("CREATE TABLE typeDogma (id BIGINT, dogmaAttributes TEXT, dogmaEffects TEXT)"))
        for type_id, raw in dogma_by_type_id.items():
            conn.execute(text("INSERT INTO typeDogma VALUES (:id, :attrs, '[]')"), {"id": type_id, "attrs": raw})
    with Session(sde_engine) as session:
        session.add(Groups(id=1304, name={"en": "Generic Decryptor"}, categoryID=35, published=True))
        for type_id, name in types.items():
            session.add(Types(id=type_id, name={"en": name}, groupID=1304, published=True))
        session.commit()


def test_t2_decryptors_are_read_from_type_dogma_of_group_1304_only(sde_engine):
    with sde_engine.begin() as conn:
        conn.execute(text("CREATE TABLE typeDogma (id BIGINT, dogmaAttributes TEXT, dogmaEffects TEXT)"))
        for type_id in (34201, 23179):
            conn.execute(
                text("INSERT INTO typeDogma VALUES (:id, :attrs, '[]')"),
                {"id": type_id, "attrs": _dogma(_ACCELERANT_DOGMA)},
            )
    with Session(sde_engine) as session:
        session.add(Groups(id=1304, name={"en": "Generic Decryptor"}, categoryID=35, published=True))
        session.add(Types(id=34201, name={"en": "Accelerant Decryptor"}, groupID=1304, published=True))
        # A racial decryptor group with the same dogma: not a T2 invention decryptor.
        session.add(Types(id=23179, name={"en": "Occult Accelerant"}, groupID=728, published=True))
        session.commit()
        rows = get_t2_invention_decryptors(session, language="en")
    assert rows == [{
        "type_id": 34201, "type_name": "Accelerant Decryptor",
        "invention_probability_multiplier": 1.2, "invention_me_modifier": 2,
        "invention_te_modifier": 10, "invention_max_run_modifier": 1,
    }]


@pytest.mark.skipif(not os.path.exists(_LOCAL_SDE), reason="local SDE database/eve_sde.db not present")
def test_the_local_sde_has_the_eight_generic_decryptors():
    engine = create_engine(f"sqlite:///file:{_LOCAL_SDE}?mode=ro&uri=true")
    try:
        with Session(engine) as session:
            rows = {r["type_id"]: r for r in get_t2_invention_decryptors(session, language="en")}
    finally:
        engine.dispose()
    assert sorted(rows) == list(range(34201, 34209))
    accelerant = rows[34201]
    assert (accelerant["invention_probability_multiplier"], accelerant["invention_me_modifier"],
            accelerant["invention_te_modifier"], accelerant["invention_max_run_modifier"]) == (1.2, 2, 10, 1)


def test_a_decryptor_without_a_success_multiplier_is_skipped_with_a_warning(sde_engine, caplog):
    # 1113/1114/1124 omitted = a real 0; a missing 1112 is never guessed as 1.0.
    _seed(sde_engine, dogma_by_type_id={34201: _dogma(_ACCELERANT_DOGMA), 34206: _dogma({1113: 1.0})},
          types={34201: "Accelerant Decryptor", 34206: "Symmetry Decryptor"})
    with Session(sde_engine) as session:
        rows = get_t2_invention_decryptors(session, language="en")
    by_id = {r["type_id"]: r for r in rows}
    assert by_id[34206]["invention_probability_multiplier"] is None
    assert (by_id[34206]["invention_te_modifier"], by_id[34206]["invention_max_run_modifier"]) == (0, 0)

    with caplog.at_level(logging.WARNING):
        options = decryptor_options_from_rows(rows)
    assert [o.type_id for o in options] == [None, 34201]
    assert options[0] is NO_DECRYPTOR
    assert any("Symmetry Decryptor" in r.getMessage() or "34206" in r.getMessage()
               for r in caplog.records if r.levelname == "WARNING")


def test_a_decryptor_with_missing_or_corrupt_dogma_is_left_out_with_a_warning(sde_engine, caplog):
    _seed(sde_engine,
          dogma_by_type_id={34201: _dogma(_ACCELERANT_DOGMA), 34202: "{not json", 34203: _dogma({9999: 1.0})},
          types={34201: "Accelerant Decryptor", 34202: "Attainment Decryptor",
                 34203: "Augmentation Decryptor", 34204: "Parity Decryptor"})  # 34204: no typeDogma row
    with caplog.at_level(logging.WARNING):
        with Session(sde_engine) as session:
            rows = get_t2_invention_decryptors(session, language="en")
    assert [r["type_id"] for r in rows] == [34201]
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    for type_id in (34202, 34203, 34204):
        assert sum(str(type_id) in m for m in warnings) == 1, (type_id, warnings)


def test_a_decryptor_query_error_propagates(sde_engine):
    # No typeDogma table: the loader raises instead of returning [] silently.
    with Session(sde_engine) as session:
        session.add(Groups(id=1304, name={"en": "Generic Decryptor"}, categoryID=35, published=True))
        session.add(Types(id=34201, name={"en": "Accelerant Decryptor"}, groupID=1304, published=True))
        session.commit()
        with pytest.raises(Exception, match="typeDogma"):
            get_t2_invention_decryptors(session, language="en")
