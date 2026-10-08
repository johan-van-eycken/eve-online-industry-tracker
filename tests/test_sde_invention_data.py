"""SDE invention data: the T2 decryptors and the encryption skills.

The in-memory tests build only the rows they need. The pin test reads the
local SDE (public game data, read-only) and skips when it is not present."""
from __future__ import annotations

import json
import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from eve_online_industry_tracker.infrastructure.models import Groups, Types
from eve_online_industry_tracker.infrastructure.sde.decryptors import get_t2_invention_decryptors

_LOCAL_SDE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "database", "eve_sde.db"))


def _dogma(**attrs):
    return json.dumps([{"attributeID": a, "value": v} for a, v in attrs.items()])


def test_t2_decryptors_are_read_from_type_dogma_of_group_1304_only(sde_engine):
    with sde_engine.begin() as conn:
        conn.execute(text("CREATE TABLE typeDogma (id BIGINT, dogmaAttributes TEXT, dogmaEffects TEXT)"))
        for type_id in (34201, 23179):
            conn.execute(
                text("INSERT INTO typeDogma VALUES (:id, :attrs, '[]')"),
                {"id": type_id, "attrs": json.dumps([
                    {"attributeID": 1112, "value": 1.2}, {"attributeID": 1113, "value": 2.0},
                    {"attributeID": 1114, "value": 10.0}, {"attributeID": 1124, "value": 1.0},
                ])},
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
