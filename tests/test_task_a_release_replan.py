"""Task A items 2, 3 and 6: blueprints a row claimed but does not use today
go to the rows that wanted them, within the same compute.

3. A row with fewer invent lanes than claimed T1 BPCs releases the rest to
   the lower rows of the same T1 source.
2. A lower row ChainPlanner marked not built because a higher row held its
   T1 BPCs is re-planned when the assigner releases them.
6. A BPO claimed for a job that gets no slot is released, and the rows that
   wanted it are re-planned by the same pass.

Fake ids, pilots and numbers throughout.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import test_blueprint_ledger as L  # noqa: E402
import test_daily_planner_assigned_inventor as A  # noqa: E402

from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner  # noqa: E402
from eve_online_industry_tracker.application.daily_planner.models import ChainPlan  # noqa: E402

OTHER_ENCRYPTION = 99_001


def _pilot(character_id, name, encryption, *, lab=0, mass=0):
    skills = [A._skill("Science A", 3, A.SCIENCE_A), A._skill("Science B", 3, A.SCIENCE_B),
              A._skill("Encryption", 3, encryption)]
    if lab:
        skills.append(A._skill("Laboratory Operation", lab, 3406))
    if mass:
        skills.append(A._skill("Mass Production", mass, 3387))
    return {"character_id": character_id, "character_name": name, "skills": {"skills": skills}}


def _t2_row(type_id, encryption, bpcs):
    row = A._row(inventor=1, best_factor=1.0, decryptor=False, multiplier=1.0)
    row["type_id"] = type_id
    row["manufacturing_job"]["invention_chance"]["encryption_skill_type_id"] = encryption
    row.update({"t1_blueprint_type_id": 998, "t1_bpc_runs_by_item": bpcs,
                "t1_bpc_runs_on_hand": sum(r for _i, r in bpcs)})
    return row


def _assign(decisions, chars, jobs=()):
    plan = decisions if isinstance(decisions, ChainPlan) else ChainPlan(decisions=list(decisions))
    return CharacterAssigner().assign(plan, list(jobs), A._Chars(chars), A._Admin(), price_map=A.PRICES)


# --- item 3: fewer lanes than claimed BPCs -----------------------------------------

def test_bpcs_beyond_a_rows_lanes_go_to_a_lower_row_of_the_same_source():
    """The higher row has two 10-run BPCs but one lane (only Hi has its
    encryption skill, one slot): it invents on 701; 702 goes to the lower
    row, which Lo invents from."""
    high = A._decision(_t2_row(12345, A.ENCRYPTION, [[701, 10], [702, 10]]), type_id=12345, score=2.0)
    low = A._decision(_t2_row(12346, OTHER_ENCRYPTION, []), type_id=12346, score=1.0)
    actions = _assign([high, low], [_pilot(1, "Hi", A.ENCRYPTION), _pilot(2, "Lo", OTHER_ENCRYPTION)])
    assert [(a.action_type, a.type_id, a.character_id, a.blueprint_item_id, a.runs) for a in actions] == [
        ("invent", 12345, 1, 701, 7), ("invent", 12346, 2, 702, 6),
    ]
    assert "; fewer invent lanes than T1 BPCs today: its T1 BPC(s) 702 go to Widget II (12346)" in high.decision_reason


def test_bpcs_beyond_a_rows_lanes_nobody_takes_stay_unused_today():
    high = A._decision(_t2_row(12345, A.ENCRYPTION, [[701, 10], [702, 10]]), type_id=12345, score=2.0)
    actions = _assign([high], [_pilot(1, "Hi", A.ENCRYPTION)])
    assert [(a.action_type, a.blueprint_item_id) for a in actions] == [("invent", 701)]
    assert "; fewer invent lanes than T1 BPCs today: its T1 BPC(s) 702 stay unused today" in high.decision_reason


def test_a_row_that_uses_every_bpc_releases_nothing():
    high = A._decision(_t2_row(12345, A.ENCRYPTION, [[701, 10], [702, 10]]), type_id=12345, score=2.0)
    actions = _assign([high], [_pilot(1, "Hi", A.ENCRYPTION, lab=1)])
    assert sorted(a.blueprint_item_id for a in actions) == [701, 702]
    assert "fewer invent lanes" not in high.decision_reason
