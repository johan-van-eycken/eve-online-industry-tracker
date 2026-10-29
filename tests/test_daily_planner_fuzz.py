"""A bounded property test over random small plans, and seed 380 pinned.

Seed 380: a row took the higher rows' leftover T1 BPCs, then invented
nothing and released them to the shared pool a second time, so a lower row
got two invent lanes on BPC 701. The pool stays unique by key now.

Properties, per plan: no blueprint item is in two invent actions, a pilot's
own BPC is invented from by that pilot only, no more copies than T1 BPOs,
no pilot takes more slots than it has free, and no placeholder leaks into a
reason. Fake ids and pilots throughout.
"""
from __future__ import annotations

import logging
import random
from collections import Counter

import pytest

import test_blueprint_ledger as L  # noqa: E402
import test_daily_planner_assigned_inventor as A  # noqa: E402
import test_daily_planner_character_blueprints as C  # noqa: E402
import test_daily_planner_release_replan as T  # noqa: E402

PILOTS = (1, 2, 3)
_MFG = {"manufacture", "sub_manufacture"}
_RESEARCH = {"invent", "copy", "me_research", "te_research"}


def _scenario(seed):
    """The probe's generator, unchanged: 0-1 T1 row and 2-4 T2 rows of one
    T1 source, 0-2 T1 BPOs and 0-3 T1 BPCs (some a pilot's own), three
    pilots with random skills and busy slots."""
    rnd = random.Random(seed)
    encs = [A.ENCRYPTION, T.OTHER_ENCRYPTION]
    rows = []
    if rnd.random() < .6:
        rows.append(L._dec(L._t1_row(20), 12344, "Widget I", rnd.uniform(0, 10), 1))
    for i in range(rnd.randint(2, 4)):
        rows.append(T._planned_t2(12345 + i, f"W{i}", rnd.uniform(0, 10), rnd.choice(encs)))
    bpos = {998: [(C._owned_bpo(70 + i, 998, owner=rnd.choice(PILOTS)) if rnd.random() < .4 else L._bpo(70 + i, 998))
                  for i in range(rnd.randint(0, 2))]}
    bpcs = {998: [(C._owned_bpc(700 + i, 998, rnd.choice([3, 10]), owner=rnd.choice(PILOTS)) if rnd.random() < .4
                   else L._bpc(700 + i, 998, rnd.choice([3, 10]))) for i in range(rnd.randint(0, 3))]}
    levels = {}
    pilots = []
    for p in PILOTS:
        enc, lab, mass = rnd.choice(encs), rnd.randint(0, 1), rnd.randint(0, 1)
        levels[p] = (lab, mass)
        pilots.append(T._pilot(p, f"P{p}", enc, lab=lab, mass=mass))
    jobs = [T._busy(p, rnd.choice([1, 8])) for p in PILOTS if rnd.random() < .5]
    return rows, bpos, bpcs, pilots, jobs, levels


def _run(seed):
    rows, bpos, bpcs, pilots, jobs, levels = _scenario(seed)
    logging.disable(logging.CRITICAL)
    try:
        _plan, actions = T._plan_and_assign(rows, bpos, bpcs, pilots, jobs)
    finally:
        logging.disable(logging.NOTSET)
    return rows, bpos, bpcs, jobs, levels, actions


def _violations(seed):
    rows, bpos, bpcs, jobs, levels, actions = _run(seed)
    found = []
    invented = Counter(a.blueprint_item_id for a in actions if a.action_type == "invent" and a.blueprint_item_id)
    found += [f"BPC {item} in {n} invent actions" for item, n in invented.items() if n > 1]
    owners = {b.item_id: getattr(b, "character_id", None) for b in bpcs[998] + bpos[998]}
    found += [
        f"BPC {a.blueprint_item_id} of pilot {owners[a.blueprint_item_id]} invented by {a.character_id}"
        for a in actions if a.action_type == "invent" and owners.get(a.blueprint_item_id) not in (None, a.character_id)
    ]
    copies = sum(1 for a in actions if a.action_type == "copy")
    if copies > len(bpos[998]):
        found.append(f"{copies} copies from {len(bpos[998])} T1 BPO(s)")
    busy = Counter((j["character_id"], j["activity_id"]) for j in jobs)
    for p in PILOTS:
        lab, mass = levels[p]
        mine = [a for a in actions if a.character_id == p and not a.queued_after_me]
        mfg = sum(1 for a in mine if a.action_type in _MFG)
        research = sum(1 for a in mine if a.action_type in _RESEARCH)
        if mfg > 1 + mass - busy[(p, 1)]:
            found.append(f"pilot {p}: {mfg} manufacturing jobs")
        if research > 1 + lab - busy[(p, 8)]:
            found.append(f"pilot {p}: {research} research jobs")
    found += [f"placeholder in {r.type_id}'s reason" for r in rows if "\x00" in (r.decision_reason or "")]
    return found


def test_seed_380_a_pooled_bpc_gets_one_lane_only():
    _rows, _bpos, _bpcs, _jobs, _levels, actions = _run(380)
    invents = [(a.type_id, a.blueprint_item_id) for a in actions if a.action_type == "invent"]
    assert len(invents) == len(set(item for _t, item in invents))
    assert _violations(380) == []


@pytest.mark.parametrize("block", range(4))
def test_random_plans_never_double_book_or_overfill_slots(block):
    """200 seeds in four blocks of 50 (fast: small fake plans)."""
    bad = {seed: v for seed in range(block * 50, block * 50 + 50) if (v := _violations(seed))}
    assert bad == {}


def test_the_final_reword_books_the_bpcs_lower_rows_took_from_the_pool(monkeypatch):
    """W0 and W1 each hold one T1 BPC; W2 is not built. Nobody can run
    W0's invention, so its BPC goes to the pool and W1 (two lanes) invents
    on both. The final reword books that take first, so W2 names W1 as the
    only holder (with no re-plan pass at all, the final call alone)."""
    from eve_online_industry_tracker.application.daily_planner import character_assigner as ca

    monkeypatch.setattr(ca, "MAX_REPLAN_PASSES", 0)
    w0 = T._planned_t2(12345, "W0", 3.0, A.ENCRYPTION)
    w1 = T._planned_t2(12346, "W1", 2.0, T.OTHER_ENCRYPTION)
    w2 = T._planned_t2(12347, "W2", 1.0, T.OTHER_ENCRYPTION)
    _plan, actions = T._plan_and_assign([w0, w1, w2], {}, {998: [L._bpc(701, 998, 10), L._bpc(702, 998, 10)]},
                                        [T._pilot(2, "Lo", T.OTHER_ENCRYPTION, lab=1)])
    assert sorted((a.type_id, a.blueprint_item_id) for a in actions) == [(12346, 701), (12346, 702)]
    assert ("not built: T1 BPCs (998) used by invention for W1 (12346) today and no T1 BPO to copy"
            ) in w2.decision_reason
    assert "W0 (12345)" not in w2.decision_reason
