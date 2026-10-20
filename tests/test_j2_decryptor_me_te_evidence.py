"""J2.2: the ME/TE of the owner's T2 BPCs as decryptor evidence.

An invented BPC has ME 2 / TE 4 plus its decryptor's modifiers, and a copy
cannot be researched, so a BPC of the invented blueprint first seen after an
invention job finished, with a run count a candidate can give, names the
decryptor when no other of the owner's invention jobs can have made it. A
tie is settled only when that evidence leaves exactly one candidate; a BPC
whose ME/TE fits no candidate (a bought BPC) gives no evidence, with a
reason. Fake ids and prices only.
"""
from __future__ import annotations

import pytest

from eve_online_industry_tracker.application.characters import asset_provenance as ap
from eve_online_industry_tracker.application.characters import character as character_module
from eve_online_industry_tracker.application.characters.asset_provenance import (
    BlueprintCopySighting,
    load_owner_invention_evidence,
)
from eve_online_industry_tracker.application.characters.character import Character
from eve_online_industry_tracker.application.industry.invention_model import NO_DECRYPTOR, DecryptorOption
from eve_online_industry_tracker.infrastructure.models import (
    CharacterAssetHistoryModel,
    CharacterIndustryJobsModel,
    CorporationAssetHistoryModel,
    CorporationIndustryJobsModel,
)
from test_realized_invention_actual_jobs import (  # noqa: F401  (the sde fixture)
    _PER_ATTEMPT,
    _PLUS_1,
    _PLUS_2A,
    _PLUS_2B,
    _PRICES,
    _TARGET_BP,
    _chance,
    _job,
    _mfg,
    _refresh,
    sde,
)

#: Same success multiplier as no decryptor, a different ME/TE (Symmetry on the real SDE).
_SYM = DecryptorOption(type_id=990044, type_name="Fake Symmetry", probability_multiplier=1.0,
                       me_modifier=1, te_modifier=8, max_run_modifier=2)
_OPTIONS = (NO_DECRYPTOR, _PLUS_1, _PLUS_2A, _SYM)
_PRICES_SYM = {**_PRICES, _SYM.type_id: 600.0}


def _bpc(item_id=77, *, me=3, te=12, runs=12, seen="2026-10-02T00:00:00Z", blueprint=_TARGET_BP):
    return BlueprintCopySighting(item_id=item_id, blueprint_type_id=blueprint, material_efficiency=me,
                                 time_efficiency=te, runs=runs, first_seen_at=seen)


def _sources(sde, jobs, bpcs, *, options=_OPTIONS, known=None):
    return ap.build_invention_cost_per_run_by_blueprint_type(
        jobs=jobs, sde_session=sde, market_price_map=dict(_PRICES_SYM), invention_refresh=_refresh(options),
        blueprint_copies=bpcs, known_invention_jobs=known,
    )[_TARGET_BP]


def test_the_invented_me_te_of_a_bpc_seen_after_the_job_settles_the_tie(sde):
    source = _sources(sde, [_job()], [_bpc(me=3, te=12, runs=12)])
    assert source["unit_cost_per_run"] == pytest.approx((10 * (_PER_ATTEMPT + 600.0) + 100.0) / (4 * 12))
    assert source["decryptor_from_me_te_evidence_job_ids"] == [1]
    assert source["decryptor_from_runs_evidence_job_ids"] == []


def test_a_base_me_te_bpc_settles_it_as_no_decryptor(sde):
    source = _sources(sde, [_job()], [_bpc(me=2, te=4, runs=7)])
    assert source["unit_cost_per_run"] == pytest.approx((10 * _PER_ATTEMPT + 100.0) / (4 * 10))
    assert source["decryptor_from_me_te_evidence_job_ids"] == [1]


def test_a_bpc_seen_before_the_job_finished_is_no_evidence(sde):
    source = _sources(sde, [_job()], [_bpc(seen="2026-09-30T00:00:00Z")])
    assert source.get("unit_cost_per_run") is None
    assert "no ME/TE evidence" in source["unknown_reason"]


def test_a_bpc_whose_me_te_fits_no_candidate_gives_no_evidence_with_a_reason(sde):
    source = _sources(sde, [_job()], [_bpc(me=10, te=20, runs=5)])
    assert source.get("unit_cost_per_run") is None
    assert "fits no candidate" in source["unknown_reason"]
    assert "ME 10 / TE 20" in source["unknown_reason"]


def test_a_bpc_with_more_runs_than_its_me_te_candidate_gives_is_no_evidence(sde):
    source = _sources(sde, [_job()], [_bpc(me=3, te=12, runs=13)])
    assert source.get("unit_cost_per_run") is None
    assert "fits no candidate" in source["unknown_reason"]


def test_another_bpc_type_or_owner_is_not_considered(sde):
    source = _sources(sde, [_job()], [_bpc(blueprint=990099)])
    assert source.get("unit_cost_per_run") is None


def test_a_bpc_another_invention_job_can_have_made_is_no_evidence(sde):
    """Job 5 (a tie too) finished before the BPC was seen and can have made it."""
    other = _job(5, completed_date="2026-09-25T00:00:00Z")
    source = _sources(sde, [_job(), other], [_bpc()])
    assert source.get("unit_cost_per_run") is None
    assert "another invention job" in source["unknown_reason"]


def test_a_bpc_another_job_cannot_have_made_by_its_me_te_is_still_evidence(sde):
    """Job 5 used Fake +2 A (read from its chance: ME 2 / TE 4), so it cannot
    have made an ME 3 / TE 12 copy."""
    other = _job(5, probability=_chance(_PLUS_2A), completed_date="2026-09-25T00:00:00Z")
    source = _sources(sde, [_job(), other], [_bpc()])
    assert source["decryptor_from_me_te_evidence_job_ids"] == [1]
    assert source["jobs"] == 2


def test_a_stored_invention_job_outside_the_esi_list_counts_as_a_possible_maker(sde):
    stored = _job(5, completed_date="2026-09-25T00:00:00Z")
    source = _sources(sde, [_job()], [_bpc()], known=[{**stored, "raw": dict(stored)}])
    assert source.get("unit_cost_per_run") is None
    assert "another invention job" in source["unknown_reason"]


def test_bpcs_naming_different_candidates_settle_nothing(sde):
    source = _sources(sde, [_job()], [_bpc(77, me=3, te=12, runs=12), _bpc(78, me=2, te=4, runs=10)])
    assert source.get("unit_cost_per_run") is None
    assert "contradict" in source["unknown_reason"]


def test_more_fitting_bpcs_than_the_jobs_successes_are_no_evidence(sde):
    """Four successes make at most four BPCs: a fifth that fits says some
    came from elsewhere (bought, an unstored job), so none is evidence."""
    bpcs = [_bpc(77 + i) for i in range(5)]
    source = _sources(sde, [_job(successful_runs=4)], bpcs)
    assert source.get("unit_cost_per_run") is None
    assert "more than the job's 4 successful run(s)" in source["unknown_reason"]
    assert _sources(sde, [_job(successful_runs=4)], bpcs[:4])["decryptor_from_me_te_evidence_job_ids"] == [1]


def test_candidates_with_the_same_me_te_are_not_settled_by_it(sde):
    """No decryptor and Fake +2 B both give ME 2 / TE 4 (Parity and
    Optimized Attainment on the real SDE)."""
    source = _sources(sde, [_job()], [_bpc(me=2, te=4, runs=9)], options=(NO_DECRYPTOR, _PLUS_1, _PLUS_2A, _PLUS_2B))
    assert source.get("unit_cost_per_run") is None
    assert "fits several" in source["unknown_reason"]


def test_runs_and_me_te_evidence_that_disagree_settle_nothing(sde):
    """A 12-run BPC rules out no decryptor (runs evidence: Symmetry); an
    ME 2 / TE 4 copy says no decryptor."""
    source = _sources(sde, [_job(), _mfg(2, blueprint_id=88, runs=12)], [_bpc(77, me=2, te=4, runs=10)])
    assert source.get("unit_cost_per_run") is None
    assert "disagree" in source["unknown_reason"]


def test_runs_and_me_te_evidence_that_agree_settle_it(sde):
    source = _sources(sde, [_job(), _mfg(2, blueprint_id=88, runs=12)], [_bpc(77, me=3, te=12, runs=12)])
    assert source["unit_cost_per_run"] is not None
    assert source["decryptor_from_runs_evidence_job_ids"] == [1]


def test_without_bpc_data_the_runs_only_behaviour_is_kept(sde):
    source = _sources(sde, [_job()], None)
    assert source.get("unit_cost_per_run") is None
    assert "no runs evidence" in source["unknown_reason"]


# --- the loader ------------------------------------------------------------------


def _hist(model, owner_field, owner, item, *, observed, me=3, te=12, runs=12, type_id=_TARGET_BP, copy=True,
          source="asset_refresh"):
    return model(**{owner_field: owner}, item_id=item, observed_at=observed, snapshot_source=source,
                 type_id=type_id, quantity=1, is_blueprint_copy=copy, blueprint_runs=runs,
                 blueprint_material_efficiency=me, blueprint_time_efficiency=te)


def test_the_loader_reads_the_owners_bpc_sightings(app_session):
    H, f = CharacterAssetHistoryModel, "character_id"
    app_session.add_all([
        _hist(H, f, 7001, 77, observed="2026-10-03T00:00:00+00:00", runs=12),
        _hist(H, f, 7001, 77, observed="2026-10-05T00:00:00+00:00", runs=8),   # used since: runs go down
        _hist(H, f, 7001, 78, observed="2026-10-04T00:00:00+00:00", me=2, te=4, runs=10),
        _hist(H, f, 7001, 79, observed="2026-10-04T00:00:00+00:00", copy=False),          # an original
        _hist(H, f, 7001, 80, observed="2026-10-04T00:00:00+00:00", source="historical_backfill"),
        _hist(H, f, 7002, 81, observed="2026-10-04T00:00:00+00:00"),                      # another owner
        _hist(H, f, 7001, 82, observed="2026-10-04T00:00:00+00:00", me=3),
        _hist(H, f, 7001, 82, observed="2026-10-06T00:00:00+00:00", me=4),               # inconsistent: dropped
    ])
    app_session.add(CharacterIndustryJobsModel(
        character_id=7001, job_id=5, activity_id=8, product_type_id=_TARGET_BP, raw=_job(5)))
    app_session.add(CharacterIndustryJobsModel(
        character_id=7001, job_id=6, activity_id=1, product_type_id=990003, raw={"job_id": 6}))
    # Another owner's invention job: its BPC may have been moved here.
    app_session.add(CorporationIndustryJobsModel(
        corporation_id=98000001, job_id=7, activity_id=8, product_type_id=_TARGET_BP, raw=_job(7)))
    app_session.commit()

    evidence = load_owner_invention_evidence(app_session, owner_kind="character", owner_id=7001)

    bpcs = sorted(evidence["blueprint_copies"], key=lambda b: b.item_id)
    assert [(b.item_id, b.material_efficiency, b.time_efficiency, b.runs, b.first_seen_at[:10]) for b in bpcs] == [
        (77, 3, 12, 12, "2026-10-03"), (78, 2, 4, 10, "2026-10-04"),
    ]
    assert sorted(j["job_id"] for j in evidence["known_invention_jobs"]) == [5, 7]


def test_the_loader_reads_corporation_history(app_session):
    app_session.add(_hist(CorporationAssetHistoryModel, "corporation_id", 98000001, 90,
                          observed="2026-10-03T00:00:00+00:00"))
    app_session.commit()
    evidence = load_owner_invention_evidence(app_session, owner_kind="corporation", owner_id=98000001)
    assert [b.item_id for b in evidence["blueprint_copies"]] == [90]


def test_a_bpc_moved_from_a_pilot_is_first_seen_in_the_pilots_history(app_session):
    app_session.add(_hist(CorporationAssetHistoryModel, "corporation_id", 98000001, 90,
                          observed="2026-10-03T00:00:00+00:00"))
    app_session.add(_hist(CharacterAssetHistoryModel, "character_id", 7001, 90,
                          observed="2026-09-20T00:00:00+00:00"))
    app_session.commit()
    evidence = load_owner_invention_evidence(app_session, owner_kind="corporation", owner_id=98000001)
    assert [b.first_seen_at[:10] for b in evidence["blueprint_copies"]] == ["2026-09-20"]


# --- the paths pass it -----------------------------------------------------------


class _Db:
    def __init__(self, session):
        self.session = session


class _Esi:
    def esi_get(self, endpoint, params=None, paginate=False):
        if endpoint.endswith("/industry/jobs"):
            return [_job()]
        if endpoint.endswith("/markets/prices"):
            return []
        raise AssertionError(endpoint)


def test_the_character_job_refresh_passes_the_owners_bpcs(app_session, monkeypatch):
    app_session.add(_hist(CharacterAssetHistoryModel, "character_id", 7001, 77, observed="2026-10-03T00:00:00+00:00"))
    app_session.commit()
    seen = {}

    def _capture(**kw):
        seen.update(kw)
        return {}

    monkeypatch.setattr(character_module, "build_invention_cost_per_run_by_blueprint_type", _capture)
    monkeypatch.setattr(character_module, "backfill_historical_market_costs", lambda **kw: None)
    character = object.__new__(Character)
    character.character_id = 7001
    character.character_name = "Inventor"
    character._db_app = _Db(app_session)
    character._db_sde = _Db(None)
    character._esi_client = _Esi()
    character._esi_service = None
    character.ensure_esi = lambda: None

    character.refresh_industry_jobs()

    assert [b.item_id for b in seen["blueprint_copies"]] == [77]
    assert "known_invention_jobs" in seen


def test_the_resnapshot_passes_the_owners_bpcs(app_session, monkeypatch):
    app_session.add(_hist(CharacterAssetHistoryModel, "character_id", 7001, 77, observed="2026-10-03T00:00:00+00:00"))
    app_session.add(CharacterIndustryJobsModel(
        character_id=7001, job_id=9, activity_id=1, blueprint_type_id=_TARGET_BP, product_type_id=990003,
        invention_cost_unknown_reason=ap.NO_DECRYPTOR_CHOICE_REASON, raw={"job_id": 9, "activity_id": 1}))
    app_session.commit()
    seen = {}

    def _capture(**kw):
        seen.update(kw)
        return {}

    monkeypatch.setattr(ap, "build_invention_cost_per_run_by_blueprint_type", _capture)
    monkeypatch.setattr(ap, "industry_job_material_type_ids", lambda **kw: [])
    monkeypatch.setattr(ap, "resolve_industry_job_cost_snapshot", lambda **kw: {"unit_cost": None})
    from eve_online_industry_tracker.application.characters import asset_history
    monkeypatch.setattr(asset_history, "build_historical_input_cost_lookup", lambda **kw: {})

    ap.resnapshot_jobs_without_decryptor_choice(
        app_session=app_session, sde_session=None, market_price_map={},
        invention_refresh=ap.RealizedInventionRefresh(inputs=None))

    assert [b.item_id for b in seen["blueprint_copies"]] == [77]


def test_the_corporation_job_refresh_passes_the_corporations_bpcs(app_session, monkeypatch):
    from types import SimpleNamespace

    from eve_online_industry_tracker.application.corporations import corporation as corporation_module
    from eve_online_industry_tracker.application.corporations.corporation import Corporation

    app_session.add(_hist(CorporationAssetHistoryModel, "corporation_id", 98000001, 90,
                          observed="2026-10-03T00:00:00+00:00"))
    app_session.commit()
    seen = {}

    def _capture(**kw):
        seen.update(kw)
        return {}

    monkeypatch.setattr(corporation_module, "build_invention_cost_per_run_by_blueprint_type", _capture)
    monkeypatch.setattr(corporation_module, "backfill_historical_market_costs", lambda **kw: None)
    corp = object.__new__(Corporation)
    corp.corporation_id = 98000001
    corp.corporation_name = "Fake Corp"
    corp._db_app = _Db(app_session)
    corp._db_sde = _Db(None)
    corp._default_esi_character = SimpleNamespace(ensure_esi=lambda: None, _esi_client=_Esi(), esi_service=None)

    corp.refresh_industry_jobs()

    assert [b.item_id for b in seen["blueprint_copies"]] == [90]
