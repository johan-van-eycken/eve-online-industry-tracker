from __future__ import annotations

import logging

from eve_online_industry_tracker.application.industry.type_metadata import TypeMetadataResolver

# Shapes mirror what a probe of the real SDE returned for get_type_data(session, "en", [987, 2488]):
#   987  -> type_name='Mammoth Blueprint', category_name='Blueprint', meta_group_id=None
#   2488 -> type_name='Warrior II',        category_name='Drone',     meta_group_id=2.0 (REAL column, a float)
# so the stub below intentionally includes both int and float meta_group_id values, plus a
# legitimate (non-error) None for a blueprint type.
TYPE_DATA = {
    12345: {"type_id": 12345, "meta_group_id": 2, "category_name": "Drone"},
    999: {"type_id": 999, "meta_group_id": 1, "category_name": "Blueprint"},
    777: {"type_id": 777, "meta_group_id": None, "category_name": ""},
    987: {"type_id": 987, "meta_group_id": None, "category_name": "Blueprint"},
    2488: {"type_id": 2488, "meta_group_id": 2.0, "category_name": "Drone"},
}


class _FakeLoader:
    def __init__(self):
        self.calls: list[list[int]] = []

    def __call__(self, session, language, type_ids):
        self.calls.append(sorted(type_ids))
        return {tid: TYPE_DATA[tid] for tid in type_ids if tid in TYPE_DATA}


def _resolver(loader):
    return TypeMetadataResolver(sde_session_provider=lambda: None, loader=loader)


def test_meta_group_id_and_category_name():
    r = _resolver(_FakeLoader())
    assert r.meta_group_id(12345) == 2
    assert r.category_name(12345) == "Drone"


def test_is_blueprint_is_category_based_and_case_insensitive():
    r = _resolver(_FakeLoader())
    assert r.is_blueprint(999) is True
    assert r.is_blueprint(12345) is False


def test_unknown_type_returns_none_not_an_exception():
    r = _resolver(_FakeLoader())
    assert r.meta_group_id(424242) is None
    assert r.category_name(424242) == ""
    assert r.is_blueprint(424242) is False


def test_missing_meta_group_is_none():
    r = _resolver(_FakeLoader())
    assert r.meta_group_id(777) is None


def test_results_are_cached_so_the_sde_is_queried_once_per_type():
    loader = _FakeLoader()
    r = _resolver(loader)
    r.meta_group_id(12345)
    r.category_name(12345)
    r.is_blueprint(12345)
    assert loader.calls == [[12345]]


def test_prefetch_batches_into_a_single_query():
    loader = _FakeLoader()
    r = _resolver(loader)
    r.prefetch([12345, 999, 777])
    assert loader.calls == [[777, 999, 12345]]
    r.meta_group_id(999)
    assert len(loader.calls) == 1


def test_unknown_types_are_not_re_queried():
    loader = _FakeLoader()
    r = _resolver(loader)
    r.meta_group_id(424242)
    r.meta_group_id(424242)
    assert len(loader.calls) == 1


def test_real_sde_shape_float_meta_group_id_is_coerced_to_int():
    """types.metaGroupID is a REAL column in the real SDE (confirmed via a live probe:
    get_type_data(session, "en", [2488]) returned meta_group_id=2.0 for Warrior II).
    The int(raw) cast in meta_group_id() is load-bearing, not defensive: callers
    (Tasks 6/7/10/11) expect a plain int, not a float."""
    r = _resolver(_FakeLoader())
    assert r.meta_group_id(2488) == 2
    assert isinstance(r.meta_group_id(2488), int)


def test_blueprint_type_with_none_meta_group_id_is_normal_not_missing():
    """Blueprint types legitimately have meta_group_id=None in the real SDE (confirmed via
    a live probe: type 987 'Mammoth Blueprint' has category_name='Blueprint',
    meta_group_id=None). This must be treated as found-but-no-meta-group, not as a lookup
    failure: is_blueprint() must still resolve True, and the type must be cached (not
    re-queried) exactly like any other found type."""
    loader = _FakeLoader()
    r = _resolver(loader)
    assert r.meta_group_id(987) is None
    assert r.is_blueprint(987) is True
    # Second access must not re-hit the loader -- 987 is a *found* entry with a None meta
    # group, not an unresolved/missing type.
    r.meta_group_id(987)
    r.category_name(987)
    r.is_blueprint(987)
    assert loader.calls == [[987]]


def test_none_meta_group_is_not_logged_as_a_problem(caplog):
    """A None meta_group_id on a real, found type (e.g. any blueprint) is normal data, not
    an error or a failed lookup -- it must not produce a warning/error log entry."""
    loader = _FakeLoader()
    r = _resolver(loader)
    with caplog.at_level(logging.WARNING, logger="eve_online_industry_tracker.application.industry.type_metadata"):
        r.meta_group_id(987)
        r.is_blueprint(987)
    assert caplog.records == []
