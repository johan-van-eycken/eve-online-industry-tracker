from __future__ import annotations

import logging

import pytest

from eve_online_industry_tracker.application.industry.type_metadata import TypeMetadataResolver

# Shapes mirror what a probe of the real SDE returned for get_type_data(session, "en", [987, 2488]):
#   987  -> type_name='Mammoth Blueprint', category_name='Blueprint', category_id=9,  meta_group_id=None
#   2488 -> type_name='Warrior II',        category_name='Drone',     category_id=18, meta_group_id=2.0 (REAL column, a float)
# plus a follow-up probe confirming categories row 9 == 'Blueprint' and 5094 types join to it, so
# the stub below intentionally includes int and float meta_group_id/category_id values, a
# legitimate (non-error) None for a blueprint type, and a non-English category_name paired with
# category_id=9 to prove is_blueprint() does not depend on the localized name.
TYPE_DATA = {
    12345: {"type_id": 12345, "meta_group_id": 2, "category_name": "Drone", "category_id": 18},
    999: {"type_id": 999, "meta_group_id": 1, "category_name": "Blueprint", "category_id": 9},
    777: {"type_id": 777, "meta_group_id": None, "category_name": "", "category_id": None},
    987: {"type_id": 987, "meta_group_id": None, "category_name": "Blueprint", "category_id": 9},
    2488: {"type_id": 2488, "meta_group_id": 2.0, "category_name": "Drone", "category_id": 18},
    555: {"type_id": 555, "meta_group_id": None, "category_name": "Blaupause", "category_id": 9},
    333: {"type_id": 333, "meta_group_id": None, "category_name": "Blueprint", "category_id": 9.0},
}


class _FakeLoader:
    def __init__(self):
        self.calls: list[list[int]] = []

    def __call__(self, session, language, type_ids):
        self.calls.append(sorted(type_ids))
        return {tid: TYPE_DATA[tid] for tid in type_ids if tid in TYPE_DATA}


class _RaisingLoader:
    """Loader that always raises -- simulates a transient failure or a bug, not a
    legitimately-absent type_id."""

    def __init__(self):
        self.calls: list[list[int]] = []

    def __call__(self, session, language, type_ids):
        self.calls.append(sorted(type_ids))
        raise RuntimeError("SDE lookup boom")


class _FlakyThenGoodLoader:
    """Raises on the first call, succeeds on every call after -- used to prove a failed
    call does not poison the ids into `_missing`."""

    def __init__(self):
        self.calls: list[list[int]] = []

    def __call__(self, session, language, type_ids):
        self.calls.append(sorted(type_ids))
        if len(self.calls) == 1:
            raise RuntimeError("transient SDE failure")
        return {tid: TYPE_DATA[tid] for tid in type_ids if tid in TYPE_DATA}


def _resolver(loader):
    return TypeMetadataResolver(sde_session_provider=lambda: None, loader=loader)


def test_meta_group_id_and_category_name():
    r = _resolver(_FakeLoader())
    assert r.meta_group_id(12345) == 2
    assert r.category_name(12345) == "Drone"


def test_is_blueprint_is_category_id_based():
    r = _resolver(_FakeLoader())
    assert r.is_blueprint(999) is True
    assert r.is_blueprint(12345) is False


def test_unknown_type_returns_none_not_an_exception():
    r = _resolver(_FakeLoader())
    assert r.meta_group_id(424242) is None
    assert r.category_name(424242) == ""
    assert r.category_id(424242) is None
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


# ---------------------------------------------------------------------------
# Fix round 1 -- finding 1 (is_blueprint must be language-independent)
# ---------------------------------------------------------------------------


def test_category_id_accessor():
    r = _resolver(_FakeLoader())
    assert r.category_id(999) == 9
    assert r.category_id(12345) == 18


def test_is_blueprint_is_language_independent():
    """category_name is localized: get_type_data runs it through
    parse_localized(..., language), and the language is a configurable setting
    (cfg["app"]["language"], currently "en"). If is_blueprint() keyed on the name, a
    language change would make category_name read as e.g. "Blaupause" and every
    blueprint would silently stop being recognized as one -- review finding 3, the
    defect this branch exists to fix. is_blueprint() must key on the
    language-independent category_id instead, so it stays True even when the name is
    not English."""
    r = _resolver(_FakeLoader())
    assert r.category_name(555) == "Blaupause"
    assert r.category_id(555) == 9
    assert r.is_blueprint(555) is True


def test_category_id_float_from_real_sde_shape_is_coerced_to_int():
    """Mirrors the meta_group_id float-coercion case: assume categoryID can also come
    back as a float from the SDE (metaGroupID does), so category_id() casts with the
    same int(raw) pattern."""
    r = _resolver(_FakeLoader())
    assert r.category_id(333) == 9
    assert isinstance(r.category_id(333), int)
    assert r.is_blueprint(333) is True


def test_is_blueprint_false_for_non_blueprint_category():
    r = _resolver(_FakeLoader())
    assert r.category_id(2488) == 18
    assert r.is_blueprint(2488) is False


# ---------------------------------------------------------------------------
# Fix round 1 -- finding 2 (a loader exception must not poison _missing)
# ---------------------------------------------------------------------------


def test_loader_exception_propagates_instead_of_being_swallowed():
    """A raising loader is a transient failure or a bug, not evidence that the
    requested type_ids do not exist. It must not be caught and turned into an empty
    result -- the caller needs to see the failure."""
    r = _resolver(_RaisingLoader())
    with pytest.raises(RuntimeError, match="SDE lookup boom"):
        r.meta_group_id(12345)


def test_loader_exception_does_not_poison_missing_cache():
    """After a failed loader call, the requested ids must NOT be marked as missing --
    otherwise a transient failure would permanently and silently present as 'this type
    does not exist' (no blueprints, no meta groups) for the rest of the instance's
    life. A retry with a working loader must be able to resolve them normally."""
    loader = _FlakyThenGoodLoader()
    r = _resolver(loader)

    with pytest.raises(RuntimeError):
        r.meta_group_id(12345)

    # Not poisoned into _missing: a subsequent call re-queries and succeeds.
    assert r.meta_group_id(12345) == 2
    assert len(loader.calls) == 2
    assert loader.calls[0] == [12345]
    assert loader.calls[1] == [12345]


def test_loader_exception_during_prefetch_does_not_poison_other_ids():
    """A batched prefetch that raises must leave every requested id un-poisoned, not
    just the one queried in isolation."""
    loader = _FlakyThenGoodLoader()
    r = _resolver(loader)

    with pytest.raises(RuntimeError):
        r.prefetch([12345, 999])

    assert r.meta_group_id(12345) == 2
    assert r.is_blueprint(999) is True
