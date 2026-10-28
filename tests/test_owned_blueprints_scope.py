"""F1 items 5-6: a malformed owned_blueprints_scope is a 400 naming the bad
id (not a scope that silently selects no assets), and the corporation name
lookup in _get_owned_blueprint_assets is cosmetic: a failed read logs a
WARNING and leaves the names empty, so the overview refresh never fails on
it. Fake values throughout."""
from __future__ import annotations

import logging

import pytest
from sqlalchemy.exc import OperationalError

from eve_online_industry_tracker.application.errors import ServiceError
from test_industry_service_history_fallback import _build_service, _make_sessions

CHARS = [{"character_id": 1, "character_name": "Builder", "corporation_id": 10}]


def _service(corporations=()):
    app_factory, sde_factory = _make_sessions()
    return _build_service(app_factory=app_factory, sde_factory=sde_factory, characters=CHARS,
                          corporations=list(corporations))


@pytest.mark.parametrize("scope, bad", [
    ("character:abc", "'abc'"),
    ("corporation:x1", "'x1'"),
    ("character_and_corporation:1:zz", "'zz'"),
    ("character_and_corporation:yy:10", "'yy'"),
    ("character_and_corporation:1", "None"),
    ("character:0", "'0'"),
])
@pytest.mark.parametrize("method", ["_get_owned_blueprint_assets", "_get_owned_item_inventory"])
def test_a_malformed_scope_is_a_400_naming_the_bad_id(method, scope, bad):
    with pytest.raises(ServiceError) as exc_info:
        getattr(_service(), method)(owned_blueprints_scope=scope)
    assert exc_info.value.status_code == 400
    assert bad in exc_info.value.message and scope in exc_info.value.message


@pytest.mark.parametrize("scope", ["character:1", "corporation:10", "character_and_corporation:1:10", "all"])
def test_a_well_formed_scope_still_works(scope):
    _service()._get_owned_blueprint_assets(owned_blueprints_scope=scope)
    _service()._get_owned_item_inventory(owned_blueprints_scope=scope)


class _FailingCorpManager:
    def get_corporations(self):
        raise OperationalError("SELECT fake", {}, Exception("fake db down"))


def test_an_unreadable_corporation_list_leaves_the_names_empty_with_a_warning(caplog):
    service = _service()
    service._state.corp_manager = _FailingCorpManager()
    with caplog.at_level(logging.WARNING):
        result = service._get_owned_blueprint_assets(owned_blueprints_scope="all")
    assert result[3] == {}            # no corporation names, but the refresh goes on
    assert "fake db down" in caplog.text and "corporation names" in caplog.text


def test_a_programming_error_in_the_corporation_list_propagates():
    class _Buggy:
        def get_corporations(self):
            raise AttributeError("fake bug")

    service = _service()
    service._state.corp_manager = _Buggy()
    with pytest.raises(AttributeError):
        service._get_owned_blueprint_assets(owned_blueprints_scope="all")


@pytest.mark.parametrize("method", ["_get_owned_blueprint_assets", "_get_owned_item_inventory"])
def test_an_unknown_scope_is_a_400_not_all_characters(method):
    with pytest.raises(ServiceError) as exc_info:
        getattr(_service(), method)(owned_blueprints_scope="every_pilot")
    assert exc_info.value.status_code == 400
    assert "every_pilot" in exc_info.value.message


@pytest.mark.parametrize("scope, expected", [
    ("", ([1, 2], [])),
    ("all_characters", ([1, 2], [])),
    ("character_and_corporations", ([1, 2], [10])),
    ("all_corporations", ([], [10])),
    ("all", ([1, 2], [10])),
    ("Character:2", ([2], [])),
    ("corporation:10", ([], [10])),
    ("character_and_corporation:1:10", ([1], [10])),
])
def test_one_scope_resolution_for_blueprints_and_inventory(scope, expected):
    from eve_online_industry_tracker.application.industry.service import IndustryService
    assert IndustryService._resolve_scope_owner_ids(scope, [1, 2], [10]) == expected


def test_all_characters_counts_only_known_pilots_blueprints(monkeypatch):
    """A pilot the app no longer knows (asset rows left in the table) must not
    count for blueprints when its materials do not count for inventory."""
    from eve_online_industry_tracker.infrastructure.persistence import blueprints_repo

    seen: dict[str, list[int]] = {}
    real = blueprints_repo.get_character_blueprint_assets_for_ids

    def spy(session, ids):
        seen["ids"] = sorted(ids)
        return real(session, ids)

    monkeypatch.setattr(blueprints_repo, "get_character_blueprint_assets_for_ids", spy)
    monkeypatch.setattr(blueprints_repo, "get_character_blueprints",
                        lambda session: pytest.fail("all_characters must not read every pilot's rows"))
    _service()._get_owned_blueprint_assets(owned_blueprints_scope="all_characters")
    assert seen["ids"] == [1]
