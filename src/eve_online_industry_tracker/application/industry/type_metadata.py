"""Batch type_id → SDE metadata lookups, with a per-instance cache.

The overview row carries `meta_group_name` but no meta group *id*, and nothing
on a corporation asset says whether it is a blueprint. Both answers live in the
SDE, which `get_type_data` already reads; this class batches and caches those
lookups so callers never hit the SDE once per item.

Note on `meta_group_id`: `types.metaGroupID` is stored as REAL in the SDE, so
`get_type_data` can hand back a float (e.g. `2.0`). The `int(raw)` cast below
is load-bearing -- callers expect a plain `int`, not a float. A `None` meta
group id is normal data (every blueprint type has one) and is cached as a
found-but-empty result, never treated as a failed lookup.

Note on `is_blueprint`: it keys on `category_id`, not `category_name`. The
name is localized (`get_type_data` runs it through `parse_localized(...,
language)`), so if the configured app language is ever anything other than
"en", `category_name` would read as e.g. "Blaupause" and a name-based check
would silently return False for every blueprint -- exactly the defect this
class exists to fix. `category_id` is a plain, language-independent integer,
so `is_blueprint` is keyed on that instead.
"""
from __future__ import annotations

from typing import Any, Callable, Iterable

from eve_online_industry_tracker.infrastructure.sde.types import get_type_data

BLUEPRINT_CATEGORY_ID = 9


class TypeMetadataResolver:
    def __init__(
        self,
        *,
        sde_session_provider: Callable[[], Any],
        language: str = "en",
        loader: Callable[[Any, str, list[int]], dict[int, dict]] = get_type_data,
    ) -> None:
        self._sde_session_provider = sde_session_provider
        self._language = language
        self._loader = loader
        self._cache: dict[int, dict[str, Any]] = {}
        self._missing: set[int] = set()

    def prefetch(self, type_ids: Iterable[int]) -> None:
        """Load metadata for many types in one SDE query.

        If the loader call itself raises, the exception propagates unchanged and
        `wanted` is NOT added to `_missing`. Recording a failed lookup as "these
        types do not exist" would silently and permanently poison every later
        meta_group_id()/category_id()/is_blueprint() call for these ids -- a
        transient failure (or a real bug) must not present as "no blueprints,
        no meta groups" for the rest of this instance's life. A missing id in an
        otherwise-successful load is a different, legitimate case and is still
        cached in `_missing` below.
        """
        wanted: set[int] = set()
        for raw_tid in type_ids:
            tid = int(raw_tid or 0)
            if tid > 0 and tid not in self._cache and tid not in self._missing:
                wanted.add(tid)
        if not wanted:
            return

        # sde_session_provider() mirrors this codebase's SessionProvider.sde_session()
        # (infrastructure/session_provider.py: StateSessionProvider.sde_session() ->
        # db.Session()) and daily_planner_repo.py's session_provider.app_session():
        # both hand back a brand-new, per-call Session from a sessionmaker factory, not
        # the shared thread-local scoped_session that application/characters/character.py
        # keeps alive for the lifetime of a Character (self._db_sde.session, never
        # closed there). Closing what we obtain here is therefore safe -- it is our own,
        # freshly-created session, not a shared one another caller still needs.
        session = self._sde_session_provider()
        try:
            loaded = self._loader(session, self._language, sorted(wanted))
        finally:
            close = getattr(session, "close", None)
            if callable(close):
                close()

        for tid in wanted:
            entry = loaded.get(tid)
            if isinstance(entry, dict):
                self._cache[tid] = entry
            else:
                self._missing.add(tid)

    def _entry(self, type_id: int) -> dict[str, Any]:
        tid = int(type_id or 0)
        if tid <= 0:
            return {}
        if tid not in self._cache and tid not in self._missing:
            self.prefetch([tid])
        return self._cache.get(tid, {})

    def meta_group_id(self, type_id: int) -> int | None:
        raw = self._entry(type_id).get("meta_group_id")
        if raw is None:
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    def category_name(self, type_id: int) -> str:
        return str(self._entry(type_id).get("category_name") or "")

    def type_name(self, type_id: int) -> str:
        return str(self._entry(type_id).get("type_name") or "")

    def category_id(self, type_id: int) -> int | None:
        raw = self._entry(type_id).get("category_id")
        if raw is None:
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    def is_blueprint(self, type_id: int) -> bool:
        return self.category_id(type_id) == BLUEPRINT_CATEGORY_ID
