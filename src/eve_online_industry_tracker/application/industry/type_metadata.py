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
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Iterable

from eve_online_industry_tracker.infrastructure.sde.types import get_type_data

logger = logging.getLogger(__name__)

BLUEPRINT_CATEGORY_NAME = "blueprint"


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
        """Load metadata for many types in one SDE query."""
        wanted = {
            int(tid)
            for tid in type_ids
            if int(tid or 0) > 0
            and int(tid) not in self._cache
            and int(tid) not in self._missing
        }
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
        except (KeyError, TypeError, ValueError):
            logger.exception("TypeMetadataResolver: SDE lookup failed for %d types", len(wanted))
            loaded = {}
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

    def is_blueprint(self, type_id: int) -> bool:
        return self.category_name(type_id).strip().lower() == BLUEPRINT_CATEGORY_NAME
