"""One plan-wide blueprint reservation (round 5 Task 14).

EVE runs one job per blueprint: a BPO is in one research, copy or
manufacturing job at a time, and a BPC in one manufacturing or invention
job. ChainPlanner books every blueprint the plan uses here, so two rows
can never plan a job on the same one.

The ledger holds the FREE blueprints (those in an active industry job are
excluded before it is built): BPOs by item, BPCs by item with their runs.
A claim takes the whole item for today, also a BPC whose runs a job does
not use up: the rest of its runs wait for the next plan (one job per
blueprint). ChainPlanner claims in this order:

1. research (research first, the user's ruling);
2. top-level manufacture and the T2 rows' copies, in score order;
3. further copy BPOs for parallel copies, in score order;
4. invention, on the BPCs no one else claimed, in score order;
5. sub-builds, on the component BPOs left: a component type has one
   decision and its own blueprint, so sub-builds claim nothing more.

An asset without an item_id (a hand-built fixture) is keyed by its type
and position, so two such assets of one type stay two blueprints.
"""
from __future__ import annotations

from typing import Any, Hashable

from eve_online_industry_tracker.application.daily_planner import research_first


def _runs(asset: Any) -> int:
    value = asset.get("blueprint_runs") if isinstance(asset, dict) else getattr(asset, "blueprint_runs", 0)
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


class BlueprintLedger:
    """The free BPOs and BPCs of one plan, and who has claimed each."""

    def __init__(self, bpo_assets_by_type_id: dict[int, list], bpc_assets_by_type_id: dict[int, list]) -> None:
        self._bpos: dict[int, list[tuple[Hashable, Any]]] = {
            int(t): [(self._key(a, "bpo", t, i), a) for i, a in enumerate(assets or [])]
            for t, assets in (bpo_assets_by_type_id or {}).items()
        }
        self._bpcs: dict[int, list[tuple[Hashable, Any]]] = {
            int(t): [(self._key(a, "bpc", t, i), a) for i, a in enumerate(assets or [])]
            for t, assets in (bpc_assets_by_type_id or {}).items()
        }
        #: {key: (owner type_id, label)}; the label names the job, for reasons.
        self._claims: dict[Hashable, tuple[int, str]] = {}

    @staticmethod
    def _key(asset: Any, kind: str, type_id: int, index: int) -> Hashable:
        item = research_first.item_id(asset)
        return int(item) if item is not None else (kind, int(type_id), index)

    # --- lookups ---------------------------------------------------------------

    def bpo_keys(self, type_id: int, assets: list[Any]) -> list[Hashable]:
        """The key of each asset in `assets` (any order of this type's BPOs),
        matched by identity; a repeated object takes the next unmatched key."""
        pool = list(self._bpos.get(int(type_id)) or [])
        keys: list[Hashable] = []
        for asset in assets:
            index = next((i for i, (_k, a) in enumerate(pool) if a is asset), None)
            if index is None:
                keys.append(("unknown", int(type_id), id(asset)))
                continue
            keys.append(pool.pop(index)[0])
        return keys

    def claim_of(self, key: Hashable) -> tuple[int, str] | None:
        return self._claims.get(key)

    def owns_bpos(self, type_id: int) -> bool:
        return bool(self._bpos.get(int(type_id)))

    def free_bpos(self, type_id: int) -> list[tuple[Hashable, Any]]:
        """This type's BPOs no one has claimed, in asset order."""
        return [(k, a) for k, a in self._bpos.get(int(type_id)) or [] if k not in self._claims]

    def bpo_claim_labels(self, type_id: int) -> list[str]:
        """Who holds this type's BPOs, first claim first (for reasons)."""
        labels: list[str] = []
        for key, _asset in self._bpos.get(int(type_id)) or []:
            claim = self._claims.get(key)
            if claim is not None and claim[1] not in labels:
                labels.append(claim[1])
        return labels

    def bpc_claim_labels(self, type_id: int) -> list[str]:
        """Who holds this type's BPCs, first claim first (for reasons)."""
        labels: list[str] = []
        for key, _asset in self._bpcs.get(int(type_id)) or []:
            claim = self._claims.get(key)
            if claim is not None and claim[1] not in labels:
                labels.append(claim[1])
        return labels

    def free_bpcs(self, type_id: int) -> list[tuple[Hashable, int | None, int]]:
        """This type's unclaimed BPCs as (key, item_id or None, runs), most runs first."""
        out = [
            (k, research_first.item_id(a), _runs(a))
            for k, a in self._bpcs.get(int(type_id)) or [] if k not in self._claims
        ]
        return sorted(out, key=lambda e: (-e[2], str(e[0])))

    def bpc_key(self, type_id: int, item_id: int) -> Hashable | None:
        for key, asset in self._bpcs.get(int(type_id)) or []:
            if research_first.item_id(asset) == int(item_id):
                return key
        return None

    # --- claims ----------------------------------------------------------------

    def claim(self, key: Hashable, owner_type_id: int, label: str) -> bool:
        """Book `key` for `label`; False when someone else already holds it."""
        held = self._claims.get(key)
        if held is not None:
            return held[0] == int(owner_type_id)
        self._claims[key] = (int(owner_type_id), label)
        return True

    def claim_bpcs_of_row(self, type_id: int, item_ids: set[int] | None, owner_type_id: int, label: str) -> None:
        """Book the BPCs a manufacture builds on: the named items, or every
        free BPC of the type when the row does not name them (conservative:
        an unnamed BPC can be any of them)."""
        for key, asset in self._bpcs.get(int(type_id)) or []:
            item = research_first.item_id(asset)
            if item_ids is None or (item is not None and item in item_ids):
                self.claim(key, owner_type_id, label)
