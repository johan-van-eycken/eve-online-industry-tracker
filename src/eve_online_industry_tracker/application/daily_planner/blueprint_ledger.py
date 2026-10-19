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

A pilot's own blueprint (Task A item 1, a character asset) is booked like a
corp one but bound to its owner (owner_of): only that pilot can install a
job from it, which CharacterAssigner enforces. Sub-builds use corp BPOs only.
"""
from __future__ import annotations

from typing import Any, Hashable

from eve_online_industry_tracker.application.daily_planner import research_first


def owner_character_id(asset: Any) -> int | None:
    """The pilot a blueprint belongs to (a character asset carries its
    character_id), or None for a corp blueprint (Task A item 1). Only that
    pilot can install a job from a character's blueprint."""
    value = asset.get("character_id") if isinstance(asset, dict) else getattr(asset, "character_id", None)
    try:
        owner = int(value) if value is not None else 0
    except (TypeError, ValueError):
        return None
    return owner if owner > 0 else None


def _runs(asset: Any) -> int:
    value = asset.get("blueprint_runs") if isinstance(asset, dict) else getattr(asset, "blueprint_runs", 0)
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


class BlueprintLedger:
    """The free BPOs and BPCs of one plan, and who has claimed each."""

    def __init__(self, bpo_assets_by_type_id: dict[int, list], bpc_assets_by_type_id: dict[int, list]) -> None:
        # One entry per key (Task A fix round 1, I1): an item listed twice (a
        # stale corp + pilot snapshot) is one blueprint, the pilot's
        # (owner-bound) entry wins. {key: (kind, type_id, asset)}.
        self._index: dict[Hashable, tuple[str, int, Any]] = {}
        for kind, source in (("bpo", bpo_assets_by_type_id), ("bpc", bpc_assets_by_type_id)):
            for t, assets in (source or {}).items():
                for i, asset in enumerate(assets or []):
                    key = self._key(asset, kind, t, i)
                    held = self._index.get(key)
                    if held is None or (owner_character_id(held[2]) is None and owner_character_id(asset) is not None):
                        self._index[key] = (kind, int(t), asset)
        self._bpos: dict[int, list[tuple[Hashable, Any]]] = {}
        self._bpcs: dict[int, list[tuple[Hashable, Any]]] = {}
        for key, (kind, t, asset) in self._index.items():
            (self._bpos if kind == "bpo" else self._bpcs).setdefault(t, []).append((key, asset))
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

    def owner_of(self, key: Hashable) -> int | None:
        """The pilot who owns this blueprint (owner-bound), or None (corp)."""
        entry = self._index.get(key)
        return owner_character_id(entry[2]) if entry is not None else None

    def claim_of(self, key: Hashable) -> tuple[int, str] | None:
        return self._claims.get(key)

    def owns_bpos(self, type_id: int) -> bool:
        return bool(self._bpos.get(int(type_id)))

    def free_bpos(self, type_id: int) -> list[tuple[Hashable, Any]]:
        """This type's BPOs no one has claimed, in asset order."""
        return [(k, a) for k, a in self._bpos.get(int(type_id)) or [] if k not in self._claims]

    def free_bpos_to_copy(self, type_id: int) -> list[tuple[Hashable, Any]]:
        """free_bpos, the corp's first (any pilot can copy one), then the
        pilots' own (only the owner can), each in asset order."""
        return sorted(self.free_bpos(type_id), key=lambda e: owner_character_id(e[1]) is not None)

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

    def release(self, key: Hashable, owner_type_id: int) -> None:
        """Give back a claim the owner turned out not to need (a copy the
        claimed BPCs make unnecessary)."""
        held = self._claims.get(key)
        if held is not None and held[0] == int(owner_type_id):
            del self._claims[key]

    def transfer(self, key: Hashable, owner_type_id: int, label: str) -> None:
        """Book `key` for `owner_type_id`, whoever held it: the assigner
        gave a released blueprint to that row's job (Task A items 2-3)."""
        self._claims[key] = (int(owner_type_id), label)

    def claims_of(self, owner_type_id: int) -> list[tuple[Hashable, str]]:
        """(key, label) of every claim `owner_type_id` holds, in claim order."""
        return [(k, c[1]) for k, c in self._claims.items() if c[0] == int(owner_type_id)]

    def type_of(self, key: Hashable) -> tuple[str, int] | None:
        """("bpo" | "bpc", blueprint type_id) of a ledger key, or None."""
        entry = self._index.get(key)
        return (entry[0], entry[1]) if entry is not None else None

    def claim_bpcs_of_row(self, type_id: int, item_ids: set[int] | None, owner_type_id: int, label: str) -> None:
        """Book the BPCs a manufacture builds on: the named items, or every
        free BPC of the type when the row does not name them (conservative:
        an unnamed BPC can be any of them)."""
        for key, asset in self._bpcs.get(int(type_id)) or []:
            item = research_first.item_id(asset)
            if item_ids is None or (item is not None and item in item_ids):
                self.claim(key, owner_type_id, label)
