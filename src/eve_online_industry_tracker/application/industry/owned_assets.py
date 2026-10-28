"""IndustryService: owned blueprints and item inventory per owner scope, live and historical.

A mixin of IndustryService (service.py); it holds no state of its own."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.exc import SQLAlchemyError
from eve_online_industry_tracker.db_models import (
    Blueprints,
    CharacterAssetHistoryModel,
    CharacterAssetsModel,
    CharacterIndustryJobsModel,
    CharacterWalletTransactionsModel,
    CorporationAssetHistoryModel,
    CorporationAssetsModel,
    CorporationIndustryJobsModel,
    CorporationWalletTransactionsModel,
)
from eve_online_industry_tracker.application.characters.asset_provenance import (
    build_fifo_remaining_lots_by_type,
    fifo_allocate_cost,
)
from eve_online_industry_tracker.application.errors import ServiceError
from eve_online_industry_tracker.infrastructure.persistence import blueprints_repo
from eve_online_industry_tracker.application.industry.facilities import _ESI_ERRORS


# The service module's logger: it carries the QuietWarningsFilter (see service.py).
logger = logging.getLogger("eve_online_industry_tracker.application.industry.service")


class OwnedAssetsMixin:
    @staticmethod
    def _as_float(value: Any) -> float | None:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _optional_int(mapping: Any, key: str) -> int | None:
        """mapping[key] as an int; None when it is absent, None or not a
        number (unknown, never 0)."""
        value = mapping.get(key) if isinstance(mapping, dict) else None
        if value is None or isinstance(value, bool):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _compact_owned_blueprint_asset(
        asset: CharacterAssetsModel | CorporationAssetsModel | None,
        *,
        character_name_by_id: dict[int, str] | None = None,
        corporation_name_by_id: dict[int, str] | None = None,
        top_location_name_by_id: dict[int, str] | None = None,
    ) -> dict[str, Any]:
        if asset is None:
            return {}

        payload = {
            "item_id": int(asset.item_id),
            "location_id": int(asset.location_id) if asset.location_id is not None else None,
            "location_type": asset.location_type,
            "location_flag": asset.location_flag,
            "top_location_id": int(asset.top_location_id) if asset.top_location_id is not None else None,
            "top_location_name": (
                (top_location_name_by_id or {}).get(int(asset.top_location_id))
                if asset.top_location_id is not None
                else None
            ),
            "container_name": asset.container_name,
            "ship_name": asset.ship_name,
            "is_singleton": bool(asset.is_singleton),
            "is_blueprint_copy": bool(asset.is_blueprint_copy),
            "runs": int(asset.blueprint_runs) if asset.blueprint_runs is not None else None,
            "material_efficiency": int(asset.blueprint_material_efficiency)
            if asset.blueprint_material_efficiency is not None
            else None,
            "time_efficiency": int(asset.blueprint_time_efficiency)
            if asset.blueprint_time_efficiency is not None
            else None,
            "quantity": int(asset.quantity),
            "acquisition_unit_cost": OwnedAssetsMixin._as_float(getattr(asset, "acquisition_unit_cost", None)),
            "acquisition_total_cost": OwnedAssetsMixin._as_float(getattr(asset, "acquisition_total_cost", None)),
        }
        if isinstance(asset, CharacterAssetsModel):
            character_id = int(asset.character_id)
            payload["character_id"] = character_id
            payload["character_name"] = (character_name_by_id or {}).get(character_id)
            payload["owner_type"] = "character"
        elif isinstance(asset, CorporationAssetsModel):
            corporation_id = int(asset.corporation_id)
            payload["corporation_id"] = corporation_id
            payload["corporation_name"] = (corporation_name_by_id or {}).get(corporation_id)
            payload["owner_type"] = "corporation"
        return payload

    @classmethod
    def _compact_owned_blueprint_assets(
        cls,
        assets: list[CharacterAssetsModel | CorporationAssetsModel] | None,
        *,
        character_name_by_id: dict[int, str] | None = None,
        corporation_name_by_id: dict[int, str] | None = None,
        top_location_name_by_id: dict[int, str] | None = None,
    ) -> dict[str, Any]:
        if not assets:
            return {}
        if len(assets) == 1:
            return cls._compact_owned_blueprint_asset(
                assets[0],
                character_name_by_id=character_name_by_id,
                corporation_name_by_id=corporation_name_by_id,
                top_location_name_by_id=top_location_name_by_id,
            )

        payloads = [
            cls._compact_owned_blueprint_asset(
                asset,
                character_name_by_id=character_name_by_id,
                corporation_name_by_id=corporation_name_by_id,
                top_location_name_by_id=top_location_name_by_id,
            )
            for asset in assets
        ]

        def common_value(field_name: str) -> Any:
            values = [payload.get(field_name) for payload in payloads if payload.get(field_name) not in {None, ""}]
            if not values:
                return None
            first_value = values[0]
            return first_value if all(value == first_value for value in values[1:]) else None

        character_name = common_value("character_name")
        corporation_name = common_value("corporation_name")
        owner_type = common_value("owner_type")
        if not character_name and not corporation_name:
            character_name = f"{len(payloads)} owned BPCs"
            owner_type = "multiple"

        return {
            "item_id": None,
            "item_ids": [payload.get("item_id") for payload in payloads if payload.get("item_id") is not None],
            "owner_type": owner_type,
            "character_id": common_value("character_id"),
            "character_name": character_name,
            "corporation_id": common_value("corporation_id"),
            "corporation_name": corporation_name,
            "location_id": common_value("location_id"),
            "location_type": common_value("location_type"),
            "location_flag": common_value("location_flag"),
            "top_location_id": common_value("top_location_id"),
            "top_location_name": common_value("top_location_name"),
            "container_name": common_value("container_name"),
            "ship_name": common_value("ship_name"),
            "is_singleton": False,
            "is_blueprint_copy": True,
            "runs": sum(max(0, int(payload.get("runs") or 0)) for payload in payloads) or None,
            "material_efficiency": common_value("material_efficiency"),
            "time_efficiency": common_value("time_efficiency"),
            "quantity": len(payloads),
            "acquisition_unit_cost": None,
            "acquisition_total_cost": sum(
                float(payload.get("acquisition_total_cost") or 0.0)
                for payload in payloads
                if payload.get("acquisition_total_cost") is not None
            ) or None,
        }

    @staticmethod
    def _latest_history_rows_by_item(rows: list[Any]) -> list[Any]:
        latest_by_item_id: dict[int, Any] = {}
        for row in sorted(
            rows,
            key=lambda value: (
                str(getattr(value, "observed_at", "") or ""),
                int(getattr(value, "id", 0) or 0),
            ),
            reverse=True,
        ):
            try:
                item_id = int(getattr(row, "item_id", 0) or 0)
            except (TypeError, ValueError):
                item_id = 0
            if item_id <= 0 or item_id in latest_by_item_id:
                continue
            latest_by_item_id[item_id] = row
        return list(latest_by_item_id.values())

    @staticmethod
    def _materialize_historical_blueprint_asset(
        row: CharacterAssetHistoryModel | CorporationAssetHistoryModel,
        *,
        owner_kind: str,
    ) -> CharacterAssetsModel | CorporationAssetsModel | None:
        try:
            item_id = int(getattr(row, "item_id", 0) or 0)
            type_id = int(getattr(row, "type_id", 0) or 0)
        except (TypeError, ValueError):
            return None
        if item_id <= 0 or type_id <= 0:
            return None

        payload = {
            "item_id": item_id,
            "type_id": type_id,
            "type_name": getattr(row, "type_name", None),
            "type_category_name": "Blueprint",
            "location_id": int(getattr(row, "location_id", 0) or 0),
            "location_type": getattr(row, "location_type", None),
            "location_flag": getattr(row, "location_flag", None),
            "top_location_id": int(getattr(row, "location_id", 0) or 0) or None,
            "is_singleton": bool(getattr(row, "is_singleton", True) if getattr(row, "is_singleton", None) is not None else True),
            "is_blueprint_copy": bool(getattr(row, "is_blueprint_copy", False)),
            "blueprint_runs": getattr(row, "blueprint_runs", None),
            "blueprint_time_efficiency": getattr(row, "blueprint_time_efficiency", None),
            "blueprint_material_efficiency": getattr(row, "blueprint_material_efficiency", None),
            "quantity": int(getattr(row, "quantity", 1) or 1),
            "is_container": False,
            "is_asset_safety_wrap": False,
            "is_ship": False,
            "is_office_folder": False,
            "acquisition_source": getattr(row, "acquisition_source", None),
            "acquisition_unit_cost": getattr(row, "acquisition_unit_cost", None),
            "acquisition_total_cost": getattr(row, "acquisition_total_cost", None),
            "acquisition_reference_type": getattr(row, "acquisition_reference_type", None),
            "acquisition_reference_id": getattr(row, "acquisition_reference_id", None),
            "acquisition_date": getattr(row, "acquisition_date", None),
        }

        if owner_kind == "character":
            payload["character_id"] = int(getattr(row, "character_id", 0) or 0)
            return CharacterAssetsModel(**payload)

        payload["corporation_id"] = int(getattr(row, "corporation_id", 0) or 0)
        return CorporationAssetsModel(**payload)

    #: Industry job statuses that no longer hold a blueprint.
    _TERMINAL_JOB_STATUSES = ("delivered", "cancelled", "reverted")

    def _active_job_blueprint_item_ids(self) -> set[int]:
        """The blueprint item_ids installed in active (not delivered,
        cancelled or reverted) character and corporation industry jobs; a
        NULL status counts as active, as in the daily planner's job read
        (NOT IN alone drops NULL rows). No error handler: an empty set would
        plan with busy BPCs' runs."""
        session: Any = self._sessions.app_session()
        try:
            out: set[int] = set()
            for model in (CharacterIndustryJobsModel, CorporationIndustryJobsModel):
                for (item_id,) in (
                    session.query(model.blueprint_item_id)
                    .filter(model.status.notin_(self._TERMINAL_JOB_STATUSES) | model.status.is_(None))
                    .filter(model.blueprint_item_id.isnot(None))
                    .all()
                ):
                    if int(item_id or 0) > 0:
                        out.add(int(item_id))
            return out
        finally:
            session.close()

    def _load_historical_blueprint_assets(
        self,
        *,
        app_session: Any,
        sde_session: Any,
        character_ids: list[int],
        corporation_ids: list[int],
        current_item_ids: set[int],
    ) -> tuple[list[CharacterAssetsModel], list[CorporationAssetsModel]]:
        candidate_character_rows: list[CharacterAssetHistoryModel] = []
        candidate_corporation_rows: list[CorporationAssetHistoryModel] = []

        normalized_character_ids = sorted({int(owner_id) for owner_id in character_ids if int(owner_id) > 0})
        normalized_corporation_ids = sorted({int(owner_id) for owner_id in corporation_ids if int(owner_id) > 0})

        if normalized_character_ids:
            candidate_character_rows = (
                app_session.query(CharacterAssetHistoryModel)
                .filter(CharacterAssetHistoryModel.character_id.in_(normalized_character_ids))
                .all()
            )
        if normalized_corporation_ids:
            candidate_corporation_rows = (
                app_session.query(CorporationAssetHistoryModel)
                .filter(CorporationAssetHistoryModel.corporation_id.in_(normalized_corporation_ids))
                .all()
            )

        latest_character_rows = self._latest_history_rows_by_item(candidate_character_rows)
        latest_corporation_rows = self._latest_history_rows_by_item(candidate_corporation_rows)
        candidate_type_ids = sorted(
            {
                int(getattr(row, "type_id", 0) or 0)
                for row in [*latest_character_rows, *latest_corporation_rows]
                if int(getattr(row, "type_id", 0) or 0) > 0
            }
        )
        if not candidate_type_ids:
            return [], []

        blueprint_type_ids = {
            int(blueprint_type_id)
            for (blueprint_type_id,) in (
                sde_session.query(Blueprints.blueprintTypeID)
                .filter(Blueprints.blueprintTypeID.in_(candidate_type_ids))
                .all()
            )
        }
        if not blueprint_type_ids:
            return [], []

        # Only include historical blueprints that are currently locked inside an
        # active industry job.  A blueprint absent from both current assets and
        # any active job has been consumed (1-run BPC) or sold — not available.
        #
        # Track char-job and corp-job item IDs separately.  A blueprint installed
        # in a CORP job can appear in both character_asset_history (previous state)
        # and corporation_asset_history (state while at the corp structure).  Using a
        # single shared set would cause it to be emitted from both history sources,
        # producing a duplicate row in the product overview.
        _active_statuses = {"active", "paused", "ready"}
        in_active_char_job_item_ids: set[int] = set()
        in_active_corp_job_item_ids: set[int] = set()

        if normalized_character_ids:
            for (bp_item_id,) in (
                app_session.query(CharacterIndustryJobsModel.blueprint_item_id)
                .filter(CharacterIndustryJobsModel.character_id.in_(normalized_character_ids))
                .filter(CharacterIndustryJobsModel.status.in_(_active_statuses))
                .filter(CharacterIndustryJobsModel.blueprint_item_id.isnot(None))
                .all()
            ):
                in_active_char_job_item_ids.add(int(bp_item_id))

        if normalized_corporation_ids:
            for (bp_item_id,) in (
                app_session.query(CorporationIndustryJobsModel.blueprint_item_id)
                .filter(CorporationIndustryJobsModel.corporation_id.in_(normalized_corporation_ids))
                .filter(CorporationIndustryJobsModel.status.in_(_active_statuses))
                .filter(CorporationIndustryJobsModel.blueprint_item_id.isnot(None))
                .all()
            ):
                in_active_corp_job_item_ids.add(int(bp_item_id))

        # A blueprint locked in a corp job must NOT also appear as a historical char
        # asset — it would produce a duplicate product row.
        char_eligible_item_ids = in_active_char_job_item_ids - in_active_corp_job_item_ids

        historical_character_assets: list[CharacterAssetsModel] = []
        for row in latest_character_rows:
            item_id = int(getattr(row, "item_id", 0) or 0)
            type_id = int(getattr(row, "type_id", 0) or 0)
            if item_id <= 0 or type_id not in blueprint_type_ids or item_id in current_item_ids:
                continue
            if item_id not in char_eligible_item_ids:
                continue
            asset = self._materialize_historical_blueprint_asset(row, owner_kind="character")
            if isinstance(asset, CharacterAssetsModel):
                historical_character_assets.append(asset)

        historical_corporation_assets: list[CorporationAssetsModel] = []
        for row in latest_corporation_rows:
            item_id = int(getattr(row, "item_id", 0) or 0)
            type_id = int(getattr(row, "type_id", 0) or 0)
            if item_id <= 0 or type_id not in blueprint_type_ids or item_id in current_item_ids:
                continue
            if item_id not in in_active_corp_job_item_ids:
                continue
            asset = self._materialize_historical_blueprint_asset(row, owner_kind="corporation")
            if isinstance(asset, CorporationAssetsModel):
                historical_corporation_assets.append(asset)

        return historical_character_assets, historical_corporation_assets

    def _load_historical_item_unit_costs(
        self,
        *,
        app_session: Any,
        character_ids: list[int],
        corporation_ids: list[int],
        type_ids: list[int],
    ) -> dict[int, float]:
        normalized_type_ids = sorted({int(type_id) for type_id in type_ids if int(type_id) > 0})
        if not normalized_type_ids:
            return {}

        out: dict[int, float] = {}

        def consume_rows(rows: list[Any]) -> None:
            for row in sorted(
                rows,
                key=lambda value: (
                    str(getattr(value, "observed_at", "") or ""),
                    int(getattr(value, "id", 0) or 0),
                ),
                reverse=True,
            ):
                type_id = int(getattr(row, "type_id", 0) or 0)
                if type_id <= 0 or type_id in out:
                    continue
                unit_cost = self._as_float(getattr(row, "acquisition_unit_cost", None))
                if unit_cost is None or unit_cost <= 0:
                    continue
                out[type_id] = float(unit_cost)

        normalized_character_ids = sorted({int(owner_id) for owner_id in character_ids if int(owner_id) > 0})
        if normalized_character_ids:
            consume_rows(
                app_session.query(CharacterAssetHistoryModel)
                .filter(CharacterAssetHistoryModel.character_id.in_(normalized_character_ids))
                .filter(CharacterAssetHistoryModel.type_id.in_(normalized_type_ids))
                .filter(CharacterAssetHistoryModel.acquisition_unit_cost.isnot(None))
                .all()
            )

        normalized_corporation_ids = sorted({int(owner_id) for owner_id in corporation_ids if int(owner_id) > 0})
        if normalized_corporation_ids:
            consume_rows(
                app_session.query(CorporationAssetHistoryModel)
                .filter(CorporationAssetHistoryModel.corporation_id.in_(normalized_corporation_ids))
                .filter(CorporationAssetHistoryModel.type_id.in_(normalized_type_ids))
                .filter(CorporationAssetHistoryModel.acquisition_unit_cost.isnot(None))
                .all()
            )

        return out

    @staticmethod
    def _scope_owner_id(owned_blueprints_scope: str, raw: str | None, kind: str) -> int:
        """The positive {kind} ID of an owned_blueprints_scope part. A missing,
        non-numeric or non-positive ID is the caller's error: a 400 naming
        it, instead of a scope that silently selects no assets."""
        try:
            owner_id = int(str(raw).strip()) if raw is not None else 0
        except ValueError:
            owner_id = 0
        if owner_id <= 0:
            raise ServiceError(
                f"owned_blueprints_scope {owned_blueprints_scope!r}: {raw!r} is not a valid {kind} ID",
                status_code=400,
            )
        return owner_id

    @classmethod
    def _resolve_scope_owner_ids(
        cls, owned_blueprints_scope: str, character_ids: list[int], corporation_ids: list[int],
    ) -> tuple[list[int], list[int]]:
        """(character IDs, corporation IDs) an owned_blueprints_scope selects:
        the one rule for owned blueprints and owned inventory. The "all" scopes
        take the known pilots and their corporations; an unknown scope is a 400."""
        normalized_scope = (owned_blueprints_scope or "all_characters").strip().lower()
        kind, _, rest = normalized_scope.partition(":")
        if kind == "character":
            return [cls._scope_owner_id(owned_blueprints_scope, rest, "character")], []
        if kind == "corporation":
            return [], [cls._scope_owner_id(owned_blueprints_scope, rest, "corporation")]
        if kind == "character_and_corporation":
            parts = normalized_scope.split(":")
            return (
                [cls._scope_owner_id(owned_blueprints_scope, parts[1] if len(parts) > 1 else None, "character")],
                [cls._scope_owner_id(owned_blueprints_scope, parts[2] if len(parts) > 2 else None, "corporation")],
            )
        by_scope = {
            "all_characters": (character_ids, []),
            "character_and_corporations": (character_ids, corporation_ids),
            "all_corporations": ([], corporation_ids),
            "all": (character_ids, corporation_ids),
        }
        if normalized_scope not in by_scope:
            raise ServiceError(f"owned_blueprints_scope {owned_blueprints_scope!r} is not a known scope", status_code=400)
        return list(by_scope[normalized_scope][0]), list(by_scope[normalized_scope][1])

    def _get_owned_blueprint_assets(
        self,
        *,
        owned_blueprints_scope: str,
    ) -> tuple[
        list[CharacterAssetsModel],
        list[CorporationAssetsModel],
        dict[int, str],
        dict[int, str],
        dict[int, str],
    ]:
        session: Any = self._sessions.app_session()
        sde_session: Any = self._sessions.sde_session()
        try:
            characters = self._state.char_manager.get_characters() or []
            character_ids: list[int] = []
            corporation_ids: list[int] = []
            for character in characters:
                if not isinstance(character, dict):
                    continue
                try:
                    character_id = int(character.get("character_id") or 0)
                except (TypeError, ValueError):
                    character_id = 0
                try:
                    corporation_id = int(character.get("corporation_id") or 0)
                except (TypeError, ValueError):
                    corporation_id = 0
                if character_id > 0:
                    character_ids.append(character_id)
                if corporation_id > 0:
                    corporation_ids.append(corporation_id)

            character_name_by_id = blueprints_repo.get_character_name_map(session, character_ids)
            corporation_name_by_id = blueprints_repo.get_corporation_name_map(session, corporation_ids)

            for character in characters:
                if not isinstance(character, dict):
                    continue
                try:
                    character_id = int(character.get("character_id") or 0)
                except (TypeError, ValueError):
                    character_id = 0
                character_name = str(character.get("character_name") or character.get("name") or "").strip()
                if character_id > 0 and character_name:
                    character_name_by_id[character_id] = character_name

            corp_manager = getattr(self._state, "corp_manager", None)
            try:
                corporations = (corp_manager.get_corporations() or []) if corp_manager is not None else []
            except SQLAlchemyError as exc:
                # Cosmetic (the corporation names): the overview refresh must not fail on it.
                logger.warning("Owned assets: the corporation names cannot be read (%s); the blueprints keep their "
                               "corporation IDs", exc)
                corporations = []
            for corporation in corporations:
                if not isinstance(corporation, dict):
                    continue
                try:
                    corporation_id = int(corporation.get("corporation_id") or 0)
                except (TypeError, ValueError):
                    corporation_id = 0
                corporation_name = str(corporation.get("corporation_name") or corporation.get("name") or "").strip()
                if corporation_id > 0 and corporation_name:
                    corporation_name_by_id[corporation_id] = corporation_name

            def resolve_top_location_name_map(
                assets: list[CharacterAssetsModel | CorporationAssetsModel],
            ) -> dict[int, str]:
                if self._state.esi_service is None:
                    return {}
                top_location_ids: set[int] = set()
                for asset in assets:
                    raw_top_location_id = getattr(asset, "top_location_id", None)
                    if raw_top_location_id is None:
                        continue
                    top_location_id = int(raw_top_location_id)
                    if top_location_id > 0:
                        top_location_ids.add(top_location_id)
                out: dict[int, str] = {}
                for top_location_id in sorted(top_location_ids):
                    try:
                        location_info = self._state.esi_service.get_location_info(
                            int(top_location_id),
                            suppress_forbidden_log=True,
                            suppress_not_found_log=True,
                        )
                    except _ESI_ERRORS:
                        continue  # cosmetic: the location keeps its ID
                    if not isinstance(location_info, dict):
                        continue
                    name = location_info.get("name")
                    if isinstance(name, str) and name:
                        out[int(top_location_id)] = name
                return out

            scoped_character_ids, scoped_corporation_ids = self._resolve_scope_owner_ids(
                owned_blueprints_scope, character_ids, corporation_ids,
            )
            character_assets = blueprints_repo.get_character_blueprint_assets_for_ids(session, scoped_character_ids)
            corporation_assets = blueprints_repo.get_corporation_blueprint_assets_for_ids(session, scoped_corporation_ids)
            historical_character_assets, historical_corporation_assets = self._load_historical_blueprint_assets(
                app_session=session,
                sde_session=sde_session,
                character_ids=scoped_character_ids,
                corporation_ids=scoped_corporation_ids,
                current_item_ids={
                    *[int(getattr(asset, "item_id", 0) or 0) for asset in character_assets],
                    *[int(getattr(asset, "item_id", 0) or 0) for asset in corporation_assets],
                },
            )
            character_assets.extend(historical_character_assets)
            corporation_assets.extend(historical_corporation_assets)
            top_location_name_by_id = resolve_top_location_name_map([*character_assets, *corporation_assets])
            return (
                character_assets,
                corporation_assets,
                character_name_by_id,
                corporation_name_by_id,
                top_location_name_by_id,
            )
        finally:
            try:
                session.close()
            except SQLAlchemyError:
                pass
            try:
                sde_session.close()
            except SQLAlchemyError:
                pass

    def _get_owned_item_inventory(
        self,
        *,
        owned_blueprints_scope: str,
        material_price_map: dict[int, dict[str, Any]] | None = None,
        corporation_hangar_flags: list[str] | None = None,
    ) -> tuple[dict[int, int], dict[int, float]]:
        session: Any = self._sessions.app_session()
        sde_session: Any = self._sessions.sde_session()
        try:
            characters = self._state.char_manager.get_characters() or []
            character_ids: list[int] = []
            corporation_ids: list[int] = []
            for character in characters:
                if not isinstance(character, dict):
                    continue
                try:
                    character_id = int(character.get("character_id") or 0)
                except (TypeError, ValueError):
                    character_id = 0
                try:
                    corporation_id = int(character.get("corporation_id") or 0)
                except (TypeError, ValueError):
                    corporation_id = 0
                if character_id > 0:
                    character_ids.append(character_id)
                if corporation_id > 0:
                    corporation_ids.append(corporation_id)

            def query_character_assets(ids: list[int]) -> list[CharacterAssetsModel]:
                normalized_ids = sorted({int(asset_id) for asset_id in ids if int(asset_id) > 0})
                if not normalized_ids:
                    return []
                return (
                    session.query(CharacterAssetsModel)
                    .filter(CharacterAssetsModel.character_id.in_(normalized_ids))
                    .all()
                )

            # Hangar flag filter is solely controlled by the caller via corporation_hangar_flags.
            # None means no filter (return all assets); a non-empty list restricts to those flags.
            _hangar_flags: list[str] | None = corporation_hangar_flags

            def query_corporation_assets(ids: list[int]) -> list[CorporationAssetsModel]:
                normalized_ids = sorted({int(asset_id) for asset_id in ids if int(asset_id) > 0})
                if not normalized_ids:
                    return []
                q = (
                    session.query(CorporationAssetsModel)
                    .filter(CorporationAssetsModel.corporation_id.in_(normalized_ids))
                )
                if _hangar_flags:
                    q = q.filter(CorporationAssetsModel.location_flag.in_(_hangar_flags))
                return q.all()

            scoped_character_ids, scoped_corporation_ids = self._resolve_scope_owner_ids(
                owned_blueprints_scope, character_ids, corporation_ids,
            )
            character_assets = query_character_assets(scoped_character_ids)
            corporation_assets = query_corporation_assets(scoped_corporation_ids)

            # Build on-hand quantity map (excluding blueprints)
            quantity_by_type_id: dict[int, int] = {}
            for asset in [*character_assets, *corporation_assets]:
                try:
                    type_id = int(asset.type_id or 0)
                except (TypeError, ValueError):
                    type_id = 0
                if type_id <= 0 or str(getattr(asset, "type_category_name", "") or "") == "Blueprint":
                    continue
                try:
                    quantity = int(getattr(asset, "quantity", 0) or 0)
                except (TypeError, ValueError):
                    quantity = 0
                if quantity <= 0:
                    continue
                quantity_by_type_id[type_id] = int(quantity_by_type_id.get(type_id, 0)) + quantity

            if not quantity_by_type_id:
                return {}, {}

            type_ids_list = list(quantity_by_type_id.keys())
            scoped_character_ids = sorted({int(getattr(a, "character_id")) for a in character_assets if getattr(a, "character_id", None) is not None})
            scoped_corporation_ids = sorted({int(getattr(a, "corporation_id")) for a in corporation_assets if getattr(a, "corporation_id", None) is not None})

            # Fetch wallet transactions and completed industry jobs for FIFO lot reconstruction
            wallet_transactions: list[Any] = []
            if scoped_character_ids:
                wallet_transactions.extend(
                    session.query(CharacterWalletTransactionsModel)
                    .filter(CharacterWalletTransactionsModel.character_id.in_(scoped_character_ids))
                    .filter(CharacterWalletTransactionsModel.type_id.in_(type_ids_list))
                    .all()
                )
            if scoped_corporation_ids:
                wallet_transactions.extend(
                    session.query(CorporationWalletTransactionsModel)
                    .filter(CorporationWalletTransactionsModel.corporation_id.in_(scoped_corporation_ids))
                    .filter(CorporationWalletTransactionsModel.type_id.in_(type_ids_list))
                    .all()
                )

            industry_jobs: list[Any] = []
            if scoped_character_ids:
                industry_jobs.extend(
                    session.query(CharacterIndustryJobsModel)
                    .filter(CharacterIndustryJobsModel.character_id.in_(scoped_character_ids))
                    .filter(CharacterIndustryJobsModel.product_type_id.in_(type_ids_list))
                    .all()
                )
            if scoped_corporation_ids:
                industry_jobs.extend(
                    session.query(CorporationIndustryJobsModel)
                    .filter(CorporationIndustryJobsModel.corporation_id.in_(scoped_corporation_ids))
                    .filter(CorporationIndustryJobsModel.product_type_id.in_(type_ids_list))
                    .all()
                )

            # Extract a flat {type_id: sell_price} map from the planning price map.
            # Used to (a) estimate build costs for jobs without a persisted unit_build_cost,
            # and (b) as a fallback before CCP type_average_price in the chain below.
            flat_market_price_map: dict[int, float] | None = None
            if material_price_map:
                flat_market_price_map = {
                    int(tid): float(v)
                    for tid, entry in material_price_map.items()
                    if (v := self._as_float((entry or {}).get("unit_price"))) and v > 0
                } or None

            # Build FIFO lots per type aligned to current on-hand quantity.
            # Jobs with a persisted unit_build_cost are used directly as lots;
            # jobs without one are estimated via flat_market_price_map when available.
            fifo_lots_by_type = build_fifo_remaining_lots_by_type(
                wallet_transactions=wallet_transactions,
                industry_jobs=industry_jobs,
                sde_session=sde_session,
                market_prices=None,
                market_price_map_direct=flat_market_price_map,
                on_hand_quantities_by_type=quantity_by_type_id,
            )

            # Build a weighted-average acquisition_unit_cost per type from asset records.
            # This is used below to back-fill FIFO gaps caused by ESI's 30-day transaction cap.
            asset_acq_cost_total: dict[int, float] = {}
            asset_acq_cost_qty: dict[int, int] = {}
            for asset in [*character_assets, *corporation_assets]:
                try:
                    a_type_id = int(asset.type_id or 0)
                except (TypeError, ValueError):
                    a_type_id = 0
                if a_type_id <= 0 or a_type_id not in quantity_by_type_id:
                    continue
                acq_cost = self._as_float(getattr(asset, "acquisition_unit_cost", None))
                if acq_cost is None or acq_cost <= 0:
                    continue
                try:
                    a_qty = int(getattr(asset, "quantity", 0) or 0)
                except (TypeError, ValueError):
                    a_qty = 0
                if a_qty <= 0:
                    continue
                asset_acq_cost_total[a_type_id] = float(asset_acq_cost_total.get(a_type_id, 0.0)) + acq_cost * a_qty
                asset_acq_cost_qty[a_type_id] = int(asset_acq_cost_qty.get(a_type_id, 0)) + a_qty

            # Derive FIFO-weighted average unit cost per type from the aligned lots.
            # When FIFO lots cover fewer units than are on hand (ESI transaction history gap),
            # back-fill the unpriced units using acquisition_unit_cost from asset records.
            unit_cost_by_type_id: dict[int, float] = {}
            for type_id, on_hand in quantity_by_type_id.items():
                lots = fifo_lots_by_type.get(type_id) or []
                if not lots:
                    continue
                total_cost, priced_qty = fifo_allocate_cost(lots=lots, quantity=on_hand)
                if priced_qty <= 0 or total_cost <= 0:
                    continue
                unpriced = on_hand - priced_qty
                if unpriced > 0:
                    acq_total = float(asset_acq_cost_total.get(type_id, 0.0))
                    acq_qty = int(asset_acq_cost_qty.get(type_id, 0))
                    if acq_qty > 0 and acq_total > 0:
                        backfill_unit = acq_total / acq_qty
                        total_cost += backfill_unit * unpriced
                        priced_qty += unpriced
                unit_cost_by_type_id[type_id] = total_cost / priced_qty

            # For types the FIFO rebuild couldn't price (no transaction/job history),
            # fall back to acquisition_unit_cost on the asset record → historical
            # snapshot → type average/adjusted price (existing behaviour).
            fallback_needed = [tid for tid in quantity_by_type_id if tid not in unit_cost_by_type_id]
            if fallback_needed:
                exact_cost_total: dict[int, float] = {}
                exact_cost_qty: dict[int, int] = {}
                fallback_cost_total: dict[int, float] = {}
                fallback_cost_qty: dict[int, int] = {}

                for asset in [*character_assets, *corporation_assets]:
                    try:
                        type_id = int(asset.type_id or 0)
                    except (TypeError, ValueError):
                        type_id = 0
                    if type_id not in fallback_needed:
                        continue
                    try:
                        quantity = int(getattr(asset, "quantity", 0) or 0)
                    except (TypeError, ValueError):
                        quantity = 0
                    if quantity <= 0:
                        continue

                    unit_cost = self._as_float(getattr(asset, "acquisition_unit_cost", None))
                    if unit_cost is not None and unit_cost > 0:
                        exact_cost_total[type_id] = float(exact_cost_total.get(type_id, 0.0)) + float(unit_cost) * quantity
                        exact_cost_qty[type_id] = int(exact_cost_qty.get(type_id, 0)) + quantity
                        continue

                    fallback_unit_cost = self._as_float(getattr(asset, "type_average_price", None))
                    if fallback_unit_cost is None or fallback_unit_cost <= 0:
                        fallback_unit_cost = self._as_float(getattr(asset, "type_adjusted_price", None))
                    if fallback_unit_cost is None or fallback_unit_cost <= 0:
                        continue
                    fallback_cost_total[type_id] = float(fallback_cost_total.get(type_id, 0.0)) + float(fallback_unit_cost) * quantity
                    fallback_cost_qty[type_id] = int(fallback_cost_qty.get(type_id, 0)) + quantity

                for type_id, total_cost in exact_cost_total.items():
                    qty = int(exact_cost_qty.get(type_id, 0))
                    if qty > 0:
                        unit_cost_by_type_id[type_id] = total_cost / qty

                historical_costs = self._load_historical_item_unit_costs(
                    app_session=session,
                    character_ids=scoped_character_ids,
                    corporation_ids=scoped_corporation_ids,
                    type_ids=fallback_needed,
                )
                for type_id in fallback_needed:
                    if type_id in unit_cost_by_type_id:
                        continue
                    hist = self._as_float(historical_costs.get(type_id))
                    if hist is not None and hist > 0:
                        unit_cost_by_type_id[type_id] = hist
                        continue
                    if flat_market_price_map:
                        market_price_val = flat_market_price_map.get(type_id)
                        if market_price_val is not None and market_price_val > 0:
                            unit_cost_by_type_id[type_id] = market_price_val
                            continue
                    fb_total = float(fallback_cost_total.get(type_id, 0.0))
                    fb_qty = int(fallback_cost_qty.get(type_id, 0))
                    if fb_qty > 0:
                        unit_cost_by_type_id[type_id] = fb_total / fb_qty

            return quantity_by_type_id, unit_cost_by_type_id
        finally:
            try:
                session.close()
            except SQLAlchemyError:
                pass
            try:
                sde_session.close()
            except SQLAlchemyError:
                pass
