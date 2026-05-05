from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from sqlalchemy import desc

from classes.database_models import (
    CharacterAssetEventModel,
    CharacterAssetHistoryModel,
    CharacterAssetsModel,
    CorporationAssetEventModel,
    CorporationAssetHistoryModel,
    CorporationAssetsModel,
)


def _safe_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        return None


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _blueprint_provenance_payload(row: Any) -> dict[str, Any] | None:
    is_blueprint_copy = getattr(row, "is_blueprint_copy", None)
    blueprint_runs = _safe_int(getattr(row, "blueprint_runs", None))
    blueprint_time_efficiency = _safe_int(getattr(row, "blueprint_time_efficiency", None))
    blueprint_material_efficiency = _safe_int(getattr(row, "blueprint_material_efficiency", None))
    if (
        is_blueprint_copy is None
        and blueprint_runs is None
        and blueprint_time_efficiency is None
        and blueprint_material_efficiency is None
    ):
        return None
    return {
        "item_id": _safe_int(getattr(row, "item_id", None)),
        "is_blueprint_copy": is_blueprint_copy,
        "blueprint_runs": blueprint_runs,
        "blueprint_time_efficiency": blueprint_time_efficiency,
        "blueprint_material_efficiency": blueprint_material_efficiency,
    }


def _blueprint_provenance_signature(payload: dict[str, Any]) -> tuple[Any, ...]:
    return (
        payload.get("is_blueprint_copy"),
        payload.get("blueprint_runs"),
        payload.get("blueprint_time_efficiency"),
        payload.get("blueprint_material_efficiency"),
    )


def _row_to_blueprint_provenance(*, row: Any, source: str) -> dict[str, Any] | None:
    payload = _blueprint_provenance_payload(row)
    if payload is None:
        return None
    return {
        **payload,
        "source": source,
        "reference_id": int(getattr(row, "id", 0) or 0),
    }


def _owner_models(owner_kind: str) -> tuple[type[Any], type[Any], type[Any], str]:
    if owner_kind == "character":
        return CharacterAssetsModel, CharacterAssetHistoryModel, CharacterAssetEventModel, "character_id"
    if owner_kind == "corporation":
        return CorporationAssetsModel, CorporationAssetHistoryModel, CorporationAssetEventModel, "corporation_id"
    raise ValueError(f"Unsupported owner_kind: {owner_kind}")


_BACKFILL_SNAPSHOT_SOURCE = "historical_backfill"
_BACKFILL_ITEM_BASE_BY_REFERENCE_TYPE = {
    "wallet_transaction": -1_000_000_000_000,
    "industry_job": -2_000_000_000_000,
}


def _backfill_item_id(*, reference_type: str, reference_id: int) -> int:
    base = int(_BACKFILL_ITEM_BASE_BY_REFERENCE_TYPE.get(str(reference_type), -9_000_000_000_000))
    return int(base) - int(reference_id)


def clear_historical_backfill(
    *,
    app_session: Any,
    owner_kind: str,
    owner_id: int,
) -> None:
    _, history_model, _, owner_field = _owner_models(owner_kind)
    (
        app_session.query(history_model)
        .filter(getattr(history_model, owner_field) == int(owner_id))
        .filter(history_model.snapshot_source == _BACKFILL_SNAPSHOT_SOURCE)
        .delete(synchronize_session=False)
    )


def record_historical_acquisition(
    *,
    app_session: Any,
    owner_kind: str,
    owner_id: int,
    observed_at: str | None,
    type_id: int,
    type_name: str | None,
    quantity: int,
    acquisition_source: str,
    acquisition_unit_cost: float | None,
    acquisition_total_cost: float | None,
    acquisition_reference_type: str,
    acquisition_reference_id: int,
    item_id: int | None = None,
    snapshot_source: str = _BACKFILL_SNAPSHOT_SOURCE,
) -> None:
    if not observed_at:
        return
    normalized_type_id = _safe_int(type_id)
    normalized_quantity = _safe_int(quantity)
    normalized_reference_id = _safe_int(acquisition_reference_id)
    if normalized_type_id is None or normalized_type_id <= 0:
        return
    if normalized_quantity is None or normalized_quantity <= 0:
        return
    if normalized_reference_id is None or normalized_reference_id <= 0:
        return

    _, history_model, _, owner_field = _owner_models(owner_kind)
    normalized_item_id = _safe_int(item_id)
    if normalized_item_id is None:
        normalized_item_id = _backfill_item_id(
            reference_type=str(acquisition_reference_type),
            reference_id=int(normalized_reference_id),
        )

    app_session.add(
        history_model(
            **{
                owner_field: int(owner_id),
                "item_id": int(normalized_item_id),
                "observed_at": str(observed_at),
                "snapshot_source": str(snapshot_source),
                "type_id": int(normalized_type_id),
                "type_name": type_name,
                "location_id": None,
                "location_type": None,
                "location_flag": None,
                "is_singleton": False,
                "quantity": int(normalized_quantity),
                "is_blueprint_copy": None,
                "blueprint_runs": None,
                "blueprint_time_efficiency": None,
                "blueprint_material_efficiency": None,
                "acquisition_source": str(acquisition_source),
                "acquisition_unit_cost": _safe_float(acquisition_unit_cost),
                "acquisition_total_cost": _safe_float(acquisition_total_cost),
                "acquisition_reference_type": str(acquisition_reference_type),
                "acquisition_reference_id": int(normalized_reference_id),
                "acquisition_date": str(observed_at),
            }
        )
    )


def backfill_wallet_buy_acquisitions(
    *,
    app_session: Any,
    owner_kind: str,
    owner_id: int,
    wallet_transactions: Iterable[Any],
) -> None:
    for tx in wallet_transactions or []:
        if bool(getattr(tx, "is_buy", None)) is not True:
            continue
        transaction_id = _safe_int(getattr(tx, "transaction_id", None))
        type_id = _safe_int(getattr(tx, "type_id", None))
        quantity = _safe_int(getattr(tx, "quantity", None))
        unit_price = _safe_float(getattr(tx, "unit_price", None))
        tx_date = getattr(tx, "date", None)
        if transaction_id is None or type_id is None or quantity is None or quantity <= 0 or unit_price is None or unit_price <= 0 or not tx_date:
            continue
        record_historical_acquisition(
            app_session=app_session,
            owner_kind=owner_kind,
            owner_id=int(owner_id),
            observed_at=str(tx_date),
            type_id=int(type_id),
            type_name=getattr(tx, "type_name", None),
            quantity=int(quantity),
            acquisition_source="wallet_transaction",
            acquisition_unit_cost=float(unit_price),
            acquisition_total_cost=float(unit_price) * float(quantity),
            acquisition_reference_type="wallet_transaction",
            acquisition_reference_id=int(transaction_id),
        )


def sync_asset_history(
    *,
    app_session: Any,
    owner_kind: str,
    owner_id: int,
    asset_rows: list[dict[str, Any]],
    observed_at: str | None = None,
    snapshot_source: str = "asset_refresh",
) -> None:
    asset_model, history_model, event_model, owner_field = _owner_models(owner_kind)
    observed_at_value = str(observed_at or _now_iso())

    existing_assets = (
        app_session.query(asset_model)
        .filter(getattr(asset_model, owner_field) == int(owner_id))
        .all()
    )
    existing_by_item = {
        int(item.item_id): item
        for item in existing_assets
        if _safe_int(getattr(item, "item_id", None)) is not None
    }

    new_by_item: dict[int, dict[str, Any]] = {}
    history_rows: list[Any] = []
    event_rows: list[Any] = []

    for asset in asset_rows:
        item_id = _safe_int((asset or {}).get("item_id"))
        type_id = _safe_int((asset or {}).get("type_id"))
        if item_id is None or type_id is None:
            continue
        new_by_item[int(item_id)] = dict(asset)
        history_rows.append(
            history_model(
                **{
                    owner_field: int(owner_id),
                    "item_id": int(item_id),
                    "observed_at": observed_at_value,
                    "snapshot_source": snapshot_source,
                    "type_id": int(type_id),
                    "type_name": asset.get("type_name"),
                    "location_id": _safe_int(asset.get("location_id")),
                    "location_type": asset.get("location_type"),
                    "location_flag": asset.get("location_flag"),
                    "is_singleton": asset.get("is_singleton"),
                    "quantity": _safe_int(asset.get("quantity")),
                    "is_blueprint_copy": asset.get("is_blueprint_copy"),
                    "blueprint_runs": _safe_int(asset.get("blueprint_runs")),
                    "blueprint_time_efficiency": _safe_int(asset.get("blueprint_time_efficiency")),
                    "blueprint_material_efficiency": _safe_int(asset.get("blueprint_material_efficiency")),
                    "acquisition_source": asset.get("acquisition_source"),
                    "acquisition_unit_cost": _safe_float(asset.get("acquisition_unit_cost")),
                    "acquisition_total_cost": _safe_float(asset.get("acquisition_total_cost")),
                    "acquisition_reference_type": asset.get("acquisition_reference_type"),
                    "acquisition_reference_id": _safe_int(asset.get("acquisition_reference_id")),
                    "acquisition_date": asset.get("acquisition_date"),
                }
            )
        )

        previous = existing_by_item.get(int(item_id))
        new_quantity = _safe_int(asset.get("quantity")) or 0
        if previous is None:
            event_rows.append(
                event_model(
                    **{
                        owner_field: int(owner_id),
                        "item_id": int(item_id),
                        "type_id": int(type_id),
                        "event_time": observed_at_value,
                        "event_kind": "appeared",
                        "quantity_delta": new_quantity,
                        "previous_quantity": 0,
                        "new_quantity": new_quantity,
                        "reason": snapshot_source,
                        "metadata_json": {"location_id": _safe_int(asset.get("location_id"))},
                    }
                )
            )
            continue

        previous_quantity = _safe_int(getattr(previous, "quantity", None)) or 0
        if previous_quantity != new_quantity:
            event_rows.append(
                event_model(
                    **{
                        owner_field: int(owner_id),
                        "item_id": int(item_id),
                        "type_id": int(type_id),
                        "event_time": observed_at_value,
                        "event_kind": "quantity_changed",
                        "quantity_delta": int(new_quantity - previous_quantity),
                        "previous_quantity": previous_quantity,
                        "new_quantity": new_quantity,
                        "reason": snapshot_source,
                        "metadata_json": {
                            "previous_location_id": _safe_int(getattr(previous, "location_id", None)),
                            "new_location_id": _safe_int(asset.get("location_id")),
                        },
                    }
                )
            )
        elif (
            _safe_int(getattr(previous, "blueprint_runs", None)) != _safe_int(asset.get("blueprint_runs"))
            or _safe_int(getattr(previous, "blueprint_material_efficiency", None)) != _safe_int(asset.get("blueprint_material_efficiency"))
            or _safe_int(getattr(previous, "blueprint_time_efficiency", None)) != _safe_int(asset.get("blueprint_time_efficiency"))
        ):
            event_rows.append(
                event_model(
                    **{
                        owner_field: int(owner_id),
                        "item_id": int(item_id),
                        "type_id": int(type_id),
                        "event_time": observed_at_value,
                        "event_kind": "state_changed",
                        "quantity_delta": 0,
                        "previous_quantity": previous_quantity,
                        "new_quantity": new_quantity,
                        "reason": snapshot_source,
                        "metadata_json": {
                            "previous_blueprint_runs": _safe_int(getattr(previous, "blueprint_runs", None)),
                            "new_blueprint_runs": _safe_int(asset.get("blueprint_runs")),
                            "previous_blueprint_material_efficiency": _safe_int(getattr(previous, "blueprint_material_efficiency", None)),
                            "new_blueprint_material_efficiency": _safe_int(asset.get("blueprint_material_efficiency")),
                            "previous_blueprint_time_efficiency": _safe_int(getattr(previous, "blueprint_time_efficiency", None)),
                            "new_blueprint_time_efficiency": _safe_int(asset.get("blueprint_time_efficiency")),
                        },
                    }
                )
            )

    for item_id, previous in existing_by_item.items():
        if int(item_id) in new_by_item:
            continue
        previous_quantity = _safe_int(getattr(previous, "quantity", None)) or 0
        event_rows.append(
            event_model(
                **{
                    owner_field: int(owner_id),
                    "item_id": int(item_id),
                    "type_id": _safe_int(getattr(previous, "type_id", None)),
                    "event_time": observed_at_value,
                    "event_kind": "disappeared",
                    "quantity_delta": -previous_quantity,
                    "previous_quantity": previous_quantity,
                    "new_quantity": 0,
                    "reason": snapshot_source,
                    "metadata_json": {"previous_location_id": _safe_int(getattr(previous, "location_id", None))},
                }
            )
        )

    if history_rows:
        app_session.bulk_save_objects(history_rows)
    if event_rows:
        app_session.bulk_save_objects(event_rows)


def lookup_historical_blueprint_provenance(
    *,
    app_session: Any,
    owner_kind: str,
    owner_id: int,
    blueprint_item_id: int | None,
    blueprint_type_id: int | None,
    as_of: str | None,
) -> dict[str, Any] | None:
    _, history_model, _, owner_field = _owner_models(owner_kind)

    base_query = app_session.query(history_model).filter(getattr(history_model, owner_field) == int(owner_id))
    historical_query = base_query
    if as_of:
        historical_query = historical_query.filter(history_model.observed_at <= str(as_of))

    if blueprint_item_id is not None:
        exact_rows = (
            historical_query.filter(history_model.item_id == int(blueprint_item_id))
            .order_by(desc(history_model.observed_at), desc(history_model.id))
            .all()
        )
        for exact in exact_rows:
            provenance = _row_to_blueprint_provenance(row=exact, source="historical_blueprint_exact_item")
            if provenance is not None:
                return provenance

    if blueprint_type_id is None:
        return None

    fallback_rows = (
        historical_query.filter(history_model.type_id == int(blueprint_type_id))
        .order_by(desc(history_model.observed_at), desc(history_model.id))
        .all()
    )
    for fallback in fallback_rows:
        provenance = _row_to_blueprint_provenance(row=fallback, source="historical_blueprint_type_fallback")
        if provenance is not None:
            return provenance

    if not as_of:
        return None

    future_rows = (
        base_query.filter(history_model.type_id == int(blueprint_type_id))
        .filter(history_model.observed_at > str(as_of))
        .order_by(history_model.observed_at.asc(), history_model.id.asc())
        .all()
    )
    informative_future_rows: list[tuple[Any, dict[str, Any]]] = []
    for row in future_rows:
        payload = _blueprint_provenance_payload(row)
        if payload is None:
            continue
        informative_future_rows.append((row, payload))
    if not informative_future_rows:
        return None

    signatures = {_blueprint_provenance_signature(payload) for _, payload in informative_future_rows}
    if len(signatures) != 1:
        return None

    return _row_to_blueprint_provenance(
        row=informative_future_rows[0][0],
        source="historical_blueprint_type_forward_fill",
    )


def build_historical_input_cost_lookup(
    *,
    app_session: Any,
    owner_kind: str,
    owner_id: int,
    as_of: str | None,
    type_ids: Iterable[int],
) -> dict[int, dict[str, Any]]:
    _, history_model, _, owner_field = _owner_models(owner_kind)
    normalized_type_ids = sorted({int(type_id) for type_id in type_ids if _safe_int(type_id) is not None})
    if not normalized_type_ids:
        return {}

    query = app_session.query(history_model).filter(getattr(history_model, owner_field) == int(owner_id))
    query = query.filter(history_model.type_id.in_(normalized_type_ids))
    query = query.filter(history_model.acquisition_unit_cost.isnot(None))
    if as_of:
        query = query.filter(history_model.observed_at <= str(as_of))
    rows = query.order_by(history_model.observed_at.asc(), history_model.id.asc()).all()

    out: dict[int, dict[str, Any]] = {}
    for row in rows:
        type_id = _safe_int(getattr(row, "type_id", None))
        unit_cost = _safe_float(getattr(row, "acquisition_unit_cost", None))
        quantity = _safe_int(getattr(row, "quantity", None))
        if type_id is None or unit_cost is None or quantity is None or quantity <= 0:
            continue
        payload = out.setdefault(
            int(type_id),
            {
                "unit_cost": None,
                "source": None,
                "reference_type": None,
                "reference_id": None,
                "history_id": None,
                "observed_at": None,
                "lots": [],
            },
        )
        lot_payload = {
            "unit_cost": float(unit_cost),
            "quantity": int(quantity),
            "source": str(getattr(row, "acquisition_source", None) or "historical_asset_acquisition_cost"),
            "reference_type": str(getattr(row, "acquisition_reference_type", None) or "asset_history"),
            "reference_id": _safe_int(getattr(row, "acquisition_reference_id", None)) or int(getattr(row, "id", 0) or 0),
            "history_id": int(getattr(row, "id", 0) or 0),
            "observed_at": getattr(row, "observed_at", None),
        }
        casted_lots = payload.setdefault("lots", [])
        if isinstance(casted_lots, list):
            casted_lots.append(lot_payload)

    for payload in out.values():
        lots = payload.get("lots") or []
        if not isinstance(lots, list) or not lots:
            continue
        total_quantity = sum(int(lot.get("quantity") or 0) for lot in lots)
        if total_quantity <= 0:
            continue
        total_cost = sum(float(lot.get("unit_cost") or 0.0) * int(lot.get("quantity") or 0) for lot in lots)
        payload["unit_cost"] = float(total_cost) / float(total_quantity)
        latest_lot = lots[-1]
        payload["source"] = latest_lot.get("source")
        payload["reference_type"] = latest_lot.get("reference_type")
        payload["reference_id"] = latest_lot.get("reference_id")
        payload["history_id"] = latest_lot.get("history_id")
        payload["observed_at"] = latest_lot.get("observed_at")
    return out