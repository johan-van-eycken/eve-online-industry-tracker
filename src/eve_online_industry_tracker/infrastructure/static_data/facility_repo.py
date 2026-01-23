from __future__ import annotations


def get_facility(facility_id: int) -> dict:
    return {
        "id": facility_id,
        "name": "Default Waira - Mining and Repro 1.5% tax (High Sec Holding Corporation)",
        "base_yield": 0.5,
        "rig_bonus": 0.02,
        "structure_bonus": 0.0,
        "tax": 0.015,
    }


def get_all_facilities() -> list[dict]:
    return [get_facility(1)]
