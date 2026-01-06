from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class IndustryProfile:
    id: int
    character_id: int
    profile_name: str
    is_default: bool

    region_id: Optional[int] = None
    system_id: Optional[int] = None
    facility_id: Optional[int] = None
    facility_type: Optional[str] = None
    facility_tax: Optional[float] = None

    material_efficiency_bonus: Optional[float] = None
    time_efficiency_bonus: Optional[float] = None

    rig_slot0_type_id: Optional[int] = None
    rig_slot1_type_id: Optional[int] = None
    rig_slot2_type_id: Optional[int] = None

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @staticmethod
    def from_model(model: Any) -> "IndustryProfile":
        return IndustryProfile(
            id=int(model.id),
            character_id=int(model.character_id),
            profile_name=str(model.profile_name),
            is_default=bool(getattr(model, "is_default", False)),
            region_id=getattr(model, "region_id", None),
            system_id=getattr(model, "system_id", None),
            facility_id=getattr(model, "facility_id", None),
            facility_type=getattr(model, "facility_type", None),
            facility_tax=getattr(model, "facility_tax", None),
            material_efficiency_bonus=getattr(model, "material_efficiency_bonus", None),
            time_efficiency_bonus=getattr(model, "time_efficiency_bonus", None),
            rig_slot0_type_id=getattr(model, "rig_slot0_type_id", None),
            rig_slot1_type_id=getattr(model, "rig_slot1_type_id", None),
            rig_slot2_type_id=getattr(model, "rig_slot2_type_id", None),
            created_at=getattr(model, "created_at", None),
            updated_at=getattr(model, "updated_at", None),
        )

    def to_dict(self) -> Dict[str, Any]:
        # Keep API payload stable (dates as ISO strings if present).
        return {
            "id": self.id,
            "character_id": self.character_id,
            "profile_name": self.profile_name,
            "is_default": self.is_default,
            "region_id": self.region_id,
            "system_id": self.system_id,
            "facility_id": self.facility_id,
            "facility_type": self.facility_type,
            "facility_tax": self.facility_tax,
            "material_efficiency_bonus": self.material_efficiency_bonus,
            "time_efficiency_bonus": self.time_efficiency_bonus,
            "rig_slot0_type_id": self.rig_slot0_type_id,
            "rig_slot1_type_id": self.rig_slot1_type_id,
            "rig_slot2_type_id": self.rig_slot2_type_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
