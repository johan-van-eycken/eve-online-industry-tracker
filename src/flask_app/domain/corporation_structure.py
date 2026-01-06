from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class CorporationStructure:
    id: int
    corporation_id: int
    structure_id: int

    name: Optional[str] = None
    solar_system_id: Optional[int] = None
    type_id: Optional[int] = None

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @staticmethod
    def from_model(model: Any) -> "CorporationStructure":
        return CorporationStructure(
            id=int(model.id),
            corporation_id=int(model.corporation_id),
            structure_id=int(model.structure_id),
            name=getattr(model, "name", None),
            solar_system_id=getattr(model, "solar_system_id", None),
            type_id=getattr(model, "type_id", None),
            created_at=getattr(model, "created_at", None),
            updated_at=getattr(model, "updated_at", None),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "corporation_id": self.corporation_id,
            "structure_id": self.structure_id,
            "name": self.name,
            "solar_system_id": self.solar_system_id,
            "type_id": self.type_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
