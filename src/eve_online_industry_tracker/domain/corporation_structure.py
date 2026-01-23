from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class CorporationStructure:
    corporation_id: int
    structure_id: int
    structure_name: str
    system_id: Optional[int] = None
    type_id: Optional[int] = None
    updated_at: Optional[datetime] = None

    @staticmethod
    def from_model(model: Any) -> "CorporationStructure":
        return CorporationStructure(
            corporation_id=int(getattr(model, "corporation_id")),
            structure_id=int(getattr(model, "structure_id")),
            structure_name=str(getattr(model, "structure_name", "")),
            system_id=getattr(model, "system_id", None),
            type_id=getattr(model, "type_id", None),
            updated_at=getattr(model, "updated_at", None),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "corporation_id": self.corporation_id,
            "structure_id": self.structure_id,
            "structure_name": self.structure_name,
            "system_id": self.system_id,
            "type_id": self.type_id,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
