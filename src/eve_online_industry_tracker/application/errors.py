from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class ServiceError(Exception):
    message: str
    status_code: int = 500
    data: Any = None
    meta: Any = None

    def __str__(self) -> str:
        return self.message
