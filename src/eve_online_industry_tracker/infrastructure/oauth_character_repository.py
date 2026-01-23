from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from eve_online_industry_tracker.db_models import OAuthCharacter


@dataclass(frozen=True)
class OAuthCharacterMetadata:
    character_name: str | None
    character_id: int | None
    scopes: str
    token_expiry: int | None
    expires_in_seconds: int | None
    has_refresh_token: bool
    has_access_token: bool


class OAuthCharacterRepository:
    def __init__(self, session: Any):
        self._session = session

    def list_metadata(self) -> list[OAuthCharacterMetadata]:
        rows = self._session.query(OAuthCharacter).all()

        now = int(datetime.now(timezone.utc).timestamp())
        out: list[OAuthCharacterMetadata] = []

        for row in rows:
            token_expiry = getattr(row, "token_expiry", None)
            expires_in_seconds: int | None
            if token_expiry is None:
                expires_in_seconds = None
            else:
                try:
                    expires_in_seconds = int(token_expiry) - now
                except Exception:
                    expires_in_seconds = None

            out.append(
                OAuthCharacterMetadata(
                    character_name=getattr(row, "character_name", None),
                    character_id=getattr(row, "character_id", None),
                    scopes=getattr(row, "scopes", "") or "",
                    token_expiry=token_expiry,
                    expires_in_seconds=expires_in_seconds,
                    has_refresh_token=bool(getattr(row, "refresh_token", None)),
                    has_access_token=bool(getattr(row, "access_token", None)),
                )
            )

        out.sort(key=lambda x: (str(x.character_name or "").lower()))
        return out
