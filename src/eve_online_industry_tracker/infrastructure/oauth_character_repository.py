from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from eve_online_industry_tracker.db_models import OAuthCharacter

_FLAGS = {"is_main", "is_corp_director", "industry_enabled"}


@dataclass(frozen=True)
class OAuthCharacterMetadata:
    character_name: str | None
    character_id: int | None
    scopes: str
    token_expiry: int | None
    expires_in_seconds: int | None
    has_refresh_token: bool
    has_access_token: bool
    is_main: bool
    is_corp_director: bool
    industry_enabled: bool


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
                except (TypeError, ValueError):
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
                    is_main=bool(row.is_main),
                    is_corp_director=bool(row.is_corp_director),
                    industry_enabled=bool(row.industry_enabled),
                )
            )

        out.sort(key=lambda x: (str(x.character_name or "").lower()))
        return out

    def _find(self, character_name: str) -> OAuthCharacter | None:
        key = character_name.casefold()
        return next((r for r in self._session.query(OAuthCharacter).all() if r.character_name.casefold() == key), None)

    def add(self, character_name: str, *, is_main: bool = False, is_corp_director: bool = False,
            industry_enabled: bool = True) -> None:
        if self._find(character_name) is not None:
            raise ValueError(f"Character {character_name!r} already exists")
        self._session.add(OAuthCharacter(
            character_name=character_name, is_main=is_main,
            is_corp_director=is_corp_director, industry_enabled=industry_enabled,
        ))
        self._session.commit()

    def set_flags(self, character_name: str, **flags: bool) -> None:
        unknown = set(flags) - _FLAGS
        if unknown:
            raise ValueError(f"Unknown flags: {sorted(unknown)}")
        row = self._find(character_name)
        if row is None:
            raise KeyError(character_name)
        if flags.get("is_main"):
            for other in self._session.query(OAuthCharacter).filter(OAuthCharacter.id != row.id).all():
                other.is_main = False
        for name, value in flags.items():
            setattr(row, name, bool(value))
        self._session.commit()

    def delete(self, character_name: str) -> None:
        row = self._find(character_name)
        if row is None:
            raise KeyError(character_name)
        self._session.delete(row)
        self._session.commit()
