"""ensure_oauth_schema rebuilds oauth_characters (nullable identity, flags)
without losing a single token column. Fake values only."""
from __future__ import annotations

from sqlalchemy import Boolean, Integer, String, Text
from sqlalchemy.orm import Mapped, declarative_base, mapped_column
from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects import sqlite
import pytest
from sqlalchemy.exc import IntegrityError

from eve_online_industry_tracker.infrastructure.database_manager import DatabaseManager
from eve_online_industry_tracker.infrastructure.schema_migrations import ensure_oauth_schema

_OldBase = declarative_base()


class _OldOAuthCharacter(_OldBase):
    """A copy of the model as it was before this change."""
    __tablename__ = "oauth_characters"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    character_name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    character_id: Mapped[int] = mapped_column(Integer, unique=True, nullable=False)
    refresh_token: Mapped[str] = mapped_column(String, nullable=True)
    access_token: Mapped[str] = mapped_column(String, nullable=True)
    token_expiry: Mapped[int] = mapped_column(Integer, nullable=True)
    scopes: Mapped[str] = mapped_column(Text)
    is_main: Mapped[bool] = mapped_column(Boolean, default=False)


def test_current_oauth_table_is_rebuilt_without_losing_tokens(tmp_path):
    db = DatabaseManager(f"sqlite:///{tmp_path / 'oauth.db'}")
    db.execute(str(CreateTable(_OldOAuthCharacter.__table__).compile(dialect=sqlite.dialect())))
    db.execute(
        "INSERT INTO oauth_characters (id, character_name, character_id, refresh_token, access_token, "
        "token_expiry, scopes, is_main) VALUES (7, 'Old', 42, 'rt', 'at', 1234, '[\"s1\"]', 1)"
    )

    ensure_oauth_schema(db)
    ensure_oauth_schema(db)

    row = db.query(
        "SELECT id, character_name, character_id, refresh_token, access_token, token_expiry, scopes, is_main, "
        "is_corp_director, industry_enabled FROM oauth_characters"
    )
    assert [tuple(r) for r in row] == [(7, "Old", 42, "rt", "at", 1234, '["s1"]', 1, 0, 1)]

    db.execute("INSERT INTO oauth_characters (character_name, character_id, scopes, is_main) VALUES ('New', NULL, NULL, 0)")
    db.execute("INSERT INTO oauth_characters (character_name, character_id, scopes, is_main) VALUES ('New2', NULL, NULL, 0)")
    with pytest.raises(IntegrityError):
        db.execute("INSERT INTO oauth_characters (character_name, is_main) VALUES ('Old', 0)")
    with pytest.raises(IntegrityError):
        db.execute("INSERT INTO oauth_characters (character_name, character_id, is_main) VALUES ('Dup', 42, 0)")

    assert db.query("SELECT count(*) FROM oauth_meta")[0][0] == 0
