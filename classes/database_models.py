from typing import Optional, Any
from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func
from sqlalchemy import DateTime, Integer, String, Text, Float, Boolean, JSON
from sqlalchemy.ext.declarative import declarative_base

# Base is the declarative base for SQLAlchamy models
BaseOauth = declarative_base()
BaseApp = declarative_base()
BaseSde = declarative_base()


# --------------------------
# OAuth
# --------------------------
# Define the ESI Cache table as an ORM model
class EsiCache(BaseOauth):
    __tablename__ = "esi_cache"

    endpoint: Mapped[Text] = mapped_column(Text, primary_key=True)
    etag: Mapped[Optional[str]] = mapped_column(Text)
    data: Mapped[str] = mapped_column(Text) # JSON data
    last_updated: Mapped[float] = mapped_column(Float)

# Define the OAuthCharacters table as an ORM model
class OAuthCharacter(BaseOauth):
    __tablename__ = "oauth_characters"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    character_name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    character_id: Mapped[Optional[int]] = mapped_column(Integer, unique=True, nullable=False)
    refresh_token: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    access_token: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    token_expiry: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    scopes: Mapped[str] = mapped_column(Text)  # JSON-formatted string of scopes
    is_main: Mapped[bool] = mapped_column(Boolean, default=False)


# --------------------------
# App
# --------------------------
# Define the Characters table as on ORM model
class CharacterModel(BaseApp):
    __tablename__ = "characters"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    character_name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    character_id: Mapped[int] = mapped_column(Integer, unique=True, nullable=False)
    is_main: Mapped[bool] = mapped_column(Boolean, default=False)
    birthday: Mapped[str] = mapped_column(String, nullable=True)
    gender: Mapped[str] = mapped_column(String, nullable=True)
    bloodline_id: Mapped[int] = mapped_column(Integer, nullable=True)
    bloodline: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    race_id: Mapped[int] = mapped_column(Integer, nullable=True)
    race: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    corporation_id: Mapped[int] = mapped_column(Integer, nullable=True)
    description: Mapped[str] = mapped_column(String, nullable=True)
    security_status: Mapped[float] = mapped_column(Float, default=0.0)
    wallet_balance: Mapped[float] = mapped_column(Float, default=0.0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

# --------------------------
# SDE
# --------------------------
class Bloodlines(BaseSde):
    __tablename__ = "bloodlines"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    charisma: Mapped[int] = mapped_column(Integer, nullable=False)
    corporationID: Mapped[int] = mapped_column(Integer, nullable=False)
    descriptionID: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON, nullable=False)
    iconID: Mapped[int] = mapped_column(Integer, nullable=True)
    intelligence: Mapped[int] = mapped_column(Integer, nullable=False)
    memory: Mapped[int] = mapped_column(Integer, nullable=False)
    nameID: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON, nullable=False)
    perception: Mapped[int] = mapped_column(Integer, nullable=False)
    raceID: Mapped[int] = mapped_column(Integer, nullable=False)
    willpower: Mapped[int] = mapped_column(Integer, nullable=False)

class Races(BaseSde):
    __tablename__ = "races"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    descriptionID: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON, nullable=False)
    iconID: Mapped[int] = mapped_column(Integer, nullable=True)
    nameID: Mapped[int] = mapped_column(Integer, nullable=False)
    shipTypeID: Mapped[int] = mapped_column(Integer, nullable=True)
    skills: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON, nullable=True)        # mapping of skillID -> value/level