from typing import Optional, Any
from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func
from sqlalchemy import BigInteger, DateTime, Integer, String, Text, Float, Boolean, JSON
from sqlalchemy.ext.declarative import declarative_base

# Base is the declarative base for SQLAlchamy models
BaseOauth = declarative_base()
BaseApp = declarative_base()
BaseSde = declarative_base()


# --------------------------
# OAuth
# --------------------------
# Define the ESI Cache table
class EsiCache(BaseOauth):
    __tablename__ = "esi_cache"

    endpoint: Mapped[Text] = mapped_column(Text, primary_key=True)
    etag: Mapped[Optional[str]] = mapped_column(Text)
    data: Mapped[str] = mapped_column(Text) # JSON data
    last_updated: Mapped[float] = mapped_column(Float)

# Define the OAuthCharacters table
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
class CharacterModel(BaseApp):
    __tablename__ = "characters"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    character_name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    character_id: Mapped[int] = mapped_column(Integer, unique=True, nullable=False)
    is_main: Mapped[bool] = mapped_column(Boolean, default=False)
    is_corp_director: Mapped[bool] = mapped_column(Boolean, default=False)
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
    skills: Mapped[str] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

class CorporationModel(BaseApp):
    __tablename__ = "corporations"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    corporation_id: Mapped[int] = mapped_column(Integer, unique=True, nullable=False)
    corporation_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    ticker: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    member_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    creator_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    ceo_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    home_station_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    shares: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tax_rate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    url: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    war_eligible: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

class StructureModel(BaseApp):
    __tablename__ = "corporation_structures"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    corporation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    structure_id: Mapped[int] = mapped_column(Integer, unique=True, nullable=False)
    structure_name: Mapped[str] = mapped_column(String, nullable=True)
    system_id: Mapped[int] = mapped_column(Integer, nullable=True)
    system_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    system_security: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    constellation_id: Mapped[int] = mapped_column(Integer, nullable=True)
    constellation_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    region_id: Mapped[int] = mapped_column(Integer, nullable=True)
    region_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_id: Mapped[int] = mapped_column(Integer, nullable=True)
    type_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    group_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    group_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    category_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    category_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    state: Mapped[str] = mapped_column(String, nullable=True)
    state_timer_end: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    state_timer_start: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    unachors_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    fuel_expires: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    reinforce_hour: Mapped[int] = mapped_column(Integer, nullable=True)
    next_reinforce_apply: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    next_reinforce_hour: Mapped[int] = mapped_column(Integer, nullable=True)
    acl_profile_id: Mapped[int] = mapped_column(Integer, nullable=True)
    services: Mapped[dict[str, str]] = mapped_column(JSON, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

class MemberModel(BaseApp):
    __tablename__ = "corporation_members"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    corporation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    character_id: Mapped[int] = mapped_column(Integer, nullable=False)
    character_name: Mapped[str] = mapped_column(String, nullable=False)
    titles: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=True)  # List of titles
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

# --------------------------
# SDE
# --------------------------
class Bloodlines(BaseSde):
    __tablename__ = "bloodlines"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    charisma: Mapped[int] = mapped_column(Integer, nullable=False)
    corporationID: Mapped[int] = mapped_column(Integer, nullable=False)
    descriptionID: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
    iconID: Mapped[int] = mapped_column(Integer, nullable=True)
    intelligence: Mapped[int] = mapped_column(Integer, nullable=False)
    memory: Mapped[int] = mapped_column(Integer, nullable=False)
    nameID: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
    perception: Mapped[int] = mapped_column(Integer, nullable=False)
    raceID: Mapped[int] = mapped_column(Integer, nullable=False)
    willpower: Mapped[int] = mapped_column(Integer, nullable=False)

class Races(BaseSde):
    __tablename__ = "races"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    descriptionID: Mapped[str] = mapped_column(JSON, nullable=False)
    iconID: Mapped[int] = mapped_column(Integer, nullable=True)
    nameID: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
    shipTypeID: Mapped[int] = mapped_column(Integer, nullable=True)
    skills: Mapped[dict[str, str]] = mapped_column(JSON, nullable=True)

class Types(BaseSde):
    __tablename__ = "types"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    groupID: Mapped[int] = mapped_column(Integer, nullable=True)
    mass: Mapped[int] = mapped_column(BigInteger, nullable=True)
    name: Mapped[dict[str, str]] = mapped_column(JSON, nullable=True)
    portionSize: Mapped[int] = mapped_column(Integer, nullable=True)
    published: Mapped[bool] = mapped_column(Boolean, nullable=True)
    volume: Mapped[float] = mapped_column(Float, nullable=True)
    radius: Mapped[int] = mapped_column(Integer, nullable=True)
    description: Mapped[dict[str, str]] = mapped_column(JSON, nullable=True)

class Groups(BaseSde):
    __tablename__ = "groups"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    anchorable: Mapped[bool] = mapped_column(Boolean, nullable=True)
    anchored: Mapped[bool] = mapped_column(Boolean, nullable=True)
    categoryID: Mapped[int] = mapped_column(Integer, nullable=True)
    fittableNonSingleton: Mapped[bool] = mapped_column(Boolean, nullable=True)
    name: Mapped[dict[str, str]] = mapped_column(JSON, nullable=True)
    published: Mapped[bool] = mapped_column(Boolean, nullable=True)
    useBasePrice: Mapped[bool] = mapped_column(Boolean, nullable=True)
    iconID: Mapped[int] = mapped_column(Integer, nullable=True)

class Categories(BaseSde):
    __tablename__ = "categories"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
    iconID: Mapped[int] = mapped_column(Integer, nullable=True)
    published: Mapped[bool] = mapped_column(Boolean, nullable=True)
