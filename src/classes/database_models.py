from typing import Optional, Any
from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column # pyright: ignore[reportMissingImports]
from sqlalchemy.sql import func # pyright: ignore[reportMissingImports]
from sqlalchemy import BigInteger, DateTime, Integer, String, Text, Float, Boolean, JSON # pyright: ignore[reportMissingImports]
from sqlalchemy.ext.declarative import declarative_base # pyright: ignore[reportMissingImports]

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
    corporation_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    description: Mapped[str] = mapped_column(String, nullable=True)
    security_status: Mapped[float] = mapped_column(Float, default=0.0)
    wallet_balance: Mapped[float] = mapped_column(Float, default=0.0)
    skills: Mapped[str] = mapped_column(Text, nullable=True)
    standings: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON string
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

class CharacterWalletJournalModel(BaseApp):
    __tablename__ = "character_wallet_journal"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    character_id: Mapped[int] = mapped_column(Integer, nullable=False)
    wallet_journal_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False)
    amount: Mapped[float] = mapped_column(Float, nullable=False)
    balance: Mapped[float] = mapped_column(Float, nullable=False)
    context_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    context_id_type: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    date: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str] = mapped_column(String, nullable=True)
    reason: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    ref_type: Mapped[str] = mapped_column(String, nullable=False)
    first_party_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    first_party_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    second_party_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    second_party_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    tax: Mapped[float] = mapped_column(Float, nullable=True)
    tax_receiver_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tax_receiver_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

class CharacterWalletTransactionsModel(BaseApp):
    __tablename__ = "character_wallet_transactions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    character_id: Mapped[int] = mapped_column(Integer, nullable=False)
    transaction_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False)
    client_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    client_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    date: Mapped[str] = mapped_column(String, nullable=True)
    is_buy: Mapped[bool] = mapped_column(Boolean, nullable=True)
    is_personal: Mapped[bool] = mapped_column(Boolean, nullable=True)
    journal_ref_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    location_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=True)
    type_id: Mapped[int] = mapped_column(Integer, nullable=True)
    type_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_group_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    type_group_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_category_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    type_category_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    unit_price: Mapped[float] = mapped_column(Float, nullable=True)
    total_price: Mapped[float] = mapped_column(Float, nullable=True)

class CharacterMarketOrdersModel(BaseApp):
    __tablename__ = "character_market_orders"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    character_id: Mapped[int] = mapped_column(Integer, nullable=False)
    order_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False)
    type_id: Mapped[int] = mapped_column(Integer, nullable=False)
    type_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_group_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    type_group_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_category_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    type_category_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    location_id: Mapped[int] = mapped_column(BigInteger, nullable=True)
    location_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    region_id: Mapped[int] = mapped_column(Integer, nullable=True)
    region_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    owner: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    is_corporation: Mapped[bool] = mapped_column(Boolean, nullable=True)
    price: Mapped[float] = mapped_column(Float, nullable=True)
    is_buy_order: Mapped[bool] = mapped_column(Boolean, nullable=True)
    escrow: Mapped[float] = mapped_column(Float, nullable=True)
    volume_total: Mapped[int] = mapped_column(Integer, nullable=True)
    volume_remain: Mapped[int] = mapped_column(Integer, nullable=True)
    duration: Mapped[int] = mapped_column(Integer, nullable=True)
    issued: Mapped[str] = mapped_column(String, nullable=True)
    min_volume: Mapped[int] = mapped_column(Integer, nullable=True)
    range: Mapped[str] = mapped_column(String, nullable=True)

class CharacterAssetsModel(BaseApp):
    __tablename__ = "character_assets"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    character_id: Mapped[int] = mapped_column(Integer, nullable=False)
    item_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False)
    type_id: Mapped[int] = mapped_column(Integer, nullable=False)
    type_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_default_volume: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    type_repackaged_volume: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    type_volume: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    type_capacity: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    type_description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    container_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    ship_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_group_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    type_group_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_category_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    type_category_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_meta_group_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    type_race_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    type_race_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_race_description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_faction_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    type_faction_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_faction_description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_faction_short_description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    location_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    location_type: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    location_flag: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    top_location_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    is_singleton: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_blueprint_copy: Mapped[bool] = mapped_column(Boolean, nullable=False)
    blueprint_runs: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    blueprint_time_efficiency: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    blueprint_material_efficiency: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    type_adjusted_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    type_average_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    is_container: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_asset_safety_wrap: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_ship: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_office_folder: Mapped[bool] = mapped_column(Boolean, nullable=False)

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
    ceo_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    home_station_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    shares: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tax_rate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    url: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    war_eligible: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    wallets: Mapped[Optional[str]] = mapped_column(JSON, nullable=True)
    standings: Mapped[Optional[str]] = mapped_column(JSON, nullable=True)
    date_founded: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

class CorporationAssetsModel(BaseApp):
    __tablename__ = "corporation_assets"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    corporation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    item_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False)
    type_id: Mapped[int] = mapped_column(Integer, nullable=False)
    type_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_default_volume: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    type_repackaged_volume: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    type_volume: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    type_capacity: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    type_description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    container_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    ship_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_group_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    type_group_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_category_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    type_category_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_meta_group_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    type_race_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    type_race_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_race_description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_faction_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    type_faction_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_faction_description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type_faction_short_description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    location_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    location_type: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    location_flag: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    top_location_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    is_singleton: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_blueprint_copy: Mapped[bool] = mapped_column(Boolean, nullable=False)
    blueprint_runs: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    blueprint_time_efficiency: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    blueprint_material_efficiency: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    type_adjusted_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    type_average_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    is_container: Mapped[bool] = mapped_column(Boolean, nullable=True)
    is_asset_safety_wrap: Mapped[bool] = mapped_column(Boolean, nullable=True)
    is_ship: Mapped[bool] = mapped_column(Boolean, nullable=True)
    is_office_folder: Mapped[bool] = mapped_column(Boolean, nullable=True)

class CorporationStructuresModel(BaseApp):
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
    state_timer_end: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    state_timer_start: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    unachors_at: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    fuel_expires: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    reinforce_hour: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    next_reinforce_apply: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    next_reinforce_hour: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    acl_profile_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    services: Mapped[Optional[dict[str, str]]] = mapped_column(JSON, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

class CorporationMemberModel(BaseApp):
    __tablename__ = "corporation_members"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    corporation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    character_id: Mapped[int] = mapped_column(Integer, nullable=False)
    character_name: Mapped[str] = mapped_column(String, nullable=False)
    character_wallet_balance: Mapped[float] = mapped_column(Float, default=0.0)
    titles: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=True)  # List of titles
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())


class PublicStructuresModel(BaseApp):
    __tablename__ = "public_structures"

    # Uses the EVE structure_id as the primary key.
    structure_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    system_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    owner_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    type_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    structure_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    services: Mapped[Optional[list[dict[str, Any]]]] = mapped_column(JSON, nullable=True)

    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())


class PublicStructuresScanStateModel(BaseApp):
    __tablename__ = "public_structures_scan_state"

    # Singleton row (id=1) storing the resume cursor for the global scanner.
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cursor: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

class IndustryProfilesModel(BaseApp):
    __tablename__ = "industry_profiles"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    character_id: Mapped[int] = mapped_column(Integer, nullable=False)
    profile_name: Mapped[str] = mapped_column(String, nullable=False)
    is_default: Mapped[bool] = mapped_column(Boolean, default=False)
    region_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    system_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    facility_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    facility_type: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    facility_tax: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    scc_surcharge: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    facility_cost_bonus: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    location_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    location_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    location_type: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    manufacturing_cost_index: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    installation_cost_modifier: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    structure_rig_material_bonus: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    structure_rig_time_bonus: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    structure_rig_cost_bonus: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    material_efficiency_bonus: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    time_efficiency_bonus: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    rig_slot0_type_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    rig_slot1_type_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    rig_slot2_type_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        return f"<IndustryProfile(id={self.id}, profile_name='{self.profile_name}', character_id={self.character_id})>"

# --------------------------
# SDE
# --------------------------
class Bloodlines(BaseSde):
    __tablename__ = "bloodlines"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    charisma: Mapped[int] = mapped_column(Integer, nullable=False)
    corporationID: Mapped[int] = mapped_column(Integer, nullable=False)
    description: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
    iconID: Mapped[int] = mapped_column(Integer, nullable=True)
    intelligence: Mapped[int] = mapped_column(Integer, nullable=False)
    memory: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
    perception: Mapped[int] = mapped_column(Integer, nullable=False)
    raceID: Mapped[int] = mapped_column(Integer, nullable=False)
    willpower: Mapped[int] = mapped_column(Integer, nullable=False)

class Races(BaseSde):
    __tablename__ = "races"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    description: Mapped[str] = mapped_column(JSON, nullable=False)
    iconID: Mapped[int] = mapped_column(Integer, nullable=True)
    name: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
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
    graphicID: Mapped[int] = mapped_column(Integer, nullable=True)
    soundID: Mapped[int] = mapped_column(Integer, nullable=True)
    iconID: Mapped[int] = mapped_column(Integer, nullable=True)
    raceID: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    basePrice: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    marketGroupID: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    capacity: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    metaGroupID: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    variationParentTypeID: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    factionID: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    repackaged_volume: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

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
    repackaged_volume: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

class Categories(BaseSde):
    __tablename__ = "categories"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
    iconID: Mapped[int] = mapped_column(Integer, nullable=True)
    published: Mapped[bool] = mapped_column(Boolean, nullable=True)

class AgentsInSpace(BaseSde):
    __tablename__ = "agentsInSpace"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    dungeonID: Mapped[int] = mapped_column(Integer, nullable=False)
    solarSystemID: Mapped[int] = mapped_column(Integer, nullable=False)
    spawnPointID: Mapped[int] = mapped_column(Integer, nullable=False)
    typeID: Mapped[int] = mapped_column(Integer, nullable=False)

class AgentTypes(BaseSde):
    __tablename__ = "agentTypes"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)

class NpcCorporations(BaseSde):
    __tablename__ = "npcCorporations"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    ceoID: Mapped[int] = mapped_column(Integer, nullable=False)
    deleted: Mapped[bool] = mapped_column(Boolean, nullable=False)
    description: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
    divisions: Mapped[str] = mapped_column(JSON, nullable=False)
    enemyID: Mapped[Optional[list[int]]] = mapped_column(JSON, nullable=False)
    extent: Mapped[str] = mapped_column(String, nullable=False)
    hasPlayerPersonnelManager: Mapped[bool] = mapped_column(Boolean, nullable=False)
    initialPrice: Mapped[int] = mapped_column(Integer, nullable=False)
    memberLimit: Mapped[int] = mapped_column(Integer, nullable=False)
    minSecurity: Mapped[float] = mapped_column(Float, nullable=False)
    minimumJoinStanding: Mapped[float] = mapped_column(Float, nullable=False)
    name: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
    raceID: Mapped[Optional[int]] = mapped_column(Integer, nullable=False)
    sendCharTerminationMessage: Mapped[bool] = mapped_column(Boolean, nullable=False)
    shares: Mapped[int] = mapped_column(Integer, nullable=False)
    size: Mapped[str] = mapped_column(String, nullable=False)
    stationID: Mapped[int] = mapped_column(Integer, nullable=False)
    taxRate: Mapped[float] = mapped_column(Float, nullable=False)
    tickerName: Mapped[str] = mapped_column(String, nullable=False)
    uniqueName: Mapped[bool] = mapped_column(Boolean, nullable=False)
    allowedMemberRaces: Mapped[Optional[list[int]]] = mapped_column(JSON, nullable=False)
    corporationTrades: Mapped[dict[int, float]] = mapped_column(JSON, nullable=True)
    divisions: Mapped[str] = mapped_column(JSON, nullable=False)
    enemyID: Mapped[Optional[list[int]]] = mapped_column(JSON, nullable=False)
    factionID: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    friendID: Mapped[Optional[list[int]]] = mapped_column(JSON, nullable=False)
    iconID: Mapped[Optional[int]] = mapped_column(Integer, nullable=False)
    investors: Mapped[Optional[list[dict[int, int]]]] = mapped_column(JSON, nullable=False)
    lpOfferTables: Mapped[Optional[list[int]]] = mapped_column(JSON, nullable=False)
    mainActivityID: Mapped[Optional[int]] = mapped_column(Integer, nullable=False)
    raceID: Mapped[Optional[int]] = mapped_column(Integer, nullable=False)
    sizeFactor: Mapped[float] = mapped_column(Float, nullable=False)  
    solarSystemID: Mapped[int] = mapped_column(Integer, nullable=False)
    secondaryActivityID: Mapped[Optional[int]] = mapped_column(Integer, nullable=False)
    exchangeRates: Mapped[dict[int, float]] = mapped_column(JSON, nullable=True)

class Factions(BaseSde):
    __tablename__ = "factions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    corporationID: Mapped[int] = mapped_column(Integer, nullable=False)
    description: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
    flatLogo: Mapped[str] = mapped_column(String, nullable=True)
    flatLogoWithName: Mapped[str] = mapped_column(String, nullable=True)
    iconID: Mapped[int] = mapped_column(Integer, nullable=True)
    memberRaces: Mapped[Optional[list[int]]] = mapped_column(JSON, nullable=False)
    militiaCorporationID: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    name: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
    shortDescription: Mapped[Optional[dict[str, str]]] = mapped_column(JSON, nullable=False)
    sizeFactor: Mapped[float] = mapped_column(Float, nullable=False)
    solarSystemID: Mapped[int] = mapped_column(Integer, nullable=False)
    uniqueName: Mapped[bool] = mapped_column(Boolean, nullable=False)

class MarketGroups(BaseSde):
    __tablename__ = "marketGroups"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    description: Mapped[Optional[dict[str, str]]] = mapped_column(JSON, nullable=True)
    iconID: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    name: Mapped[Optional[dict[str, str]]] = mapped_column(JSON, nullable=True)
    parentGroupID: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    hasTypes: Mapped[bool] = mapped_column(Boolean, nullable=False)

class TypeMaterials(BaseSde):
    __tablename__ = "typeMaterials"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    materials: Mapped[list[dict[str, int]]] = mapped_column(JSON, nullable=False)

class NpcStations(BaseSde):
    __tablename__ = "npcStations"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    celestialIndex: Mapped[int] = mapped_column(Integer, nullable=False)
    operationID: Mapped[int] = mapped_column(Integer, nullable=False)
    orbitID: Mapped[int] = mapped_column(Integer, nullable=False)
    orbitIndex: Mapped[int] = mapped_column(Integer, nullable=False)
    ownerID: Mapped[int] = mapped_column(Integer, nullable=False)
    position: Mapped[dict[str, float]] = mapped_column(JSON, nullable=False)
    reprocessingEfficiency: Mapped[float] = mapped_column(Float, nullable=False)
    reprocessingHangarFlag: Mapped[str] = mapped_column(String, nullable=False)
    reprocessingStationsTake: Mapped[float] = mapped_column(Float, nullable=False)
    solarSystemID: Mapped[int] = mapped_column(Integer, nullable=False)
    typeID: Mapped[int] = mapped_column(Integer, nullable=False)
    useOperationName: Mapped[bool] = mapped_column(Boolean, nullable=False)

class Blueprints(BaseSde):
    __tablename__ = "blueprints"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    blueprintTypeID: Mapped[int] = mapped_column(Integer, nullable=False)
    maxProductionLimit: Mapped[int] = mapped_column(Integer, nullable=False)
    activities: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

class MapConstellations(BaseSde):
    __tablename__ = "mapConstellations"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    factionID: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    name: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
    position: Mapped[dict[str, float]] = mapped_column(JSON, nullable=False)
    regionID: Mapped[int] = mapped_column(Integer, nullable=False)
    solarSystemIDs: Mapped[Optional[list[int]]] = mapped_column(JSON, nullable=True)
    wormholeClassID: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

class MapRegions(BaseSde):
    __tablename__ = "mapRegions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    constellationIDs: Mapped[Optional[list[int]]] = mapped_column(JSON, nullable=True)
    description: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
    factionID: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    name: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
    nebulaID: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    position: Mapped[dict[str, float]] = mapped_column(JSON, nullable=False)
    wormholeClassID: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

class MapSolarSystems(BaseSde):
    __tablename__ = "mapSolarSystems"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    border: Mapped[bool] = mapped_column(Boolean, nullable=False)
    constellationID: Mapped[int] = mapped_column(Integer, nullable=False)
    hub: Mapped[bool] = mapped_column(Boolean, nullable=False)
    international: Mapped[bool] = mapped_column(Boolean, nullable=False)
    luminosity: Mapped[float] = mapped_column(Float, nullable=False)
    name: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
    planetIDs: Mapped[Optional[list[int]]] = mapped_column(JSON, nullable=True)
    position: Mapped[dict[str, float]] = mapped_column(JSON, nullable=False)
    position2D: Mapped[dict[str, float]] = mapped_column(JSON, nullable=False)
    radius: Mapped[float] = mapped_column(Float, nullable=False)
    regionID: Mapped[int] = mapped_column(Integer, nullable=False)
    regional: Mapped[bool] = mapped_column(Boolean, nullable=False)
    securityClass: Mapped[str] = mapped_column(String, nullable=False)
    securityStatus: Mapped[float] = mapped_column(Float, nullable=False)
    starID: Mapped[int] = mapped_column(Integer, nullable=False)
    stargateIDs: Mapped[Optional[list[int]]] = mapped_column(JSON, nullable=True)
    corridor: Mapped[bool] = mapped_column(Boolean, nullable=False)
    fringe: Mapped[bool] = mapped_column(Boolean, nullable=False)
    wormholeClassID: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    visualEffect: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    disallowedAnchorCategories: Mapped[Optional[list[int]]] = mapped_column(JSON, nullable=True)
    disallowedAnchorGroups: Mapped[Optional[list[int]]] = mapped_column(JSON, nullable=True)
    factionID: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

class StationOperations(BaseSde):
    __tablename__ = "stationOperations"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    activityID: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    border: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    corridor: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    description: Mapped[Optional[dict[str, str]]] = mapped_column(JSON, nullable=True)
    fringe: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    hub: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    manufacturingFactor: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    operationName: Mapped[Optional[dict[str, str]]] = mapped_column(JSON, nullable=True)
    ratio: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    researchFactor: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    services: Mapped[Optional[list[int]]] = mapped_column(JSON, nullable=True)
    stationTypes: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON, nullable=True)

class StationServices(BaseSde):
    __tablename__ = "stationServices"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    serviceName: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
    description: Mapped[Optional[dict[str, str]]] = mapped_column(JSON, nullable=True)
