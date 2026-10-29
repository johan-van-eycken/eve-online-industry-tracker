import logging
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from sqlalchemy.exc import SQLAlchemyError

from eve_online_industry_tracker.application import market_fees
from eve_online_industry_tracker.config.admin_settings import AdminSettingInvalid
from eve_online_industry_tracker.infrastructure.database_manager import DatabaseManager
from eve_online_industry_tracker.config.config_manager import ConfigManager
from eve_online_industry_tracker.infrastructure.esi_client import ESI_REQUEST_ERRORS, ESIClient, esi_get_back_to_known
from eve_online_industry_tracker.infrastructure.esi_service import ESIService
from eve_online_industry_tracker.infrastructure.sde.types import asset_volumes
from eve_online_industry_tracker.infrastructure.models import CharacterModel, CharacterWalletJournalModel \
    , CharacterWalletTransactionsModel, CharacterMarketOrdersModel, CharacterAssetsModel \
    , CharacterIndustryJobsModel \
    , NpcCorporations, NpcStations, Bloodlines, Races, Types \
    , Groups, Categories, Factions

from eve_online_industry_tracker.application.characters.asset_provenance import (
    build_market_price_map,
    build_cost_map_for_assets,
    build_invention_cost_per_run_by_blueprint_type,
    RealizedInventionRefresh,
    invention_cost_source_for,
    industry_job_material_type_ids,
    load_owner_invention_evidence,
    resolve_industry_job_cost_snapshot,
    backfill_historical_market_costs,
    add_consumed_input_lots,
    subtract_consumed_input_lots,
)
from eve_online_industry_tracker.application.characters.corp_transfer_costs import (
    character_job_unit_costs,
    recosted_product_type_ids,
    restamp_blended_corp_transfer_lots,
)
from eve_online_industry_tracker.infrastructure.esi_versions import (
    ESI_CHAR_PUBLIC,
    ESI_CHAR_WALLET,
    ESI_CHAR_WALLET_JOURNAL,
    ESI_CHAR_WALLET_TRANSACTIONS,
    ESI_CHAR_STANDINGS,
    ESI_CHAR_SKILLS,
    ESI_CHAR_SKILLQUEUE,
    ESI_CHAR_IMPLANTS,
    ESI_CHAR_ORDERS,
    ESI_CHAR_ASSETS,
    ESI_CHAR_ASSETS_NAMES,
    ESI_CHAR_BLUEPRINTS,
    ESI_CHAR_INDUSTRY_JOBS,
    ESI_CORP_PUBLIC,
    ESI_MARKETS_PRICES,
)

from eve_online_industry_tracker.application.characters.asset_history import (
    backfill_wallet_buy_acquisitions,
    build_historical_input_cost_lookup,
    clear_historical_backfill,
    lookup_historical_blueprint_provenance,
    record_historical_acquisition,
    sync_asset_history,
    enrich_assets_with_acquisition_costs,
)


JITA_4_4_STATION_ID = 60003760
SKILL_ID_ACCOUNTING = 16622
SKILL_ID_BROKER_RELATIONS = 3446


def industry_job_location_id(job: Dict[str, Any]) -> Optional[int]:
    """The facility location of an ESI industry job: 'station_id' on
    /characters/{id}/industry/jobs, 'location_id' on the corporation route."""
    value = job.get("location_id")
    if value is None:
        value = job.get("station_id")
    return value



class CharacterSaveError(Exception):
    """Character.save_character could not write the profile (the app DB
    failed; the session is rolled back). An Exception, as before, so the
    callers that catch Exception still do."""


class Character:
    """Ingame entity of a character."""

    _ESI_INIT_LOCK = threading.Lock()

    def __init__(self, 
                 cfgManager: ConfigManager, 
                 db_oauth: DatabaseManager,
                 db_app: DatabaseManager,
                 db_sde: DatabaseManager,
                 character_name: str, 
                 is_main: bool = False,
                 is_corp_director: bool = False,
                 refresh_token: Optional[str] = None,
                 industry_enabled: bool = True,
                 admin_settings: Any = None,
        ) -> None:
        try:
            # Private properties
            # The admin settings (industry/* market fee rates); None: the schema defaults.
            self._admin_settings = admin_settings
            self._cfgManager = cfgManager
            self._cfg = cfgManager.all()
            self._db_oauth = db_oauth
            self._db_app = db_app
            self._db_sde = db_sde
            self._refresh_token = refresh_token
            
            # Public properties
            self.character_id: Optional[int] = None
            self.character_name = character_name
            self.image_url: Optional[str] = None
            self.birthday: Optional[str] = None
            self.bloodline_id: Optional[int] = None
            self.bloodline: Optional[str] = None
            self.race_id: Optional[int] = None
            self.race: Optional[str] = None
            self.gender: Optional[str] = None
            self.corporation_id: Optional[int] = None
            self.corporation_name: Optional[str] = None
            self.description: Optional[str] = None
            self.security_status: Optional[float] = None
            self.is_main = is_main
            self.is_corp_director = is_corp_director
            self.industry_enabled = industry_enabled
            self.updated_at: Optional[datetime] = None
            
            self.wallet_balance: Optional[float] = None
            self.skills: Optional[Dict[str, Any]] = None
            self.standings: Optional[List[Dict[str, str]]] = None
            self.implants: Optional[List[int]] = None
            self.market_fees: Optional[Dict[str, Any]] = None
            self.wallet_journal: Optional[List[Dict[str, Any]]] = None
            self.wallet_transactions: Optional[List[Dict[str, Any]]] = None
            self.reprocessing_skills: Optional[Dict[int, int]] = None  # {skill type id: level}
            self.market_orders: Optional[List[Dict[str, Any]]] = None
            self.assets: Optional[List[Dict[str, Any]]] = None

            # Lazy-init ESI. Creating the ESI client can trigger token handling/network.
            self._esi_client: Optional[ESIClient] = None
            self._esi_service: Optional[ESIService] = None

            # Intentionally do NOT call refresh methods here.
            # Constructors should be cheap; refresh is handled explicitly by managers/bootstrap.
        except Exception as e:
            error_message = f"Failed to initialize Character '{character_name}'. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    def ensure_esi(self) -> None:
        """Ensure ESI client/service are initialized.

        This is intentionally NOT done in __init__ to keep construction cheap.
        """
        if self._esi_client is not None and self._esi_service is not None:
            return

        # Building the client loads the tokens or runs the OAuth registration: once, not once per thread.
        # shortcut: one lock for all characters, so a first use waits on another pilot's registration;
        # per-character locks if registrations ever run while other pilots refresh.
        with Character._ESI_INIT_LOCK:
            if self._esi_client is not None and self._esi_service is not None:
                return
            client = ESIClient(
                self._cfgManager,
                self._db_oauth,
                self.character_name,
                self.is_main,
                self.is_corp_director,
                self._refresh_token,
            )
            # The admin ESI settings (pacing, retries, cache TTLs) from the first request on.
            self._esi_service = ESIService(client, admin_settings=getattr(self, "_admin_settings", None))
            self.character_id = client.character_id
            self._esi_client = client

    @property
    def esi_client(self) -> ESIClient:
        self.ensure_esi()
        assert self._esi_client is not None
        return self._esi_client

    @property
    def esi_service(self) -> ESIService:
        self.ensure_esi()
        assert self._esi_service is not None
        return self._esi_service
    
    # -------------------
    # Get Character Model as Dict
    # -------------------
    def get_character(self) -> Dict[str, Any]:
        """Convert character model to Dict, serializing all nested objects."""
        def serialize_model_list(model_list, model_cls):
            if not model_list:
                return []
            # If already dicts, return as is
            if isinstance(model_list[0], dict):
                return model_list
            return [
                {col: getattr(obj, col) for col in model_cls.__table__.columns.keys()}
                for obj in model_list
            ]

        return {
            "character_id": self.character_id,
            "character_name": self.character_name,
            "image_url": self.image_url,
            "birthday": self.birthday,
            "bloodline_id": self.bloodline_id,
            "bloodline": self.bloodline,
            "race_id": self.race_id,
            "race": self.race,
            "gender": self.gender,
            "corporation_id": self.corporation_id,
            "corporation_name": self.corporation_name,
            "description": self.description,
            "security_status": self.security_status,
            "wallet_balance": self.wallet_balance,
            "is_main": self.is_main,
            "is_corp_director": self.is_corp_director,
            "industry_enabled": self.industry_enabled,
            "skills": self.skills if isinstance(self.skills, (dict, list)) else {},
            "reprocessing_skills": self.reprocessing_skills if isinstance(self.reprocessing_skills, dict) else {},
            "standings": self.standings if isinstance(self.standings, list) else [],
            "implants": self.implants if isinstance(self.implants, list) else [],
            "market_fees": self.market_fees if isinstance(self.market_fees, (dict, list)) else {},
            "wallet_journal": serialize_model_list(self.wallet_journal, CharacterWalletJournalModel),
            "wallet_transactions": serialize_model_list(self.wallet_transactions, CharacterWalletTransactionsModel),
            "market_orders": serialize_model_list(self.market_orders, CharacterMarketOrdersModel),
            "assets": serialize_model_list(self.assets, CharacterAssetsModel),
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    # -------------------
    # Get Wallet Balance
    # -------------------
    def get_wallet_balance(self) -> Dict[str, Any]:
        """Return the character's wallet balance."""
        return {"character_name": self.character_name, "character_id": self.character_id, "wallet_balance": self.wallet_balance if self.wallet_balance is not None else 0.0}

    # -------------------
    # Get Assets
    # -------------------
    def get_assets(self) -> Dict[str, Any]:
        """Return the character's assets."""
        return {"character_name": self.character_name, "character_id": self.character_id, "assets": self.assets if self.assets is not None else []}

    # -------------------
    # Get Market Orders
    # -------------------
    def get_market_orders(self) -> Dict[str, Any]:
        """Return the character's market orders."""
        return {"character_name": self.character_name, "character_id": self.character_id, "corporation_id": self.corporation_id, "market_orders": self.market_orders if self.market_orders is not None else []}

    # -------------------
    # Save Character
    # -------------------
    def save_character(self) -> None:
        """Save the current character's profile data to the database."""
        try:
            character_record = (self._db_app.session.query(CharacterModel).filter_by(character_name=self.character_name).first())

            if not character_record:
                # Create new record if it doesn't exist
                character_record = CharacterModel(
                    character_name=self.character_name, 
                    character_id=self.character_id, 
                    is_main=self.is_main, 
                    is_corp_director=self.is_corp_director
                )
                self._db_app.session.add(character_record)

            # Dynamically update based on CharacterModel's columns
            for column in CharacterModel.__table__.columns.keys():
                if hasattr(self, column):
                    value = getattr(self, column)
                    if column == "skills" and isinstance(value, (dict, list)):
                        value = json.dumps(value)  # convert dict → string
                    elif column == "standings" and isinstance(value, (dict, list)):
                        value = json.dumps(value)  # convert dict → string
                    elif column == "implants" and isinstance(value, (dict, list)):
                        value = json.dumps(value)  # convert list[int] → string
                    elif column == "market_fees" and isinstance(value, (dict, list)):
                        value = json.dumps(value)
                    setattr(character_record, column, value)

            character_record.updated_at = datetime.now(timezone.utc)
            self._db_app.session.commit()
            logging.debug(f"Character '{self.character_name}' saved to database.")
    
        except SQLAlchemyError as e:
            error_message = f"Failed to save character '{self.character_name}' to database. Error: {str(e)}"
            logging.error(error_message)
            try:
                self._db_app.session.rollback()
            except SQLAlchemyError as rollback_exc:
                logging.warning("Character %s: rollback after the failed save failed: %s", self.character_name,
                                rollback_exc)
            raise CharacterSaveError(error_message) from e

    # -------------------
    # Save Wallet Journal
    # -------------------
    def save_wallet_journal(self, journal_entries: List[Dict[str, Any]]) -> None:
        """Save wallet journal entries to the database."""
        if not journal_entries:
            logging.debug(f"No wallet journal entries to save for {self.character_name}.")
            return

        try:
            self.wallet_journal = []
            for entry in journal_entries:
                new_entry = CharacterWalletJournalModel(**entry)
                self.wallet_journal.append(new_entry)

            if self.wallet_journal:
                try:
                    self._db_app.session.bulk_save_objects(self.wallet_journal)
                    self._db_app.session.commit()
                except Exception as bulk_err:
                    self._db_app.session.rollback()
                    # Fall back to one-by-one insert, skipping duplicates.
                    saved = 0
                    for obj in self.wallet_journal:
                        try:
                            self._db_app.session.add(obj)
                            self._db_app.session.commit()
                            saved += 1
                        except SQLAlchemyError:  # a duplicate row: skipped, counted below
                            self._db_app.session.rollback()
                    logging.debug(
                        f"Wallet journal fallback insert: {saved}/{len(self.wallet_journal)} for {self.character_name} "
                        f"(bulk failed: {bulk_err.__class__.__name__})."
                    )
                    return
                logging.debug(f"Bulk wallet journal entries saved ({len(self.wallet_journal)}) for {self.character_name}.")
            else:
                logging.debug(f"No new wallet journal entries to save for {self.character_name}.")

        except Exception as e:
            error_message = f"Failed to save wallet journal entries for {self.character_name}. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    # -------------------
    # Save Wallet Transactions
    # -------------------
    def save_wallet_transactions(self, transactions: List[Dict[str, Any]]) -> None:
        """Save wallet transactions to the database."""
        if not transactions:
            logging.debug(f"No wallet transactions to save for {self.character_name}.")
            return

        try:
            self.wallet_transactions = []
            for transaction in transactions:
                new_trans = CharacterWalletTransactionsModel(**transaction)
                self.wallet_transactions.append(new_trans)
            
            if self.wallet_transactions:
                self._db_app.session.bulk_save_objects(self.wallet_transactions)
                self._db_app.session.commit()
            else:
                logging.debug(f"No new wallet transactions to save for {self.character_name}.")
            
            logging.debug(f"Bulk wallet transactions saved ({len(self.wallet_transactions)}) for {self.character_name}.")
        
        except Exception as e:
            error_message = f"Failed to save wallet transactions for {self.character_name}. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    # -------------------
    # Save Market Orders
    # -------------------
    def save_market_orders(self, market_orders: List[Dict[str, Any]]) -> None:
        """Save market orders to the database."""
        if not market_orders:
            logging.debug(f"No market orders to save for {self.character_name}.")
            return

        try:
            model_objects = []
            for order in market_orders:
                new_order = CharacterMarketOrdersModel(**order)
                model_objects.append(new_order)

            if model_objects:
                # Delete existing orders for this character and add new ones
                self._db_app.session.query(CharacterMarketOrdersModel).filter_by(character_id=self.character_id).delete()
                self._db_app.session.bulk_save_objects(model_objects)
                self._db_app.session.commit()
            else:
                logging.debug(f"No new market orders to save for {self.character_name}.")

            self.market_orders = market_orders

            logging.debug(f"Market orders saved ({len(model_objects)}) for {self.character_name}.")
        except Exception as e:
            error_message = f"Failed to save market orders for {self.character_name}. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    # -------------------
    # Save Assets
    # -------------------
    def save_assets(self, asset_list: List[Dict[str, Any]]) -> None:
        """Save assets to the database."""
        if not asset_list:
            logging.debug(f"No assets to save for {self.character_name}.")
            return

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                enriched_assets = enrich_assets_with_acquisition_costs(
                    app_session=self._db_app.session,
                    owner_kind="character",
                    owner_id=int(self.character_id),
                    asset_list=asset_list,
                )

                self.assets = []
                for asset in enriched_assets:
                    new_asset = CharacterAssetsModel(**asset)
                    self.assets.append(new_asset)

                if self.assets:
                    sync_asset_history(
                        app_session=self._db_app.session,
                        owner_kind="character",
                        owner_id=int(self.character_id),
                        asset_rows=enriched_assets,
                    )
                    # Delete existing assets for this character and add new ones
                    self._db_app.session.query(CharacterAssetsModel).filter_by(character_id=self.character_id).delete()
                    self._db_app.session.bulk_save_objects(self.assets)
                    self._db_app.session.commit()
                else:
                    logging.debug(f"No new assets to save for {self.character_name}.")

                logging.debug(f"Assets saved ({len(self.assets)}) for {self.character_name}.")
                return

            except Exception as e:
                self._db_app.session.rollback()
                if "database is locked" in str(e) and attempt < max_retries:
                    wait = attempt * 2
                    logging.warning(f"Database locked saving assets for {self.character_name}, retry {attempt}/{max_retries} in {wait}s")
                    time.sleep(wait)
                    continue
                error_message = f"Failed to save assets for {self.character_name}. Error: {str(e)}"
                logging.error(error_message)
                raise Exception(error_message)
    
    # -------------------
    # Refresh All
    # -------------------
    def refresh_all(self) -> None:
        """Refresh all data for the current character."""
        try:
            self.ensure_esi()

            # Phase 1: profile must run first (sets corporation_id, standings, etc.)
            self.refresh_profile()

            # Phase 2: skills + implants are independent of each other
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(self.refresh_skills), pool.submit(self.refresh_implants)]
                for f in as_completed(futures):
                    f.result()

            # Phase 3: market_fees depends on profile + skills
            self.refresh_market_fees()

            # Phase 4: remaining methods are independent — run in parallel
            parallel_methods = [
                self.refresh_wallet_journal,
                self.refresh_wallet_transactions,
                self.refresh_market_orders,
                self.refresh_assets,
                self.refresh_industry_jobs,
            ]
            with ThreadPoolExecutor(max_workers=5) as pool:
                futures = [pool.submit(m) for m in parallel_methods]
                for f in as_completed(futures):
                    f.result()

            logging.debug(f"All data successfully refreshed for {self.character_name}.")
        except Exception as e:
            error_message = f"Failed to refresh all data for {self.character_name}. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    def _get_skill_level(self, *, skill_id: int, skill_name: str | None = None) -> int:
        skills_obj = self.skills or {}
        skills_list = (skills_obj.get("skills") or []) if isinstance(skills_obj, dict) else []
        if not isinstance(skills_list, list):
            skills_list = []

        for s in skills_list:
            if not isinstance(s, dict):
                continue
            try:
                if int(s.get("skill_id") or -1) == int(skill_id):
                    return int(s.get("trained_skill_level") or 0)
            except (TypeError, ValueError) as exc:
                logging.warning("Unreadable skill row for %s skipped (%s): %r", self.character_name, exc, s)
                continue

        if skill_name:
            skill_name_norm = str(skill_name).strip().lower()
            for s in skills_list:
                if not isinstance(s, dict):
                    continue
                try:
                    if str(s.get("skill_name") or "").strip().lower() == skill_name_norm:
                        return int(s.get("trained_skill_level") or 0)
                except (TypeError, ValueError) as exc:
                    logging.warning("Unreadable skill row for %s skipped (%s): %r", self.character_name, exc, s)
                    continue

        return 0

    def _get_standing(self, *, from_type: str, from_id: int) -> float:
        standings_list = self.standings if isinstance(self.standings, list) else []
        for s in standings_list:
            if not isinstance(s, dict):
                continue
            try:
                if str(s.get("from_type")) != str(from_type):
                    continue
                if int(s.get("from_id") or -1) != int(from_id):
                    continue
                return float(s.get("standing") or 0.0)
            except (TypeError, ValueError) as exc:
                logging.warning("Unreadable standing row for %s skipped (%s): %r", self.character_name, exc, s)
                continue
        return 0.0

    def refresh_market_fees(self) -> None:
        """Compute and persist character-specific market fees (Jita 4-4 only).

        Uses character skills + standings plus SDE station ownership, with the
        admin settings industry/* through application.market_fees (the
        producer's formula). Without loaded skills the rates are None with
        an unknown_reason (an untrained character is not assumed)."""
        try:
            fee_settings = market_fees.MarketFeeSettings.from_admin(getattr(self, "_admin_settings", None))
            skills_list = (self.skills or {}).get("skills") if isinstance(self.skills, dict) else None
            if not isinstance(skills_list, list) or not skills_list:
                self.market_fees = {
                    "jita_4_4": {
                        "station_id": int(JITA_4_4_STATION_ID),
                        "rates": {"sales_tax_fraction": None, "broker_fee_fraction": None},
                        "unknown_reason": "the character's skills are not loaded (missing scope or not refreshed yet)",
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                        "source": "computed",
                    }
                }
                logging.warning("Market fees of %s (%s) unknown: skills not loaded", self.character_name,
                                self.character_id)
                self.save_character()
                return

            accounting_level = self._get_skill_level(skill_id=SKILL_ID_ACCOUNTING, skill_name="Accounting")
            broker_rel_level = self._get_skill_level(skill_id=SKILL_ID_BROKER_RELATIONS, skill_name="Broker Relations")
            sales_tax_fraction = market_fees.sales_tax_fraction(fee_settings, accounting_level=accounting_level)

            owner_corp_id: int | None = None
            owner_faction_id: int | None = None
            try:
                station = (
                    self._db_sde.session.query(NpcStations)
                    .filter_by(id=int(JITA_4_4_STATION_ID))
                    .first()
                )
                if station is not None:
                    owner_corp_id = int(station.ownerID)
            except SQLAlchemyError as exc:
                logging.warning("Market fees of %s: the Jita 4-4 station owner could not be read from the SDE (%s: %s); "
                                "its corporation and faction standings count as 0", self.character_name,
                                type(exc).__name__, exc)
                owner_corp_id = None

            if owner_corp_id is not None:
                try:
                    corp = (
                        self._db_sde.session.query(NpcCorporations)
                        .filter_by(id=int(owner_corp_id))
                        .first()
                    )
                    if corp is not None and corp.factionID is not None:
                        owner_faction_id = int(corp.factionID)
                except SQLAlchemyError as exc:
                    logging.warning("Market fees of %s: the faction of the Jita 4-4 owner %s could not be read from "
                                    "the SDE (%s: %s); its faction standing counts as 0", self.character_name,
                                    owner_corp_id, type(exc).__name__, exc)
                    owner_faction_id = None

            faction_standing = (
                self._get_standing(from_type="faction", from_id=int(owner_faction_id))
                if owner_faction_id is not None
                else 0.0
            )
            corp_standing = (
                self._get_standing(from_type="npc_corp", from_id=int(owner_corp_id))
                if owner_corp_id is not None
                else 0.0
            )
            broker_fee_fraction = market_fees.broker_fee_fraction(
                fee_settings, broker_relations_level=broker_rel_level,
                faction_standing=faction_standing, corp_standing=corp_standing)

            self.market_fees = {
                "jita_4_4": {
                    "station_id": int(JITA_4_4_STATION_ID),
                    "owner_corp_id": int(owner_corp_id) if owner_corp_id is not None else None,
                    "owner_faction_id": int(owner_faction_id) if owner_faction_id is not None else None,
                    "skills": {
                        "accounting_level": int(accounting_level),
                        "broker_relations_level": int(broker_rel_level),
                    },
                    "standings": {
                        "faction_standing": float(faction_standing),
                        "corp_standing": float(corp_standing),
                    },
                    "rates": {
                        "base_sales_tax_fraction": float(fee_settings.base_sales_tax_rate),
                        "sales_tax_fraction": float(sales_tax_fraction),
                        "broker_fee_fraction": float(broker_fee_fraction),
                    },
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "source": "computed",
                }
            }

            self.save_character()
        except AdminSettingInvalid as exc:
            # An invalid industry/* fee setting: the fees are unknown, naming it (not a silent default).
            logging.error("Market fees of %s (%s) unknown: %s", self.character_name, self.character_id, exc.message)
            self.market_fees = {"jita_4_4": {"station_id": int(JITA_4_4_STATION_ID),
                                             "rates": {"sales_tax_fraction": None, "broker_fee_fraction": None},
                                             "unknown_reason": exc.message,
                                             "updated_at": datetime.now(timezone.utc).isoformat(),
                                             "source": "computed"}}
        except (SQLAlchemyError, CharacterSaveError, *ESI_REQUEST_ERRORS) as e:   # a bug propagates
            logging.warning(
                "Failed to compute market fees for %s (%s): %s",
                self.character_name,
                self.character_id,
                str(e),
            )

    # -------------------
    # Refresh Profile
    # -------------------
    def refresh_profile(self) -> None:
        """Fetch and update the character's profile data from ESI. Enrich with SDE data."""
        try:
            self.ensure_esi()
            profile_data = self._esi_client.esi_get(ESI_CHAR_PUBLIC.format(character_id=self.character_id))
            corporation_data = self._esi_client.esi_get(ESI_CORP_PUBLIC.format(corporation_id=profile_data.get('corporation_id')))
            wallet_balance = self._esi_client.esi_get(ESI_CHAR_WALLET.format(character_id=self.character_id))
            standings_data = self._esi_client.esi_get(ESI_CHAR_STANDINGS.format(character_id=self.character_id))

            # Load additional details from the SDE database
            race_data = self._db_sde.session.query(Races).filter_by(id=profile_data.get("race_id")).first()
            bloodline_data = self._db_sde.session.query(Bloodlines).filter_by(id=profile_data.get("bloodline_id")).first()
            faction_data = self._db_sde.load_df("factions", json_columns=["name"])
            npccorp_data = self._db_sde.load_df("npcCorporations", json_columns=["name"])

            # Lookup tables
            def get_name(name, language):
                if isinstance(name, dict):
                    return name.get(language, next(iter(name.values()), "Unknown"))
                return name

            faction_lookup = {row['id']: get_name(row['name'], self._cfg["app"]["language"]) for _, row in faction_data.iterrows()}
            npccorp_lookup = {row['id']: get_name(row['name'], self._cfg["app"]["language"]) for _, row in npccorp_data.iterrows()}

            # Update runtime properties
            self.wallet_balance = wallet_balance
            self.image_url = f"https://images.evetech.net/characters/{self.character_id}/portrait?size=128"
            self.birthday = profile_data["birthday"]
            self.bloodline_id = profile_data["bloodline_id"]
            self.bloodline = bloodline_data.name[self._db_sde.language] if bloodline_data else None
            self.race_id = profile_data["race_id"]
            self.race = race_data.name[self._db_sde.language] if race_data else None
            self.gender = profile_data.get("gender")
            self.corporation_id = profile_data.get("corporation_id")
            self.corporation_name = corporation_data.get("name")
            self.description = profile_data.get("description")
            self.security_status = profile_data.get("security_status")

            # Additional properties
            self.standings = []
            for s in standings_data:
                entry = {
                    "from_id": str(s.get("from_id")),
                    "from_type": s.get("from_type"),
                    "standing": str(s.get("standing"))
                }
                # Add name if available
                if s.get("from_type") == "faction":
                    entry["name"] = faction_lookup.get(s.get("from_id"), "Unknown Faction")
                elif s.get("from_type") == "npc_corp":
                    entry["name"] = npccorp_lookup.get(s.get("from_id"), "Unknown Corporation")
                else:
                    entry["name"] = ""
                self.standings.append(entry)

            # Save to database
            self.save_character()

            # Best-effort: refresh market fees now that standings are updated.
            # If skills are not loaded yet, this will compute partial data and be overwritten later.
            self.refresh_market_fees()

            logging.debug(f"Profile data successfully refreshed for {self.character_name}.")
        
        except Exception as e:
            error_message = f"Failed to refresh profile data for {self.character_name}. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    # -------------------
    # Refresh Wallet Balance
    # -------------------
    def refresh_wallet_balance(self) -> None:
        """Fetch and update the character's wallet balance from ESI."""
        try:
            self.ensure_esi()
            self.wallet_balance = self._esi_client.esi_get(ESI_CHAR_WALLET.format(character_id=self.character_id))

            # Save to database
            self.save_character()

            logging.debug(f"Wallet balance successfully refreshed for {self.character_name}. Balance: {self.wallet_balance:.2f}")
        
        except Exception as e:
            error_message = f"Failed to refresh wallet balance for {self.character_name}. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)
    
    # -------------------
    # Refresh Wallet Journal
    # -------------------
    def refresh_wallet_journal(self) -> None:
        """Fetch and update the character's wallet journal from ESI. Enrich with SDE and ESI data."""
        try:
            self.ensure_esi()
            journal_entries = self._esi_client.esi_get(
                ESI_CHAR_WALLET_JOURNAL.format(character_id=self.character_id), paginate=True
            )

            known_ids = {
                row[0]
                for row in self._db_app.session.query(CharacterWalletJournalModel.wallet_journal_id)
                .filter_by(character_id=self.character_id)
            }
            new_journal_entries = [
                entry for entry in journal_entries
                if entry.get("id") is not None and entry.get("id") not in known_ids
            ]

            # Step 1: Collect unique party IDs
            party_ids = set()
            for entry in new_journal_entries:
                for key in ("first_party_id", "second_party_id", "tax_receiver_id"):
                    pid = entry.get(key)
                    if pid:
                        party_ids.add(pid)

            # Step 2: Resolve names via batch endpoint (handles all ID ranges)
            party_names = self._resolve_client_names(party_ids)

            # Step 3: Assign names to journal entries
            new_entries = []
            for entry in new_journal_entries:
                for key, name_key in [
                    ("first_party_id", "first_party_name"),
                    ("second_party_id", "second_party_name"),
                    ("tax_receiver_id", "tax_receiver_name"),
                ]:
                    pid = entry.get(key)
                    entry[name_key] = party_names.get(pid)

                new_entry = {
                    "character_id": self.character_id,
                    "wallet_journal_id": entry.get("id", None),
                    "amount": entry.get("amount", 0.0),
                    "balance": entry.get("balance", 0.0),
                    "context_id": entry.get("context_id", None),
                    "context_id_type": entry.get("context_id_type", None),
                    "date": entry.get("date"),
                    "description": entry.get("description", None),
                    "reason": entry.get("reason", None),
                    "ref_type": entry.get("ref_type", None),
                    "tax": entry.get("tax", 0.0),
                    "tax_receiver_id": entry.get("tax_receiver_id", None),
                    "tax_receiver_name": entry.get("tax_receiver_name", None),
                    "first_party_id": entry.get("first_party_id", None),
                    "first_party_name": entry.get("first_party_name", None),
                    "second_party_id": entry.get("second_party_id", None),
                    "second_party_name": entry.get("second_party_name", None)
                }
                new_entries.append(new_entry)

            self.save_wallet_journal(new_entries)
            self._backfill_wallet_journal_party_names()

            # Assign loaded entries to self.wallet_journal for runtime access
            character_wallet_journal = (self._db_app.session.query(CharacterWalletJournalModel).filter_by(character_id=self.character_id).all())
            self.wallet_journal = [
                {col: getattr(entry, col) for col in CharacterWalletJournalModel.__table__.columns.keys()}
                for entry in character_wallet_journal
            ]
            logging.debug(f"Wallet journal successfully refreshed for {self.character_name}. New entries: {len(new_entries)}")

        except Exception as e:
            error_message = f"Failed to refresh wallet journal for {self.character_name}. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)
    
    # -------------------
    # Refresh Wallet Transactions
    # -------------------
    def refresh_wallet_transactions(self) -> None:
        try:
            self.ensure_esi()
            known_ids = {
                row[0]
                for row in self._db_app.session.query(CharacterWalletTransactionsModel.transaction_id)
                .filter_by(character_id=self.character_id)
            }
            new_transaction_entries = esi_get_back_to_known(
                self._esi_client.esi_get, ESI_CHAR_WALLET_TRANSACTIONS.format(character_id=self.character_id), known_ids,
            )

            # Step 1: Collect unique client IDs
            client_ids = set()
            for entry in new_transaction_entries:
                cid = entry.get("client_id")
                if cid:
                    client_ids.add(cid)

            # Step 2: Resolve names via batch endpoint (handles all ID ranges)
            client_names = self._resolve_client_names(client_ids)

            # Step 3: Assign names to transaction entries
            new_entries = []
            for entry in new_transaction_entries:
                cid = entry.get("client_id")
                entry["client_name"] = client_names.get(cid)

                type_id = entry.get("type_id")
                type = self._db_sde.session.query(Types).filter_by(id=type_id).first()
                entry["type_name"] = type.name[self._db_sde.language] if type else None
                group = self._db_sde.session.query(Groups).filter_by(id=type.groupID).first() if type else None
                entry["type_group_id"] = group.id if group else None
                entry["type_group_name"] = group.name[self._db_sde.language] if group else None
                category = self._db_sde.session.query(Categories).filter_by(id=group.categoryID).first() if group else None
                entry["type_category_id"] = category.id if category else None
                entry["type_category_name"] = category.name[self._db_sde.language] if category else None

                new_entry = {
                    "character_id": self.character_id,
                    "transaction_id": entry.get("transaction_id", None),
                    "client_id": entry.get("client_id", None),
                    "client_name": entry.get("client_name", None),
                    "date": entry.get("date"),
                    "is_buy": entry.get("is_buy", False),
                    "is_personal": entry.get("is_personal", False),
                    "journal_ref_id": entry.get("journal_ref_id", None),
                    "location_id": entry.get("location_id", None),
                    "quantity": entry.get("quantity", 0),
                    "type_id": entry.get("type_id", None),
                    "type_name": entry.get("type_name", None),
                    "type_group_id": entry.get("type_group_id", None),
                    "type_group_name": entry.get("type_group_name", None),
                    "type_category_id": entry.get("type_category_id", None),
                    "type_category_name": entry.get("type_category_name", None),
                    "unit_price": entry.get("unit_price", 0.0),
                    "total_price": entry.get("unit_price", 0.0) * entry.get("quantity", 1)
                }
                new_entries.append(new_entry)

            self.save_wallet_transactions(new_entries)
            self._backfill_wallet_transaction_client_names()

            # Assign loaded entries to self.wallet_transactions for runtime access
            character_wallet_transactions = (self._db_app.session.query(CharacterWalletTransactionsModel).filter_by(character_id=self.character_id).all())
            self.wallet_transactions = [
                {col: getattr(entry, col) for col in CharacterWalletTransactionsModel.__table__.columns.keys()}
                for entry in character_wallet_transactions
            ]

            logging.debug(f"Wallet transactions successfully refreshed for {self.character_name}. New entries: {len(new_entries)}")

        except Exception as e:
            error_message = f"Failed to refresh wallet transactions for {self.character_name}. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    def _resolve_client_names(self, client_ids: set) -> dict:
        ids = [int(cid) for cid in client_ids if cid is not None]
        resolved = self.esi_service.resolve_universe_names(ids)
        # Fallback to SDE for NPC corporations not returned by /universe/names/
        for cid in ids:
            if cid not in resolved and 1000000 <= cid <= 1999999:
                try:
                    npc_corp = self._db_sde.session.query(NpcCorporations).filter_by(id=cid).first()
                    if npc_corp:
                        resolved[cid] = npc_corp.name[self._db_sde.language]
                except (SQLAlchemyError, KeyError, TypeError) as exc:
                    logging.warning("NPC corporation %s: no SDE name either (%s)", cid, exc)
        return resolved

    def _backfill_wallet_transaction_client_names(self) -> None:
        unresolved = (
            self._db_app.session.query(CharacterWalletTransactionsModel)
            .filter(
                CharacterWalletTransactionsModel.character_id == self.character_id,
                CharacterWalletTransactionsModel.client_id.isnot(None),
                CharacterWalletTransactionsModel.client_name.is_(None),
            )
            .all()
        )
        if not unresolved:
            return
        unique_ids = {int(row.client_id) for row in unresolved}
        resolved = self._resolve_client_names(unique_ids)
        updated = 0
        for row in unresolved:
            name = resolved.get(int(row.client_id))
            if name is not None:
                row.client_name = name
                updated += 1
        if updated:
            self._db_app.session.commit()
            logging.debug(f"Backfilled {updated} client names for {self.character_name}")

    def _backfill_wallet_journal_party_names(self) -> None:
        _PARTY_PAIRS = [
            ("first_party_id", "first_party_name"),
            ("second_party_id", "second_party_name"),
            ("tax_receiver_id", "tax_receiver_name"),
        ]
        filters = []
        for id_col, name_col in _PARTY_PAIRS:
            id_attr = getattr(CharacterWalletJournalModel, id_col)
            name_attr = getattr(CharacterWalletJournalModel, name_col)
            filters.append((id_attr.isnot(None)) & (name_attr.is_(None)))
        from sqlalchemy import or_
        unresolved = (
            self._db_app.session.query(CharacterWalletJournalModel)
            .filter(
                CharacterWalletJournalModel.character_id == self.character_id,
                or_(*filters),
            )
            .all()
        )
        if not unresolved:
            return
        unique_ids: set = set()
        for row in unresolved:
            for id_col, name_col in _PARTY_PAIRS:
                pid = getattr(row, id_col, None)
                name = getattr(row, name_col, None)
                if pid is not None and name is None:
                    unique_ids.add(int(pid))
        resolved = self._resolve_client_names(unique_ids)
        updated = 0
        for row in unresolved:
            for id_col, name_col in _PARTY_PAIRS:
                pid = getattr(row, id_col, None)
                if pid is not None and getattr(row, name_col, None) is None:
                    name = resolved.get(int(pid))
                    if name is not None:
                        setattr(row, name_col, name)
                        updated += 1
        if updated:
            self._db_app.session.commit()
            logging.debug(f"Backfilled {updated} journal party names for {self.character_name}")

    # -------------------
    # Industry Jobs
    # -------------------
    def refresh_industry_jobs(self) -> None:
        """Fetch and store industry job history for provenance/costing.

        Requires scope: esi-industry.read_character_jobs.v1

        Best-effort: if not authorized, it will keep running with no jobs.
        """
        try:
            self.ensure_esi()

            jobs = self._esi_client.esi_get(
                ESI_CHAR_INDUSTRY_JOBS.format(character_id=self.character_id),
                params={"include_completed": True},
                paginate=True,
            )
            if not jobs or not isinstance(jobs, list):
                # No jobs or not authorized
                return

            market_prices = self._esi_client.esi_get(ESI_MARKETS_PRICES)
            market_price_map = build_market_price_map(market_prices)
            # The producer's inputs (or why they are missing) read once per
            # refresh and passed to every snapshot, with the SDE memoized.
            invention_refresh = RealizedInventionRefresh.read()
            invention_cost_by_blueprint_type = build_invention_cost_per_run_by_blueprint_type(
                jobs=jobs,
                sde_session=self._db_sde.session,
                market_price_map=market_price_map,
                invention_refresh=invention_refresh,
                # The ME/TE of the character's BPCs settles a decryptor tie.
                **load_owner_invention_evidence(
                    self._db_app.session, owner_kind="character", owner_id=int(self.character_id),
                ),
            )

            clear_historical_backfill(
                app_session=self._db_app.session,
                owner_kind="character",
                owner_id=int(self.character_id),
            )
            wallet_transactions = (
                self._db_app.session.query(CharacterWalletTransactionsModel)
                .filter_by(character_id=int(self.character_id))
                .all()
            )
            backfill_wallet_buy_acquisitions(
                app_session=self._db_app.session,
                owner_kind="character",
                owner_id=int(self.character_id),
                wallet_transactions=wallet_transactions,
            )

            completed_statuses = {"delivered", "ready", "completed"}
            # The jobs' costs before this refresh: a re-costed job re-stamps
            # the blended corp transfer lots of its type below.
            unit_costs_before = character_job_unit_costs(self._db_app.session, character_id=int(self.character_id))
            existing_jobs = (
                self._db_app.session.query(CharacterIndustryJobsModel)
                .filter_by(character_id=int(self.character_id))
                .all()
            )
            fetched_job_ids = {
                int(job.get("job_id"))
                for job in jobs
                if isinstance(job, dict) and job.get("job_id") is not None
            }

            historical_job_events: list[tuple[str, int, str, Any]] = []
            for job in existing_jobs:
                job_id = int(getattr(job, "job_id", 0) or 0)
                if job_id <= 0 or job_id in fetched_job_ids:
                    continue
                observed_at = getattr(job, "completed_date", None) or getattr(job, "end_date", None)
                historical_job_events.append((
                    str(getattr(job, "start_date", None) or observed_at or ""),
                    job_id,
                    "existing",
                    job,
                ))
            for job in jobs:
                if not isinstance(job, dict) or job.get("job_id") is None:
                    continue
                observed_at = job.get("completed_date") or job.get("end_date")
                historical_job_events.append((
                    str(job.get("start_date") or observed_at or ""),
                    int(job.get("job_id") or 0),
                    "fetched",
                    job,
                ))

            rows: list[dict[str, Any]] = []
            # Jobs in start order: each takes its inputs FIFO from what earlier jobs left.
            consumed_inputs: dict[tuple[Any, Any], int] = {}
            for _, _, entry_kind, payload in sorted(historical_job_events, key=lambda entry: (entry[0], entry[1])):
                if entry_kind == "existing":
                    existing_job = payload
                    add_consumed_input_lots(consumed_inputs, getattr(existing_job, "historical_input_costs", None))
                    status = str(getattr(existing_job, "status", "") or "").lower()
                    completed_at = getattr(existing_job, "completed_date", None) or getattr(existing_job, "end_date", None)
                    output_quantity = int(getattr(existing_job, "output_quantity", 0) or 0)
                    unit_cost = getattr(existing_job, "unit_build_cost", None)
                    if (
                        status in completed_statuses
                        and completed_at
                        and output_quantity > 0
                        and unit_cost is not None
                        and float(unit_cost) > 0
                    ):
                        record_historical_acquisition(
                            app_session=self._db_app.session,
                            owner_kind="character",
                            owner_id=int(self.character_id),
                            observed_at=str(completed_at),
                            type_id=int(getattr(existing_job, "product_type_id", 0) or 0),
                            type_name=None,
                            quantity=output_quantity,
                            acquisition_source="industry_build",
                            acquisition_unit_cost=float(unit_cost),
                            acquisition_total_cost=float(unit_cost) * float(output_quantity),
                            acquisition_reference_type="industry_job",
                            acquisition_reference_id=int(getattr(existing_job, "job_id", 0) or 0),
                        )
                    continue

                j = payload
                job_id = j.get("job_id")
                completed_at = j.get("completed_date") or j.get("end_date")
                blueprint_type_id = int(j.get("blueprint_type_id") or 0)
                blueprint_provenance = lookup_historical_blueprint_provenance(
                    app_session=self._db_app.session,
                    owner_kind="character",
                    owner_id=int(self.character_id),
                    blueprint_item_id=(int(j.get("blueprint_id")) if j.get("blueprint_id") is not None else None),
                    blueprint_type_id=(blueprint_type_id if blueprint_type_id > 0 else None),
                    as_of=completed_at,
                )
                historical_input_costs = subtract_consumed_input_lots(build_historical_input_cost_lookup(
                    app_session=self._db_app.session,
                    owner_kind="character",
                    owner_id=int(self.character_id),
                    as_of=j.get("start_date") or completed_at,
                    type_ids=industry_job_material_type_ids(
                        sde_session=self._db_sde.session,
                        blueprint_type_id=blueprint_type_id,
                    ),
                ), consumed_inputs)
                snapshot = resolve_industry_job_cost_snapshot(
                    job=type("IndustryJobPayload", (), j)(),
                    sde_session=self._db_sde.session,
                    market_price_map=market_price_map,
                    **invention_cost_source_for(invention_cost_by_blueprint_type, j.get("blueprint_type_id")),
                    blueprint_provenance=blueprint_provenance,
                    owned_input_unit_cost_by_type_id=historical_input_costs,
                    invention_refresh=invention_refresh,
                )
                add_consumed_input_lots(consumed_inputs, snapshot.get("historical_input_costs"))
                rows.append(
                    {
                        "character_id": int(self.character_id),
                        "job_id": int(job_id),
                        "status": j.get("status"),
                        "start_date": j.get("start_date"),
                        "end_date": j.get("end_date"),
                        "completed_date": j.get("completed_date"),
                        "activity_id": int(j.get("activity_id") or 0) or None,
                        "blueprint_type_id": j.get("blueprint_type_id"),
                        "product_type_id": j.get("product_type_id"),
                        "runs": j.get("runs"),
                        "successful_runs": j.get("successful_runs"),
                        "installer_id": j.get("installer_id"),
                        "facility_id": j.get("facility_id"),
                        "location_id": industry_job_location_id(j),
                        "output_location_id": j.get("output_location_id"),
                        "blueprint_item_id": snapshot.get("blueprint_item_id"),
                        "blueprint_is_blueprint_copy": snapshot.get("blueprint_is_blueprint_copy"),
                        "blueprint_runs": snapshot.get("blueprint_runs"),
                        "blueprint_time_efficiency": snapshot.get("blueprint_time_efficiency"),
                        "blueprint_material_efficiency": snapshot.get("blueprint_material_efficiency"),
                        "blueprint_provenance_source": snapshot.get("blueprint_provenance_source"),
                        "blueprint_provenance_ref_id": snapshot.get("blueprint_provenance_ref_id"),
                        "cost": j.get("cost"),
                        "output_quantity": snapshot.get("output_quantity"),
                        "materials_cost": snapshot.get("materials_cost"),
                        "historical_materials_cost": snapshot.get("historical_materials_cost"),
                        "historical_material_cost_source": snapshot.get("historical_material_cost_source"),
                        "historical_material_coverage_fraction": snapshot.get("historical_material_coverage_fraction"),
                        "historical_input_costs": snapshot.get("historical_input_costs"),
                        "materials_cost_lower_bound_reason": snapshot.get("materials_cost_lower_bound_reason"),
                        "copy_cost": snapshot.get("copy_cost"),
                        "invention_cost": snapshot.get("invention_cost"),
                        "invention_cost_unknown_reason": snapshot.get("invention_cost_unknown_reason"),
                        "total_build_cost": snapshot.get("total_cost"),
                        "unit_build_cost": snapshot.get("unit_cost"),
                        "build_cost_source": snapshot.get("source"),
                        "raw": j,
                    }
                )
                output_quantity = int(snapshot.get("output_quantity") or 0)
                unit_cost = snapshot.get("unit_cost")
                status = str(j.get("status") or "").lower()
                if status in completed_statuses and completed_at and output_quantity > 0 and unit_cost is not None and float(unit_cost) > 0:
                    record_historical_acquisition(
                        app_session=self._db_app.session,
                        owner_kind="character",
                        owner_id=int(self.character_id),
                        observed_at=str(completed_at),
                        type_id=int(j.get("product_type_id") or 0),
                        type_name=None,
                        quantity=output_quantity,
                        acquisition_source="industry_build",
                        acquisition_unit_cost=float(unit_cost),
                        acquisition_total_cost=float(unit_cost) * float(output_quantity),
                        acquisition_reference_type="industry_job",
                        acquisition_reference_id=int(job_id),
                    )

            if fetched_job_ids:
                (
                    self._db_app.session.query(CharacterIndustryJobsModel)
                    .filter_by(character_id=self.character_id)
                    .filter(CharacterIndustryJobsModel.job_id.in_(sorted(fetched_job_ids)))
                    .delete(synchronize_session=False)
                )
            self._db_app.session.bulk_save_objects([CharacterIndustryJobsModel(**r) for r in rows])
            self._db_app.session.commit()

            backfill_historical_market_costs(
                app_session=self._db_app.session,
                sde_session=self._db_sde.session,
                esi_service=self._esi_service,
                job_model=CharacterIndustryJobsModel,
                owner_filter={"character_id": int(self.character_id)},
            )
            recosted_types = recosted_product_type_ids(
                unit_costs_before,
                character_job_unit_costs(self._db_app.session, character_id=int(self.character_id)),
            )
            if recosted_types:
                restamp_blended_corp_transfer_lots(self._db_app.session, recosted_types)
                self._db_app.session.commit()
        except Exception as e:
            logging.warning(
                "Skipping industry jobs refresh for %s (%s): %s",
                self.character_name,
                self.character_id,
                str(e),
            )
            return

    # -------------------
    # Skillpoints
    # -------------------
    def extract_reprocessing_skills(self) -> Dict[int, int]:
        """The character's reprocessing skills: {skill type id: trained level}
        for every trained skill with a reprocessing bonus in the SDE (typeDogma
        attribute 379: Reprocessing, Reprocessing Efficiency, the ore, ice and
        moon processing skills). Keyed by id, whatever the SDE language."""
        if not self.skills or "skills" not in self.skills:
            self.refresh_skills()

        try:
            from eve_online_industry_tracker.infrastructure.sde.ore import refining_skill_levels

            self.reprocessing_skills = refining_skill_levels(self._db_sde.session, self.skills["skills"])

            logging.debug(f"Reprocessing skills successfully updated for {self.character_name}.")
            return self.reprocessing_skills
        
        except Exception as e:
            error_message = f"Failed to extract reprocessing skills for {self.character_name}. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    def refresh_skills(self) -> None:
        """Fetch and update the character's skills from ESI. Enrich with SDE data."""
        try:
            self.ensure_esi()
            # All trained skills for the character from ESI
            skills = self._esi_client.esi_get(ESI_CHAR_SKILLS.format(character_id=self.character_id))
            skill_list = skills.get("skills", [])

            # Current skill queue for the character from ESI
            skill_queue = self._esi_client.esi_get(ESI_CHAR_SKILLQUEUE.format(character_id=self.character_id))

            # Map character skills and skill queue
            character_skill_ids = {s["skill_id"]: s for s in skill_list} 
            character_skill_queue_ids = {s["skill_id"]: s for s in skill_queue}
            
            # All skill groups (categoryID=16) and all skills for those groups from SDE
            all_groups = self._db_sde.session.query(Groups).filter(Groups.categoryID == 16, Groups.published == 1).all()
            all_skills = self._db_sde.session.query(Types).filter(Types.groupID.in_([g.id for g in all_groups]), Types.published == 1).all()
            
            group_map_names = {g.id: g.name[self._db_sde.language] for g in all_groups}
            skill_map = {}
            for t in all_skills:
                group_name = group_map_names.get(t.groupID, "Unknown")
                skill_map[t.id] = {
                    "skill_id": t.id,
                    "skill_name": t.name[self._db_sde.language],
                    "skill_desc": t.description[self._db_sde.language],
                    "group_id": t.groupID,
                    "group_name":group_name
                }
            
            enriched_skill_queue = []
            for skill_id, sde_skill in skill_map.items():
                if skill_id in character_skill_queue_ids:
                    # Character has skill in training queue
                    s = character_skill_queue_ids[skill_id]
                    enriched_skill_queue.append({
                        **sde_skill,
                        "start_date" : s.get("start_date"),
                        "finish_date" : s.get("finish_date"),
                        "finished_level": s.get("finished_level", 0),
                        "level_start_sp": s.get("level_start_sp", 0),
                        "level_end_sp": s.get("level_end_sp", 0),
                        "queue_position": s.get("queue_position", 0),
                        "training_start_sp": s.get("training_start_sp", 0)
                    })

            full_skill_list = []
            for skill_id, sde_skill in skill_map.items():
                if skill_id in character_skill_ids:
                    # Character has skillbook or trained it
                    s = character_skill_ids[skill_id]
                    trained = s["trained_skill_level"] > 0
                    full_skill_list.append({
                        **sde_skill,
                        "trained_skill_level": s.get("trained_skill_level", 0),
                        "skillpoints_in_skill": s.get("skillpoints_in_skill", 0),
                        "status": "trained" if trained else "available"
                    })
                else:
                    # Character has not acquired this skill yet
                    full_skill_list.append({
                        **sde_skill,
                        "trained_skill_level": 0,
                        "skillpoints_in_skill": 0,
                        "status": "unavailable"
                    })

            
            self.skills = {
                "total_skillpoints": skills.get("total_sp"),
                "unallocated_skillpoints": skills.get("unallocated_sp"),
                "skills": full_skill_list,
                "skill_queue": enriched_skill_queue
            }
            # Extract reprocessing skills after updating self.skills
            self.reprocessing_skills = self.extract_reprocessing_skills()

            # Save to database
            self.save_character()

            # Best-effort: refresh market fees now that skills are updated.
            self.refresh_market_fees()

            logging.debug(f"Skills successfully updated for {self.character_name}. Total skill points: {self.skills['total_skillpoints']}")
        
        except Exception as e:
            error_message = f"Failed to refresh skills for {self.character_name}. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    # -------------------
    # Refresh Implants
    # -------------------
    def refresh_implants(self) -> None:
        """Fetch and update the character's implants from ESI.

        Requires scope: esi-clones.read_implants.v1

        This is best-effort: if the character is not re-authed with the new
        scope yet, ESI may return 403 and we keep the app running.
        """
        try:
            self.ensure_esi()

            data = self._esi_client.esi_get(ESI_CHAR_IMPLANTS.format(character_id=self.character_id))
            if not isinstance(data, list):
                logging.warning(
                    "Unexpected implants payload for %s (%s): %r",
                    self.character_name,
                    self.character_id,
                    type(data),
                )
                return

            implant_ids: list[int] = []
            for x in data:
                try:
                    implant_ids.append(int(x))
                except (TypeError, ValueError):
                    continue

            self.implants = sorted(set(implant_ids))
            self.save_character()

            logging.debug(
                "Implants refreshed for %s (%s): %s",
                self.character_name,
                self.character_id,
                len(self.implants or []),
            )
        except Exception as e:
            logging.warning(
                "Skipping implant refresh for %s (%s): %s",
                self.character_name,
                self.character_id,
                str(e),
            )
            return
    
    # -------------------
    # Market Orders
    # -------------------
    def refresh_market_orders(self) -> None:
        """Fetch and update the character's market orders from ESI. Enrich with SDE and ESI data."""
        try:
            self.ensure_esi()
            order_list = self._esi_client.esi_get(ESI_CHAR_ORDERS.format(character_id=self.character_id))

            # Cache name lookups for this refresh to avoid repeated ESI calls.
            location_name_cache: Dict[int, Optional[str]] = {}
            region_name_cache: Dict[int, Optional[str]] = {}

            def resolve_name(location_id: Optional[int], cache: Dict[int, Optional[str]], kind: str) -> Optional[str]:
                if not location_id or not isinstance(location_id, int):
                    return None
                if location_id in cache:
                    return cache[location_id]
                name: Optional[str] = None
                try:
                    info = self._esi_service.get_location_info(location_id)
                    if isinstance(info, dict) and isinstance(info.get("name"), str) and info.get("name"):
                        name = info["name"]
                except ESI_REQUEST_ERRORS as exc:
                    # Do not fail refresh_market_orders if enrichment fails.
                    logging.warning("Market orders: %s %s name unavailable: %s", kind, location_id, exc)
                cache[location_id] = name
                return name

            orders = []
            type_ids = set(order.get("type_id") for order in order_list)
            type_data_map = {t.id: t for t in self._db_sde.session.query(Types).filter(Types.id.in_(list(type_ids))).all()}
            group_ids = set(t.groupID for t in type_data_map.values())
            group_data_map = {g.id: g for g in self._db_sde.session.query(Groups).filter(Groups.id.in_(list(group_ids))).all()}
            category_ids = set(g.categoryID for g in group_data_map.values())
            category_data_map = {c.id: c for c in self._db_sde.session.query(Categories).filter(Categories.id.in_(list(category_ids))).all()}
            for order in order_list:
                type_id = order.get("type_id")
                type_data = type_data_map.get(type_id)
                group_data = group_data_map.get(type_data.groupID) if type_data else None
                category_data = category_data_map.get(group_data.categoryID) if group_data else None

                location_id = order.get("location_id", None)
                region_id = order.get("region_id", None)  # ESI orders always carry their region

                new_order = {
                    "character_id": self.character_id,
                    "order_id": order.get("order_id", None),
                    "type_id": order.get("type_id", None),
                    "type_name": type_data.name[self._db_sde.language] if type_data else None,
                    "type_group_id": type_data.groupID if type_data else None,
                    "type_group_name": group_data.name[self._db_sde.language] if group_data else None,
                    "type_category_id": group_data.categoryID if group_data else None,
                    "type_category_name": category_data.name[self._db_sde.language] if category_data else None,
                    "location_id": location_id,
                    "location_name": resolve_name(location_id, location_name_cache, "location"),
                    "region_id": region_id,
                    "region_name": resolve_name(region_id, region_name_cache, "region"),
                    "owner": self.corporation_name if order.get("is_corporation", False) else self.character_name,
                    "is_corporation": order.get("is_corporation", False),
                    "price": order.get("price", 0.0),
                    "is_buy_order": order.get("is_buy_order", False),
                    "escrow": order.get("escrow", 0.0),
                    "volume_total": order.get("volume_total", 0),
                    "volume_remain": order.get("volume_remain", 0),
                    "duration": order.get("duration", 0),
                    "issued": order.get("issued", None),
                    "min_volume": order.get("min_volume", 0),
                    "range": order.get("range", None)
                }

                orders.append(new_order)

            self.save_market_orders(orders)

            # Assign loaded entries to self.market_orders for runtime access
            character_market_orders = (self._db_app.session.query(CharacterMarketOrdersModel).filter_by(character_id=self.character_id).all())
            self.market_orders = [
                {col: getattr(entry, col) for col in CharacterMarketOrdersModel.__table__.columns.keys()}
                for entry in character_market_orders
            ]

            logging.debug(f"Market orders successfully refreshed for {self.character_name}. Total orders: {len(orders)}")
        
        except Exception as e:
            error_message = f"Failed to refresh market orders for {self.character_name}. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)
        
    # -------------------
    # Assets
    # -------------------
    def refresh_assets(self) -> None:
        """Fetch and update the character's assets from ESI. Enrich with SDE and market price data."""
        try:
            self.ensure_esi()
            assets = self._esi_client.esi_get(ESI_CHAR_ASSETS.format(character_id=self.character_id), paginate=True)
            logging.debug(f"ESI assets fetched for {self.character_name}: {len(assets) if assets else 0} items")

            blueprints = self._esi_client.esi_get(ESI_CHAR_BLUEPRINTS.format(character_id=self.character_id), paginate=True)
            market_prices = self._esi_client.esi_get(ESI_MARKETS_PRICES)

            # Precompute per-type cost basis using stored wallet tx / industry jobs.
            type_ids_for_cost = [a.get("type_id") for a in assets if isinstance(a, dict)]
            qty_by_type: dict[int, int] = {}
            for a in assets:
                if not isinstance(a, dict):
                    continue
                t = a.get("type_id")
                q = a.get("quantity")
                if not (isinstance(t, int) or (isinstance(t, str) and t.isdigit())):
                    continue
                if not (isinstance(q, int) or (isinstance(q, str) and str(q).isdigit())):
                    continue
                tid = int(t)
                qty_by_type[tid] = qty_by_type.get(tid, 0) + int(q)
            cost_map = build_cost_map_for_assets(
                app_session=self._db_app.session,
                sde_session=self._db_sde.session,
                owner_kind="character_id",
                owner_id=int(self.character_id),
                asset_type_ids=[int(t) for t in type_ids_for_cost if isinstance(t, int) or str(t).isdigit()],
                asset_quantities_by_type=qty_by_type,
                wallet_tx_model=CharacterWalletTransactionsModel,
                industry_job_model=CharacterIndustryJobsModel,
                market_prices=market_prices if isinstance(market_prices, list) else [],
            )

            asset_list = []
            type_ids = set(asset.get("type_id") for asset in assets)
            type_data_map = {t.id: t for t in self._db_sde.session.query(Types).filter(Types.id.in_(type_ids)).all()}
            group_ids = set(t.groupID for t in type_data_map.values())
            group_data_map = {g.id: g for g in self._db_sde.session.query(Groups).filter(Groups.id.in_(group_ids)).all()}
            category_ids = set(g.categoryID for g in group_data_map.values())
            category_data_map = {c.id: c for c in self._db_sde.session.query(Categories).filter(Categories.id.in_(category_ids)).all()}
            race_ids = set(t.raceID for t in type_data_map.values() if hasattr(t, "raceID") and t.raceID is not None)
            race_data_map = {r.id: r for r in self._db_sde.session.query(Races).filter(Races.id.in_(race_ids)).all()}
            faction_ids = set(t.factionID for t in type_data_map.values() if hasattr(t, "factionID") and t.factionID is not None)
            faction_data_map = {f.id: f for f in self._db_sde.session.query(Factions).filter(Factions.id.in_(faction_ids)).all()}
            blueprint_data_map = {bp["item_id"]: bp for bp in blueprints}
            for asset in assets:
                type_id = asset.get("type_id")
                type_data = type_data_map.get(type_id)
                type_adjusted_price = next((item.get("adjusted_price", 0.0) for item in market_prices if item.get("type_id") == type_id), 0.0)
                type_average_price = next((item.get("average_price", 0.0) for item in market_prices if item.get("type_id") == type_id), 0.0)
                group_data = group_data_map.get(type_data.groupID) if type_data else None
                category_data = category_data_map.get(group_data.categoryID) if group_data else None
                race_data = race_data_map.get(type_data.raceID) if type_data and hasattr(type_data, "raceID") else None
                faction_data = faction_data_map.get(type_data.factionID) if type_data and hasattr(type_data, "factionID") else None
                blueprint_data = blueprint_data_map.get(asset.get("item_id"))
                
                # --- Calculate actual volume (SDE packagedVolume for a packaged item) ---
                sde_volume, repackaged_volume, actual_volume = asset_volumes(
                    type_data, is_singleton=bool(asset.get("is_singleton", False)))

                asset_entry = {
                    "character_id": self.character_id,
                    "item_id": asset.get("item_id"),
                    "type_id": type_id,
                    "type_name": getattr(type_data, "name", {}).get(self._db_sde.language, "") if type_data else "",
                    "type_default_volume": sde_volume,
                    "type_repackaged_volume": repackaged_volume,
                    "type_volume": actual_volume,
                    "type_capacity": getattr(type_data, "capacity", None) if type_data else None,
                    "type_description": getattr(type_data, "description", {}).get(self._db_sde.language, "") if type_data and getattr(type_data, "description", None) else "",
                    "container_name": None,
                    "ship_name": None,
                    "type_group_id": getattr(type_data, "groupID", None) if type_data else None,
                    "type_group_name": getattr(group_data, "name", {}).get(self._db_sde.language, "") if group_data else "",
                    "type_category_id": getattr(group_data, "categoryID", None) if group_data else None,
                    "type_category_name": getattr(category_data, "name", {}).get(self._db_sde.language, "") if category_data else "",
                    "type_meta_group_id": getattr(type_data, "metaGroupID", None) if type_data else None,
                    "type_race_id": getattr(type_data, "raceID", None) if type_data else None,
                    "type_race_name": getattr(race_data, "name", {}).get(self._db_sde.language, "") if race_data and getattr(race_data, "name", None) else "",
                    "type_race_description": getattr(race_data, "description", {}).get(self._db_sde.language, "") if race_data and getattr(race_data, "description", None) else "",
                    "type_faction_id": getattr(type_data, "factionID", None) if type_data else None,
                    "type_faction_name": getattr(faction_data, "name", {}).get(self._db_sde.language, "") if faction_data and getattr(faction_data, "name", None) else "",
                    "type_faction_description": getattr(faction_data, "description", {}).get(self._db_sde.language, "") if faction_data and getattr(faction_data, "description", None) else "",
                    "type_faction_short_description": getattr(faction_data, "shortDescription", {}).get(self._db_sde.language, "") if faction_data and getattr(faction_data, "shortDescription", None) else "",
                    "location_id": asset.get("location_id"),
                    "location_type": asset.get("location_type"),
                    "location_flag": asset.get("location_flag"),
                    "top_location_id": None,
                    "is_singleton": asset.get("is_singleton"),
                    "is_blueprint_copy": asset.get("is_blueprint_copy", False),
                    "blueprint_runs": blueprint_data.get("runs") if blueprint_data else None,
                    "blueprint_time_efficiency": blueprint_data.get("time_efficiency") if blueprint_data else None,
                    "blueprint_material_efficiency": blueprint_data.get("material_efficiency") if blueprint_data else None,
                    "quantity": asset.get("quantity", 0),
                    "type_adjusted_price": type_adjusted_price,
                    "type_average_price": type_average_price,
                    "is_container": type_id == 17366 and asset.get("is_singleton", False) == True,
                    "is_asset_safety_wrap": type_id == 60 and asset.get("is_singleton", False) == True,
                    "is_ship": group_data.categoryID == 6 if group_data else False,
                    "is_office_folder": type_id == 27,
                }

                # Best-effort provenance + cost basis
                ci = cost_map.get(int(type_id)) if type_id is not None else None
                if ci is not None:
                    unit_cost = ci.unit_cost
                    qty = asset_entry.get("quantity", 0) or 0
                    total_cost = (unit_cost * float(qty)) if (unit_cost is not None and qty is not None) else None
                    asset_entry["acquisition_source"] = ci.source
                    asset_entry["acquisition_unit_cost"] = unit_cost
                    asset_entry["acquisition_total_cost"] = total_cost
                    asset_entry["acquisition_reference_type"] = ci.reference_type
                    asset_entry["acquisition_reference_id"] = ci.reference_id
                    asset_entry["acquisition_date"] = ci.acquisition_date
                    asset_entry["acquisition_updated_at"] = datetime.now(timezone.utc).isoformat()

                asset_list.append(asset_entry)
            
            # Fetch custom names for containers
            container_ids = [a["item_id"] for a in asset_list if a.get("is_container")]
            container_names = {}
            if container_ids:
                names_response = self._esi_client.esi_post(
                    ESI_CHAR_ASSETS_NAMES.format(character_id=self.character_id),
                    json=container_ids
                )
                if names_response and isinstance(names_response, list):
                    container_names = {c["item_id"]: c["name"] for c in names_response}

            for asset in asset_list:
                if asset.get("item_id") in container_names:
                    asset["container_name"] = container_names[asset["item_id"]]
            
            # Fetch custom names for ships
            ship_ids = [a["item_id"] for a in asset_list if a.get("is_ship")]
            ship_names = {}
            if ship_ids:
                names_response = self._esi_client.esi_post(
                    ESI_CHAR_ASSETS_NAMES.format(character_id=self.character_id),
                    json=ship_ids
                )
                if names_response and isinstance(names_response, list):
                    ship_names = {s["item_id"]: s["name"] for s in names_response}

            for asset in asset_list:
                if asset.get("item_id") in ship_names:
                    asset["ship_name"] = ship_names[asset["item_id"]]
            
            # Lookup for all containers and ships by item_id
            container_lookup = {a["item_id"]: a for a in asset_list if a.get("is_container")}
            ship_lookup = {a["item_id"]: a for a in asset_list if a.get("is_ship")}

            for asset in asset_list:
                # Assign container_name for non-containers
                if not (asset.get("is_container")):
                    container = container_lookup.get(asset.get("location_id"))
                    if container:
                        asset["container_name"] = container.get("container_name")
                # Assign ship_name for non-ships (modules, etc.)
                if not (asset.get("is_ship")):
                    ship = ship_lookup.get(asset.get("location_id"))
                    if ship:
                        asset["ship_name"] = ship.get("ship_name")

            # Calculate top_location_id for each asset
            location_flags = ["CorpDeliveries", "OfficeFolder", "AssetSafety","CorpSAG1", "CorpSAG2", "CorpSAG3", "CorpSAG4","CorpSAG5", "CorpSAG6", "CorpSAG7"]
            assetsafety_wrap_ids = {a["item_id"] for a in asset_list if a.get("is_asset_safety_wrap")}
            office_folder_ids = {a["item_id"] for a in asset_list if a.get("is_office_folder")}
            parent_location_map = {a["item_id"]: a["location_id"] for a in asset_list}
            valid_top_location_ids = {a["location_id"] for a in asset_list
                if a.get("location_flag") in location_flags
                    and a.get("location_id") not in assetsafety_wrap_ids
                    and a.get("location_id") not in office_folder_ids
            }

            def resolve_top_location_id(item):
                loc_id = item["location_id"]
                while loc_id in parent_location_map:
                    # Stop if we've reached a valid top-level location
                    if loc_id in valid_top_location_ids:
                        break
                    parent_id = parent_location_map[loc_id]
                    if parent_id == loc_id:
                        break
                    loc_id = parent_id
                return loc_id

            # Assign top_location_id for each asset
            for asset in asset_list:
                asset["top_location_id"] = resolve_top_location_id(asset)

            self.save_assets(asset_list)

            # Assign loaded entries to self.assets for runtime access
            character_assets = (self._db_app.session.query(CharacterAssetsModel).filter_by(character_id=self.character_id).all())
            self.assets = [
                {col: getattr(entry, col) for col in CharacterAssetsModel.__table__.columns.keys()}
                for entry in character_assets
            ]

            logging.debug(f"Assets successfully updated for {self.character_name}. Total assets: {len(asset_list)}")
        except Exception as e:
            error_message = f"Failed to refresh assets for {self.character_name}. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

