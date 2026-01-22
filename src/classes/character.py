import logging
import json
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from classes.database_manager import DatabaseManager
from classes.config_manager import ConfigManager
from classes.esi import ESIClient
from classes.esi_service import ESIService
from classes.database_models import CharacterModel, CharacterWalletJournalModel \
    , CharacterWalletTransactionsModel, CharacterMarketOrdersModel, CharacterAssetsModel \
    , CharacterIndustryJobsModel \
    , NpcCorporations, Bloodlines, Races, Types \
    , Groups, Categories, Factions

from classes.asset_provenance import build_cost_map_for_assets

class Character:
    """Ingame entity of a character."""

    def __init__(self, 
                 cfgManager: ConfigManager, 
                 db_oauth: DatabaseManager,
                 db_app: DatabaseManager,
                 db_sde: DatabaseManager,
                 character_name: str, 
                 is_main: bool = False,
                 is_corp_director: bool = False,
                 refresh_token: Optional[str] = None
        ) -> None:
        try:
            # Private properties
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
            self.updated_at: Optional[datetime] = None
            
            self.wallet_balance: Optional[float] = None
            self.skills: Optional[Dict[str, Any]] = None
            self.standings: Optional[List[Dict[str, str]]] = None
            self.implants: Optional[List[int]] = None
            self.wallet_journal: Optional[List[Dict[str, Any]]] = None
            self.wallet_transactions: Optional[List[Dict[str, Any]]] = None
            self.reprocessing_skills: Optional[Dict[str, int]] = None
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

        self._esi_client = ESIClient(
            self._cfgManager,
            self._db_oauth,
            self.character_name,
            self.is_main,
            self.is_corp_director,
            self._refresh_token,
        )
        self._esi_service = ESIService(self._esi_client)
        self.character_id = self._esi_client.character_id

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
            "skills": self.skills if isinstance(self.skills, (dict, list)) else {},
            "reprocessing_skills": self.reprocessing_skills if isinstance(self.reprocessing_skills, dict) else {},
            "standings": self.standings if isinstance(self.standings, list) else [],
            "implants": self.implants if isinstance(self.implants, list) else [],
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
        return {"character_name": self.character_name, "character_id": self.character_id, "market_orders": self.market_orders if self.market_orders is not None else []}

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
                    setattr(character_record, column, value)

            character_record.updated_at = datetime.now(timezone.utc)
            self._db_app.session.commit()
            logging.debug(f"Character '{self.character_name}' saved to database.")
    
        except Exception as e:
            error_message = f"Failed to save character '{self.character_name}' to database. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

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
                self._db_app.session.bulk_save_objects(self.wallet_journal)
                self._db_app.session.commit()
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
            self.market_orders = []
            for order in market_orders:
                new_order = CharacterMarketOrdersModel(**order)
                self.market_orders.append(new_order)

            if self.market_orders:
                # Delete existing orders for this character and add new ones
                self._db_app.session.query(CharacterMarketOrdersModel).filter_by(character_id=self.character_id).delete()
                self._db_app.session.bulk_save_objects(self.market_orders)
                self._db_app.session.commit()
            else:
                logging.debug(f"No new market orders to save for {self.character_name}.")

            logging.debug(f"Market orders saved ({len(self.market_orders)}) for {self.character_name}.")
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

        try:
            self.assets = []
            for asset in asset_list:
                new_asset = CharacterAssetsModel(**asset)
                self.assets.append(new_asset)
            
            if self.assets:
                # Delete existing assets for this character and add new ones
                self._db_app.session.query(CharacterAssetsModel).filter_by(character_id=self.character_id).delete()
                self._db_app.session.bulk_save_objects(self.assets)
                self._db_app.session.commit()
            else:
                logging.debug(f"No new assets to save for {self.character_name}.")

            logging.debug(f"Assets saved ({len(self.assets)}) for {self.character_name}.")

        except Exception as e:
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
             # Call individual data refresh methods
            self.refresh_profile()
            self.refresh_skills()
            self.refresh_implants()
            self.refresh_wallet_journal()
            self.refresh_wallet_transactions()
            self.refresh_market_orders()
            self.refresh_industry_jobs()
            self.refresh_assets()

            logging.debug(f"All data successfully refreshed for {self.character_name}.")
        except Exception as e:
            error_message = f"Failed to refresh all data for {self.character_name}. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    # -------------------
    # Refresh Profile
    # -------------------
    def refresh_profile(self) -> None:
        """Fetch and update the character's profile data from ESI. Enrich with SDE data."""
        try:
            self.ensure_esi()
            profile_data = self._esi_client.esi_get(f"/characters/{self.character_id}/")
            corporation_data = self._esi_client.esi_get(f"/corporations/{profile_data.get('corporation_id')}/")
            wallet_balance = self._esi_client.esi_get(f"/characters/{self.character_id}/wallet/")
            standings_data = self._esi_client.esi_get(f"/characters/{self.character_id}/standings/")

            # Load additional details from the SDE database
            race_data = self._db_sde.session.query(Races).filter_by(id=profile_data.get("race_id")).first()
            bloodline_data = self._db_sde.session.query(Bloodlines).filter_by(id=profile_data.get("bloodline_id")).first()
            faction_data = self._db_sde.load_df("factions")
            npccorp_data = self._db_sde.load_df("npcCorporations")

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
            self.wallet_balance = self._esi_client.esi_get(f"/characters/{self.character_id}/wallet/")

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
            journal_entries = self._esi_client.esi_get(f"/characters/{self.character_id}/wallet/journal/")

            new_journal_entries = []
            for entry in journal_entries:
                entry_wallet_journal_id = entry.get("id")
                if entry_wallet_journal_id is None:
                    continue  # Skip entries without an ID

                existing_entry = (
                    self._db_app.session.query(CharacterWalletJournalModel)
                    .filter_by(character_id=self.character_id, wallet_journal_id=entry_wallet_journal_id)
                    .first()
                )
                if existing_entry:
                    continue  # Skip if already exists
                new_journal_entries.append(entry)

            # Step 1: Collect unique party IDs
            party_ids = set()
            for entry in new_journal_entries:
                for key in ("first_party_id", "second_party_id", "tax_receiver_id"):
                    pid = entry.get(key)
                    if pid:
                        party_ids.add(pid)

            # Step 2: Lookup names for each unique ID
            party_names = {}
            for pid in party_ids:
                name = None
                id_type = self._esi_client.get_id_type(pid)
                if id_type == "character":
                    data = self._esi_client.esi_get(f"/characters/{pid}/")
                    if data and "name" in data:
                        name = data["name"]
                elif id_type == "alliance":
                    data = self._esi_client.esi_get(f"/alliances/{pid}/")
                    if data and "name" in data:
                        name = data["name"]
                elif id_type == "corporation":
                    data = self._esi_client.esi_get(f"/corporations/{pid}/")
                    if data and "name" in data:
                        name = data["name"]
                elif id_type == "npc_corporation":
                    npc_corp = self._db_sde.session.query(NpcCorporations).filter_by(id=pid).first()
                    name = npc_corp.name[self._db_sde.language] if npc_corp else None
                else:
                    continue  # Unknown type, skip

                party_names[pid] = name

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
            transactions = self._esi_client.esi_get(f"/characters/{self.character_id}/wallet/transactions/")

            new_transaction_entries = []
            for entry in transactions:
                entry_transaction_id = entry.get("transaction_id")
                if entry_transaction_id is None:
                    continue  # Skip entries without an ID

                existing_entry = (
                    self._db_app.session.query(CharacterWalletTransactionsModel)
                    .filter_by(character_id=self.character_id, transaction_id=entry_transaction_id)
                    .first()
                )
                if existing_entry:
                    continue  # Skip if already exists
                new_transaction_entries.append(entry)

            # Step 1: Collect unique client IDs
            client_ids = set()
            for entry in new_transaction_entries:
                cid = entry.get("client_id")
                if cid:
                    client_ids.add(cid)

            # Step 2: Lookup names for each unique ID
            client_names = {}
            for cid in client_ids:
                name = None
                id_type = self._esi_client.get_id_type(cid)
                if id_type == "character":
                    data = self._esi_client.esi_get(f"/characters/{cid}/")
                    if data and "name" in data:
                        name = data["name"]
                elif id_type == "alliance":
                    data = self._esi_client.esi_get(f"/alliances/{cid}/")
                    if data and "name" in data:
                        name = data["name"]
                elif id_type == "corporation":
                    data = self._esi_client.esi_get(f"/corporations/{cid}/")
                    if data and "name" in data:
                        name = data["name"]
                elif id_type == "npc_corporation":
                    npc_corp = self._db_sde.session.query(NpcCorporations).filter_by(id=cid).first()
                    name = npc_corp.name[self._db_sde.language] if npc_corp else None
                else:
                    continue  # Unknown type, skip

                client_names[cid] = name

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
                f"/characters/{self.character_id}/industry/jobs/",
                params={"include_completed": True},
                paginate=True,
            )
            if not jobs or not isinstance(jobs, list):
                # No jobs or not authorized
                return

            rows: list[dict[str, Any]] = []
            for j in jobs:
                if not isinstance(j, dict):
                    continue
                job_id = j.get("job_id")
                if job_id is None:
                    continue
                rows.append(
                    {
                        "character_id": int(self.character_id),
                        "job_id": int(job_id),
                        "status": j.get("status"),
                        "start_date": j.get("start_date"),
                        "end_date": j.get("end_date"),
                        "completed_date": j.get("completed_date"),
                        "blueprint_type_id": j.get("blueprint_type_id"),
                        "product_type_id": j.get("product_type_id"),
                        "runs": j.get("runs"),
                        "successful_runs": j.get("successful_runs"),
                        "installer_id": j.get("installer_id"),
                        "facility_id": j.get("facility_id"),
                        "location_id": j.get("location_id"),
                        "output_location_id": j.get("output_location_id"),
                        "cost": j.get("cost"),
                        "raw": j,
                    }
                )

            # Replace snapshot for this character.
            self._db_app.session.query(CharacterIndustryJobsModel).filter_by(character_id=self.character_id).delete()
            self._db_app.session.bulk_save_objects([CharacterIndustryJobsModel(**r) for r in rows])
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
    def extract_reprocessing_skills(self) -> str:
        """Extract reprocessing related skills and levels for the current character."""
        if not self.skills or "skills" not in self.skills:
            self.refresh_skills()

        try:
            # Find all relevant skill groups
            skill_groups = self._db_sde.session.query(Groups).filter(
                Groups.categoryID == 16,  # Skills category
                Groups.published == 1,
                Groups.name[self._db_sde.language].ilike("%Processing%")
            ).all()

            # Get all skill type IDs in these groups
            skill_ids = set()
            skills_in_groups = self._db_sde.session.query(Types).filter(
                Types.groupID.in_([g.id for g in skill_groups]),
                Types.published == 1,
                Types.name[self._db_sde.language].ilike("%Processing%")
            ).all()
            for skill in skills_in_groups:
                skill_ids.add(skill.id)

            # Build mapping: skill ID → skill name
            skill_map = {}
            all_skills = self._db_sde.session.query(Types).filter(Types.id.in_(list(skill_ids))).all()
            for skill in all_skills:
                skill_map[skill.id] = skill.name[self._db_sde.language]

            # Extract trained levels
            self.reprocessing_skills = {}
            for skill in self.skills["skills"]:
                skill_id = skill.get("skill_id")
                if skill_id in skill_map:
                    self.reprocessing_skills[skill_map[skill_id]] = skill.get("trained_skill_level", 0)

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
            skills = self._esi_client.esi_get(f"/characters/{self.character_id}/skills/")
            skill_list = skills.get("skills", [])

            # Current skill queue for the character from ESI
            skill_queue = self._esi_client.esi_get(f"/characters/{self.character_id}/skillqueue/")

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

            data = self._esi_client.esi_get(f"/characters/{self.character_id}/implants/")
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
                except Exception:
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
            order_list = self._esi_client.esi_get(f"/characters/{self.character_id}/orders/")

            # Cache location/region lookups for this refresh to avoid repeated ESI calls.
            # { location_id: (location_name, region_id, region_name) }
            location_region_cache: Dict[int, tuple[Optional[str], int, Optional[str]]] = {}
            region_name_cache: Dict[int, Optional[str]] = {}

            def resolve_region_name(region_id: int) -> Optional[str]:
                if not region_id or not isinstance(region_id, int):
                    return None

                cached = region_name_cache.get(region_id)
                if cached is not None:
                    return cached

                try:
                    region_info = self._esi_service.get_location_info(region_id)
                    if isinstance(region_info, dict):
                        name = region_info.get("name")
                        if isinstance(name, str) and name:
                            region_name_cache[region_id] = name
                            return name
                except Exception:
                    pass

                region_name_cache[region_id] = None
                return None

            def resolve_location_region(location_id: Optional[int]) -> tuple[Optional[str], int, Optional[str]]:
                if not location_id or not isinstance(location_id, int):
                    return None, 0, None

                cached = location_region_cache.get(location_id)
                if cached is not None:
                    return cached

                location_name: Optional[str] = None
                region_id: int = 0
                region_name: Optional[str] = None

                try:
                    location_info = self._esi_service.get_location_info(location_id)
                    if isinstance(location_info, dict):
                        location_name = location_info.get("name")

                        # Stations have system_id; structures have solar_system_id.
                        system_id = location_info.get("system_id") or location_info.get("solar_system_id")
                        if system_id and isinstance(system_id, int):
                            system_info = self._esi_service.get_location_info(system_id)
                            if isinstance(system_info, dict):
                                # /universe/systems/{id} does not contain region_id; it contains constellation_id.
                                constellation_id = system_info.get("constellation_id")
                                if isinstance(constellation_id, int) and constellation_id:
                                    constellation_info = self._esi_service.get_location_info(constellation_id)
                                    if isinstance(constellation_info, dict):
                                        region_id_val = constellation_info.get("region_id")
                                        if isinstance(region_id_val, int) and region_id_val:
                                            region_id = region_id_val

                    if region_id:
                        region_name = resolve_region_name(region_id)
                except Exception:
                    # Do not fail refresh_market_orders if enrichment fails.
                    pass

                resolved = (location_name, region_id, region_name)
                location_region_cache[location_id] = resolved
                return resolved

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
                resolved_location_name, resolved_region_id, resolved_region_name = resolve_location_region(location_id)

                # Prefer the region_id coming from the order payload when present,
                # but always resolve a human-readable region_name from region_id.
                final_region_id = order.get("region_id", None) or resolved_region_id
                final_region_name = resolve_region_name(final_region_id) or resolved_region_name

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
                    "location_name": resolved_location_name,
                    "region_id": final_region_id,
                    "region_name": final_region_name,
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
            assets = self._esi_client.esi_get(f"/characters/{self.character_id}/assets/", paginate=True)
            blueprints = self._esi_client.esi_get(f"/characters/{self.character_id}/blueprints/", paginate=True)
            market_prices = self._esi_client.esi_get(f"/markets/prices/")

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
                
                # --- Calculate actual volume ---
                sde_volume = getattr(type_data, "volume", 0.0) if type_data else 0.0
                repackaged_volume = None

                # Check type repackaged_volume
                if type_data and hasattr(type_data, "repackaged_volume") and type_data.repackaged_volume:
                    repackaged_volume = type_data.repackaged_volume
                # If not, check group repackaged_volume
                elif group_data and hasattr(group_data, "repackaged_volume") and group_data.repackaged_volume:
                    repackaged_volume = group_data.repackaged_volume

                # Use repackaged_volume if repackaged, else normal volume
                if asset.get("is_singleton", False) == False and repackaged_volume is not None:
                    actual_volume = repackaged_volume
                else:
                    actual_volume = getattr(type_data, "volume", 0.0) if type_data else 0.0

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
                    f"/characters/{self.character_id}/assets/names",
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
                    f"/characters/{self.character_id}/assets/names",
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

