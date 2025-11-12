import logging
import json
import traceback
from datetime import datetime
from typing import Optional, List, Dict, Any

from classes.database_manager import DatabaseManager
from classes.config_manager import ConfigManager
from classes.esi import ESIClient
from classes.database_models import CharacterModel, CharacterWalletJournalModel \
    , CharacterWalletTransactionsModel, CharacterMarketOrdersModel, CharacterAssetsModel
from classes.database_models import NpcCorporations, Bloodlines, Races, Types, Groups, Categories

class Character:
    """Handles authentication and profile for an in-game character using ESIClient."""

    def __init__(self, 
                 cfgManager: ConfigManager, 
                 db_oauth: DatabaseManager,
                 db_app: DatabaseManager,
                 db_sde: DatabaseManager,
                 character_name: str, 
                 is_main: bool = False,
                 is_corp_director: bool = False,
                 refresh_token: Optional[str] = None
        ):
        self.cfgManager = cfgManager
        self.cfg = cfgManager.all()
        self.db_oauth = db_oauth
        self.db_app = db_app
        self.db_sde = db_sde
        self.character_name = character_name
        self.is_main = is_main
        self.is_corp_director = is_corp_director
        self.refresh_token = refresh_token
        
        # Runtime properties
        self.character_id: Optional[int] = None
        self.image_url: Optional[str] = None
        self.birthday: Optional[str] = None
        self.bloodline_id: Optional[int] = None
        self.bloodline: Optional[str] = None
        self.race_id: Optional[int] = None
        self.race: Optional[str] = None
        self.gender: Optional[str] = None
        self.corporation_id: Optional[int] = None
        self.description: Optional[str] = None
        self.security_status: Optional[float] = None
        self.updated_at: Optional[datetime] = None
        
        # Additional properties
        self.wallet_balance: Optional[float] = None
        self.skills: Optional[Dict[str, Any]] = None
        self.standings: Optional[List[Dict[str, str]]] = None
        self.wallet_journal: Optional[List[Dict[str, Any]]] = None
        self.wallet_transactions: Optional[List[Dict[str, Any]]] = None
        self.reprocessing_skills: Optional[Dict[str, int]] = None
        self.market_orders: Optional[List[Dict[str, Any]]] = None
        self.assets: Optional[List[Dict[str, Any]]] = None

        # Initialize ESI Client (handles token registration/refresh automatically)
        logging.debug(f"Initializing ESIClient for {self.character_name}...")
        self.esi_client = ESIClient(cfgManager, self.db_oauth, self.character_name, self.is_main, self.is_corp_director, self.refresh_token)
        self.character_id = self.esi_client.character_id
        logging.debug(f"ESIClient initialized for {self.character_name}.")

        if not self.load_character():
            self.refresh_all()
        else:
            logging.debug(f"Character data loaded from database for {self.character_name} ({self.character_id})")
    
    # -------------------
    # Safe Character
    # -------------------
    def save_character(self) -> None:
        """Save the current runtime properties of the character to the database."""

        character_record = (self.db_app.session.query(CharacterModel).filter_by(character_name=self.character_name).first())

        if not character_record:
            # Create new record if it doesn't exist
            character_record = CharacterModel(
                character_name=self.character_name, 
                character_id=self.character_id, 
                is_main=self.is_main, 
                is_corp_director=self.is_corp_director
            )
            self.db_app.session.add(character_record)

        # Dynamically update based on CharacterModel's columns
        for column in CharacterModel.__table__.columns.keys():
            if hasattr(self, column):
                value = getattr(self, column)
                if column == "skills" and isinstance(value, dict):
                    value = json.dumps(value)  # convert dict → string
                setattr(character_record, column, value)
        
            if hasattr(self, "standings"):
                character_record.standings = json.dumps(self.standings)

        character_record.updated_at = datetime.utcnow()

        self.db_app.session.commit()
        logging.debug(f"Character '{self.character_name}' saved to database.")

    # -------------------
    # Load Character
    # -------------------
    def load_character(self) -> bool:
        """Load character data from the database into the instance. Returns True if found."""

        character_record = (self.db_app.session.query(CharacterModel).filter_by(character_name=self.character_name).first())
        if not character_record:
            logging.debug(f"No database record found for character '{self.character_name}'.")
            return False

        # Dynamically use attributes from CharacterModel
        for column in CharacterModel.__table__.columns.keys():
            if hasattr(self, column):
                if column == "skills" and getattr(character_record, column):
                    setattr(self, column, json.loads(getattr(character_record, column)))
                elif column == "standings" and getattr(character_record, column):
                    setattr(self, column, json.loads(getattr(character_record, column)))
                else:
                    setattr(self, column, getattr(character_record, column))

        # Assign loaded entries to self.wallet_journal for runtime access
        character_wallet_journal = (self.db_app.session.query(CharacterWalletJournalModel).filter_by(character_id=self.character_id).all())
        self.wallet_journal = [
            {col: getattr(entry, col) for col in CharacterWalletJournalModel.__table__.columns.keys()}
            for entry in character_wallet_journal
        ]

        # Assign loaded entries to self.wallet_transactions for runtime access
        character_wallet_transactions = (self.db_app.session.query(CharacterWalletTransactionsModel).filter_by(character_id=self.character_id).all())
        self.wallet_transactions = [
            {col: getattr(entry, col) for col in CharacterWalletTransactionsModel.__table__.columns.keys()}
            for entry in character_wallet_transactions
        ]

        # Assign loaded entries to self.market_orders for runtime access
        character_market_orders = (self.db_app.session.query(CharacterMarketOrdersModel).filter_by(character_id=self.character_id).all())
        self.market_orders = [
            {col: getattr(entry, col) for col in CharacterMarketOrdersModel.__table__.columns.keys()}
            for entry in character_market_orders
        ]

        # Assign loaded entries to self.assets for runtime access
        character_assets = (self.db_app.session.query(CharacterAssetsModel).filter_by(character_id=self.character_id).all())
        self.assets = [
            {col: getattr(entry, col) for col in CharacterAssetsModel.__table__.columns.keys()}
            for entry in character_assets
        ]

        logging.debug(f"Character '{self.character_name}' loaded from database.")
        logging.debug(f"- Wallet journal entries loaded: {len(self.wallet_journal)}")
        logging.debug(f"- Wallet transactions loaded: {len(self.wallet_transactions)}")
        logging.debug(f"- Market orders loaded: {len(self.market_orders)}")
        logging.debug(f"- Assets loaded: {len(self.assets)}")
        return True

    # -------------------
    # Save Wallet Journal
    # -------------------
    def save_wallet_journal(self, journal_entries: List[Dict[str, Any]]) -> None:
        """Save wallet journal entries to the database."""
        if not journal_entries:
            logging.debug(f"No wallet journal entries to save for {self.character_name}.")
            return

        self.wallet_journal = []
        for entry in journal_entries:
            new_entry = CharacterWalletJournalModel(**entry)
            self.wallet_journal.append(new_entry)

        if self.wallet_journal:
            self.db_app.session.bulk_save_objects(self.wallet_journal)
            self.db_app.session.commit()
            logging.debug(f"Bulk wallet journal entries saved ({len(self.wallet_journal)}) for {self.character_name}.")
        else:
            logging.debug(f"No new wallet journal entries to save for {self.character_name}.")

    # -------------------
    # Save Wallet Transactions
    # -------------------
    def save_wallet_transactions(self, transactions: List[Dict[str, Any]]) -> None:
        """Save wallet transactions to the database."""
        if not transactions:
            logging.debug(f"No wallet transactions to save for {self.character_name}.")
            return

        self.wallet_transactions = []
        for transaction in transactions:
            new_trans = CharacterWalletTransactionsModel(**transaction)
            self.wallet_transactions.append(new_trans)
        
        if self.wallet_transactions:
            self.db_app.session.bulk_save_objects(self.wallet_transactions)
            self.db_app.session.commit()
            logging.debug(f"Bulk wallet transactions saved ({len(self.wallet_transactions)}) for {self.character_name}.")
        else:
            logging.debug(f"No new wallet transactions to save for {self.character_name}.")

    # -------------------
    # Save Market Orders
    # -------------------
    def save_market_orders(self, order_list: List[Dict[str, Any]]) -> None:
        """Save market orders to the database."""
        if not order_list:
            logging.debug(f"No market orders to save for {self.character_name}.")
            return

        self.market_orders = []
        for order in order_list:
            new_order = CharacterMarketOrdersModel(**order)
            self.market_orders.append(new_order)

        if self.market_orders:
            # Delete existing orders for this character and add new ones
            self.db_app.session.query(CharacterMarketOrdersModel).filter_by(character_id=self.character_id).delete()
            self.db_app.session.bulk_save_objects(self.market_orders)
            self.db_app.session.commit()
        else:
            logging.debug(f"No new market orders to save for {self.character_name}.")

        logging.debug(f"Market orders saved ({len(self.market_orders)}) for {self.character_name}.")

    # -------------------
    # Save Assets
    # -------------------
    def save_assets(self, asset_list: List[Dict[str, Any]]) -> None:
        """Save assets to the database."""
        if not asset_list:
            logging.debug(f"No assets to save for {self.character_name}.")
            return

        self.assets = []
        for asset in asset_list:
            new_asset = CharacterAssetsModel(**asset)
            self.assets.append(new_asset)
        
        if self.assets:
            # Delete existing assets for this character and add new ones
            self.db_app.session.query(CharacterAssetsModel).filter_by(character_id=self.character_id).delete()
            self.db_app.session.bulk_save_objects(self.assets)
            self.db_app.session.commit()
        else:
            logging.debug(f"No new assets to save for {self.character_name}.")

        logging.debug(f"Assets saved ({len(self.assets)}) for {self.character_name}.")
    
    # -------------------
    # Refresh All
    # -------------------
    def refresh_all(self) -> str:
        """Refresh all data for the current character and return a JSON string."""
        try:
             # Call individual data refresh methods
            profile_data = json.loads(self.refresh_profile(False))
            wallet_balance = json.loads(self.refresh_wallet_balance(False))
            skills = json.loads(self.refresh_skills(False))
            wallet_journal = json.loads(self.refresh_wallet_journal())
            wallet_transactions = json.loads(self.refresh_wallet_transactions())
            market_orders = json.loads(self.refresh_market_orders())
            assets = json.loads(self.refresh_assets())

            # Safe character
            self.save_character()

            # Merge all dictionaries into one
            combined_data = {
                "character_name": self.character_name,
                **profile_data,
                **wallet_balance,
                **skills,
                **wallet_journal,
                **wallet_transactions,
                **market_orders,
                **assets
            }

            # Convert to JSON string
            combined_json = json.dumps(combined_data, indent=4)

            return combined_json

        except Exception as e:
            raise e

    # -------------------
    # Refresh Profile
    # -------------------
    def refresh_profile(self, safe_character_fl: bool = True) -> str:
        """Fetch and update character profile data from ESI, saving data to `characters` table."""
        try:
            logging.debug(f"Refreshing profile for {self.character_name}...")
            profile_data = self.esi_client.esi_get(f"/characters/{self.character_id}/")
            standings_data = self.esi_client.esi_get(f"/characters/{self.character_id}/standings/")

            # Load additional details from the SDE database
            race_data = self.db_sde.session.query(Races).filter_by(id=profile_data.get("race_id")).first()
            bloodline_data = self.db_sde.session.query(Bloodlines).filter_by(id=profile_data.get("bloodline_id")).first()
            faction_data = self.db_sde.load_df("factions")
            npccorp_data = self.db_sde.load_df("npcCorporations")

            # Lookup tables
            def get_name(nameID, language):
                if isinstance(nameID, dict):
                    return nameID.get(language, next(iter(nameID.values()), "Unknown"))
                return nameID

            faction_lookup = {row['id']: get_name(row['nameID'], self.cfg["app"]["language"]) for _, row in faction_data.iterrows()}
            npccorp_lookup = {row['id']: get_name(row['nameID'], self.cfg["app"]["language"]) for _, row in npccorp_data.iterrows()}

            # Update runtime properties
            self.image_url = f"https://images.evetech.net/characters/{self.character_id}/portrait?size=128"
            self.birthday = profile_data["birthday"]
            self.bloodline_id = profile_data["bloodline_id"]
            self.bloodline = bloodline_data.nameID[self.db_sde.language] if bloodline_data else None
            self.race_id = profile_data["race_id"]
            self.race = race_data.nameID[self.db_sde.language] if race_data else None
            self.gender = profile_data.get("gender")
            self.corporation_id = profile_data.get("corporation_id")
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
            if safe_character_fl == True:
                self.save_character()

            logging.debug(f"Profile data successfully updated for {self.character_name}.")
            return json.dumps({'character_name': self.character_name, 'profile_data': profile_data}, indent=4)
        
        except Exception as e:
            logging.error(f"Failed to refresh profile for {self.character_name}. Error: {e}")
            return json.dumps({'character_name': self.character_name, 'error': str(e)}, indent=4)

    # -------------------
    # Refresh Wallet Balance
    # -------------------
    def refresh_wallet_balance(self, safe_character_fl: bool = True) -> str:
        """
        Refresh the wallet balance for this character. Updates the `characters` table in the database.

        :return: JSON response with character_name and wallet_balance.
        """
        try:
            logging.debug(f"Refreshing wallet balance for {self.character_name}...")
            self.wallet_balance = self.esi_client.esi_get(f"/characters/{self.character_id}/wallet/")

            # Save to database
            if safe_character_fl == True:
                self.save_character()   

            logging.debug(f"Wallet balance successfully updated for {self.character_name}. Balance: {self.wallet_balance:.2f}")
            return json.dumps({'character_name': self.character_name, 'wallet_balance': self.wallet_balance}, indent=4)
        
        except Exception as e:
            logging.error(f"Failed to refresh wallet balance for {self.character_name}. Error: {e}")
            return json.dumps({'character_name': self.character_name, 'error': str(e)}, indent=4)
    
    # -------------------
    # Refresh Wallet Journal
    # -------------------
    def refresh_wallet_journal(self, save_wallet_journal_fl: bool = True) -> str:
        try:
            logging.debug(f"Getting wallet journal for {self.character_name}...")
            journal_entries = self.esi_client.esi_get(f"/characters/{self.character_id}/wallet/journal/")

            new_journal_entries = []
            for entry in journal_entries:
                entry_wallet_journal_id = entry.get("id")
                if entry_wallet_journal_id is None:
                    continue  # Skip entries without an ID

                existing_entry = (
                    self.db_app.session.query(CharacterWalletJournalModel)
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
                id_type = self.esi_client.get_id_type(pid)
                if id_type == "character":
                    data = self.esi_client.esi_get(f"/characters/{pid}/")
                    if data and "name" in data:
                        name = data["name"]
                elif id_type == "alliance":
                    data = self.esi_client.esi_get(f"/alliances/{pid}/")
                    if data and "name" in data:
                        name = data["name"]
                elif id_type == "corporation":
                    data = self.esi_client.esi_get(f"/corporations/{pid}/")
                    if data and "name" in data:
                        name = data["name"]
                elif id_type == "npc_corporation":
                    npc_corp = self.db_sde.session.query(NpcCorporations).filter_by(id=pid).first()
                    name = npc_corp.nameID[self.db_sde.language] if npc_corp else None
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

            if save_wallet_journal_fl:
                self.save_wallet_journal(new_entries)

            return json.dumps({'character_name': self.character_name, 'wallet_journal_new': new_entries}, indent=4)

        except Exception as e:
            logging.error(f"Failed to refresh wallet journal for {self.character_name}. Error: {e}")
            return json.dumps({'character_name': self.character_name, 'error': str(e)}, indent=4)
    
    # -------------------
    # Refresh Wallet Transactions
    # -------------------
    def refresh_wallet_transactions(self, save_wallet_transaction_fl: bool=True) -> str:
        try:
            logging.debug(f"Getting wallet transactions for {self.character_name}...")
            transactions = self.esi_client.esi_get(f"/characters/{self.character_id}/wallet/transactions/")

            new_transaction_entries = []
            for entry in transactions:
                entry_transaction_id = entry.get("transaction_id")
                if entry_transaction_id is None:
                    continue  # Skip entries without an ID

                existing_entry = (
                    self.db_app.session.query(CharacterWalletTransactionsModel)
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
                id_type = self.esi_client.get_id_type(cid)
                if id_type == "character":
                    data = self.esi_client.esi_get(f"/characters/{cid}/")
                    if data and "name" in data:
                        name = data["name"]
                elif id_type == "alliance":
                    data = self.esi_client.esi_get(f"/alliances/{cid}/")
                    if data and "name" in data:
                        name = data["name"]
                elif id_type == "corporation":
                    data = self.esi_client.esi_get(f"/corporations/{cid}/")
                    if data and "name" in data:
                        name = data["name"]
                elif id_type == "npc_corporation":
                    npc_corp = self.db_sde.session.query(NpcCorporations).filter_by(id=cid).first()
                    name = npc_corp.nameID[self.db_sde.language] if npc_corp else None
                else:
                    continue  # Unknown type, skip

                client_names[cid] = name

            # Step 3: Assign names to transaction entries
            new_entries = []
            for entry in new_transaction_entries:
                cid = entry.get("client_id")
                entry["client_name"] = client_names.get(cid)

                type_id = entry.get("type_id")
                type = self.db_sde.session.query(Types).filter_by(id=type_id).first()
                entry["type_name"] = type.name[self.db_sde.language] if type else None
                group = self.db_sde.session.query(Groups).filter_by(id=type.groupID).first() if type else None
                entry["type_group_id"] = group.id if group else None
                entry["type_group_name"] = group.name[self.db_sde.language] if group else None
                category = self.db_sde.session.query(Categories).filter_by(id=group.categoryID).first() if group else None
                entry["type_category_id"] = category.id if category else None
                entry["type_category_name"] = category.name[self.db_sde.language] if category else None

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

            if save_wallet_transaction_fl:
                self.save_wallet_transactions(new_entries)

            return json.dumps({'character_name': self.character_name, 'wallet_transactions_new': new_entries}, indent=4)
        
        except Exception as e:
            logging.error(f"Failed to refresh wallet transactions for {self.character_name}. Error: {e}")
            return json.dumps({'character_name': self.character_name, 'error': str(e)}, indent=4)

    # -------------------
    # Skillpoints
    # -------------------
    def extract_reprocessing_skills(self) -> str:
        """Extract reprocessing-related skills and levels from self.skills."""
        if not self.skills or "skills" not in self.skills:
            self.refresh_skills()

        try:
            logging.debug(f"Extracting reprocessing skills for {self.character_name}...")

            # Find all relevant skill groups
            skill_groups = self.db_sde.session.query(Groups).filter(
                Groups.categoryID == 16,  # Skills category
                Groups.published == 1,
                Groups.name[self.db_sde.language].ilike("%Processing%")
            ).all()

            # Get all skill type IDs in these groups
            skill_ids = set()
            skills_in_groups = self.db_sde.session.query(Types).filter(
                Types.groupID.in_([g.id for g in skill_groups]),
                Types.published == 1,
                Types.name[self.db_sde.language].ilike("%Processing%")
            ).all()
            for skill in skills_in_groups:
                skill_ids.add(skill.id)

            # Build mapping: skill ID → skill name
            skill_map = {}
            all_skills = self.db_sde.session.query(Types).filter(Types.id.in_(list(skill_ids))).all()
            for skill in all_skills:
                skill_map[skill.id] = skill.name[self.db_sde.language]

            # Extract trained levels
            self.reprocessing_skills = {}
            for skill in self.skills["skills"]:
                skill_id = skill.get("skill_id")
                if skill_id in skill_map:
                    self.reprocessing_skills[skill_map[skill_id]] = skill.get("trained_skill_level", 0)

            logging.debug(f"Reprocessing skills successfully updated for {self.character_name}.")
            return self.reprocessing_skills
        except Exception as e:
            logging.error(f"Failed to refresh reprocessing skills for {self.character_name}. Error: {e}")
            return json.dumps({'character_name': self.character_name, 'error': f"Failed to refresh reprocessing skills: {str(e)}"}, indent=4)

    def refresh_skills(self, save_character_fl: bool = True) -> str:
        try:
            logging.debug(f"Getting skills for {self.character_name}...")
            # All trained skills for the character from ESI
            skills = self.esi_client.esi_get(f"/characters/{self.character_id}/skills/")
            skill_list = skills.get("skills", [])

            # Current skill queue for the character from ESI
            skill_queue = self.esi_client.esi_get(f"/characters/{self.character_id}/skillqueue/")

            # Map character skills and skill queue
            character_skill_ids = {s["skill_id"]: s for s in skill_list} 
            character_skill_queue_ids = {s["skill_id"]: s for s in skill_queue}
            
            # All skill groups (categoryID=16) and all skills for those groups from SDE
            all_groups = self.db_sde.session.query(Groups).filter(Groups.categoryID == 16, Groups.published == 1).all()
            all_skills = self.db_sde.session.query(Types).filter(Types.groupID.in_([g.id for g in all_groups]), Types.published == 1).all()
            
            group_map_names = {g.id: g.name[self.db_sde.language] for g in all_groups}
            skill_map = {}
            for t in all_skills:
                group_name = group_map_names.get(t.groupID, "Unknown")
                skill_map[t.id] = {
                    "skill_id": t.id,
                    "skill_name": t.name[self.db_sde.language],
                    "skill_desc": t.description[self.db_sde.language],
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
            if save_character_fl == True:
                self.save_character()

            logging.debug(f"Skills successfully updated for {self.character_name}. Total skill points: {self.skills['total_skillpoints']}")
            return json.dumps({'character_name': self.character_name, 'skills': self.skills}, indent=4)
        
        except Exception as e:
            logging.error(f"Failed to refresh skills for {self.character_name}. Error: {e}")
            return json.dumps({'character_name': self.character_name, 'error': str(e)}, indent=4)
    
    # -------------------
    # Market Orders
    # -------------------
    def refresh_market_orders(self, safe_market_orders_fl=True) -> str:
        try:
            logging.debug(f"Getting market orders for {self.character_name}...")
            order_list = self.esi_client.esi_get(f"/characters/{self.character_id}/orders/")

            orders = []
            type_ids = set(order.get("type_id") for order in order_list)
            type_data_map = {t.id: t for t in self.db_sde.session.query(Types).filter(Types.id.in_(type_ids)).all()}
            group_ids = set(t.groupID for t in type_data_map.values())
            group_data_map = {g.id: g for g in self.db_sde.session.query(Groups).filter(Groups.id.in_(group_ids)).all()}
            category_ids = set(g.categoryID for g in group_data_map.values())
            category_data_map = {c.id: c for c in self.db_sde.session.query(Categories).filter(Categories.id.in_(category_ids)).all()}
            for order in order_list:
                type_id = order.get("type_id")
                type_data = type_data_map.get(type_id)
                group_data = group_data_map.get(type_data.groupID) if type_data else None
                category_data = category_data_map.get(group_data.categoryID) if group_data else None
                new_order = {
                    "character_id": self.character_id,
                    "order_id": order.get("order_id", None),
                    "type_id": order.get("type_id", None),
                    "type_name": type_data.name[self.db_sde.language] if type_data else None,
                    "type_group_id": type_data.groupID if type_data else None,
                    "type_group_name": group_data.name[self.db_sde.language] if group_data else None,
                    "type_category_id": group_data.categoryID if group_data else None,
                    "type_category_name": category_data.name[self.db_sde.language] if category_data else None,
                    "location_id": order.get("location_id", None),
                    "region_id": order.get("region_id", None),
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

            if safe_market_orders_fl == True:
                self.save_market_orders(orders)

            return json.dumps({'character_name': self.character_name, 'market_orders': orders}, indent=4)
        
        except Exception as e:
            logging.error(f"Failed to refresh market orders for {self.character_name}. Error: {e}")
            return json.dumps({'character_name': self.character_name, 'error': str(e)}, indent=4)
        
    # -------------------
    # Assets
    # -------------------
    def refresh_assets(self, save_assets_fl: bool = True) -> str:
        """
        Refresh the asset list of the character from ESI and enrich with SDE and container custom names.
        Returns:
            str: JSON string with refreshed asset list.
        """
        try:
            logging.debug(f"Getting assets for {self.character_name}...")
            assets = self.esi_client.esi_get(f"/characters/{self.character_id}/assets/", paginate=True)
            market_prices = self.esi_client.esi_get(f"/markets/prices/")

            asset_list = []
            type_ids = set(asset.get("type_id") for asset in assets)
            type_data_map = {t.id: t for t in self.db_sde.session.query(Types).filter(Types.id.in_(type_ids)).all()}
            group_ids = set(t.groupID for t in type_data_map.values())
            group_data_map = {g.id: g for g in self.db_sde.session.query(Groups).filter(Groups.id.in_(group_ids)).all()}
            category_ids = set(g.categoryID for g in group_data_map.values())
            category_data_map = {c.id: c for c in self.db_sde.session.query(Categories).filter(Categories.id.in_(category_ids)).all()}
            for asset in assets:
                type_id = asset.get("type_id")
                type_data = type_data_map.get(type_id)
                type_adjusted_price = next((item.get("adjusted_price", 0.0) for item in market_prices if item.get("type_id") == type_id), 0.0)
                type_average_price = next((item.get("average_price", 0.0) for item in market_prices if item.get("type_id") == type_id), 0.0)
                group_data = group_data_map.get(type_data.groupID) if type_data else None
                category_data = category_data_map.get(group_data.categoryID) if group_data else None

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
                    "type_name": getattr(type_data, "name", {}).get(self.db_sde.language, "") if type_data else "",
                    "type_default_volume": sde_volume,
                    "type_repackaged_volume": repackaged_volume,
                    "type_volume": actual_volume,
                    "type_capacity": getattr(type_data, "capacity", None) if type_data else None,
                    "type_description": getattr(type_data, "description", {}).get(self.db_sde.language, "") if type_data and getattr(type_data, "description", None) else "",
                    "container_name": None,
                    "ship_name": None,
                    "type_group_id": getattr(type_data, "groupID", None) if type_data else None,
                    "type_group_name": getattr(group_data, "name", {}).get(self.db_sde.language, "") if group_data else "",
                    "type_category_id": getattr(group_data, "categoryID", None) if group_data else None,
                    "type_category_name": getattr(category_data, "name", {}).get(self.db_sde.language, "") if category_data else "",
                    "location_id": asset.get("location_id"),
                    "location_type": asset.get("location_type"),
                    "location_flag": asset.get("location_flag"),
                    "is_singleton": asset.get("is_singleton"),
                    "is_blueprint_copy": asset.get("is_blueprint_copy", False),
                    "quantity": asset.get("quantity", 0),
                    "type_adjusted_price": type_adjusted_price,
                    "type_average_price": type_average_price
                }
                asset_list.append(asset_entry)
            
            # Fetch custom names for containers (type_id == 17366 and is_singleton == True)
            container_ids = [a["item_id"] for a in asset_list if a.get("type_id") == 17366 and a.get("is_singleton") == True]
            container_names = {}
            if container_ids:
                names_response = self.esi_client.esi_post(
                    f"/characters/{self.character_id}/assets/names",
                    json=container_ids
                )
                if names_response and isinstance(names_response, list):
                    container_names = {c["item_id"]: c["name"] for c in names_response}

            for asset in asset_list:
                if asset.get("item_id") in container_names:
                    asset["container_name"] = container_names[asset["item_id"]]
            
            # Fetch custom names for ships (type_category_id == 6 and is_singleton == True)
            ship_ids = [a["item_id"] for a in asset_list if a.get("type_category_id") == 6 and a.get("is_singleton") == True]
            ship_names = {}
            if ship_ids:
                names_response = self.esi_client.esi_post(
                    f"/characters/{self.character_id}/assets/names",
                    json=ship_ids
                )
                if names_response and isinstance(names_response, list):
                    ship_names = {s["item_id"]: s["name"] for s in names_response}

            for asset in asset_list:
                if asset.get("item_id") in ship_names:
                    asset["ship_name"] = ship_names[asset["item_id"]]
            
            # Lookup for all containers and ships by item_id
            container_lookup = {a["item_id"]: a for a in asset_list if a.get("type_id") == 17366 and a.get("is_singleton")}
            ship_lookup = {a["item_id"]: a for a in asset_list if a.get("type_category_id") == 6 and a.get("is_singleton")}

            for asset in asset_list:
                # Assign container_name for non-containers
                if not (asset.get("type_id") == 17366 and asset.get("is_singleton")):
                    container = container_lookup.get(asset.get("location_id"))
                    if container:
                        asset["container_name"] = container.get("container_name")
                # Assign ship_name for non-ships (modules, etc.)
                if not (asset.get("type_category_id") == 6 and asset.get("is_singleton")):
                    ship = ship_lookup.get(asset.get("location_id"))
                    if ship:
                        asset["ship_name"] = ship.get("ship_name")

            if save_assets_fl:
                self.save_assets(asset_list)

            logging.debug(f"Assets successfully updated for {self.character_name}. Total assets: {len(asset_list)}")
            return json.dumps({'character_name': self.character_name, 'assets': asset_list}, indent=4)
        except Exception as e:
            logging.error(f"Failed to refresh assets for {self.character_name}. Error: {e}\n{traceback.format_exc()}")
            return json.dumps({'character_name': self.character_name, 'assets': [], 'error': str(e)}, indent=4)