import logging
from datetime import datetime
from typing import Optional, Dict, Any

from classes.database_manager import DatabaseManager
from classes.config_manager import ConfigManager
from classes.esi import ESIClient
from classes.database_models import CharacterModel, Bloodlines, Races



class Character:
    """Handles authentication and profile for an in-game character using ESIClient."""


    def __init__(self, 
                 cfg: ConfigManager, 
                 db_oauth: DatabaseManager, 
                 db_app: DatabaseManager, 
                 db_sde: DatabaseManager, 
                 character_name: str, 
                 is_main: bool = False,
                 refresh_token: Optional[str] = None
        ):
        self.cfg = cfg
        self.db_oauth = db_oauth
        self.db_app = db_app
        self.db_sde = db_sde
        self.character_name = character_name
        self.is_main = is_main
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
        
        # Wallet balance
        self.wallet_balance: Optional[float] = None

        # Initialize ESI Client (handles token registration/refresh automatically)
        logging.debug(f"Initializing ESIClient for {self.character_name}...")
        self.esi_client = ESIClient(cfg, db_oauth, character_name, is_main, refresh_token)
        self.character_id = self.esi_client.character_id
        logging.debug(f"ESIClient initialized for {self.character_name}.")

        if not self.load_character():
            # Initialize character profile
            logging.debug(f"Initializing characters profile for {self.character_name}...")
            self.refresh_profile()
            logging.debug(f"Character profile initialized for {self.character_name} - {self.character_id}")

            # Initialize character's wallet balance
            logging.debug(f"Initializing characters wallet balance for {self.character_name}...")
            self.refresh_wallet_balance()
            logging.debug(f"Characters wallet balance initialized for {self.character_name} - {self.wallet_balance}")
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
            character_record = CharacterModel(character_name=self.character_name, character_id=self.character_id, is_main=self.is_main)
            self.db_app.session.add(character_record)

        # Dynamically update all attributes in the runtime instance that match DB columns
        for attr in [
            "character_id", "character_name", "birthday", "bloodline_id",
            "bloodline", "race_id", "race", "gender", "corporation_id",
            "description", "security_status", "wallet_balance"
        ]:
            if hasattr(self, attr):
                setattr(character_record, attr, getattr(self, attr))

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
            logging.info(f"No database record found for character '{self.character_name}'.")
            return False

        # Copy attributes from DB record to instance
        for attr in [
            "character_id", "character_name", "birthday", "bloodline_id",
            "bloodline", "race_id", "race", "gender", "corporation_id",
            "description", "security_status", "wallet_balance", "updated_at"
        ]:
            setattr(self, attr, getattr(character_record, attr))

        logging.info(f"Character '{self.character_name}' loaded from database.")
        return True

    # -------------------
    # Refresh Profile
    # -------------------
    def refresh_profile(self) -> Dict[str, Any]:
        """Fetch and update character profile data from ESI, saving data to `characters` table."""
        try:
            logging.info(f"Refreshing profile for {self.character_name}...")
            profile_data = self.esi_client.esi_get(f"/characters/{self.character_id}/")

            # Load additional details from the SDE database
            race_data = self.db_sde.session.query(Races).filter_by(id=profile_data.get("race_id")).first()
            bloodline_data = self.db_sde.session.query(Bloodlines).filter_by(id=profile_data.get("bloodline_id")).first()

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

            # Save to database
            self.save_character()

            logging.info(f"Profile data successfully updated for {self.character_name}.")
            return {"character_name": self.character_name, "profile_data": profile_data}
        
        except Exception as e:
            logging.error(f"Failed to refresh profile for {self.character_name}. Error: {e}")
            return {"character_name": self.character_name, "error": str(e)}

    # -------------------
    # Refresh Wallet Balance
    # -------------------
    def refresh_wallet_balance(self) -> Dict:
        """
        Refresh the wallet balance for this character. Updates the `characters` table in the database.

        :return: JSON response with character_name and wallet_balance.
        """
        try:
            logging.info(f"Refreshing wallet balance for {self.character_name}...")
            wallet_balance = self.esi_client.esi_get(f"/characters/{self.character_id}/wallet/")
            self.wallet_balance = wallet_balance

            # Save to database
            self.save_character()

            logging.info(f"Wallet balance successfully updated for {self.character_name}. Balance: {self.wallet_balance:.2f}")
            return {"character_name": self.character_name, "wallet_balance": self.wallet_balance}
        
        except Exception as e:
            logging.error(f"Failed to refresh wallet balance for {self.character_name}. Error: {e}")
            return {"character_name": self.character_name, "error": str(e)}
