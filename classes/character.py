import logging
import pandas as pd
from datetime import datetime
from typing import Optional, Dict, Any

from classes.database_manager import DatabaseManager
from classes.config_manager import ConfigManager
from classes.esi import ESIClient
from classes.database_models import OAuthCharacter

class Character:
    """Class `Character` handles authentication and properties for a given in-game character."""
    def __init__(self, 
                 cfg: ConfigManager, 
                 db_oauth: DatabaseManager, 
                 db_app: DatabaseManager, 
                 db_sde: DatabaseManager, 
                 character_name: str, 
                 is_main: bool,
                 refresh_token: Optional[str]
        ):
        self.cfg = cfg
        self.scopes = cfg.get("defaults")["scopes"]
        self.db_oauth = db_oauth
        self.db_app = db_app
        self.db_sde = db_sde
        self.character_name = character_name
        self.is_main = is_main
        self.refresh_token = refresh_token

        # Initialize ESI Client
        logging.debug(f"Initializing ESI Client for {self.character_name}...")
        self.esi_client = ESIClient(self.cfg, self.db_oauth, self.character_name, self.is_main, self.refresh_token)
        logging.debug("Initialization succesfull.")
        # Authenticate character (if no refresh_token exists, register it)
        logging.debug(f"Authenticating {self.character_name}...")
        self._authenticate_or_register()
        logging.debug(f"Character {self.character_name} authenticated succesfully.")
        
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
        
        # Wallet balance
        self.wallet_balance: Optional[float] = None
    
    def _authenticate_or_register(self):
        """Handle authentication or registration of the character."""
        if self.refresh_token:
            try:
                logging.debug(f"Authenticating character: {self.character_name} with existing token.")
                self.esi_client.refresh_access_token()  # Refresh tokens for authentication
            except Exception as e:
                logging.error(f"Failed to authenticate character {self.character_name}. Error: {e}")
                raise RuntimeError(f"Authentication failed for character: {self.character_name}")
        else:
            try:
                logging.info(f"No token found for {self.character_name}. Registering new character.")
                access_token, refresh_token, expires_in = self.esi_client.register_new_character()
                self.refresh_token = refresh_token

                # Save newly registered character tokens in the database
                self.db_oauth.session.add(
                    OAuthCharacter(
                        character_name=self.character_name,
                        refresh_token=refresh_token,
                        scopes=" ".join(self.scopes),
                        is_main=self.is_main,
                    )
                )
                self.db_oauth.session.commit()
                logging.info(f"Character {self.character_name} registered successfully.")
            except Exception as e:
                logging.error(f"Failed to register new character {self.character_name}. Error: {e}")
                raise RuntimeError(f"Registration failed for character: {self.character_name}")

    # -------------------
    # Refresh Profile
    # -------------------
    def refresh_profile(self) -> Dict[str, Any]:
        """Fetch and update character profile data from ESI, saving data to `characters` table."""
        try:
            logging.info(f"Refreshing profile for {self.character_name}...")
            profile_data = self.esi_client.esi_get(f"/characters/{self.character_name}/")
            # Load additional details from the SDE database
            race_data = self.db_sde.session.query().filter_by(id=profile_data.get("race_id")).first()
            bloodline_data = self.db_sde.session.query().filter_by(id=profile_data.get("bloodline_id")).first()

            # Update character attributes
            self.character_id = profile_data["character_id"]
            self.image_url = f"https://images.evetech.net/characters/{self.character_id}/portrait?size=128"
            self.birthday = profile_data["birthday"]
            self.bloodline_id = profile_data["bloodline_id"]
            self.bloodline = bloodline_data.name if bloodline_data else None
            self.race_id = profile_data["race_id"]
            self.race = race_data.name if race_data else None
            self.gender = profile_data.get("gender")
            self.corporation_id = profile_data.get("corporation_id")
            self.description = profile_data.get("description")
            self.security_status = profile_data.get("security_status")

            # Update database using SQLAlchemy ORM
            character_record = (
                self.db_oauth_session.query(OAuthCharacter)
                .filter_by(character_name=self.character_name)
                .first()
            )
            if character_record:
                character_record.character_id = self.character_id
                character_record.birthday = self.birthday
                character_record.bloodline_id = self.bloodline_id
                character_record.bloodline = self.bloodline
                character_record.race_id = self.race_id
                character_record.race = self.race
                character_record.gender = self.gender
                character_record.corporation_id = self.corporation_id
                character_record.description = self.description
                character_record.security_status = self.security_status
                character_record.updated_at = datetime.utcnow()
                self.db_oauth_session.commit()
            else:
                raise RuntimeError(f"Database entry not found for character: {self.character_name}")

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
            wallet_data = self.esi_client.esi_get(f"/characters/{self.character_name}/wallet/")
            self.wallet_balance = wallet_data

            # Update wallet balance field in the database
            character_record = (
                self.db_oauth_session.query(OAuthCharacter)
                .filter_by(character_name=self.character_name)
                .first()
            )
            if character_record:
                character_record.wallet_balance = self.wallet_balance
                character_record.updated_at = datetime.utcnow()
                self.db_oauth_session.commit()
            else:
                raise RuntimeError(f"Database entry not found for character: {self.character_name}")

            logging.info(
                f"Wallet balance successfully updated for {self.character_name}. Balance: {self.wallet_balance:.2f}"
            )
            return {"character_name": self.character_name, "wallet_balance": self.wallet_balance}
        except Exception as e:
            logging.error(f"Failed to refresh wallet balance for {self.character_name}. Error: {e}")
            return {"character_name": self.character_name, "error": str(e)}
