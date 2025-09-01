import logging
import json
from datetime import datetime
from typing import Optional, Dict, Any

from classes.database_manager import DatabaseManager
from classes.config_manager import ConfigManager
from classes.esi import ESIClient
from classes.database_models import CharacterModel, Bloodlines, Races, Types, Groups

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

        # Skills
        self.skills: Optional[Dict[str, Any]] = None

        # Initialize ESI Client (handles token registration/refresh automatically)
        logging.debug(f"Initializing ESIClient for {self.character_name}...")
        self.esi_client = ESIClient(cfg, db_oauth, character_name, is_main, refresh_token)
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
            character_record = CharacterModel(character_name=self.character_name, character_id=self.character_id, is_main=self.is_main)
            self.db_app.session.add(character_record)

        # Dynamically update based on CharacterModel's columns
        for column in CharacterModel.__table__.columns.keys():
            if hasattr(self, column):
                value = getattr(self, column)
                if column == "skills" and isinstance(value, dict):
                    value = json.dumps(value)  # convert dict → string
                setattr(character_record, column, value)
        
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
                else:
                    setattr(self, column, getattr(character_record, column))

        logging.debug(f"Character '{self.character_name}' loaded from database.")
        return True

    # -------------------
    # Refresh All
    # -------------------
    def refresh_all(self) -> str:
        """Refresh all data for the current character and return a JSON string."""
        try:
             # Call individual data refresh methods
            profile_data = json.loads(self.refresh_profile())
            wallet_balance = json.loads(self.refresh_wallet_balance())
            skills = json.loads(self.refresh_skills())

            # Merge all dictionaries into one
            combined_data = {
                "character_name": self.character_name,
                **profile_data,
                **wallet_balance,
                **skills
            }

            # Convert to JSON string
            combined_json = json.dumps(combined_data, indent=4)

            return combined_json

        except Exception as e:
            raise e

    # -------------------
    # Refresh Profile
    # -------------------
    def refresh_profile(self) -> str:
        """Fetch and update character profile data from ESI, saving data to `characters` table."""
        try:
            logging.debug(f"Refreshing profile for {self.character_name}...")
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

            logging.debug(f"Profile data successfully updated for {self.character_name}.")
            return json.dumps({'character_name': self.character_name, 'profile_data': profile_data}, indent=4)
        
        except Exception as e:
            logging.error(f"Failed to refresh profile for {self.character_name}. Error: {e}")
            return json.dumps({'character_name': self.character_name, 'error': str(e)}, indent=4)

    # -------------------
    # Refresh Wallet Balance
    # -------------------
    def refresh_wallet_balance(self) -> str:
        """
        Refresh the wallet balance for this character. Updates the `characters` table in the database.

        :return: JSON response with character_name and wallet_balance.
        """
        try:
            logging.debug(f"Refreshing wallet balance for {self.character_name}...")
            self.wallet_balance = self.esi_client.esi_get(f"/characters/{self.character_id}/wallet/")

            # Save to database
            self.save_character()

            logging.debug(f"Wallet balance successfully updated for {self.character_name}. Balance: {self.wallet_balance:.2f}")
            return json.dumps({'character_name': self.character_name, 'wallet_balance': self.wallet_balance}, indent=4)
        
        except Exception as e:
            logging.error(f"Failed to refresh wallet balance for {self.character_name}. Error: {e}")
            return json.dumps({'character_name': self.character_name, 'error': str(e)}, indent=4)
    
    # -------------------
    # Skillpoints
    # -------------------
    def refresh_skills(self) -> str:
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

            # Save to database
            self.save_character()

            logging.debug(f"Skills successfully updated for {self.character_name}. Total skill points: {self.skills['total_skillpoints']}")
            return json.dumps({'character_name': self.character_name, 'skills': self.skills}, indent=4)
        
        except Exception as e:
            logging.error(f"Failed to refresh skills for {self.character_name}. Error: {e}")
            return json.dumps({'character_name': self.character_name, 'error': str(e)}, indent=4)