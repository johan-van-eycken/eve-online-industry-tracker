import logging
import json
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from classes.config_manager import ConfigManager
from classes.database_manager import DatabaseManager
from classes.database_models import CorporationModel, CorporationStructuresModel \
    , CorporationMemberModel, CorporationAssetsModel
from classes.database_models import Types, Groups, Categories, Factions, Races, NpcCorporations
from classes.database_models import CorporationWalletTransactionsModel, CorporationIndustryJobsModel
from classes.character import Character
from classes.character_manager import CharacterManager
from classes.asset_provenance import build_cost_map_for_assets

class Corporation:
    """Ingame entity of a corporation."""

    def __init__(
        self,
        cfgManager: ConfigManager,
        db_app: DatabaseManager,
        db_sde: DatabaseManager,
        corporation_id: int,
        char_manager: CharacterManager
    ) -> None:
        try:
            # Private arguments
            self._cfgManager = cfgManager
            self._cfg = cfgManager.all()
            self._db_app = db_app
            self._db_sde = db_sde
            self._char_manager = char_manager
            self._default_esi_character: Character = self._char_manager.get_corp_director()

            # Default ESI character
            if not self._default_esi_character:
                logging.warning("No CEO character found. Defaulting to main character.")
                self._default_esi_character = self._char_manager.get_main_character()
                if not self._default_esi_character:
                    logging.warning("No main character found. Defaulting to first character in list.")
                    self._default_esi_character = self._char_manager.character_list[0]
            if not self._default_esi_character:
                raise ValueError("No valid character available for corporation operations.")

            # Public arguments
            self.corporation_id = corporation_id
            self.corporation_name: Optional[str] = None
            self.creator_id: Optional[int] = None
            self.ceo_id: Optional[int] = None
            self.ceo_name: Optional[str] = None
            self.date_founded: Optional[datetime] = None
            self.description: Optional[str] = None
            self.home_station_id: Optional[int] = None
            self.member_count: Optional[int] = None
            self.shares: Optional[int] = None
            self.tax_rate: Optional[float] = None
            self.ticker: Optional[str] = None
            self.url: Optional[str] = None
            self.war_eligible: Optional[bool] = None
            self.image_url: Optional[str] = None
            self.wallets: List[Dict[str, str]] = []
            self.standings: List[Dict[str, str]] = []
            self.structures: List[CorporationStructuresModel] = []
            self.members: List[CorporationMemberModel] = []
            self.assets: List[CorporationAssetsModel] = []
            self.updated_at: Optional[datetime] = None

            # Intentionally do NOT call refresh methods here.
            # Constructors should be cheap; refresh is handled explicitly by managers/bootstrap.
        except Exception as e:
            error_message = f"Failed to initialize Corporation ID '{corporation_id}'. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    # -------------------
    # Get Corporation as dict
    # -------------------
    def get_corporation(self) -> Dict[str, Any]:
        """Convert corporation model to Dict, serializing all nested objects."""
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
            "corporation_id": self.corporation_id,
            "corporation_name": self.corporation_name,
            "ticker": self.ticker,
            "description": self.description,
            "member_count": self.member_count,
            "creator_id": self.creator_id,
            "ceo_id": self.ceo_id,
            "ceo_name": self.ceo_name,
            "home_station_id": self.home_station_id,
            "shares": self.shares,
            "tax_rate": self.tax_rate,
            "url": self.url,
            "war_eligible": self.war_eligible,
            "image_url": self.image_url,
            "date_founded": self.date_founded,
            "wallets": self.wallets,
            "standings": self.standings,
            "structures": serialize_model_list(self.structures, CorporationStructuresModel),
            "members": serialize_model_list(self.members, CorporationMemberModel),
            "assets": serialize_model_list(self.assets, CorporationAssetsModel),
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    # -------------------
    # Get Assets
    # -------------------
    def get_assets(self) -> Dict[str, Any]:
        """Return the corporation assets."""
        return {"corporation_name": self.corporation_name, "corporation_id": self.corporation_id, "assets": self.assets if self.assets is not None else []}

    # -------------------
    # Get Members
    # -------------------
    def get_members(self) -> Dict[str, Any]:
        """Return the corporation members."""
        return {"corporation_name": self.corporation_name, "corporation_id": self.corporation_id, "members": self.members if self.members is not None else []}

    # -------------------
    # Get Structures
    # -------------------
    def get_structures(self) -> Dict[str, Any]:
        """Return the corporation structures."""
        return {"corporation_name": self.corporation_name, "corporation_id": self.corporation_id, "structures": self.structures if self.structures is not None else []}

    # -------------------
    # Save Corporation
    # -------------------
    def save_corporation(self) -> None:
        """Save the current corporation data to the database."""
        try:
            corporation_record = self._db_app.session.query(CorporationModel).filter_by(corporation_id=self.corporation_id).first()
            if not corporation_record:
                corporation_record = CorporationModel(corporation_id=self.corporation_id)
                self._db_app.session.add(corporation_record)
            for column in CorporationModel.__table__.columns.keys():
                if column == "id":
                    continue
                if hasattr(self, column):
                    value = getattr(self, column)
                    setattr(corporation_record, column, value)
            if hasattr(self, "wallets"):
                corporation_record.wallets = json.dumps(self.wallets)
            if hasattr(self, "standings"):
                corporation_record.standings = json.dumps(self.standings)
            corporation_record.updated_at = datetime.now(timezone.utc)
            self._db_app.session.commit()
            logging.debug(f"Corporation '{self.corporation_name}' saved to database.")
        except Exception as e:
            error_message = f"Failed to save Corporation '{self.corporation_name}'. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    # -------------------
    # Save Structures
    # -------------------
    def save_corporation_structures(self, corporation_structures: List[CorporationStructuresModel]) -> None:
        """Save the corporation structures to the database."""
        try:
            for structure in corporation_structures:
                existing_structure = self._db_app.session.query(CorporationStructuresModel).filter_by(structure_id=structure.structure_id).first()
                if existing_structure:
                    for column in CorporationStructuresModel.__table__.columns.keys():
                        if column == "id":
                            continue
                        if hasattr(structure, column):
                            value = getattr(structure, column)
                            setattr(existing_structure, column, value)
                    existing_structure.updated_at = datetime.utcnow()
                else:
                    self._db_app.session.add(structure)
            self._db_app.session.commit()
            logging.debug(f"Corporation structures ({len(corporation_structures)}) saved for '{self.corporation_name}'.")
        except Exception as e: 
            error_message = f"Failed to save Corporation structures for '{self.corporation_name}'. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    # -------------------
    # Save Members
    # -------------------
    def save_corporation_members(self, corporation_members: List[CorporationMemberModel]) -> None:
        """Save the corporation members to the database."""
        try:
            for member in corporation_members:
                existing_member = self._db_app.session.query(CorporationMemberModel).filter_by(character_id=member.character_id).first()
                if existing_member:
                    for column in CorporationMemberModel.__table__.columns.keys():
                        if column == "id":
                            continue
                        if hasattr(member, column):
                            value = getattr(member, column)
                            setattr(existing_member, column, value)
                    existing_member.updated_at = datetime.utcnow()
                else:
                    self._db_app.session.add(member)
            self._db_app.session.commit()
            logging.debug(f"Corporation members ({len(corporation_members)}) saved to database.")
        except Exception as e:
            error_message = f"Failed to save Corporation members for '{self.corporation_name}'. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    # -------------------
    # Safe Assets
    # -------------------
    def save_corporation_assets(self, corporation_assets: List[Dict]) -> None:
        """Save the corporation assets to the database."""
        if not corporation_assets:
            logging.debug(f"No corporation assets to save for {self.corporation_name}.")
            return
        try:
            self.assets = []
            for asset in corporation_assets:
                new_asset = CorporationAssetsModel(**asset)
                self.assets.append(new_asset)
            if self.assets:
                self._db_app.session.query(CorporationAssetsModel).filter_by(corporation_id=self.corporation_id).delete()
                self._db_app.session.bulk_save_objects(self.assets)
                self._db_app.session.commit()
            else:
                logging.debug(f"No new corporation assets to save for {self.corporation_name}.")
            logging.debug(f"Corporation assets saved ({len(self.assets)}) for {self.corporation_name}.")
        except Exception as e:
            error_message = f"Failed to save Corporation assets for '{self.corporation_name}'. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    # -------------------
    # Refresh All Data
    # -------------------
    def refresh_all(self) -> None:
        """Refresh all data for the current corporation."""
        try:
            if hasattr(self._default_esi_character, "ensure_esi"):
                self._default_esi_character.ensure_esi()
            self.refresh_corporation()
            self.refresh_wallet_transactions()
            self.refresh_industry_jobs()
            self.refresh_members()
            self.refresh_structures()
            self.refresh_assets()

            logging.debug(f"All data successfully refreshed for {self.corporation_name}.")
        except Exception as e:
            error_message = f"Failed to refresh all data for '{self.corporation_name}'. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    # -------------------
    # Refresh Corporation
    # -------------------
    def refresh_corporation(self) -> None:
        """Refresh the runtime data of the corporation."""
        try:
            corp_data = self._default_esi_character._esi_client.esi_get(f"/corporations/{self.corporation_id}/")
            corp_divisions = self._default_esi_character._esi_client.esi_get(f"/corporations/{self.corporation_id}/divisions/")
            corp_wallets = self._default_esi_character._esi_client.esi_get(f"/corporations/{self.corporation_id}/wallets/")
            corp_standings = self._default_esi_character._esi_client.esi_get(f"/corporations/{self.corporation_id}/standings/")
            ceo_character = self._default_esi_character._esi_client.esi_get(f"/characters/{corp_data.get('ceo_id')}/")
            if isinstance(corp_wallets, str):
                corp_wallets = json.loads(corp_wallets)
            if isinstance(corp_divisions, str):
                corp_divisions = json.loads(corp_divisions)
            if isinstance(corp_standings, str):
                corp_standings = json.loads(corp_standings)
            faction_data = self._db_sde.load_df("factions")
            npccorp_data = self._db_sde.load_df("npcCorporations")
            def get_name(name, language):
                if isinstance(name, dict):
                    return name.get(language, next(iter(name.values()), "Unknown"))
                return name
            faction_lookup = {row["id"]: get_name(row["name"], self._cfg["app"]["language"]) for _, row in faction_data.iterrows()}
            npccorp_lookup = {row["id"]: get_name(row["name"], self._cfg["app"]["language"]) for _, row in npccorp_data.iterrows()}
            divisions_lookup = {
                d["division"]: (
                    "Master Wallet" if d["division"] == 1 else d.get("name", f"Division {d['division']}")
                )
                for d in corp_divisions.get("wallet", [])
            }
            self.image_url = f"https://images.evetech.net/corporations/{self.corporation_id}/portrait?size=128"
            self.corporation_name = corp_data.get("name")
            self.creator_id = corp_data.get("creator_id")
            self.ceo_id = corp_data.get("ceo_id")
            self.ceo_name = ceo_character.get("name", "Unknown") if ceo_character else "Unknown"
            self.date_founded = corp_data.get("date_founded")
            self.description = corp_data.get("description")
            self.home_station_id = corp_data.get("home_station_id")
            self.member_count = corp_data.get("member_count")
            self.shares = corp_data.get("shares")
            self.tax_rate = corp_data.get("tax_rate")
            self.ticker = corp_data.get("ticker")
            self.url = corp_data.get("url")
            self.war_eligible = corp_data.get("war_eligible")
            self.wallets = [
                {
                    "division": str(w.get("division")),
                    "division_name": divisions_lookup.get(w.get("division"), f"Division {w.get('division')}"),
                    "balance": str(w.get("balance"))
                }
                for w in corp_wallets
            ]
            self.standings = []
            for s in corp_standings:
                entry = {
                    "from_id": str(s.get("from_id")),
                    "from_type": s.get("from_type"),
                    "standing": str(s.get("standing"))
                }
                if s.get("from_type") == "faction":
                    entry["name"] = faction_lookup.get(s.get("from_id"), "Unknown Faction")
                elif s.get("from_type") == "npc_corp":
                    entry["name"] = npccorp_lookup.get(s.get("from_id"), "Unknown Corporation")
                else:
                    entry["name"] = ""
                self.standings.append(entry)
            
            self.save_corporation()
            logging.debug(f"Corporation data successfully updated for {self.corporation_name}.")
        except Exception as e:
            error_message = f"Failed to refresh corporation data for '{self.corporation_name}'. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    # -------------------
    # Refresh Structures
    # -------------------
    def refresh_structures(self) -> None:
        """Refresh the structures of the corporation from ESI."""
        try:
            structures_data = self._default_esi_character._esi_client.esi_get(f"/corporations/{self.corporation_id}/structures/")
            self.structures = []
            for structure in structures_data:
                system_data = self._default_esi_character._esi_client.esi_get(f"/universe/systems/{structure.get('system_id')}/")
                constellation_data = self._default_esi_character._esi_client.esi_get(f"/universe/constellations/{system_data.get('constellation_id')}/")
                region_data = self._default_esi_character._esi_client.esi_get(f"/universe/regions/{constellation_data.get('region_id')}/")
                type_data = self._db_sde.session.query(Types).filter_by(id=structure.get("type_id")).first()
                group_data = self._db_sde.session.query(Groups).filter_by(id=type_data.groupID).first()
                category_data = self._db_sde.session.query(Categories).filter_by(id=group_data.categoryID).first()
                self.structures.append(CorporationStructuresModel(
                    corporation_id=structure.get("corporation_id"),
                    structure_id=structure.get("structure_id"),
                    structure_name=structure.get("name", "Unknown"),
                    system_id=structure.get("system_id"),
                    system_name=system_data.get("name", "Unknown"),
                    system_security=system_data.get("security_status"),
                    constellation_id=system_data.get("constellation_id"),
                    constellation_name=constellation_data.get("name", "Unknown"),
                    region_id=constellation_data.get("region_id"),
                    region_name=region_data.get("name", "Unknown"),
                    type_id=structure.get("type_id"),
                    type_name=type_data.name[self._db_sde.language],
                    type_description=type_data.description[self._db_sde.language],
                    group_id=type_data.groupID,
                    group_name=group_data.name[self._db_sde.language],
                    category_id=group_data.categoryID,
                    category_name=category_data.name[self._db_sde.language],
                    state=structure.get("state"),
                    state_timer_end=structure.get("state_timer_end"),
                    state_timer_start=structure.get("state_timer_start"),
                    unachors_at=structure.get("unanchors_at"),
                    fuel_expires=structure.get("fuel_expires"),
                    reinforce_hour=structure.get("reinforce_hour"),
                    next_reinforce_apply=structure.get("next_reinforce_apply"),
                    next_reinforce_hour=structure.get("next_reinforce_hour"),
                    acl_profile_id=structure.get("profile_id"),
                    services=structure.get("services", {})
                ))
            
            self.save_corporation_structures(self.structures)
            logging.debug(f"Corporation structures successfully updated for {self.corporation_name}.")
        except Exception as e:
            error_message = f"Failed to refresh corporation structures for '{self.corporation_name}'. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    # -------------------
    # Refresh Members
    # -------------------
    def refresh_members(self) -> None:
        """Refresh the member list of the corporation from ESI."""
        try:
            members = self._default_esi_character._esi_client.esi_get(f"/corporations/{self.corporation_id}/members/")
            member_roles = self._default_esi_character._esi_client.esi_get(f"/corporations/{self.corporation_id}/members/titles/")
            corporation_titles = self._default_esi_character._esi_client.esi_get(f"/corporations/{self.corporation_id}/titles/")
            self.members = []
            for character_id in members:
                character = self._char_manager.get_character_by_id(character_id)
                if not character:
                    logging.debug(f"Character ID {character_id} not found in character manager. Skipping.")
                    continue
                titles: List[Dict[str, str]] = []
                member_role_entry = next((role for role in member_roles if role["character_id"] == character_id), None)
                if member_role_entry and "titles" in member_role_entry:
                    for title_id in member_role_entry["titles"]:
                        title = {
                            "title_id": title_id,
                            "title_name": next((corp_title["name"] for corp_title in corporation_titles if corp_title["title_id"] == title_id), "Unknown")
                        }
                        titles.append(title)
                self.members.append(CorporationMemberModel(
                    corporation_id=self.corporation_id,
                    character_id=character_id,
                    character_name=character.character_name,
                    character_wallet_balance=character.wallet_balance,
                    titles=titles
                ))
            self.save_corporation_members(self.members)
            logging.debug(f"Corporation members successfully updated for '{self.corporation_name}'. Total members: {len(self.members)}")
        except Exception as e:
            error_message = f"Failed to refresh corporation members for '{self.corporation_name}'. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    # -------------------
    # Wallet Transactions
    # -------------------
    def refresh_wallet_transactions(self) -> None:
        """Fetch and store corporation wallet transactions.

        Requires scope: esi-wallet.read_corporation_wallets.v1

        Best-effort: if not authorized, it will keep running with no transactions.
        """
        try:
            if hasattr(self._default_esi_character, "ensure_esi"):
                self._default_esi_character.ensure_esi()

            corp_wallets = self._default_esi_character._esi_client.esi_get(
                f"/corporations/{self.corporation_id}/wallets/"
            )
            if isinstance(corp_wallets, str):
                corp_wallets = json.loads(corp_wallets)
            if not corp_wallets or not isinstance(corp_wallets, list):
                return

            new_entries: List[Dict[str, Any]] = []
            for wallet in corp_wallets:
                if not isinstance(wallet, dict):
                    continue
                division = wallet.get("division")
                if division is None:
                    continue

                transactions = self._default_esi_character._esi_client.esi_get(
                    f"/corporations/{self.corporation_id}/wallets/{division}/transactions/"
                )
                if isinstance(transactions, str):
                    transactions = json.loads(transactions)
                if not transactions or not isinstance(transactions, list):
                    continue

                for entry in transactions:
                    if not isinstance(entry, dict):
                        continue
                    tx_id = entry.get("transaction_id")
                    if tx_id is None:
                        continue
                    exists = (
                        self._db_app.session.query(CorporationWalletTransactionsModel)
                        .filter_by(transaction_id=tx_id)
                        .first()
                    )
                    if exists:
                        continue
                    entry["division"] = division
                    new_entries.append(entry)

            if not new_entries:
                return

            client_ids: set[int] = set()
            for e in new_entries:
                cid = e.get("client_id")
                if isinstance(cid, int):
                    client_ids.add(cid)
                elif isinstance(cid, str) and cid.isdigit():
                    client_ids.add(int(cid))

            client_names: Dict[int, Optional[str]] = {}
            for cid in client_ids:
                name = None
                try:
                    id_type = self._default_esi_character._esi_client.get_id_type(cid)
                except Exception:
                    id_type = None

                try:
                    if id_type == "character":
                        data = self._default_esi_character._esi_client.esi_get(f"/characters/{cid}/")
                        if data and "name" in data:
                            name = data["name"]
                    elif id_type == "alliance":
                        data = self._default_esi_character._esi_client.esi_get(f"/alliances/{cid}/")
                        if data and "name" in data:
                            name = data["name"]
                    elif id_type == "corporation":
                        data = self._default_esi_character._esi_client.esi_get(f"/corporations/{cid}/")
                        if data and "name" in data:
                            name = data["name"]
                    elif id_type == "npc_corporation":
                        npc_corp = self._db_sde.session.query(NpcCorporations).filter_by(id=cid).first()
                        name = npc_corp.name[self._db_sde.language] if npc_corp else None
                except Exception:
                    name = None

                client_names[cid] = name

            rows: List[Dict[str, Any]] = []
            for entry in new_entries:
                cid = entry.get("client_id")
                if isinstance(cid, int):
                    entry["client_name"] = client_names.get(cid)
                elif isinstance(cid, str) and cid.isdigit():
                    entry["client_name"] = client_names.get(int(cid))
                else:
                    entry["client_name"] = None

                type_id = entry.get("type_id")
                type = self._db_sde.session.query(Types).filter_by(id=type_id).first()
                entry["type_name"] = type.name[self._db_sde.language] if type else None
                group = self._db_sde.session.query(Groups).filter_by(id=type.groupID).first() if type else None
                entry["type_group_id"] = group.id if group else None
                entry["type_group_name"] = group.name[self._db_sde.language] if group else None
                category = self._db_sde.session.query(Categories).filter_by(id=group.categoryID).first() if group else None
                entry["type_category_id"] = category.id if category else None
                entry["type_category_name"] = category.name[self._db_sde.language] if category else None

                qty = entry.get("quantity", 0) or 0
                unit_price = entry.get("unit_price", 0.0) or 0.0
                rows.append(
                    {
                        "corporation_id": int(self.corporation_id),
                        "division": entry.get("division"),
                        "transaction_id": entry.get("transaction_id"),
                        "client_id": entry.get("client_id"),
                        "client_name": entry.get("client_name"),
                        "date": entry.get("date"),
                        "is_buy": entry.get("is_buy"),
                        "location_id": entry.get("location_id"),
                        "quantity": qty,
                        "type_id": entry.get("type_id"),
                        "type_name": entry.get("type_name"),
                        "type_group_id": entry.get("type_group_id"),
                        "type_group_name": entry.get("type_group_name"),
                        "type_category_id": entry.get("type_category_id"),
                        "type_category_name": entry.get("type_category_name"),
                        "unit_price": unit_price,
                        "total_price": float(unit_price) * float(qty),
                    }
                )

            if rows:
                self._db_app.session.bulk_save_objects([CorporationWalletTransactionsModel(**r) for r in rows])
                self._db_app.session.commit()
        except Exception as e:
            logging.warning(
                "Skipping corporation wallet transactions refresh for %s (%s): %s",
                self.corporation_name,
                self.corporation_id,
                str(e),
            )
            return

    # -------------------
    # Industry Jobs
    # -------------------
    def refresh_industry_jobs(self) -> None:
        """Fetch and store corporation industry job history for provenance/costing.

        Requires scope: esi-industry.read_corporation_jobs.v1

        Best-effort: if not authorized, it will keep running with no jobs.
        """
        try:
            if hasattr(self._default_esi_character, "ensure_esi"):
                self._default_esi_character.ensure_esi()

            jobs = self._default_esi_character._esi_client.esi_get(
                f"/corporations/{self.corporation_id}/industry/jobs/",
                params={"include_completed": True},
                paginate=True,
            )
            if not jobs or not isinstance(jobs, list):
                return

            rows: List[Dict[str, Any]] = []
            for j in jobs:
                if not isinstance(j, dict):
                    continue
                job_id = j.get("job_id")
                if job_id is None:
                    continue
                rows.append(
                    {
                        "corporation_id": int(self.corporation_id),
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

            self._db_app.session.query(CorporationIndustryJobsModel).filter_by(corporation_id=self.corporation_id).delete()
            self._db_app.session.bulk_save_objects([CorporationIndustryJobsModel(**r) for r in rows])
            self._db_app.session.commit()
        except Exception as e:
            logging.warning(
                "Skipping corporation industry jobs refresh for %s (%s): %s",
                self.corporation_name,
                self.corporation_id,
                str(e),
            )
            return

    # -------------------
    # Refresh Assets
    # -------------------
    def refresh_assets(self) -> None:
        """Refresh the asset list of the corporation from ESI and enrich with SDE and container custom names."""
        try:
            assets = self._default_esi_character._esi_client.esi_get(
                f"/corporations/{self.corporation_id}/assets/",
                paginate=True,
            )
            if isinstance(assets, str):
                assets = json.loads(assets)
            if not assets or not isinstance(assets, list):
                return
            blueprints = self._default_esi_character._esi_client.esi_get(f"/corporations/{self.corporation_id}/blueprints/", paginate=True)
            market_prices = self._default_esi_character._esi_client.esi_get(f"/markets/prices/")

            type_ids_for_cost: List[int] = []
            qty_by_type: Dict[int, int] = {}
            for a in assets:
                if not isinstance(a, dict):
                    continue
                t = a.get("type_id")
                q = a.get("quantity")
                if isinstance(t, int):
                    type_ids_for_cost.append(t)
                    if isinstance(q, int):
                        qty_by_type[t] = qty_by_type.get(t, 0) + q
                    elif isinstance(q, str) and q.isdigit():
                        qty_by_type[t] = qty_by_type.get(t, 0) + int(q)
                elif isinstance(t, str) and t.isdigit():
                    tid = int(t)
                    type_ids_for_cost.append(tid)
                    if isinstance(q, int):
                        qty_by_type[tid] = qty_by_type.get(tid, 0) + q
                    elif isinstance(q, str) and q.isdigit():
                        qty_by_type[tid] = qty_by_type.get(tid, 0) + int(q)
            cost_map = build_cost_map_for_assets(
                app_session=self._db_app.session,
                sde_session=self._db_sde.session,
                owner_kind="corporation_id",
                owner_id=int(self.corporation_id),
                asset_type_ids=type_ids_for_cost,
                asset_quantities_by_type=qty_by_type,
                wallet_tx_model=CorporationWalletTransactionsModel,
                industry_job_model=CorporationIndustryJobsModel,
                market_prices=market_prices if isinstance(market_prices, list) else [],
            )

            self.asset_list = []
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
                    "corporation_id": self.corporation_id,
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
                    "is_office_folder": type_id == 27
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
                self.asset_list.append(asset_entry)
            
            # Fetch custom names for containers (type_id == 17366 and is_singleton == True)
            container_ids = [a["item_id"] for a in self.asset_list if a.get("type_id") == 17366 and a.get("is_singleton") == True]
            container_names = {}
            if container_ids:
                names_response = self._default_esi_character._esi_client.esi_post(
                    f"/corporations/{self.corporation_id}/assets/names",
                    json=container_ids
                )
                if names_response and isinstance(names_response, list):
                    container_names = {c["item_id"]: c["name"] for c in names_response}
            
            for asset in self.asset_list:
                if asset.get("item_id") in container_names:
                    asset["container_name"] = container_names[asset["item_id"]]
            
            # Fetch custom names for ships (type_category_id == 6 and is_singleton == True)
            ship_ids = [a["item_id"] for a in self.asset_list if a.get("type_category_id") == 6 and a.get("is_singleton") == True]
            ship_names = {}
            if ship_ids:
                names_response = self._default_esi_character._esi_client.esi_post(
                    f"/corporations/{self.corporation_id}/assets/names",
                    json=ship_ids
                )
                if names_response and isinstance(names_response, list):
                    ship_names = {s["item_id"]: s["name"] for s in names_response}
            
            for asset in self.asset_list:
                if asset.get("item_id") in ship_names:
                    asset["ship_name"] = ship_names[asset["item_id"]]
            
            # Lookup for all containers and ships by item_id
            container_lookup = {a["item_id"]: a for a in self.asset_list if a.get("is_container")}
            ship_lookup = {a["item_id"]: a for a in self.asset_list if a.get("is_ship")}

            for asset in self.asset_list:
                # Assign container_name for non-containers
                if not (asset.get("is_container")) and not (asset.get("is_office_folder")):
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
            assetsafety_wrap_ids = {a["item_id"] for a in self.asset_list if a.get("is_asset_safety_wrap")}
            office_folder_ids = {a["item_id"] for a in self.asset_list if a.get("is_office_folder")}
            parent_location_map = {a["item_id"]: a["location_id"] for a in self.asset_list}
            valid_top_location_ids = {a["location_id"] for a in self.asset_list
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
            for asset in self.asset_list:
                asset["top_location_id"] = resolve_top_location_id(asset)

            self.save_corporation_assets(self.asset_list)
            
            logging.debug(f"Corporation assets successfully updated for {self.corporation_name}. Total assets: {len(self.asset_list)}")

        except Exception as e:
            error_message = f"Failed to refresh corporation assets for '{self.corporation_name}'. Error: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)