import logging
import json
from typing import Optional, List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

from eve_online_industry_tracker.config.config_manager import ConfigManager
from eve_online_industry_tracker.infrastructure.database_manager import DatabaseManager
from eve_online_industry_tracker.application.characters.character_manager import CharacterManager
from eve_online_industry_tracker.application.corporations.corporation import Corporation

class CorporationManager:
    """Manages multiple Character objects and provides batch utilities."""

    def __init__(
        self,
        cfgManager: ConfigManager,
        db_oauth: DatabaseManager,
        db_app: DatabaseManager,
        db_sde: DatabaseManager,
        char_manager: CharacterManager
    ) -> None:
        try:
            # Private arguments
            self._cfgManager = cfgManager
            self._cfg = cfgManager.all()
            self._db_oauth = db_oauth
            self._db_app = db_app
            self._db_sde = db_sde
            self._char_manager = char_manager
            self._corporations: List[Corporation] = []
            self._corp_by_id: Dict[int, Corporation] = {}
            self._corp_by_name: Dict[str, Corporation] = {}
            # One line per corporation that could not be initialised (bootstrap: init_warnings).
            self.init_errors: List[str] = []

            # Determine unique corporation IDs of player owned corporations
            self._corporation_ids = {
                char.corporation_id
                for char in self._char_manager._character_list
                if char.corporation_id is not None and char.is_corp_director is True
            }

            self._initialize_corporations()
        except Exception as e:
            error_message = f"Failed to initialize CorporationManager: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)
    
    def _initialize_corporations(self) -> None:
        """One Corporation per director's corporation. A corporation whose
        CharacterManager or Corporation cannot be built is logged as an
        ERROR, named in init_errors (bootstrap: init_warnings) and skipped;
        the other corporations go on."""
        for corp_id in self._corporation_ids:
            label = self._corporation_label(corp_id)
            try:
                char_manager_corp = CharacterManager(self._cfgManager, self._db_oauth, self._db_app, self._db_sde, corp_id, self._char_manager,
                                                     admin_settings=getattr(self._char_manager, "admin_settings", None))
            except Exception as e:   # one corporation's failure does not stop the others; named in init_errors
                self._init_failed(f"Error initializing the character manager of {label}", e)
                continue

            try:
                corp = Corporation(self._cfgManager, self._db_app, self._db_sde, corp_id, char_manager_corp)
            except Exception as e:   # one corporation's failure does not stop the others; named in init_errors
                self._init_failed(f"Error initializing {label}", e)
                continue

            # Constructor is cheap now; refresh is handled explicitly (e.g., by Flask bootstrap).
            self._corporations.append(corp)
            self._corp_by_id[corp_id] = corp
            if corp.corporation_name:
                self._corp_by_name[corp.corporation_name.lower()] = corp

        return None

    def _corporation_label(self, corp_id: int) -> str:
        """'corporation <id> (director <name>)': the corporation's name is not known before it is built."""
        directors = sorted(
            char.character_name
            for char in self._char_manager._character_list
            if char.corporation_id == corp_id and char.is_corp_director is True
        )
        return f"corporation {corp_id} (director {', '.join(directors)})" if directors else f"corporation {corp_id}"

    def _init_failed(self, what: str, exc: Exception) -> None:
        detail = f"missing key {exc}" if isinstance(exc, KeyError) else str(exc)
        message = f"{what}: {type(exc).__name__}: {detail}"
        logging.error(message, exc_info=True)
        self.init_errors.append(message)

    def get_corporation(self, corporation_id: Optional[int] = None, corporation_name: Optional[str] = None) -> Optional[Corporation]:
        """Return the Corporation object for a given ID or name, or None if not found."""
        if corporation_id is not None:
            return self._corp_by_id.get(corporation_id)
        if corporation_name:
            return self._corp_by_name.get(corporation_name.lower())
        return None

    def get_corporation_name_by_character_id(self, character_id: int) -> Optional[str]:
        """Retrieve the corporation name for a given character_id."""
        character = next(
            (char for char in self._char_manager._character_list if char.character_id == character_id),
            None
        )
        if not character or character.corporation_id is None:
            return None
        corp = self.get_corporation(corporation_id=character.corporation_id)
        if corp and corp.corporation_name:
            return corp.corporation_name
        return None

    def get_corporations(self) -> List[Dict[str, object]]:
        corporations_data = [c.get_corporation() for c in self._corporations]
        return corporations_data
    
    def get_assets(
        self,
        corporation_name: Optional[str] = None,
        corporation_id: Optional[int] = None,
    ) -> List[Dict[str, object]]:
        all_assets = []
        if corporation_id is not None:
            corporations = [self.get_corporation(corporation_id=corporation_id)]
        elif corporation_name:
            corporations = [self.get_corporation(corporation_name=corporation_name)]
        else:
            corporations = self._corporations

        corporations = [corp for corp in corporations if corp is not None]

        for corp in corporations:
            try:
                self._refresh_batch("refresh_assets", corporation_id=corp.corporation_id)
                corp_assets = corp.get_assets()
                all_assets.append(corp_assets)
            except Exception as e:
                logging.error(f"Error retrieving assets for corporation {corp.corporation_name}: {e}")
        return all_assets
    
    def get_members(self) -> List[Dict[str, object]]:
        all_members = []
        for corp in self._corporations:
            try:
                self._refresh_batch("refresh_members", corporation_id=corp.corporation_id)
                corp_members = corp.get_members()
                all_members.extend(corp_members)
            except Exception as e:
                logging.error(f"Error retrieving members for corporation {corp.corporation_name}: {e}")
        return all_members
    
    def get_structures(self) -> List[Dict[str, object]]:
        all_structures = []
        for corp in self._corporations:
            try:
                self._refresh_batch("refresh_structures", corporation_id=corp.corporation_id)
                corp_structures = corp.get_structures()
                all_structures.extend(corp_structures)
            except Exception as e:
                logging.error(f"Error retrieving structures for corporation {corp.corporation_name}: {e}")
        return all_structures

    def _refresh_batch(self, method_name: str, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> None:
        """Internal helper to refresh data for one or all corporations."""
        try:
            # Filter corporations
            if corporation_id is not None:
                corps = [self.get_corporation(corporation_id=corporation_id)]
            elif corporation_name:
                corps = [self.get_corporation(corporation_name=corporation_name)]
            else:
                corps = self._corporations

            corps = [c for c in corps if c is not None]

            def _run_one(corp):
                try:
                    getattr(corp, method_name)()
                except Exception as e:
                    error_message = f"Error during {method_name} for {corp.corporation_name}: {e}"
                    logging.error(error_message)
                    raise Exception(error_message)

            if len(corps) > 1:
                with ThreadPoolExecutor(max_workers=len(corps)) as pool:
                    futures = [pool.submit(_run_one, c) for c in corps]
                    for f in as_completed(futures):
                        f.result()
            else:
                for corp in corps:
                    _run_one(corp)
            logging.debug(f"Batch refresh '{method_name}' completed for {len(corps)} corporations.")
        except Exception as e:
            error_message = f"Failed to refresh batch for method '{method_name}': {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    def refresh_all(self, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> None:
        """Refresh all corporation data for one or all corporations."""
        try:
            self._refresh_batch("refresh_all", corporation_name, corporation_id)
        except Exception as e:
            error_message = f"Failed to refresh all corporation data: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    def refresh_wallet_transactions(self, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> None:
        try:
            self._refresh_batch("refresh_wallet_transactions", corporation_name, corporation_id)
        except Exception as e:
            error_message = f"Failed to refresh corporation wallet transactions: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    def refresh_wallet_journal(self, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> None:
        try:
            self._refresh_batch("refresh_wallet_journal", corporation_name, corporation_id)
        except Exception as e:
            error_message = f"Failed to refresh corporation wallet journal: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    def refresh_industry_jobs(self, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> None:
        try:
            self._refresh_batch("refresh_industry_jobs", corporation_name, corporation_id)
        except Exception as e:
            error_message = f"Failed to refresh corporation industry jobs: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    def refresh_assets(self, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> None:
        try:
            self._refresh_batch("refresh_assets", corporation_name, corporation_id)
        except Exception as e:
            error_message = f"Failed to refresh corporation assets: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    def refresh_realized_profit_inputs(self, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> None:
        self.refresh_wallet_transactions(corporation_name=corporation_name, corporation_id=corporation_id)
        self.refresh_wallet_journal(corporation_name=corporation_name, corporation_id=corporation_id)
        self.refresh_industry_jobs(corporation_name=corporation_name, corporation_id=corporation_id)
        self.refresh_assets(corporation_name=corporation_name, corporation_id=corporation_id)

    def get_market_orders(
        self,
        corporation_name: Optional[str] = None,
        corporation_id: Optional[int] = None,
        *,
        refresh: bool = False,
    ) -> List[Dict]:
        """Return market orders for one or all corporations."""
        if corporation_id is not None:
            corps = [self.get_corporation(corporation_id=corporation_id)]
        elif corporation_name:
            corps = [self.get_corporation(corporation_name=corporation_name)]
        else:
            corps = self._corporations

        corps = [c for c in corps if c is not None]

        if refresh:
            self._refresh_batch("refresh_market_orders", corporation_name, corporation_id)

        orders_list = []
        for corp in corps:
            try:
                orders_list.append(corp.get_market_orders())
            except Exception as e:
                logging.error(f"Error retrieving market orders for corporation {corp.corporation_name}: {e}")
        return orders_list

    def refresh_market_orders(self, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> None:
        try:
            self._refresh_batch("refresh_market_orders", corporation_name, corporation_id)
        except Exception as e:
            error_message = f"Failed to refresh corporation market orders: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)
