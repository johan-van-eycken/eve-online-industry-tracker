import logging
from typing import Any, Optional, List, Dict
from concurrent.futures import ThreadPoolExecutor

from eve_online_industry_tracker.config.config_manager import ConfigManager
from eve_online_industry_tracker.infrastructure.database_manager import DatabaseManager
from eve_online_industry_tracker.infrastructure.esi_client import ESI_REQUEST_ERRORS
from eve_online_industry_tracker.application.characters.character_manager import CharacterManager
from eve_online_industry_tracker.application.corporations.corporation import Corporation


class CorporationRefreshError(Exception):
    """A batch refresh or read in which one or more corporations failed.
    Every corporation was tried; `errors` is {corporation name: error
    message}, each named in the message. Not a RuntimeError, so the ESI
    catch sites do not take it for an ESI failure."""

    def __init__(self, method_name: str, errors: Dict[str, str]):
        self.method_name = method_name
        self.errors = dict(errors)
        details = "; ".join(f"{name}: {message}" for name, message in self.errors.items())
        super().__init__(f"{method_name} failed for {len(self.errors)} corporation(s): {details}")


class UnknownCorporationError(CorporationRefreshError):
    """A corporation_id or name the manager does not have: nothing was
    refreshed or read. `errors` names the unknown selector; the routes
    answer 404."""

    def __init__(self, method_name: str, selector: str):
        self.selector = selector
        super().__init__(method_name, {selector: "no such corporation in this manager"})


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

    def get_corporation(self, corporation_id: Optional[int] = None, corporation_name: Optional[str] = None) -> Corporation:
        """The Corporation with this id, else this name. UnknownCorporationError
        when it matches none; ValueError when neither is given."""
        return self._select_corporations("get_corporation", corporation_name, corporation_id, require=True)[0]

    def _select_corporations(
        self,
        method_name: str,
        corporation_name: Optional[str] = None,
        corporation_id: Optional[int] = None,
        *,
        require: bool = False,
    ) -> List[Corporation]:
        """The corporation with this id, else with this name, else every
        corporation (a ValueError with `require`). UnknownCorporationError
        when the id or name matches none."""
        if corporation_id is not None:
            corp = self._corp_by_id.get(corporation_id)
            if corp is None:
                raise UnknownCorporationError(method_name, f"corporation_id {corporation_id}")
            return [corp]
        if corporation_name:
            corp = self._corp_by_name.get(corporation_name.lower())
            if corp is None:
                raise UnknownCorporationError(method_name, f"corporation_name {corporation_name!r}")
            return [corp]
        if require:
            raise ValueError(f"{method_name}: give a corporation_id or corporation_name")
        return list(self._corporations)

    def get_corporation_name_by_character_id(self, character_id: int) -> Optional[str]:
        """The name of this character's corporation; None when the character
        is unknown or its corporation is not one of the director corporations
        managed here."""
        character = next(
            (char for char in self._char_manager._character_list if char.character_id == character_id),
            None
        )
        if not character or character.corporation_id is None:
            return None
        corp = self._corp_by_id.get(character.corporation_id)
        if corp and corp.corporation_name:
            return corp.corporation_name
        return None

    def get_corporations(self) -> List[Dict[str, object]]:
        corporations_data = [c.get_corporation() for c in self._corporations]
        return corporations_data

    def _read_each(
        self,
        corps: List[Corporation],
        getter: str,
        refresh_method: Optional[str] = None,
    ) -> List[List[Any]]:
        """[corp.<getter>()] per corporation, after its `refresh_method`.
        Every corporation is tried; then CorporationRefreshError names each
        one that failed (a refresh or a read)."""
        results: List[Any] = []
        errors: Dict[str, str] = {}
        for corp in corps:
            try:
                if refresh_method:
                    self._refresh_batch(refresh_method, corporation_id=corp.corporation_id)
                results.append(getattr(corp, getter)())
            except CorporationRefreshError as e:
                errors.update(e.errors)
            except Exception as e:   # the batch boundary: one corporation must not stop the others
                logging.error("Error during %s for %s: %s", getter, _corp_label(corp), e,
                              exc_info=not isinstance(e, ESI_REQUEST_ERRORS))
                errors[_corp_label(corp)] = f"{type(e).__name__}: {e}"
        if errors:
            raise CorporationRefreshError(getter, errors)
        return results

    def get_assets(
        self,
        corporation_name: Optional[str] = None,
        corporation_id: Optional[int] = None,
    ) -> List[Dict[str, object]]:
        corps = self._select_corporations("get_assets", corporation_name, corporation_id)
        return self._read_each(corps, "get_assets", "refresh_assets")

    def get_members(self) -> List[Dict[str, object]]:
        return [member for members in self._read_each(self._corporations, "get_members", "refresh_members")
                for member in members]

    def get_structures(self) -> List[Dict[str, object]]:
        return [structure for structures in self._read_each(self._corporations, "get_structures", "refresh_structures")
                for structure in structures]

    def _refresh_batch(self, method_name: str, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> None:
        """Run `method_name` on each selected corporation (in parallel when
        more than one). Every corporation is tried: one that fails is logged
        as an ERROR and the others go on; then CorporationRefreshError names
        each failed corporation. An unknown corporation_id or name raises
        UnknownCorporationError before any refresh."""
        corps = self._select_corporations(method_name, corporation_name, corporation_id)

        def _run_one(corp: Corporation) -> Optional[str]:
            try:
                getattr(corp, method_name)()
                return None
            except Exception as e:   # the batch boundary: one corporation must not stop the others
                logging.error("Error during %s for %s: %s", method_name, _corp_label(corp), e,
                              exc_info=not isinstance(e, ESI_REQUEST_ERRORS))   # a traceback unless ESI failed
                return f"{type(e).__name__}: {e}"

        if len(corps) > 1:
            with ThreadPoolExecutor(max_workers=len(corps)) as pool:
                outcomes = dict(zip(corps, pool.map(_run_one, corps)))
        else:
            outcomes = {corp: _run_one(corp) for corp in corps}
        errors = {_corp_label(corp): message for corp, message in outcomes.items() if message is not None}
        if errors:
            raise CorporationRefreshError(method_name, errors)
        logging.debug("Batch refresh '%s' completed for %d corporations.", method_name, len(corps))

    def refresh_all(self, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> None:
        """refresh_all of each corporation (see _refresh_batch)."""
        self._refresh_batch("refresh_all", corporation_name, corporation_id)

    def refresh_wallet_transactions(self, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> None:
        self._refresh_batch("refresh_wallet_transactions", corporation_name, corporation_id)

    def refresh_wallet_journal(self, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> None:
        self._refresh_batch("refresh_wallet_journal", corporation_name, corporation_id)

    def refresh_industry_jobs(self, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> None:
        self._refresh_batch("refresh_industry_jobs", corporation_name, corporation_id)

    def refresh_assets(self, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> None:
        self._refresh_batch("refresh_assets", corporation_name, corporation_id)

    def refresh_realized_profit_inputs(self, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> None:
        """The wallet transactions, journal, industry jobs and assets, each for
        every corporation; then one CorporationRefreshError naming each failure."""
        errors: Dict[str, str] = {}
        for refresh in (self.refresh_wallet_transactions, self.refresh_wallet_journal,
                        self.refresh_industry_jobs, self.refresh_assets):
            try:
                refresh(corporation_name=corporation_name, corporation_id=corporation_id)
            except UnknownCorporationError:
                raise   # nothing was refreshed: the same unknown corporation for every kind
            except CorporationRefreshError as exc:
                for name, message in exc.errors.items():
                    errors[f"{name} ({exc.method_name})"] = message
        if errors:
            raise CorporationRefreshError("refresh_realized_profit_inputs", errors)

    def get_market_orders(
        self,
        corporation_name: Optional[str] = None,
        corporation_id: Optional[int] = None,
        *,
        refresh: bool = False,
    ) -> List[Dict]:
        """Market orders for one or all corporations; with `refresh`, the
        selected corporations are refreshed first (once)."""
        corps = self._select_corporations("get_market_orders", corporation_name, corporation_id)
        if refresh:
            self._refresh_batch("refresh_market_orders", corporation_name, corporation_id)
        return self._read_each(corps, "get_market_orders")

    def refresh_market_orders(self, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> None:
        self._refresh_batch("refresh_market_orders", corporation_name, corporation_id)


def _corp_label(corp: Corporation) -> str:
    """The corporation's name, else 'corporation <id>'."""
    return corp.corporation_name or f"corporation {corp.corporation_id}"
