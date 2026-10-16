"""A process-wide cache for data read once from the SDE.

The app reads some SDE tables into immutable objects (the rig catalog, the
ore compression, the industry activities) from code that has no session, so
the app configures a loader at startup (flask_app.bootstrap) and the first
use loads it. A loader raises SdeIndustryDataMissing (its message names the
re-import) when the SDE lacks what it reads; the failure is logged once as
an ERROR, raised to every use and retried on the next one, so a re-import is
picked up without a restart. There is no fallback.
"""
from __future__ import annotations

import logging
import threading
from typing import Callable, Generic, TypeVar

T = TypeVar("T")


class SdeDataCache(Generic[T]):
    def __init__(self, *, missing: Callable[[], Exception], logger: logging.Logger):
        """missing() is the error a use raises while no loader is configured."""
        self._missing = missing
        self._logger = logger
        self._lock = threading.Lock()
        self._data: T | None = None
        self._loader: Callable[[], T] | None = None
        self._logged_failures: set[str] = set()

    def install(self, data: T) -> None:
        """Use `data` from now on (tests; or a loader's result)."""
        with self._lock:
            self._data = data

    def configure_loader(self, loader: Callable[[], T] | None) -> None:
        """The loader the first use calls (and a later use, while it fails)."""
        with self._lock:
            self._loader = loader

    def reset(self) -> None:
        """Forget the data, the loader and the logged failures."""
        with self._lock:
            self._data, self._loader = None, None
            self._logged_failures.clear()

    def get(self, failure: type[Exception]) -> T:
        """The cached data, loading it on first use. A `failure` from the
        loader (or no loader) is logged once per message as an ERROR and
        raised."""
        with self._lock:
            if self._data is not None:
                return self._data
            loader = self._loader
            try:
                if loader is None:
                    raise self._missing()
                self._data = loader()
                return self._data
            except failure as exc:
                message = str(exc)
                if message not in self._logged_failures:
                    self._logged_failures.add(message)
                    self._logger.error("%s", message)
                raise
