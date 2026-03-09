from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import traceback


@dataclass(frozen=True)
class AgGridImports:
    AgGrid: Any | None
    GridOptionsBuilder: Any | None
    JsCode: Any | None
    import_error: str | None


def import_aggrid() -> AgGridImports:
    """Best-effort import for streamlit-aggrid.

    Streamlit pages frequently want to show a friendly message when the dependency
    isn't installed in the current Streamlit process.
    """

    try:
        from st_aggrid import AgGrid, GridOptionsBuilder, JsCode  # type: ignore
    except Exception:  # pragma: no cover
        return AgGridImports(
            AgGrid=None,
            GridOptionsBuilder=None,
            JsCode=None,
            import_error=traceback.format_exc(),
        )

    return AgGridImports(
        AgGrid=AgGrid,
        GridOptionsBuilder=GridOptionsBuilder,
        JsCode=JsCode,
        import_error=None,
    )
