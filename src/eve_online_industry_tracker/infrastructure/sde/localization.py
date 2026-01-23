from __future__ import annotations

import json
import re
from typing import Any


def parse_localized(raw: Any, language: str) -> str:
    if raw is None:
        return ""

    if isinstance(raw, dict):
        text = raw.get(language) or next(iter(raw.values()), "")
    elif isinstance(raw, str):
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                text = data.get(language) or next(iter(data.values()), raw)
            else:
                text = raw
        except json.JSONDecodeError:
            text = raw
    else:
        text = str(raw)

    clean = re.sub(r"<[^>]+>", "", text).replace("\r\n", "<br>").strip()
    return clean
