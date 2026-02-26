from datetime import datetime, timezone
import math
from typing import Any, Optional


def format_decimal_eu(value: Any, *, decimals: int = 2, missing: str = "-") -> str:
    """Format a numeric value using EU separators.

    - Thousands separator: '.'
    - Decimal separator  : ','
    """

    try:
        if value is None:
            return missing
        if isinstance(value, float) and math.isnan(value):
            return missing
        if isinstance(value, str) and not value.strip():
            return missing
        v = float(value)
    except Exception:
        return missing

    s = f"{v:,.{int(decimals)}f}"  # 1,234,567.89
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def format_isk_eu(value: Any, *, decimals: int = 2, missing: str = "-") -> str:
    s = format_decimal_eu(value, decimals=decimals, missing=missing)
    return f"{s} ISK" if s != missing else missing


def format_pct_eu(value: Any, *, decimals: int = 2, missing: str = "-") -> str:
    s = format_decimal_eu(value, decimals=decimals, missing=missing)
    return f"{s}%" if s != missing else missing


def format_duration(seconds: float | int | None) -> str:
    try:
        s = int(round(float(seconds or 0.0)))
    except Exception:
        s = 0
    if s < 0:
        s = 0

    day_s = 24 * 3600

    days = s // day_s
    s = s % day_s
    hours = s // 3600
    s = s % 3600
    minutes = s // 60
    secs = s % 60

    parts: list[str] = []
    if days:
        parts.append(f"{days}D")
    if hours or days:
        parts.append(f"{hours}h")
    if minutes or hours or days:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    return " ".join(parts)


def type_icon_url(type_id: Any, *, size: int = 32) -> str | None:
    try:
        tid = int(type_id)
    except Exception:
        return None
    if tid <= 0:
        return None
    return f"https://images.evetech.net/types/{tid}/icon?size={int(size)}"


def blueprint_image_url(blueprint_type_id: Any, *, is_bpc: bool, size: int = 32) -> str | None:
    try:
        tid = int(blueprint_type_id)
    except Exception:
        return None
    if tid <= 0:
        return None
    variation = "bpc" if bool(is_bpc) else "bp"
    return f"https://images.evetech.net/types/{tid}/{variation}?size={int(size)}"

def format_isk(value: Optional[float]) -> str:
    """Format ISK balance in EVE style: ISK 1.234.567.890,00"""
    if value is None or value == "":
        value = 0.0
    
    try:
        value = float(value)
        return "ISK {:,.2f}".format(value).replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "N/A"

def format_isk_short(value):
    if value >= 1_000_000_000:
        return f"{value/1_000_000_000:.2f}b"
    elif value >= 1_000_000:
        return f"{value/1_000_000:.2f}m"
    elif value >= 1_000:
        return f"{value/1_000:.2f}k"
    else:
        return f"{value:.2f}"

def format_date_into_age(iso_date: Optional[str]) -> str:
    """Convert ISO8601 (%Y-%m-%dT%H:%M:%SZ) date into Age in years, months, days."""
    if not iso_date:
        return "N/A"
    try:
        # Parse the input (e.g. "2023-07-17T07:17:18Z")
        dt = datetime.strptime(iso_date, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        today = datetime.now(timezone.utc)

        # Calculate difference
        delta_days = (today - dt).days
        years, rem_days = divmod(delta_days, 365)
        months, days = divmod(rem_days, 30)  # approx months

        return f"{years}y {months}m {days}d"
    except Exception:
        return "N/A"
    
def format_date_countdown(iso_date: Optional[str]) -> str:
    """Convert ISO8601 (%Y-%m-%dT%H:%M:%SZ) date into countdown in days, hours, minutes."""
    if not iso_date:
        return "N/A"
    try:
        # Parse the input (e.g. "2023-07-17T07:17:18Z")
        dt = datetime.strptime(iso_date, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)

        if dt < now:
            return "Expired"

        # Calculate difference
        delta = dt - now
        days = delta.days
        hours, rem = divmod(delta.seconds, 3600)
        minutes, seconds = divmod(rem, 60)

        return f"{days}d {hours}h {minutes}m"
    except Exception:
        return "N/A"

def format_date(iso_date: Optional[str]) -> str:
    """Convert ISO8601 (%Y-%m-%dT%H:%M:%SZ) date to dd-mm-yyyy."""
    if not iso_date:
        return "N/A"
    try:
        # Parse the input (e.g. "2023-07-17T07:17:18Z")
        dt = datetime.strptime(iso_date, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

        # Basic date string
        formatted_date = dt.strftime("%d-%m-%Y")

        return f"{formatted_date}"
    except Exception:
        return "N/A"

def format_datetime(iso_date: Optional[str]) -> str:
    """Convert ISO8601 (%Y-%m-%dT%H:%M:%SZ) date to dd-mm-yyyy HH:MM:SS."""
    if not iso_date:
        return "N/A"
    try:
        # Parse the input (e.g. "2023-07-17T07:17:18Z")
        dt = datetime.strptime(iso_date, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

        # Basic date string
        formatted_date = dt.strftime("%d-%m-%Y %H:%M:%S")

        return f"{formatted_date}"
    except Exception:
        return "N/A"
