import logging
from datetime import datetime, timezone, timedelta
from dateutil import parser # pyright: ignore[reportMissingModuleSource]
from typing import Optional

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
        today = datetime.now(timezone.utc)

        # Basic date string
        formatted_date = dt.strftime("%d-%m-%Y")

        return f"{formatted_date}"
    except Exception:
        return "N/A"

def format_datetime(iso_date: Optional[str]) -> str:
    """Convert ISO8601 (%Y-%m-%dT%H:%M:%SZ) date to dd-mm-yyyy HH:MM."""
    if not iso_date:
        return "N/A"
    try:
        # Parse the input (e.g. "2023-07-17T07:17:18Z")
        dt = datetime.strptime(iso_date, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        today = datetime.now(timezone.utc)

        # Basic date string
        formatted_date = dt.strftime("%d-%m-%Y %H:%M:%S")

        return f"{formatted_date}"
    
    except Exception:
        return "N/A"

def parse_datetime(dt_str):
    if dt_str is None:
        return None
    return parser.isoparse(dt_str)

def format_expires_in(row, now):
    # Calculate expiration datetime
    issued = row["Issued"]
    duration_minutes = int(row["duration"] * 1440)
    expires_at = issued + timedelta(minutes=duration_minutes)
    remaining = expires_at - now
    if remaining.total_seconds() < 0:
        return "Expired"
    days = remaining.days
    hours, rem = divmod(remaining.seconds, 3600)
    minutes = rem // 60
    return f"{days}d {hours}h {minutes}m"