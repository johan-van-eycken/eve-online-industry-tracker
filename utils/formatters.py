from datetime import datetime, timezone
from dateutil import parser
from typing import Optional

def format_isk(value: Optional[float]) -> str:
    """Format ISK balance in EVE style: ISK 1.234.567.890,00"""
    if not value:
        value = 0.0
    
    try:
        return "ISK {:,.2f}".format(value).replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "N/A"

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

def parse_datetime(dt_str):
    if dt_str is None:
        return None
    return parser.isoparse(dt_str)
