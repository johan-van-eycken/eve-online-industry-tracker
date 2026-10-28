from __future__ import annotations

from eve_online_industry_tracker.cli import run_flask
from utils.logging_setup import configure_logging


def main() -> None:
    configure_logging(default_level="INFO")
    run_flask()


if __name__ == "__main__":
    main()
