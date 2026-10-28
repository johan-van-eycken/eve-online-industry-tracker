import sys
from pathlib import Path

# Ensure `src/` is on sys.path so `flask_app` and the package can be imported
# when running `python main.py` from the repo root without an install.
_SRC = Path(__file__).resolve().parent / "src"
if _SRC.exists() and str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from eve_online_industry_tracker.cli import main


if __name__ == "__main__":
    main()
