from __future__ import annotations

from pathlib import Path

from monitor_core import main


if __name__ == "__main__":
    raise SystemExit(main(entry_script=Path(__file__).resolve()))
