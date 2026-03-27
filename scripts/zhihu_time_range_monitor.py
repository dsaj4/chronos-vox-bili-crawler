from __future__ import annotations

from pathlib import Path

from monitor_core import main_for_platform


if __name__ == "__main__":
    raise SystemExit(main_for_platform("zhihu", entry_script=Path(__file__).resolve()))
