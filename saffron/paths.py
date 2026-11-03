"""Where things live, measured from the companion folder, so a script runs from any folder."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent  # the companion folder
DATA = ROOT / "data" / "raw"
OUTPUT = ROOT / "output"


def raw(table, day=1):
    """Path of one night's CSV as text, e.g. raw("transactions") or raw("customers", day=2)."""
    return str(DATA / f"day{day}" / f"{table}.csv")


def raw_folder(day=1):
    """Folder with all five CSV files of one night."""
    return str(DATA / f"day{day}")
