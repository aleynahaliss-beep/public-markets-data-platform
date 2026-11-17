"""Small output helpers (plain CSV files, no pandas dependency)."""

from __future__ import annotations

import csv
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame


def _fmt(v):
    if v is None:
        return ""
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


def write_rows(path: str | Path, fields: list[str], rows: Iterable[dict]) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(fields)
        for r in rows:
            w.writerow([_fmt(r[f]) for f in fields])
    return path


def write_df_csv(df: DataFrame, path: str | Path) -> Path:
    """Collect a (small) DataFrame to the driver and write one tidy CSV file."""
    rows = [r.asDict() for r in df.collect()]
    return write_rows(path, df.columns, rows)
