"""Load NASR CSV files into a fresh SQLite database."""

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

# CSVs whose presence describes schema, not data.
_STRUCTURE_SUFFIX = "_CSV_DATA_STRUCTURE.csv"


def build(csv_dir: Path, db_path: Path, obstacle_csv: Path | None = None) -> None:
    """Create a SQLite database, one table per NASR CSV (plus DOF if provided)."""
    db_path = db_path.resolve()
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA page_size = 4096")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA journal_mode = MEMORY")

        for csv_path in sorted(csv_dir.glob("*.csv")):
            name = csv_path.name
            if name.endswith(_STRUCTURE_SUFFIX):
                continue
            _load_csv(conn, csv_path, table_name=csv_path.stem)

        if obstacle_csv is not None:
            _load_csv(conn, obstacle_csv, table_name="OBSTACLE")
        conn.commit()
    finally:
        conn.close()


def _load_csv(conn: sqlite3.Connection, csv_path: Path, table_name: str) -> None:
    """Discover columns from the CSV header and bulk-insert all rows in one transaction."""
    with csv_path.open("r", newline="", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return
        columns = [_safe_col(c) for c in header]
        ddl_cols = ", ".join(f'"{c}" TEXT' for c in columns)
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        conn.execute(f'CREATE TABLE "{table_name}" ({ddl_cols})')
        placeholders = ",".join("?" * len(columns))
        insert = f'INSERT INTO "{table_name}" VALUES ({placeholders})'

        # Skip rows that don't match the column count (defensive; FAA CSVs are usually clean).
        def _rows():
            for row in reader:
                if len(row) == len(columns):
                    yield row
                elif len(row) < len(columns):
                    yield row + [""] * (len(columns) - len(row))
                else:
                    yield row[: len(columns)]

        conn.executemany(insert, _rows())


def _safe_col(name: str) -> str:
    """Normalize a CSV header to a SQL-safe column name (preserve original casing)."""
    cleaned = name.strip().replace(" ", "_")
    return cleaned or "_"
