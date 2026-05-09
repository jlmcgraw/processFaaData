"""Load NASR CSV files into a fresh SQLite database."""

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from faa_nasr import _log

# CSVs whose presence describes schema, not data.
_STRUCTURE_SUFFIX = "_CSV_DATA_STRUCTURE.csv"


def build(csv_dir: Path, db_path: Path, obstacle_csv: Path | None = None) -> None:
    """Create a SQLite database, one table per NASR CSV (plus DOF if provided)."""
    db_path = db_path.resolve()
    _log.step(f"build-tables -> {db_path}")
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA page_size = 4096")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA journal_mode = MEMORY")

        csv_files = [
            p for p in sorted(csv_dir.glob("*.csv")) if not p.name.endswith(_STRUCTURE_SUFFIX)
        ]
        total = len(csv_files) + (1 if obstacle_csv is not None else 0)
        for i, csv_path in enumerate(csv_files, start=1):
            n = _load_csv(conn, csv_path, table_name=csv_path.stem)
            _log.info(f"  [{i:>3}/{total}] {csv_path.stem:<24} {n:>9,} rows")

        if obstacle_csv is not None:
            n = _load_csv(conn, obstacle_csv, table_name="OBSTACLE")
            _log.info(f"  [{total:>3}/{total}] {'OBSTACLE':<24} {n:>9,} rows")

        conn.commit()
    finally:
        conn.close()


def _load_csv(conn: sqlite3.Connection, csv_path: Path, table_name: str) -> int:
    """Discover columns from the CSV header, bulk-insert, return row count."""
    with csv_path.open("r", newline="", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return 0
        columns = [_safe_col(c) for c in header]
        ddl_cols = ", ".join(f'"{c}" TEXT' for c in columns)
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        conn.execute(f'CREATE TABLE "{table_name}" ({ddl_cols})')
        placeholders = ",".join("?" * len(columns))
        insert = f'INSERT INTO "{table_name}" VALUES ({placeholders})'

        count = 0

        def _rows():
            nonlocal count
            for row in reader:
                # Defensive: FAA CSVs are usually clean, but pad/truncate just in case.
                if len(row) < len(columns):
                    row = row + [""] * (len(columns) - len(row))
                elif len(row) > len(columns):
                    row = row[: len(columns)]
                count += 1
                yield row

        conn.executemany(insert, _rows())
        return count


def _safe_col(name: str) -> str:
    """Normalize a CSV header to a SQL-safe column name (preserve original casing)."""
    cleaned = name.strip().replace(" ", "_")
    return cleaned or "_"
