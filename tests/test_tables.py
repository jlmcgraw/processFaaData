"""Tests for faa_nasr.tables.build and _load_csv."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from faa_nasr.tables import _load_csv, build


def _write(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_load_csv_creates_table_with_text_columns(tmp_path):
    csv = tmp_path / "APT_BASE.csv"
    _write(csv, '"ARPT_ID","CITY"\n"IAD","CHANTILLY"\n"DCA","WASHINGTON"\n')
    conn = sqlite3.connect(":memory:")

    n = _load_csv(conn, csv, table_name="APT_BASE")

    assert n == 2
    cols = [r[1] for r in conn.execute("PRAGMA table_info(APT_BASE)").fetchall()]
    assert cols == ["ARPT_ID", "CITY"]
    rows = conn.execute("SELECT * FROM APT_BASE ORDER BY ARPT_ID").fetchall()
    assert rows == [("DCA", "WASHINGTON"), ("IAD", "CHANTILLY")]


def test_load_csv_returns_zero_for_empty_file(tmp_path):
    csv = tmp_path / "EMPTY.csv"
    csv.write_text("", encoding="utf-8")
    conn = sqlite3.connect(":memory:")
    assert _load_csv(conn, csv, table_name="EMPTY") == 0


def test_load_csv_pads_short_rows(tmp_path):
    # Last row has fewer columns than the header.
    csv = tmp_path / "PADDED.csv"
    _write(csv, "a,b,c\n1,2,3\n4,5\n")
    conn = sqlite3.connect(":memory:")

    n = _load_csv(conn, csv, table_name="PADDED")

    assert n == 2
    rows = conn.execute("SELECT a, b, c FROM PADDED").fetchall()
    assert rows == [("1", "2", "3"), ("4", "5", "")]


def test_load_csv_truncates_long_rows(tmp_path):
    csv = tmp_path / "LONG.csv"
    _write(csv, "a,b\n1,2,3,4\n")  # extra cells dropped
    conn = sqlite3.connect(":memory:")

    n = _load_csv(conn, csv, table_name="LONG")

    assert n == 1
    assert conn.execute("SELECT a, b FROM LONG").fetchall() == [("1", "2")]


def test_load_csv_handles_utf8_bom(tmp_path):
    # FAA CSVs sometimes start with a UTF-8 BOM; utf-8-sig should strip it.
    csv = tmp_path / "BOM.csv"
    csv.write_bytes(b"\xef\xbb\xbfa,b\n1,2\n")
    conn = sqlite3.connect(":memory:")

    n = _load_csv(conn, csv, table_name="BOM")

    assert n == 1
    cols = [r[1] for r in conn.execute("PRAGMA table_info(BOM)").fetchall()]
    assert cols == ["a", "b"]


def test_load_csv_replaces_spaces_in_headers(tmp_path):
    # DOF.CSV has columns like "VERIFIED STATUS" -- _safe_col converts the space.
    csv = tmp_path / "DOF.csv"
    _write(csv, "OAS,VERIFIED STATUS\n01-001,O\n")
    conn = sqlite3.connect(":memory:")

    _load_csv(conn, csv, table_name="OBSTACLE")

    cols = [r[1] for r in conn.execute("PRAGMA table_info(OBSTACLE)").fetchall()]
    assert cols == ["OAS", "VERIFIED_STATUS"]


def test_load_csv_drops_existing_table(tmp_path):
    """Running build twice should replace the table, not error or append."""
    csv = tmp_path / "T.csv"
    _write(csv, "a\n1\n")
    conn = sqlite3.connect(":memory:")
    _load_csv(conn, csv, table_name="T")
    _load_csv(conn, csv, table_name="T")  # second run
    assert conn.execute("SELECT COUNT(*) FROM T").fetchone()[0] == 1


def test_build_loads_all_csvs_in_directory(tmp_path):
    csv_dir = tmp_path / "csvs"
    csv_dir.mkdir()
    _write(csv_dir / "APT_BASE.csv", "ARPT_ID\nIAD\nDCA\n")
    _write(csv_dir / "NAV_BASE.csv", "NAV_ID\nDCA\n")
    db_path = tmp_path / "out.sqlite"

    build(csv_dir=csv_dir, db_path=db_path)

    assert db_path.exists()
    conn = sqlite3.connect(db_path)
    try:
        tables = sorted(
            r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        )
        assert "APT_BASE" in tables
        assert "NAV_BASE" in tables
        assert conn.execute("SELECT COUNT(*) FROM APT_BASE").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM NAV_BASE").fetchone()[0] == 1
    finally:
        conn.close()


def test_build_skips_data_structure_csvs(tmp_path):
    """*_CSV_DATA_STRUCTURE.csv files describe schema, not data, and must be ignored."""
    csv_dir = tmp_path / "csvs"
    csv_dir.mkdir()
    _write(csv_dir / "APT_BASE.csv", "ARPT_ID\nIAD\n")
    _write(csv_dir / "APT_CSV_DATA_STRUCTURE.csv", "CSV File,Column Name\nAPT_BASE,ARPT_ID\n")
    db_path = tmp_path / "out.sqlite"

    build(csv_dir=csv_dir, db_path=db_path)

    conn = sqlite3.connect(db_path)
    try:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert "APT_BASE" in tables
        assert "APT_CSV_DATA_STRUCTURE" not in tables
    finally:
        conn.close()


def test_build_loads_obstacle_csv_separately(tmp_path):
    csv_dir = tmp_path / "csvs"
    csv_dir.mkdir()
    _write(csv_dir / "APT_BASE.csv", "ARPT_ID\nIAD\n")
    obstacle = tmp_path / "DOF.CSV"
    _write(obstacle, "OAS,LATDEC\n01-001,30.0\n02-002,31.0\n")
    db_path = tmp_path / "out.sqlite"

    build(csv_dir=csv_dir, db_path=db_path, obstacle_csv=obstacle)

    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM OBSTACLE").fetchone()[0] == 2
    finally:
        conn.close()


def test_build_overwrites_existing_db(tmp_path):
    csv_dir = tmp_path / "csvs"
    csv_dir.mkdir()
    _write(csv_dir / "T.csv", "a\n1\n")
    db_path = tmp_path / "out.sqlite"
    db_path.write_text("not actually a sqlite file")  # bogus pre-existing content

    build(csv_dir=csv_dir, db_path=db_path)

    # If overwrite worked, the file is now a real SQLite DB we can open.
    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM T").fetchone()[0] == 1
    finally:
        conn.close()
