"""Tests for CIFP record parsing against real CIFP file records."""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

from faa_nasr.cifp import _slice_record, build, parse_cifp_coord
from faa_nasr.cifp_records import PRIMARY_SPECS, resolve_dups

_CIFP_FILE = Path("/Users/jlmcgraw/PycharmProjects/processFaaData/parse_cifp/CIFP_260514/FAACIFP18")

# ---------------------------------------------------------------------------
# Unit tests against known record strings (no file I/O needed)
# ---------------------------------------------------------------------------

_VOR_RECORD = (
    "SCAND        ADK   PA011400 DUW                    ADK N51521587W176402739E0070003291"
    "     NARMOUNT MOFFETT                 002422105"
)

_AIRPORT_RECORD = (
    "SCANP 00AKPAA        0     025NSN59565600W151413200E013400252         1800018000P"
    "    MNAR    LOWELL FLD                    047712605"
)

_RUNWAY_RECORD = (
    "SCANP 00ANPAGRW03    0045170305 N59052000W156275000               00070000050060D"
    "                                          047732605"
)


def _parse_primary(record: str, sc: str, ssc: str) -> dict[str, str]:
    fields = resolve_dups(PRIMARY_SPECS[(sc, ssc)])
    return _slice_record(record, fields)


def test_vor_record_fields() -> None:
    parsed = _parse_primary(_VOR_RECORD, "D", "")
    assert parsed["VORIdentifier"].strip() == "ADK"
    assert parsed["VORFrequency"].strip() == "11400"
    assert parsed["DMELatitude"].strip() == "N51521587"


def test_vor_record_dme_latitude_wgs84() -> None:
    parsed = _parse_primary(_VOR_RECORD, "D", "")
    dme_lat = parsed["DMELatitude"].strip()
    wgs84 = parse_cifp_coord(dme_lat)
    assert wgs84 is not None
    assert abs(wgs84 - 51.871) < 0.001


def test_airport_record_fields() -> None:
    parsed = _parse_primary(_AIRPORT_RECORD, "P", "A")
    assert parsed["LandingFacilityIcaoIdentifier"].strip() == "00AK"
    assert parsed["AirportName"].strip() == "LOWELL FLD"


def test_airport_record_lat_wgs84() -> None:
    parsed = _parse_primary(_AIRPORT_RECORD, "P", "A")
    lat = parsed["AirportReferencePtLatitude"].strip()
    wgs84 = parse_cifp_coord(lat)
    assert wgs84 is not None
    assert abs(wgs84 - 59.949) < 0.001


def test_runway_record_fields() -> None:
    parsed = _parse_primary(_RUNWAY_RECORD, "P", "G")
    assert parsed["RunwayIdentifier"].strip() == "RW03"


def test_runway_record_lat_wgs84() -> None:
    parsed = _parse_primary(_RUNWAY_RECORD, "P", "G")
    lat = parsed["RunwayLatitude"].strip()
    wgs84 = parse_cifp_coord(lat)
    assert wgs84 is not None
    assert abs(wgs84 - 59.089) < 0.001


# ---------------------------------------------------------------------------
# Unit tests for _primary_id FK linkage
# ---------------------------------------------------------------------------

# 38-byte record key for a synthetic P/F record:
#   RecordType(1)='S' CustomerAreaCode(3)='USA' SectionCode(1)='P'
#   BlankSpacing(1)=' ' LandingFacilityIcaoIdentifier(4)='KBOS'
#   LandingFacilityIcaoRegionCode(2)='K6' SubSectionCode(1)='F'
#   SIDSTARApproachIdentifier(6)='I09L  ' RouteType(1)='A'
#   TransitionIdentifier(5)='     ' BlankSpacing(1)=' '
#   SequenceNumber(3)='010' FixIdentifier(5)='KMALO'
#   FixIcaoRegionCode(2)='K6' FixSectionCode(1)='E' FixSubSectionCode(1)='A'
_PF_KEY = "SUSAP KBOSK6FI09L  A      010KMALOK6EA"  # exactly 38 chars
assert len(_PF_KEY) == 38

# Primary P/F record: CRN='1', remaining 93 chars padded.
_PF_PRIMARY = _PF_KEY + "1" + " " * 93
assert len(_PF_PRIMARY) == 132

# Continuation P/F/W record: CRN='2', ApplicationType='W', 92 chars padded.
_PF_CONT_W = _PF_KEY + "2W" + " " * 92
assert len(_PF_CONT_W) == 132


def test_primary_id_links_continuation_to_primary() -> None:
    """Continuation _primary_id equals the matching primary row's _id."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cifp = Path(tmpdir) / "FAACIFP18"
        cifp.write_text(_PF_PRIMARY + "\n" + _PF_CONT_W + "\n", encoding="latin-1")
        db_path = Path(tmpdir) / "cifp.sqlite"
        build(cifp_path=cifp, db_path=db_path)

        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            tables = [
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            ]
            primary_table = next(t for t in tables if t.startswith("primary_P_F"))
            cont_table = next(t for t in tables if t.startswith("continuation_P_F"))

            primary_id = conn.execute(f'SELECT _id FROM "{primary_table}"').fetchone()[0]
            cont_row = conn.execute(f'SELECT _primary_id FROM "{cont_table}"').fetchone()

            assert cont_row is not None, "No continuation row found"
            assert cont_row["_primary_id"] == primary_id


def test_primary_id_column_is_integer_type() -> None:
    """_primary_id column is declared INTEGER in the continuation table schema."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cifp = Path(tmpdir) / "FAACIFP18"
        cifp.write_text(_PF_PRIMARY + "\n" + _PF_CONT_W + "\n", encoding="latin-1")
        db_path = Path(tmpdir) / "cifp.sqlite"
        build(cifp_path=cifp, db_path=db_path)

        with sqlite3.connect(db_path) as conn:
            tables = [
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            ]
            cont_table = next(t for t in tables if t.startswith("continuation_P_F"))
            col_info = conn.execute(f'PRAGMA table_info("{cont_table}")').fetchall()
            col_types = {row[1]: row[2] for row in col_info}  # name → type
            assert col_types.get("_primary_id") == "INTEGER"


# ---------------------------------------------------------------------------
# Integration test against the real CIFP file
# ---------------------------------------------------------------------------

_SKIP = not _CIFP_FILE.exists()


@pytest.mark.skipif(_SKIP, reason="FAACIFP18 not available")
def test_cifp_build_creates_db() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "cifp.sqlite"
        build(cifp_path=_CIFP_FILE, db_path=db_path)
        assert db_path.exists()
        assert db_path.stat().st_size > 0


@pytest.mark.skipif(_SKIP, reason="FAACIFP18 not available")
def test_cifp_build_vor_table() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "cifp.sqlite"
        build(cifp_path=_CIFP_FILE, db_path=db_path)
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            tables = [
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            ]
            vor_table = next(
                (t for t in tables if "Navaid - VHF Navaid" in t and t.startswith("primary")),
                None,
            )
            assert vor_table is not None, f"VOR table not found; tables={tables}"
            count = conn.execute(f'SELECT count(*) FROM "{vor_table}"').fetchone()[0]
            assert count > 100


@pytest.mark.skipif(_SKIP, reason="FAACIFP18 not available")
def test_cifp_pf_continuations_all_linked() -> None:
    """Every P/F/W continuation row has a non-NULL _primary_id."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "cifp.sqlite"
        build(cifp_path=_CIFP_FILE, db_path=db_path)
        with sqlite3.connect(db_path) as conn:
            tables = [
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            ]
            cont_table = next(
                (t for t in tables if "continuation" in t and "_P_F_W_" in t),
                None,
            )
            assert cont_table is not None, "P/F/W continuation table not found"
            total = conn.execute(f'SELECT count(*) FROM "{cont_table}"').fetchone()[0]
            null_count = conn.execute(
                f'SELECT count(*) FROM "{cont_table}" WHERE _primary_id IS NULL'
            ).fetchone()[0]
            assert total > 0, "No P/F/W continuation rows found"
            assert null_count == 0, f"{null_count}/{total} rows have NULL _primary_id"


@pytest.mark.skipif(_SKIP, reason="FAACIFP18 not available")
def test_cifp_build_adk_vor_coords() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "cifp.sqlite"
        build(cifp_path=_CIFP_FILE, db_path=db_path)
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            tables = [
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            ]
            vor_table = next(
                (t for t in tables if "Navaid - VHF Navaid" in t and t.startswith("primary")),
                None,
            )
            assert vor_table is not None

            row = conn.execute(
                f'SELECT * FROM "{vor_table}" WHERE VORIdentifier=?', ("ADK",)
            ).fetchone()
            assert row is not None, "ADK VOR not found"

            dme_lat_wgs84 = float(row["DMELatitude_WGS84"])
            assert abs(dme_lat_wgs84 - 51.871) < 0.001
