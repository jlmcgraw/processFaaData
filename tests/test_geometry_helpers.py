"""Tests for the idempotency helpers in faa_nasr.geometry.

These functions only run SQL against tables that SpatiaLite (or InitSpatialMetadata)
creates -- so we test them by stubbing those tables in an in-memory SQLite DB
rather than loading mod_spatialite for real.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator

import pytest

from faa_nasr.geometry import (
    PointGeom,
    _already_geometric,
    _existing_tables,
    _has_spatial_index,
    _spatialite_initialized,
)


@pytest.fixture
def conn() -> Iterator[sqlite3.Connection]:
    c = sqlite3.connect(":memory:")
    yield c
    c.close()


def _stub_geometry_columns(conn: sqlite3.Connection) -> None:
    """Create a minimal geometry_columns matching SpatiaLite's shape."""
    conn.execute("CREATE TABLE geometry_columns (f_table_name TEXT, f_geometry_column TEXT)")


# ---------------------------------------------------------------------------
# _spatialite_initialized
# ---------------------------------------------------------------------------


def test_spatialite_initialized_false_for_plain_sqlite(conn):
    assert _spatialite_initialized(conn) is False


def test_spatialite_initialized_true_when_geometry_columns_exists(conn):
    _stub_geometry_columns(conn)
    assert _spatialite_initialized(conn) is True


# ---------------------------------------------------------------------------
# _existing_tables
# ---------------------------------------------------------------------------


def test_existing_tables_filters_to_present_tables_only(conn):
    conn.execute("CREATE TABLE APT_BASE (a TEXT)")
    conn.execute("CREATE TABLE NAV_BASE (b TEXT)")
    geoms = [
        PointGeom("APT_BASE", "geometry", "LON", "LAT"),
        PointGeom("DOES_NOT_EXIST", "geometry", "LON", "LAT"),
        PointGeom("NAV_BASE", "geometry", "LON", "LAT"),
    ]
    result = _existing_tables(geoms, conn)
    assert [g.table for g in result] == ["APT_BASE", "NAV_BASE"]


def test_existing_tables_preserves_input_order(conn):
    conn.execute("CREATE TABLE B (x TEXT)")
    conn.execute("CREATE TABLE A (x TEXT)")
    geoms = [
        PointGeom("A", "geometry", "LON", "LAT"),
        PointGeom("B", "geometry", "LON", "LAT"),
    ]
    result = _existing_tables(geoms, conn)
    # Function iterates `geoms` in order and emits matches; should be ["A", "B"].
    assert [g.table for g in result] == ["A", "B"]


def test_existing_tables_empty_when_nothing_matches(conn):
    geoms = [PointGeom("FOO", "geometry", "LON", "LAT")]
    assert _existing_tables(geoms, conn) == []


# ---------------------------------------------------------------------------
# _already_geometric
# ---------------------------------------------------------------------------


def test_already_geometric_false_when_table_not_in_geometry_columns(conn):
    _stub_geometry_columns(conn)
    g = PointGeom("APT_BASE", "geometry", "LON", "LAT")
    assert _already_geometric(conn, g) is False


def test_already_geometric_true_for_exact_match(conn):
    _stub_geometry_columns(conn)
    conn.execute("INSERT INTO geometry_columns VALUES (?, ?)", ("APT_BASE", "geometry"))
    g = PointGeom("APT_BASE", "geometry", "LON", "LAT")
    assert _already_geometric(conn, g) is True


def test_already_geometric_handles_lowercase_storage(conn):
    """Regression: SpatiaLite stores f_table_name / f_geometry_column lowercased
    (or in whatever case the original entity used), so the lookup must compare
    via LOWER(). Without this, build-spatial-twice fails idempotency."""
    _stub_geometry_columns(conn)
    conn.execute("INSERT INTO geometry_columns VALUES (?, ?)", ("apt_base", "geometry"))
    g = PointGeom("APT_BASE", "geometry", "LON", "LAT")
    assert _already_geometric(conn, g) is True


def test_already_geometric_handles_mixed_case_query(conn):
    _stub_geometry_columns(conn)
    conn.execute("INSERT INTO geometry_columns VALUES (?, ?)", ("APT_BASE", "GEOMETRY"))
    g = PointGeom("apt_base", "geometry", "LON", "LAT")
    assert _already_geometric(conn, g) is True


def test_already_geometric_distinguishes_different_tables(conn):
    _stub_geometry_columns(conn)
    conn.execute("INSERT INTO geometry_columns VALUES (?, ?)", ("OTHER_TABLE", "geometry"))
    g = PointGeom("APT_BASE", "geometry", "LON", "LAT")
    assert _already_geometric(conn, g) is False


def test_already_geometric_distinguishes_different_columns(conn):
    _stub_geometry_columns(conn)
    conn.execute("INSERT INTO geometry_columns VALUES (?, ?)", ("APT_BASE", "other_geom"))
    g = PointGeom("APT_BASE", "geometry", "LON", "LAT")
    assert _already_geometric(conn, g) is False


# ---------------------------------------------------------------------------
# _has_spatial_index
# ---------------------------------------------------------------------------


def test_has_spatial_index_false_when_rtree_backing_table_missing(conn):
    g = PointGeom("APT_BASE", "geometry", "LON", "LAT")
    assert _has_spatial_index(conn, g) is False


def test_has_spatial_index_finds_rtree_backing_table(conn):
    # SpatiaLite names the R-tree backing table idx_<table>_<column>.
    conn.execute("CREATE TABLE idx_APT_BASE_geometry (id INTEGER)")
    g = PointGeom("APT_BASE", "geometry", "LON", "LAT")
    assert _has_spatial_index(conn, g) is True


def test_has_spatial_index_handles_case_insensitive_match(conn):
    conn.execute("CREATE TABLE idx_apt_base_geometry (id INTEGER)")
    g = PointGeom("APT_BASE", "geometry", "LON", "LAT")
    assert _has_spatial_index(conn, g) is True


def test_has_spatial_index_does_not_match_other_tables(conn):
    conn.execute("CREATE TABLE idx_NAV_BASE_geometry (id INTEGER)")
    g = PointGeom("APT_BASE", "geometry", "LON", "LAT")
    assert _has_spatial_index(conn, g) is False


def test_has_spatial_index_does_not_match_partial_name(conn):
    # A real-world possible false positive: an "idx_APT_BASE_other_column"
    # rtree backing table for a different geometry column on the same table.
    conn.execute("CREATE TABLE idx_APT_BASE_other (id INTEGER)")
    g = PointGeom("APT_BASE", "geometry", "LON", "LAT")
    assert _has_spatial_index(conn, g) is False
