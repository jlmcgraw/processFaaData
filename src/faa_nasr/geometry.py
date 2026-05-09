"""Add SpatiaLite geometry columns and spatial indexes to a NASR SQLite database in-place."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from tqdm import tqdm

from faa_nasr import _log

# Candidate paths to try for mod_spatialite. Order matters only for noise:
# the loader picks the first one that resolves.
_MOD_SPATIALITE_CANDIDATES = (
    "mod_spatialite",
    "mod_spatialite.so",
    # Debian / Ubuntu multi-arch locations (used inside the container image).
    "/usr/lib/aarch64-linux-gnu/mod_spatialite.so",
    "/usr/lib/x86_64-linux-gnu/mod_spatialite.so",
    # Homebrew on macOS (Apple Silicon / Intel).
    "/opt/homebrew/lib/mod_spatialite",
    "/usr/local/lib/mod_spatialite",
)


@dataclass(frozen=True)
class PointGeom:
    table: str
    geom_column: str
    lon_column: str
    lat_column: str


# Tables that have decimal lon/lat columns in the CSV subscription.
# The CSV bundle uses LAT_DECIMAL/LONG_DECIMAL (NASR) and DOF uses LATDEC/LONDEC.
_POINT_GEOMS: tuple[PointGeom, ...] = (
    PointGeom("APT_BASE", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    PointGeom("AWOS", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    PointGeom("FIX_BASE", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    PointGeom("NAV_BASE", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    PointGeom("ILS_BASE", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    PointGeom("FSS_BASE", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    PointGeom("ATC_BASE", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    PointGeom("HPF_BASE", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    PointGeom("OBSTACLE", "geometry", "LONDEC", "LATDEC"),
)


def build(db_path: Path) -> None:
    """Open the NASR SQLite DB in-place, load mod_spatialite, add geometry + indexes.

    Idempotent: if geometry has already been added for a table, that table is
    skipped. Re-running is safe (and cheap, since `_already_geometric` short-
    circuits before any UPDATE runs).
    """
    db_path = db_path.resolve()
    _log.step(f"build-spatial -> {db_path}")
    if not db_path.exists():
        raise FileNotFoundError(f"database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        conn.enable_load_extension(True)
        _load_mod_spatialite(conn)
        # SpatiaLite 5+ uses RTreeAlign() inside CreateSpatialIndex; the SQLite
        # untrusted-schema guard otherwise rejects it, leaving spatial indexes
        # structurally present but with NULL data.
        conn.execute("PRAGMA trusted_schema = ON")
        # Write-perf PRAGMAs (per-connection).
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA journal_mode = MEMORY")
        if not _spatialite_initialized(conn):
            conn.execute("SELECT InitSpatialMetadata(1)")

        existing = _existing_tables(_POINT_GEOMS, conn)
        to_populate = [g for g in existing if not _already_geometric(conn, g)]
        skipped = len(existing) - len(to_populate)
        if skipped:
            _log.info(f"  {skipped} table(s) already have geometry -- skipping")

        # Pass 1: add column + populate geometries (no spatial index yet, so the
        # bulk UPDATE doesn't pay per-row R-tree trigger cost).
        total_geoms = 0
        bar = tqdm(
            to_populate,
            desc="  geometries",
            unit="table",
            disable=_log.is_quiet(),
            leave=True,
        )
        for geom in bar:
            bar.set_postfix_str(geom.table, refresh=False)
            total_geoms += _populate_point_geometry(conn, geom)
        if to_populate:
            _log.info(f"  added {total_geoms:,} geometries across {len(to_populate)} tables")

        # Pass 2: build spatial indexes against the populated columns.
        needs_index = [g for g in existing if not _has_spatial_index(conn, g)]
        bar = tqdm(
            needs_index,
            desc="  spatial indexes",
            unit="table",
            disable=_log.is_quiet(),
            leave=True,
        )
        for geom in bar:
            bar.set_postfix_str(geom.table, refresh=False)
            conn.execute("SELECT CreateSpatialIndex(?, ?)", (geom.table, geom.geom_column))

        conn.commit()
    finally:
        conn.close()


def _load_mod_spatialite(conn: sqlite3.Connection) -> None:
    last_err: Exception | None = None
    for path in _MOD_SPATIALITE_CANDIDATES:
        try:
            conn.load_extension(path)
            _log.info(f"  loaded mod_spatialite from {path}")
            return
        except sqlite3.OperationalError as exc:
            last_err = exc
    raise RuntimeError(
        "could not load mod_spatialite from any of: " + ", ".join(_MOD_SPATIALITE_CANDIDATES)
    ) from last_err


def _spatialite_initialized(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='geometry_columns'"
    ).fetchone()
    return row is not None


def _existing_tables(geoms: Iterable[PointGeom], conn: sqlite3.Connection) -> list[PointGeom]:
    out: list[PointGeom] = []
    for g in geoms:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (g.table,),
        ).fetchone()
        if row is not None:
            out.append(g)
    return out


def _already_geometric(conn: sqlite3.Connection, g: PointGeom) -> bool:
    """True if g.geom_column is already registered in geometry_columns for g.table.

    SpatiaLite stores f_table_name / f_geometry_column case-insensitively in
    its metadata, so compare via LOWER() rather than literal equality.
    """
    row = conn.execute(
        "SELECT 1 FROM geometry_columns "
        "WHERE LOWER(f_table_name) = LOWER(?) AND LOWER(f_geometry_column) = LOWER(?)",
        (g.table, g.geom_column),
    ).fetchone()
    return row is not None


def _has_spatial_index(conn: sqlite3.Connection, g: PointGeom) -> bool:
    """True if a SpatialIndex (R-tree) virtual table exists for g.table.g.geom_column."""
    # SpatiaLite names the R-tree backing table after the (lowercased) table+column.
    row = conn.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND LOWER(name) = LOWER(?)",
        (f"idx_{g.table}_{g.geom_column}",),
    ).fetchone()
    return row is not None


def _populate_point_geometry(conn: sqlite3.Connection, g: PointGeom) -> int:
    """Add the geometry column and fill it from the table's lon/lat columns."""
    conn.execute(
        "SELECT AddGeometryColumn(?, ?, 4326, 'POINT', 'XY')",
        (g.table, g.geom_column),
    )
    cur = conn.execute(
        f'UPDATE "{g.table}" '
        f'SET "{g.geom_column}" = MakePoint('
        f'CAST("{g.lon_column}" AS DOUBLE), '
        f'CAST("{g.lat_column}" AS DOUBLE), 4326) '
        f'WHERE "{g.lon_column}" IS NOT NULL AND "{g.lon_column}" <> "" '
        f'AND "{g.lat_column}" IS NOT NULL AND "{g.lat_column}" <> ""'
    )
    return cur.rowcount
