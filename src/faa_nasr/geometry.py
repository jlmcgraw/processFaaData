"""Add SpatiaLite geometry columns and spatial indexes to a NASR SQLite database."""

from __future__ import annotations

import shutil
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


def build(src: Path, dst: Path) -> None:
    """Copy the SQLite DB, load mod_spatialite, and add geometry + indexes."""
    src = src.resolve()
    dst = dst.resolve()
    _log.step(f"build-spatial -> {dst}")
    if dst.exists():
        dst.unlink()
    _log.info(f"  copying {src.name} ({src.stat().st_size / 1e6:.0f} MB)")
    shutil.copy(src, dst)

    conn = sqlite3.connect(dst)
    try:
        conn.enable_load_extension(True)
        _load_mod_spatialite(conn)
        # SpatiaLite 5+ uses RTreeAlign() inside CreateSpatialIndex; the SQLite
        # untrusted-schema guard otherwise rejects it, leaving spatial indexes
        # structurally present but with NULL data.
        conn.execute("PRAGMA trusted_schema = ON")
        # Write-perf PRAGMAs: these don't carry over from the source DB because
        # they're per-connection.
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA journal_mode = MEMORY")
        conn.execute("SELECT InitSpatialMetadata(1)")

        existing = _existing(_POINT_GEOMS, conn)

        # Pass 1: add column + populate geometries (no spatial index yet, so the
        # bulk UPDATE doesn't pay per-row R-tree trigger cost).
        total_geoms = 0
        bar = tqdm(existing, desc="  geometries", unit="table", disable=_log.is_quiet(), leave=True)
        for geom in bar:
            bar.set_postfix_str(geom.table, refresh=False)
            total_geoms += _populate_point_geometry(conn, geom)
        _log.info(f"  added {total_geoms:,} geometries across {len(existing)} tables")

        # Pass 2: build spatial indexes against the populated columns. This is a
        # single bulk R-tree load per table -- much faster than maintaining the
        # index incrementally during the UPDATE above (where per-row trigger
        # overhead would dominate, especially for OBSTACLE's 635k rows).
        bar = tqdm(existing, desc="  spatial indexes", unit="table", disable=_log.is_quiet(), leave=True)
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


def _existing(geoms: Iterable[PointGeom], conn: sqlite3.Connection) -> list[PointGeom]:
    out: list[PointGeom] = []
    for g in geoms:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (g.table,),
        ).fetchone()
        if row is not None:
            out.append(g)
    return out


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
