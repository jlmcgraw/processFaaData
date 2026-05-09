"""Add SpatiaLite geometry columns and spatial indexes to a NASR SQLite database."""

from __future__ import annotations

import shutil
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

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
        conn.execute("SELECT InitSpatialMetadata(1)")
        existing = _existing(_POINT_GEOMS, conn)
        for i, geom in enumerate(existing, start=1):
            n = _add_point_geometry(conn, geom)
            _log.info(f"  [{i}/{len(existing)}] {geom.table:<10} {n:>9,} geometries")
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


def _add_point_geometry(conn: sqlite3.Connection, g: PointGeom) -> int:
    conn.execute(
        "SELECT AddGeometryColumn(?, ?, 4326, 'POINT', 'XY')",
        (g.table, g.geom_column),
    )
    conn.execute("SELECT CreateSpatialIndex(?, ?)", (g.table, g.geom_column))
    cur = conn.execute(
        f'UPDATE "{g.table}" '
        f'SET "{g.geom_column}" = MakePoint('
        f'CAST("{g.lon_column}" AS DOUBLE), '
        f'CAST("{g.lat_column}" AS DOUBLE), 4326) '
        f'WHERE "{g.lon_column}" IS NOT NULL AND "{g.lon_column}" <> "" '
        f'AND "{g.lat_column}" IS NOT NULL AND "{g.lat_column}" <> ""'
    )
    return cur.rowcount
