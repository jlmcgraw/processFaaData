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
    """A POINT geometry built directly from lon/lat columns on the same table."""

    table: str
    geom_column: str
    lon_column: str
    lat_column: str


@dataclass(frozen=True)
class JoinedPointGeom:
    """A POINT geometry on `table` populated by joining to another table's
    already-populated geometry. Used when one CSV references another by ID
    (e.g. ATC_BASE.SITE_NO -> APT_BASE.SITE_NO) instead of carrying its own
    coordinates.
    """

    table: str
    geom_column: str
    join_table: str
    join_geom_column: str  # the source of truth on `join_table`
    self_key: str  # column on `table` to match
    other_key: str  # column on `join_table` to match


@dataclass(frozen=True)
class RunwayLineGeom:
    """A LINESTRING geometry on APT_RWY built from the two APT_RWY_END points
    keyed by (SITE_NO, RWY_ID). Specific to the runway/end split in the FAA
    CSV schema; not generalised because no other table uses this pattern.
    """

    geom_column: str = "runway_geometry"


# -----------------------------------------------------------------------------
# Direct POINT geometries: the table itself carries lon/lat columns.
# -----------------------------------------------------------------------------
_POINT_GEOMS: tuple[PointGeom, ...] = (
    # Airports + runway ends.
    PointGeom("APT_BASE", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    PointGeom("APT_RWY_END", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    PointGeom(
        "APT_RWY_END",
        "displaced_threshold_geometry",
        "LONG_DISPLACED_THR_DECIMAL",
        "LAT_DISPLACED_THR_DECIMAL",
    ),
    PointGeom(
        # Land And Hold Short Operations point on a runway end (when present).
        "APT_RWY_END",
        "lahso_geometry",
        "LONG_LAHSO_DECIMAL",
        "LAT_LAHSO_DECIMAL",
    ),
    # ARTCC reference point + per-segment boundary vertices.
    PointGeom("ARB_BASE", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    PointGeom("ARB_SEG", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    # Weather + comm + AWOS facilities.
    PointGeom("AWOS", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    PointGeom("COM", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    PointGeom("WXL_BASE", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    # Navaids + secondary TACAN/DME (TACAN_DME_LAT_DECIMAL is empty for VOR-only).
    PointGeom("NAV_BASE", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    PointGeom(
        "NAV_BASE",
        "tacan_dme_geometry",
        "TACAN_DME_LONG_DECIMAL",
        "TACAN_DME_LAT_DECIMAL",
    ),
    # Fixes.
    PointGeom("FIX_BASE", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    # ILS components.
    PointGeom("ILS_BASE", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    PointGeom("ILS_DME", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    PointGeom("ILS_GS", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    PointGeom("ILS_MKR", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    # FSS, frequencies, military training routes, parachute jump areas.
    PointGeom("FSS_BASE", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    PointGeom("FRQ", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    PointGeom("MTR_PT", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    PointGeom("PJA_BASE", "geometry", "LONG_DECIMAL", "LAT_DECIMAL"),
    # Daily DOF obstacle file.
    PointGeom("OBSTACLE", "geometry", "LONDEC", "LATDEC"),
)

# -----------------------------------------------------------------------------
# Joined POINT geometries: the table doesn't carry lon/lat itself, so we
# borrow the geometry from another table via a key. Both ATC_BASE and HPF_BASE
# only carry IDs (SITE_NO, FIX_ID, NAV_ID).
# -----------------------------------------------------------------------------
_JOINED_POINT_GEOMS: tuple[JoinedPointGeom, ...] = (
    # The tower/approach/departure facility's location is the airport itself.
    JoinedPointGeom(
        table="ATC_BASE",
        geom_column="geometry",
        join_table="APT_BASE",
        join_geom_column="geometry",
        self_key="SITE_NO",
        other_key="SITE_NO",
    ),
    # Holding patterns reference both a fix and (often) a navaid -- expose both.
    JoinedPointGeom(
        table="HPF_BASE",
        geom_column="fix_geometry",
        join_table="FIX_BASE",
        join_geom_column="geometry",
        self_key="FIX_ID",
        other_key="FIX_ID",
    ),
    JoinedPointGeom(
        table="HPF_BASE",
        geom_column="navaid_geometry",
        join_table="NAV_BASE",
        join_geom_column="geometry",
        self_key="NAV_ID",
        other_key="NAV_ID",
    ),
)

# Single LINESTRING geometry: APT_RWY -> two APT_RWY_END rows.
_RUNWAY_LINE_GEOM = RunwayLineGeom()


def build(db_path: Path) -> None:
    """Open the NASR SQLite DB in-place, load mod_spatialite, add geometry + indexes.

    Idempotent: tables that already have a registered geometry column are
    skipped. Re-running is safe (and cheap, since `_already_geometric`
    short-circuits before any UPDATE runs).
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

        # Pass 1: direct POINT geometries (lon/lat columns on the same table).
        existing_points = _existing_tables(_POINT_GEOMS, conn)
        to_populate_points = [g for g in existing_points if not _already_geometric(conn, g)]
        skipped = len(existing_points) - len(to_populate_points)
        if skipped:
            _log.info(f"  {skipped} POINT(s) already have geometry -- skipping")

        total_geoms = 0
        points_bar = tqdm(
            to_populate_points,
            desc="  POINT geoms",
            unit="col",
            disable=_log.is_quiet(),
            leave=True,
        )
        for geom in points_bar:
            points_bar.set_postfix_str(f"{geom.table}.{geom.geom_column}", refresh=False)
            total_geoms += _populate_point_geometry(conn, geom)
        if to_populate_points:
            _log.info(f"  added {total_geoms:,} points across {len(to_populate_points)} columns")

        # Pass 2: JOIN-based POINT geometries (ATC, HPF). Must run after Pass 1
        # so the source-of-truth geometry on the join_table is already filled in.
        # Without indexes on the join keys these correlated subqueries do
        # full-table scans per row -- order-of-magnitude slowdown.
        _ensure_join_indexes(
            conn,
            indexes=[
                ("APT_BASE", "SITE_NO"),
                ("FIX_BASE", "FIX_ID"),
                ("NAV_BASE", "NAV_ID"),
                ("APT_RWY_END", "SITE_NO,RWY_ID"),
            ],
        )
        existing_joins = _existing_joined_geoms(_JOINED_POINT_GEOMS, conn)
        to_populate_joins = [
            jg for jg in existing_joins if not _joined_geom_already_present(conn, jg)
        ]
        joined_total = 0
        joined_bar = tqdm(
            to_populate_joins,
            desc="  joined POINT geoms",
            unit="col",
            disable=_log.is_quiet(),
            leave=True,
        )
        for jg in joined_bar:
            joined_bar.set_postfix_str(f"{jg.table}.{jg.geom_column}", refresh=False)
            joined_total += _populate_joined_point_geometry(conn, jg)
        if to_populate_joins:
            _log.info(
                f"  added {joined_total:,} joined points across {len(to_populate_joins)} columns"
            )

        # Pass 3: APT_RWY runway LINESTRING (joins to APT_RWY_END).
        if (
            _table_exists(conn, "APT_RWY")
            and _table_exists(conn, "APT_RWY_END")
            and not _column_already_registered(conn, "APT_RWY", _RUNWAY_LINE_GEOM.geom_column)
        ):
            runway_count = _populate_runway_lines(conn, _RUNWAY_LINE_GEOM.geom_column)
            _log.info(f"  added {runway_count:,} runway LINESTRINGs")

        # Pass 4: build spatial indexes against everything that doesn't have one.
        all_indexed = _all_geom_columns(conn)
        needs_index = [
            (table, col)
            for table, col in all_indexed
            if not _spatial_index_exists(conn, table, col)
        ]
        index_bar = tqdm(
            needs_index,
            desc="  spatial indexes",
            unit="col",
            disable=_log.is_quiet(),
            leave=True,
        )
        for table, col in index_bar:
            index_bar.set_postfix_str(f"{table}.{col}", refresh=False)
            conn.execute("SELECT CreateSpatialIndex(?, ?)", (table, col))

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


def _ensure_join_indexes(conn: sqlite3.Connection, indexes: Iterable[tuple[str, str]]) -> None:
    """Create supporting indexes for the join queries used by the joined-point
    and runway-line passes. Names follow `idx_<table>_<col>(s)` convention.
    Each entry is (table, comma-separated columns)."""
    for table, cols in indexes:
        if not _table_exists(conn, table):
            continue
        col_list = cols.split(",")
        # Validate column existence first -- avoid SQLite's silent string-literal
        # fallback on unknown identifiers.
        present = _table_columns(conn, table)
        if not all(c in present for c in col_list):
            continue
        idx_name = f"idx_join_{table}_{cols.replace(',', '_')}"
        quoted_cols = ", ".join(f'"{c}"' for c in col_list)
        conn.execute(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{table}" ({quoted_cols})')


def _spatialite_initialized(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='geometry_columns'"
    ).fetchone()
    return row is not None


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f'PRAGMA table_info("{table}")').fetchall()}


def _existing_tables(geoms: Iterable[PointGeom], conn: sqlite3.Connection) -> list[PointGeom]:
    return [g for g in geoms if _table_exists(conn, g.table)]


def _existing_joined_geoms(
    geoms: Iterable[JoinedPointGeom], conn: sqlite3.Connection
) -> list[JoinedPointGeom]:
    return [g for g in geoms if _table_exists(conn, g.table) and _table_exists(conn, g.join_table)]


def _column_already_registered(conn: sqlite3.Connection, table: str, geom_column: str) -> bool:
    """SpatiaLite stores f_table_name / f_geometry_column case-insensitively."""
    row = conn.execute(
        "SELECT 1 FROM geometry_columns "
        "WHERE LOWER(f_table_name) = LOWER(?) AND LOWER(f_geometry_column) = LOWER(?)",
        (table, geom_column),
    ).fetchone()
    return row is not None


def _already_geometric(conn: sqlite3.Connection, g: PointGeom) -> bool:
    return _column_already_registered(conn, g.table, g.geom_column)


def _joined_geom_already_present(conn: sqlite3.Connection, jg: JoinedPointGeom) -> bool:
    return _column_already_registered(conn, jg.table, jg.geom_column)


def _has_spatial_index(conn: sqlite3.Connection, g: PointGeom) -> bool:
    return _spatial_index_exists(conn, g.table, g.geom_column)


def _spatial_index_exists(conn: sqlite3.Connection, table: str, geom_column: str) -> bool:
    """SpatiaLite names the R-tree backing table idx_<table>_<column>."""
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND LOWER(name) = LOWER(?)",
        (f"idx_{table}_{geom_column}",),
    ).fetchone()
    return row is not None


def _all_geom_columns(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """Return every (table, geom_column) registered in geometry_columns."""
    return [
        (row[0], row[1])
        for row in conn.execute(
            "SELECT f_table_name, f_geometry_column FROM geometry_columns"
        ).fetchall()
    ]


def _populate_point_geometry(conn: sqlite3.Connection, g: PointGeom) -> int:
    """Add the geometry column and fill it from the table's lon/lat columns.

    Validates that the source columns actually exist before building the SQL.
    SQLite has a quirk where a double-quoted identifier silently falls back
    to a string literal when the column doesn't exist -- without this guard,
    a typo in lon_column / lat_column produces all-zero "POINT(0 0)" rows
    instead of an error.
    """
    cols = _table_columns(conn, g.table)
    missing = {g.lon_column, g.lat_column} - cols
    if missing:
        raise ValueError(
            f"{g.table} is missing source columns {sorted(missing)} -- check _POINT_GEOMS "
            f"(SQLite would silently produce POINT(0 0) rows otherwise)"
        )

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


def _populate_joined_point_geometry(conn: sqlite3.Connection, jg: JoinedPointGeom) -> int:
    """Register a POINT column on `jg.table` and fill it from `jg.join_table`'s
    geometry, matching `self_key = other_key`.

    Pre-validates both key columns and the join-side geometry column to avoid
    SQLite's silent string-literal fallback for unknown identifiers.
    """
    self_cols = _table_columns(conn, jg.table)
    other_cols = _table_columns(conn, jg.join_table)
    if jg.self_key not in self_cols:
        raise ValueError(
            f"{jg.table} is missing key column {jg.self_key!r} -- check _JOINED_POINT_GEOMS"
        )
    if jg.other_key not in other_cols or jg.join_geom_column not in other_cols:
        raise ValueError(
            f"{jg.join_table} is missing {jg.other_key!r}/{jg.join_geom_column!r} "
            f"-- check _JOINED_POINT_GEOMS"
        )

    conn.execute(
        "SELECT AddGeometryColumn(?, ?, 4326, 'POINT', 'XY')",
        (jg.table, jg.geom_column),
    )
    cur = conn.execute(
        f'UPDATE "{jg.table}" '
        f'SET "{jg.geom_column}" = ('
        f'  SELECT "{jg.join_geom_column}" FROM "{jg.join_table}" '
        f'  WHERE "{jg.join_table}"."{jg.other_key}" = "{jg.table}"."{jg.self_key}" '
        f'  AND "{jg.join_geom_column}" IS NOT NULL '
        f"  LIMIT 1"
        f") "
        f'WHERE "{jg.self_key}" IS NOT NULL AND "{jg.self_key}" <> ""'
    )
    return cur.rowcount


def _populate_runway_lines(conn: sqlite3.Connection, geom_column: str) -> int:
    """Build a LINESTRING per runway from its two APT_RWY_END rows.

    Each runway has at most two rows in APT_RWY_END (one per end, e.g. "16"
    and "34" for runway "16/34"). We pair them by (SITE_NO, RWY_ID) and order
    by RWY_END_ID so the line direction is deterministic.
    """
    rwy_cols = _table_columns(conn, "APT_RWY")
    end_cols = _table_columns(conn, "APT_RWY_END")
    for col in ("SITE_NO", "RWY_ID"):
        if col not in rwy_cols or col not in end_cols:
            raise ValueError(f"APT_RWY/APT_RWY_END missing key column {col!r}")
    for col in ("RWY_END_ID", "LAT_DECIMAL", "LONG_DECIMAL"):
        if col not in end_cols:
            raise ValueError(f"APT_RWY_END missing required column {col!r}")

    conn.execute(
        "SELECT AddGeometryColumn(?, ?, 4326, 'LINESTRING', 'XY')",
        ("APT_RWY", geom_column),
    )
    cur = conn.execute(
        f"""
        UPDATE "APT_RWY"
        SET "{geom_column}" = (
            SELECT MakeLine(
                (SELECT MakePoint(CAST(LONG_DECIMAL AS DOUBLE),
                                  CAST(LAT_DECIMAL AS DOUBLE), 4326)
                 FROM APT_RWY_END
                 WHERE SITE_NO = APT_RWY.SITE_NO AND RWY_ID = APT_RWY.RWY_ID
                   AND LAT_DECIMAL <> '' AND LONG_DECIMAL <> ''
                 ORDER BY RWY_END_ID LIMIT 1),
                (SELECT MakePoint(CAST(LONG_DECIMAL AS DOUBLE),
                                  CAST(LAT_DECIMAL AS DOUBLE), 4326)
                 FROM APT_RWY_END
                 WHERE SITE_NO = APT_RWY.SITE_NO AND RWY_ID = APT_RWY.RWY_ID
                   AND LAT_DECIMAL <> '' AND LONG_DECIMAL <> ''
                 ORDER BY RWY_END_ID DESC LIMIT 1)
            )
        )
        WHERE (
            SELECT COUNT(*) FROM APT_RWY_END
            WHERE SITE_NO = APT_RWY.SITE_NO AND RWY_ID = APT_RWY.RWY_ID
              AND LAT_DECIMAL <> '' AND LONG_DECIMAL <> ''
        ) >= 2
        """
    )
    return cur.rowcount
