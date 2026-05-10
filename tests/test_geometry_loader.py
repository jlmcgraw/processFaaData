"""Tests for the spatialite-loader retry logic and _populate_point_geometry.

These functions exercise sqlite3 directly; we don't have a real spatialite
extension at test time, so we use sqlite3.OperationalError to simulate
load failures and a recording-Mock connection for SQL assertions.
"""

from __future__ import annotations

import sqlite3
from typing import Any, cast

import pytest

from faa_nasr import geometry


def _as_conn(c: object) -> sqlite3.Connection:
    """Pretend a duck-typed mock is a real sqlite3.Connection. The mocks
    implement just enough of the Connection interface for the function under
    test; this cast keeps three different type checkers (mypy, basedpyright,
    ty) happy without spreading per-checker pragmas at every call site."""
    return cast(sqlite3.Connection, c)


class _RecordingConn:
    """Drop-in stand-in for sqlite3.Connection that records load_extension
    attempts and returns a configured (success-after-N) result."""

    def __init__(self, succeed_at_path: str | None) -> None:
        self.succeed_at_path = succeed_at_path
        self.attempts: list[str] = []
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    def load_extension(self, path: str) -> None:
        self.attempts.append(path)
        if path != self.succeed_at_path:
            raise sqlite3.OperationalError(f"can't load {path}")

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> _RecordingConn:
        self.executed.append((sql, params))
        return self


# ---------------------------------------------------------------------------
# _load_mod_spatialite
# ---------------------------------------------------------------------------


def test_load_mod_spatialite_succeeds_on_first_path():
    conn = _RecordingConn(succeed_at_path=geometry._MOD_SPATIALITE_CANDIDATES[0])
    geometry._load_mod_spatialite(_as_conn(conn))
    # First candidate worked, no further attempts.
    assert conn.attempts == [geometry._MOD_SPATIALITE_CANDIDATES[0]]


def test_load_mod_spatialite_falls_through_to_later_candidate():
    """If earlier paths fail, the loader keeps trying later ones."""
    conn = _RecordingConn(succeed_at_path=geometry._MOD_SPATIALITE_CANDIDATES[2])
    geometry._load_mod_spatialite(_as_conn(conn))
    # Tried the first 3 in order.
    assert conn.attempts == list(geometry._MOD_SPATIALITE_CANDIDATES[:3])


def test_load_mod_spatialite_raises_runtime_error_when_all_paths_fail():
    """Exhaust every candidate -> RuntimeError mentioning all of them. The
    last sqlite3.OperationalError is exposed via __cause__."""
    conn = _RecordingConn(succeed_at_path=None)
    with pytest.raises(RuntimeError, match="could not load mod_spatialite"):
        geometry._load_mod_spatialite(_as_conn(conn))
    assert conn.attempts == list(geometry._MOD_SPATIALITE_CANDIDATES)


def test_load_mod_spatialite_message_lists_all_candidates():
    conn = _RecordingConn(succeed_at_path=None)
    with pytest.raises(RuntimeError) as exc_info:
        geometry._load_mod_spatialite(_as_conn(conn))
    msg = str(exc_info.value)
    for candidate in geometry._MOD_SPATIALITE_CANDIDATES:
        assert candidate in msg


# ---------------------------------------------------------------------------
# _populate_point_geometry
# ---------------------------------------------------------------------------


class _PopulationConn:
    """Records SQL strings + reports rowcount for the UPDATE call.

    Mimics just enough of sqlite3.Connection for _populate_point_geometry,
    _populate_joined_point_geometry, and _ensure_join_indexes:
    - PRAGMA table_info(...) returns rows shaped (cid, name, type, notnull, default, pk).
    - SELECT FROM sqlite_master returns one row when `tables_seen` includes the
      argument (used by _table_exists).
    - UPDATE returns the configured rowcount.
    """

    def __init__(
        self,
        update_rowcount: int,
        columns: tuple[str, ...] = (),
        tables_seen: tuple[str, ...] = (),
    ) -> None:
        self.update_rowcount = update_rowcount
        self._columns = columns
        self._tables_seen = set(tables_seen)
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> _PopulationCursor:
        self.calls.append((sql, params))
        if sql.startswith("PRAGMA table_info"):
            return _PopulationCursor.with_rows(
                [(i, name, "TEXT", 0, None, 0) for i, name in enumerate(self._columns)]
            )
        if "sqlite_master" in sql and "name=?" in sql:
            (name,) = params
            return _PopulationCursor(row=(name,) if name in self._tables_seen else None)
        if sql.startswith("UPDATE"):
            return _PopulationCursor(rowcount=self.update_rowcount)
        return _PopulationCursor(rowcount=0)


class _PopulationCursor:
    def __init__(
        self,
        rowcount: int = 0,
        rows: list[Any] | None = None,
        row: tuple[Any, ...] | None = None,
    ) -> None:
        self.rowcount = rowcount
        self._rows = rows or []
        self._row = row

    @classmethod
    def with_rows(cls, rows: list[Any]) -> _PopulationCursor:
        return cls(rows=rows)

    def fetchall(self) -> list[Any]:
        return self._rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


def test_populate_point_geometry_calls_add_geometry_then_update():
    conn = _PopulationConn(update_rowcount=42, columns=("LONG_DECIMAL", "LAT_DECIMAL", "OTHER"))
    g = geometry.PointGeom(
        table="APT_BASE",
        geom_column="geometry",
        lon_column="LONG_DECIMAL",
        lat_column="LAT_DECIMAL",
    )

    n = geometry._populate_point_geometry(_as_conn(conn), g)

    assert n == 42
    # Three SQL calls: PRAGMA (column existence check), AddGeometryColumn, UPDATE.
    assert len(conn.calls) == 3
    pragma_sql, _ = conn.calls[0]
    add_sql, add_params = conn.calls[1]
    update_sql, _ = conn.calls[2]

    assert pragma_sql.startswith("PRAGMA table_info")
    assert "AddGeometryColumn" in add_sql
    assert add_params == ("APT_BASE", "geometry")
    assert update_sql.startswith("UPDATE")
    assert "APT_BASE" in update_sql
    assert "LONG_DECIMAL" in update_sql
    assert "LAT_DECIMAL" in update_sql
    # The WHERE clause filters out blanks/NULLs (regression: blank coords
    # should not produce a (0,0) point).
    assert "IS NOT NULL" in update_sql
    assert '<> ""' in update_sql


def test_populate_point_geometry_returns_zero_when_no_rows_updated():
    conn = _PopulationConn(update_rowcount=0, columns=("LON", "LAT"))
    g = geometry.PointGeom("T", "geometry", "LON", "LAT")
    assert geometry._populate_point_geometry(_as_conn(conn), g) == 0


def test_populate_point_geometry_raises_when_source_columns_missing():
    """Regression: SQLite silently treats unknown double-quoted identifiers as
    string literals, so without the PRAGMA-based column check the UPDATE would
    produce all-zero POINT(0 0) rows. The validation should fail loudly."""
    conn = _PopulationConn(update_rowcount=0, columns=("ARPT_ID", "FACILITY_NAME"))
    g = geometry.PointGeom("T", "geometry", "LONG_DECIMAL", "LAT_DECIMAL")
    with pytest.raises(ValueError, match="missing source columns"):
        geometry._populate_point_geometry(_as_conn(conn), g)


# ---------------------------------------------------------------------------
# _populate_joined_point_geometry / _populate_runway_lines / _ensure_join_indexes
# ---------------------------------------------------------------------------


class _JoinedConn:
    """Like _PopulationConn but supports a per-table column map -- the
    `_populate_joined_point_geometry` helper PRAGMAs both `table` and
    `join_table` to validate keys, so we need to return different column
    sets for each."""

    def __init__(self, columns_per_table: dict[str, tuple[str, ...]]) -> None:
        self._columns_per_table = columns_per_table
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> _PopulationCursor:
        self.calls.append((sql, params))
        if sql.startswith("PRAGMA table_info"):
            # PRAGMA table_info("X") -- extract X.
            table = sql.split('"')[1]
            cols = self._columns_per_table.get(table, ())
            return _PopulationCursor.with_rows(
                [(i, name, "TEXT", 0, None, 0) for i, name in enumerate(cols)]
            )
        if sql.startswith("UPDATE"):
            return _PopulationCursor(rowcount=0)
        return _PopulationCursor(rowcount=0)


def test_populate_joined_point_geometry_validates_self_key():
    conn = _PopulationConn(update_rowcount=0, columns=("OTHER",))
    jg = geometry.JoinedPointGeom(
        table="ATC_BASE",
        geom_column="geometry",
        join_table="APT_BASE",
        join_geom_column="geometry",
        self_key="SITE_NO",
        other_key="SITE_NO",
    )
    with pytest.raises(ValueError, match="missing key column 'SITE_NO'"):
        geometry._populate_joined_point_geometry(_as_conn(conn), jg)


def test_populate_joined_point_geometry_validates_join_table_columns():
    """The join target's key + geom column are also validated -- if either is
    missing on join_table, raise."""
    conn = _JoinedConn(
        columns_per_table={
            "ATC_BASE": ("SITE_NO",),
            "APT_BASE": ("SITE_NO",),  # missing 'geometry' column
        }
    )
    jg = geometry.JoinedPointGeom(
        table="ATC_BASE",
        geom_column="geometry",
        join_table="APT_BASE",
        join_geom_column="geometry",
        self_key="SITE_NO",
        other_key="SITE_NO",
    )
    with pytest.raises(ValueError, match=r"APT_BASE is missing"):
        geometry._populate_joined_point_geometry(_as_conn(conn), jg)


def test_populate_joined_point_geometry_runs_full_query_when_valid():
    """Both keys + join geom present -> AddGeometryColumn + UPDATE-with-subquery
    runs without raising."""
    conn = _JoinedConn(
        columns_per_table={
            "ATC_BASE": ("SITE_NO",),
            "APT_BASE": ("SITE_NO", "geometry"),
        }
    )
    jg = geometry.JoinedPointGeom(
        table="ATC_BASE",
        geom_column="geometry",
        join_table="APT_BASE",
        join_geom_column="geometry",
        self_key="SITE_NO",
        other_key="SITE_NO",
    )
    geometry._populate_joined_point_geometry(_as_conn(conn), jg)

    add_calls = [c for c in conn.calls if "AddGeometryColumn" in c[0]]
    update_calls = [c for c in conn.calls if c[0].lstrip().startswith("UPDATE")]
    assert len(add_calls) == 1
    assert add_calls[0][1] == ("ATC_BASE", "geometry")
    assert len(update_calls) == 1
    assert "ATC_BASE" in update_calls[0][0]
    assert "APT_BASE" in update_calls[0][0]


def test_populate_runway_lines_validates_required_columns():
    """If APT_RWY_END is missing RWY_END_ID/LAT_DECIMAL/LONG_DECIMAL, raise."""
    conn = _JoinedConn(
        columns_per_table={
            "APT_RWY": ("SITE_NO", "RWY_ID"),
            "APT_RWY_END": ("SITE_NO", "RWY_ID"),  # missing RWY_END_ID etc.
        }
    )
    with pytest.raises(ValueError, match="APT_RWY_END missing required column"):
        geometry._populate_runway_lines(_as_conn(conn), "runway_geometry")


def test_populate_runway_lines_validates_shared_keys():
    """SITE_NO/RWY_ID must exist in both APT_RWY and APT_RWY_END."""
    conn = _JoinedConn(
        columns_per_table={
            "APT_RWY": ("SITE_NO",),  # missing RWY_ID
            "APT_RWY_END": (
                "SITE_NO",
                "RWY_ID",
                "RWY_END_ID",
                "LAT_DECIMAL",
                "LONG_DECIMAL",
            ),
        }
    )
    with pytest.raises(ValueError, match="missing key column 'RWY_ID'"):
        geometry._populate_runway_lines(_as_conn(conn), "runway_geometry")


class _LookupSegConn:
    """Mock connection for the lookup-segment / self-segment / point-lookup
    helpers. Tracks `tables_seen` (return rows from sqlite_master), per-table
    column maps, and records every SQL call."""

    def __init__(
        self,
        *,
        tables_seen: tuple[str, ...] = (),
        columns_per_table: dict[str, tuple[str, ...]] | None = None,
    ) -> None:
        self._tables_seen = set(tables_seen)
        self._columns_per_table = columns_per_table or {}
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> _PopulationCursor:
        self.calls.append((sql, params))
        if sql.startswith("PRAGMA table_info"):
            table = sql.split('"')[1]
            cols = self._columns_per_table.get(table, ())
            return _PopulationCursor.with_rows(
                [(i, name, "TEXT", 0, None, 0) for i, name in enumerate(cols)]
            )
        if "sqlite_master" in sql and "name=?" in sql:
            (name,) = params
            return _PopulationCursor(row=(name,) if name in self._tables_seen else None)
        if sql.startswith("UPDATE"):
            return _PopulationCursor(rowcount=42)
        return _PopulationCursor(rowcount=0)


# ---------------------------------------------------------------------------
# _ensure_point_lookup
# ---------------------------------------------------------------------------


def test_ensure_point_lookup_unions_present_tables():
    """Builds a UNION over whichever of FIX_BASE/NAV_BASE/APT_BASE exist."""
    conn = _LookupSegConn(tables_seen=("FIX_BASE", "NAV_BASE", "APT_BASE"))
    geometry._ensure_point_lookup(_as_conn(conn))
    create_calls = [c[0] for c in conn.calls if c[0].startswith("CREATE TEMP TABLE")]
    assert len(create_calls) == 1
    sql = create_calls[0]
    assert "FIX_BASE" in sql and "NAV_BASE" in sql and "APT_BASE" in sql
    # And an index on the name column.
    assert any("CREATE INDEX point_lookup_name_idx" in c[0] for c in conn.calls)


def test_ensure_point_lookup_skips_missing_tables():
    """If only FIX_BASE exists, the UNION is just that one source."""
    conn = _LookupSegConn(tables_seen=("FIX_BASE",))
    geometry._ensure_point_lookup(_as_conn(conn))
    create_calls = [c[0] for c in conn.calls if c[0].startswith("CREATE TEMP TABLE")]
    assert len(create_calls) == 1
    sql = create_calls[0]
    assert "FIX_BASE" in sql
    assert "NAV_BASE" not in sql
    assert "APT_BASE" not in sql


def test_ensure_point_lookup_no_op_when_no_sources():
    """If none of FIX/NAV/APT exist, no table is created."""
    conn = _LookupSegConn(tables_seen=())
    geometry._ensure_point_lookup(_as_conn(conn))
    create_calls = [c[0] for c in conn.calls if c[0].startswith("CREATE TEMP TABLE")]
    assert create_calls == []


# ---------------------------------------------------------------------------
# _populate_lookup_segment_lines
# ---------------------------------------------------------------------------


def test_populate_lookup_segment_lines_validates_columns():
    conn = _LookupSegConn(columns_per_table={"AWY_SEG_ALT": ("OTHER",)})
    lsg = geometry.LookupSegmentLineGeom(
        "AWY_SEG_ALT", "segment_geometry", "FROM_POINT", "TO_POINT"
    )
    with pytest.raises(ValueError, match="missing source columns"):
        geometry._populate_lookup_segment_lines(_as_conn(conn), lsg)


def test_populate_lookup_segment_lines_runs_when_columns_present():
    conn = _LookupSegConn(columns_per_table={"AWY_SEG_ALT": ("FROM_POINT", "TO_POINT")})
    lsg = geometry.LookupSegmentLineGeom(
        "AWY_SEG_ALT", "segment_geometry", "FROM_POINT", "TO_POINT"
    )
    geometry._populate_lookup_segment_lines(_as_conn(conn), lsg)

    add_calls = [c for c in conn.calls if "AddGeometryColumn" in c[0]]
    update_calls = [c for c in conn.calls if c[0].startswith("UPDATE")]
    assert len(add_calls) == 1
    assert "LINESTRING" in add_calls[0][0]
    assert add_calls[0][1] == ("AWY_SEG_ALT", "segment_geometry")
    assert len(update_calls) == 1
    assert "MakeLine" in update_calls[0][0]
    assert "point_lookup" in update_calls[0][0]


# ---------------------------------------------------------------------------
# _populate_self_segment_lines
# ---------------------------------------------------------------------------


def test_populate_self_segment_lines_validates_columns():
    conn = _LookupSegConn(columns_per_table={"MTR_PT": ("OTHER",)})
    ssg = geometry.SelfSegmentLineGeom(
        table="MTR_PT",
        geom_column="segment_geometry",
        next_id_column="NEXT_ROUTE_PT_ID",
        other_id_column="ROUTE_PT_ID",
        group_columns=("ROUTE_TYPE_CODE", "ROUTE_ID"),
    )
    with pytest.raises(ValueError, match="missing source columns"):
        geometry._populate_self_segment_lines(_as_conn(conn), ssg)


def test_populate_self_segment_lines_runs_when_columns_present():
    conn = _LookupSegConn(
        columns_per_table={
            "MTR_PT": (
                "ROUTE_TYPE_CODE",
                "ROUTE_ID",
                "ROUTE_PT_ID",
                "NEXT_ROUTE_PT_ID",
                "geometry",
            )
        }
    )
    ssg = geometry.SelfSegmentLineGeom(
        table="MTR_PT",
        geom_column="segment_geometry",
        next_id_column="NEXT_ROUTE_PT_ID",
        other_id_column="ROUTE_PT_ID",
        group_columns=("ROUTE_TYPE_CODE", "ROUTE_ID"),
    )
    geometry._populate_self_segment_lines(_as_conn(conn), ssg)

    add_calls = [c for c in conn.calls if "AddGeometryColumn" in c[0]]
    index_calls = [c for c in conn.calls if "CREATE INDEX" in c[0]]
    update_calls = [c for c in conn.calls if c[0].startswith("UPDATE")]
    assert len(add_calls) == 1
    assert "LINESTRING" in add_calls[0][0]
    # An index supporting the next-point self-join is created.
    assert len(index_calls) == 1
    assert "ROUTE_TYPE_CODE" in index_calls[0][0]
    assert "ROUTE_PT_ID" in index_calls[0][0]
    # The UPDATE uses MakeLine + a self-join on the table.
    assert len(update_calls) == 1
    assert "MakeLine" in update_calls[0][0]
    assert "MTR_PT" in update_calls[0][0]


def test_populate_runway_lines_runs_when_keys_present():
    conn = _JoinedConn(
        columns_per_table={
            "APT_RWY": ("SITE_NO", "RWY_ID"),
            "APT_RWY_END": (
                "SITE_NO",
                "RWY_ID",
                "RWY_END_ID",
                "LAT_DECIMAL",
                "LONG_DECIMAL",
            ),
        }
    )
    geometry._populate_runway_lines(_as_conn(conn), "runway_geometry")

    add_calls = [c for c in conn.calls if "AddGeometryColumn" in c[0]]
    update_calls = [c for c in conn.calls if c[0].lstrip().startswith("UPDATE")]
    assert len(add_calls) == 1
    assert "LINESTRING" in add_calls[0][0]
    assert add_calls[0][1] == ("APT_RWY", "runway_geometry")
    assert len(update_calls) == 1
    assert "MakeLine" in update_calls[0][0]


def test_ensure_join_indexes_creates_index():
    conn = _PopulationConn(update_rowcount=0, columns=("SITE_NO",), tables_seen=("APT_BASE",))
    geometry._ensure_join_indexes(_as_conn(conn), [("APT_BASE", "SITE_NO")])
    create_calls = [c for c in conn.calls if c[0].startswith("CREATE INDEX")]
    assert len(create_calls) == 1
    assert "APT_BASE" in create_calls[0][0]
    assert "SITE_NO" in create_calls[0][0]


def test_ensure_join_indexes_skips_when_column_missing():
    conn = _PopulationConn(update_rowcount=0, columns=("OTHER",), tables_seen=("APT_BASE",))
    geometry._ensure_join_indexes(_as_conn(conn), [("APT_BASE", "MISSING_COL")])
    create_calls = [c for c in conn.calls if c[0].startswith("CREATE INDEX")]
    assert create_calls == []


def test_ensure_join_indexes_skips_when_table_missing():
    conn = _PopulationConn(update_rowcount=0, columns=("SITE_NO",), tables_seen=())
    geometry._ensure_join_indexes(_as_conn(conn), [("APT_BASE", "SITE_NO")])
    create_calls = [c for c in conn.calls if c[0].startswith("CREATE INDEX")]
    assert create_calls == []


# ---------------------------------------------------------------------------
# build() -- top-level error path
# ---------------------------------------------------------------------------


def test_build_raises_filenotfound_for_missing_db(tmp_path):
    """build() refuses to operate on a path that doesn't exist (rather than
    silently creating a fresh -- and empty -- spatialite DB)."""
    with pytest.raises(FileNotFoundError, match="database not found"):
        geometry.build(db_path=tmp_path / "does_not_exist.sqlite")


# ---------------------------------------------------------------------------
# build() orchestration -- mocks the small helpers rather than every SQL call.
# Each helper is exhaustively tested above / in test_geometry_helpers; here we
# verify the orchestration logic on top of trustworthy helpers.
# ---------------------------------------------------------------------------


class _RecordingExecConn:
    """Records every SQL call. PRAGMA / SELECT queries return empty cursors;
    UPDATE statements report a configurable rowcount."""

    def __init__(self, update_rowcount: int = 0) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.committed = False
        self.closed = False
        self._update_rowcount = update_rowcount

    def enable_load_extension(self, _enabled: bool) -> None:
        pass

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> _PopulationCursor:
        self.executed.append((sql, params))
        if sql.startswith("UPDATE"):
            return _PopulationCursor(rowcount=self._update_rowcount)
        return _PopulationCursor.with_rows([])

    def commit(self) -> None:
        self.committed = True

    def close(self) -> None:
        self.closed = True


def _patch_geometry_internals(
    monkeypatch: pytest.MonkeyPatch,
    conn: _RecordingExecConn,
    *,
    spatialite_init: bool = True,
    existing_point_geoms: list[geometry.PointGeom] | None = None,
    already_geometric_keys: set[tuple[str, str]] | None = None,
    existing_joined_geoms: list[geometry.JoinedPointGeom] | None = None,
    tables_present: set[str] | None = None,
    apt_rwy_present: bool = False,
    indexed_columns: set[tuple[str, str]] | None = None,
    all_geom_columns: list[tuple[str, str]] | None = None,
) -> None:
    """Stub all geometry helpers so tests can focus on orchestration."""
    monkeypatch.setattr(geometry.sqlite3, "connect", lambda _path: conn)
    monkeypatch.setattr(geometry, "_load_mod_spatialite", lambda _c: None)
    monkeypatch.setattr(geometry, "_spatialite_initialized", lambda _c: spatialite_init)
    monkeypatch.setattr(geometry, "_existing_tables", lambda _g, _c: existing_point_geoms or [])
    monkeypatch.setattr(
        geometry, "_existing_joined_geoms", lambda _g, _c: existing_joined_geoms or []
    )
    monkeypatch.setattr(
        geometry,
        "_already_geometric",
        lambda _c, g: (g.table, g.geom_column) in (already_geometric_keys or set()),
    )
    monkeypatch.setattr(
        geometry,
        "_joined_geom_already_present",
        lambda _c, g: (g.table, g.geom_column) in (already_geometric_keys or set()),
    )
    seen_tables = (tables_present or set()) | (
        {"APT_RWY", "APT_RWY_END"} if apt_rwy_present else set()
    )
    monkeypatch.setattr(geometry, "_table_exists", lambda _c, t: t in seen_tables)
    monkeypatch.setattr(
        geometry,
        "_column_already_registered",
        lambda _c, t, col: (t, col) in (already_geometric_keys or set()),
    )
    # Default no-op populators; tests can override individually.
    monkeypatch.setattr(geometry, "_populate_point_geometry", lambda _c, _g: 0)
    monkeypatch.setattr(geometry, "_populate_joined_point_geometry", lambda _c, _g: 0)
    monkeypatch.setattr(geometry, "_populate_runway_lines", lambda _c, _col: 0)
    monkeypatch.setattr(geometry, "_populate_lookup_segment_lines", lambda _c, _g: 0)
    monkeypatch.setattr(geometry, "_populate_self_segment_lines", lambda _c, _g: 0)
    monkeypatch.setattr(geometry, "_ensure_point_lookup", lambda _c: None)
    monkeypatch.setattr(geometry, "_ensure_join_indexes", lambda _c, indexes: None)
    monkeypatch.setattr(geometry, "_all_geom_columns", lambda _c: all_geom_columns or [])
    monkeypatch.setattr(
        geometry,
        "_spatial_index_exists",
        lambda _c, t, col: (t, col) in (indexed_columns or set()),
    )


def test_build_skips_tables_already_geometric(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """Idempotency: PointGeoms whose column is already registered are skipped."""
    conn = _RecordingExecConn()
    apt_geom = geometry.PointGeom("APT_BASE", "geometry", "LONG_DECIMAL", "LAT_DECIMAL")
    awos_geom = geometry.PointGeom("AWOS", "geometry", "LONG_DECIMAL", "LAT_DECIMAL")
    populate_calls: list[geometry.PointGeom] = []

    def fake_populate(_c: object, g: geometry.PointGeom) -> int:
        populate_calls.append(g)
        return 5

    _patch_geometry_internals(
        monkeypatch,
        conn,
        existing_point_geoms=[apt_geom, awos_geom],
        already_geometric_keys={("APT_BASE", "geometry")},
    )
    monkeypatch.setattr(geometry, "_populate_point_geometry", fake_populate)
    db = tmp_path / "nasr.sqlite"
    db.write_text("")

    geometry.build(db_path=db)

    assert [g.table for g in populate_calls] == ["AWOS"]
    assert conn.committed and conn.closed


def test_build_skips_existing_spatial_indexes(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """A registered geometry whose R-tree backing table already exists doesn't
    get a CreateSpatialIndex call."""
    conn = _RecordingExecConn()
    _patch_geometry_internals(
        monkeypatch,
        conn,
        all_geom_columns=[("AWOS", "geometry")],
        indexed_columns={("AWOS", "geometry")},
    )
    db = tmp_path / "nasr.sqlite"
    db.write_text("")

    geometry.build(db_path=db)

    create_idx_calls = [params for sql, params in conn.executed if "CreateSpatialIndex" in sql]
    assert create_idx_calls == []


def test_build_runs_spatial_index_for_unindexed_columns(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """Pass 4 builds CreateSpatialIndex for every registered column without one."""
    conn = _RecordingExecConn()
    _patch_geometry_internals(
        monkeypatch,
        conn,
        all_geom_columns=[("APT_BASE", "geometry"), ("AWOS", "geometry")],
        indexed_columns=set(),
    )
    db = tmp_path / "nasr.sqlite"
    db.write_text("")

    geometry.build(db_path=db)

    create_idx_calls = sorted(
        params for sql, params in conn.executed if "CreateSpatialIndex" in sql
    )
    assert create_idx_calls == [("APT_BASE", "geometry"), ("AWOS", "geometry")]


def test_build_initialises_spatial_metadata_when_missing(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """If geometry_columns doesn't exist yet, build() runs InitSpatialMetadata."""
    conn = _RecordingExecConn()
    _patch_geometry_internals(monkeypatch, conn, spatialite_init=False)
    db = tmp_path / "nasr.sqlite"
    db.write_text("")

    geometry.build(db_path=db)

    init_calls = [sql for sql, _ in conn.executed if "InitSpatialMetadata" in sql]
    assert init_calls == ["SELECT InitSpatialMetadata(1)"]


def test_build_does_not_re_init_spatial_metadata(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """If geometry_columns already exists, InitSpatialMetadata isn't re-run."""
    conn = _RecordingExecConn()
    _patch_geometry_internals(monkeypatch, conn, spatialite_init=True)
    db = tmp_path / "nasr.sqlite"
    db.write_text("")

    geometry.build(db_path=db)

    init_calls = [sql for sql, _ in conn.executed if "InitSpatialMetadata" in sql]
    assert init_calls == []


def test_build_populates_joined_point_geoms(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """Pass 2 calls _populate_joined_point_geometry for each existing
    JoinedPointGeom that's not already registered."""
    conn = _RecordingExecConn()
    atc_jg = geometry.JoinedPointGeom(
        "ATC_BASE", "geometry", "APT_BASE", "geometry", "SITE_NO", "SITE_NO"
    )
    populate_calls: list[geometry.JoinedPointGeom] = []

    def fake_populate(_c: object, jg: geometry.JoinedPointGeom) -> int:
        populate_calls.append(jg)
        return 7

    _patch_geometry_internals(
        monkeypatch,
        conn,
        existing_joined_geoms=[atc_jg],
    )
    monkeypatch.setattr(geometry, "_populate_joined_point_geometry", fake_populate)
    db = tmp_path / "nasr.sqlite"
    db.write_text("")

    geometry.build(db_path=db)

    assert populate_calls == [atc_jg]


def test_build_runs_runway_lines_when_apt_rwy_tables_present(
    monkeypatch: pytest.MonkeyPatch, tmp_path
):
    """Pass 3 fires _populate_runway_lines only when both APT_RWY and
    APT_RWY_END tables exist."""
    conn = _RecordingExecConn()
    runway_calls: list[str] = []

    _patch_geometry_internals(monkeypatch, conn, apt_rwy_present=True)

    def fake_runway(_c: object, col: str) -> int:
        runway_calls.append(col)
        return 100

    monkeypatch.setattr(geometry, "_populate_runway_lines", fake_runway)
    db = tmp_path / "nasr.sqlite"
    db.write_text("")

    geometry.build(db_path=db)

    assert runway_calls == [geometry._RUNWAY_LINE_GEOM.geom_column]


def test_build_runs_lookup_segment_lines_when_fix_base_present(
    monkeypatch: pytest.MonkeyPatch, tmp_path
):
    """Pass 4 builds AWY/DP/STAR LINESTRINGs by joining to a point lookup
    table. Only fires when FIX_BASE exists (so the lookup has a source)
    and at least one of the AWY/DP/STAR tables is present."""
    conn = _RecordingExecConn()
    seg_calls: list[str] = []
    ensure_lookup_called = [False]

    def fake_ensure(_c: object) -> None:
        ensure_lookup_called[0] = True

    def fake_seg(_c: object, ls: geometry.LookupSegmentLineGeom) -> int:
        seg_calls.append(ls.table)
        return 100

    _patch_geometry_internals(
        monkeypatch,
        conn,
        tables_present={"FIX_BASE", "AWY_SEG_ALT", "DP_RTE", "STAR_RTE"},
    )
    monkeypatch.setattr(geometry, "_ensure_point_lookup", fake_ensure)
    monkeypatch.setattr(geometry, "_populate_lookup_segment_lines", fake_seg)
    db = tmp_path / "nasr.sqlite"
    db.write_text("")

    geometry.build(db_path=db)

    assert ensure_lookup_called[0] is True
    assert sorted(seg_calls) == ["AWY_SEG_ALT", "DP_RTE", "STAR_RTE"]


def test_build_skips_lookup_segments_when_no_source_tables(
    monkeypatch: pytest.MonkeyPatch, tmp_path
):
    """If neither FIX_BASE nor any of the AWY/DP/STAR tables exist, the
    lookup-segments pass is a no-op."""
    conn = _RecordingExecConn()
    seg_calls: list[str] = []

    def fake_seg(_c: object, ls: geometry.LookupSegmentLineGeom) -> int:
        seg_calls.append(ls.table)
        return 0

    _patch_geometry_internals(monkeypatch, conn, tables_present=set())
    monkeypatch.setattr(geometry, "_populate_lookup_segment_lines", fake_seg)
    db = tmp_path / "nasr.sqlite"
    db.write_text("")

    geometry.build(db_path=db)

    assert seg_calls == []


def test_build_runs_self_segment_lines_when_mtr_pt_present(
    monkeypatch: pytest.MonkeyPatch, tmp_path
):
    """Pass 5 builds MTR_PT self-segments only when the table exists."""
    conn = _RecordingExecConn()
    seg_calls: list[str] = []

    def fake_seg(_c: object, ss: geometry.SelfSegmentLineGeom) -> int:
        seg_calls.append(ss.table)
        return 50

    _patch_geometry_internals(monkeypatch, conn, tables_present={"MTR_PT"})
    monkeypatch.setattr(geometry, "_populate_self_segment_lines", fake_seg)
    db = tmp_path / "nasr.sqlite"
    db.write_text("")

    geometry.build(db_path=db)

    assert seg_calls == ["MTR_PT"]


def test_build_skips_self_segments_when_mtr_pt_missing(monkeypatch: pytest.MonkeyPatch, tmp_path):
    conn = _RecordingExecConn()
    seg_calls: list[str] = []

    def fake_seg(_c: object, ss: geometry.SelfSegmentLineGeom) -> int:
        seg_calls.append(ss.table)
        return 0

    _patch_geometry_internals(monkeypatch, conn, tables_present=set())
    monkeypatch.setattr(geometry, "_populate_self_segment_lines", fake_seg)
    db = tmp_path / "nasr.sqlite"
    db.write_text("")

    geometry.build(db_path=db)

    assert seg_calls == []


def test_build_skips_runway_lines_when_apt_rwy_missing(monkeypatch: pytest.MonkeyPatch, tmp_path):
    conn = _RecordingExecConn()
    runway_calls: list[str] = []

    _patch_geometry_internals(monkeypatch, conn, apt_rwy_present=False)

    def fake_runway(_c: object, col: str) -> int:
        runway_calls.append(col)
        return 0

    monkeypatch.setattr(geometry, "_populate_runway_lines", fake_runway)
    db = tmp_path / "nasr.sqlite"
    db.write_text("")

    geometry.build(db_path=db)

    assert runway_calls == []
