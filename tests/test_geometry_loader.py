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

    Mimics just enough of sqlite3.Connection for _populate_point_geometry.
    """

    def __init__(self, update_rowcount: int) -> None:
        self.update_rowcount = update_rowcount
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> _PopulationCursor:
        self.calls.append((sql, params))
        # The function uses .rowcount only on the UPDATE result.
        if sql.startswith("UPDATE"):
            return _PopulationCursor(rowcount=self.update_rowcount)
        return _PopulationCursor(rowcount=0)


class _PopulationCursor:
    def __init__(self, rowcount: int) -> None:
        self.rowcount = rowcount


def test_populate_point_geometry_calls_add_geometry_then_update():
    conn = _PopulationConn(update_rowcount=42)
    g = geometry.PointGeom(
        table="APT_BASE",
        geom_column="geometry",
        lon_column="LONG_DECIMAL",
        lat_column="LAT_DECIMAL",
    )

    n = geometry._populate_point_geometry(_as_conn(conn), g)

    assert n == 42
    # Two SQL calls: AddGeometryColumn (parametrized), then UPDATE (built dynamically).
    assert len(conn.calls) == 2
    add_sql, add_params = conn.calls[0]
    update_sql, _ = conn.calls[1]

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
    conn = _PopulationConn(update_rowcount=0)
    g = geometry.PointGeom("T", "geometry", "LON", "LAT")
    assert geometry._populate_point_geometry(_as_conn(conn), g) == 0


# ---------------------------------------------------------------------------
# build() -- top-level error path (file missing)
# ---------------------------------------------------------------------------


def test_build_raises_filenotfound_for_missing_db(tmp_path):
    """build() refuses to operate on a path that doesn't exist (rather than
    silently creating a fresh -- and empty -- spatialite DB)."""
    with pytest.raises(FileNotFoundError, match="database not found"):
        geometry.build(db_path=tmp_path / "does_not_exist.sqlite")


# ---------------------------------------------------------------------------
# build() orchestration -- mocks sqlite3.connect so we don't need spatialite
# ---------------------------------------------------------------------------


class _OrchestrationConn:
    """sqlite3.Connection stand-in that records executes and reports configurable
    sqlite_master / geometry_columns rows."""

    def __init__(
        self,
        *,
        existing_user_tables: set[str],
        already_geometric: set[str],
        existing_indexes: set[str],
        update_rowcount: int,
    ) -> None:
        self.executed: list[tuple[str, tuple]] = []
        self.committed = False
        self.closed = False
        self._user_tables = existing_user_tables
        self._already_geometric = already_geometric
        self._existing_indexes = existing_indexes
        self._update_rowcount = update_rowcount

    def enable_load_extension(self, _: bool) -> None:
        pass

    def execute(self, sql: str, params: tuple = ()) -> _OrchestrationCursor:
        self.executed.append((sql, params))
        # Route _existing_tables's SELECT FROM sqlite_master query.
        if "sqlite_master" in sql and "type='table'" in sql and "name=?" in sql:
            (name,) = params
            return (
                _OrchestrationCursor.with_row(name)
                if name in self._user_tables
                else _OrchestrationCursor.empty()
            )
        # Route _spatialite_initialized's check.
        if "geometry_columns" in sql and "sqlite_master" in sql:
            return _OrchestrationCursor.with_row("geometry_columns")  # pretend already-init
        # Route _already_geometric's geometry_columns lookup.
        if "geometry_columns" in sql and "f_table_name" in sql:
            (table, _column) = params
            if table.lower() in {n.lower() for n in self._already_geometric}:
                return _OrchestrationCursor.with_row(1)
            return _OrchestrationCursor.empty()
        # Route _has_spatial_index's idx_<table>_<col> lookup.
        if "sqlite_master" in sql and "LOWER(name)" in sql:
            (idx_name,) = params
            if idx_name in self._existing_indexes:
                return _OrchestrationCursor.with_row(idx_name)
            return _OrchestrationCursor.empty()
        # The bulk UPDATE returns rowcount.
        if sql.startswith("UPDATE"):
            return _OrchestrationCursor(rowcount=self._update_rowcount)
        return _OrchestrationCursor.empty()

    def commit(self) -> None:
        self.committed = True

    def close(self) -> None:
        self.closed = True


class _OrchestrationCursor:
    def __init__(self, *, row: tuple | None = None, rowcount: int = 0) -> None:
        self._row = row
        self.rowcount = rowcount

    @classmethod
    def with_row(cls, *values: object) -> _OrchestrationCursor:
        return cls(row=tuple(values))

    @classmethod
    def empty(cls) -> _OrchestrationCursor:
        return cls(row=None)

    def fetchone(self) -> tuple | None:
        return self._row


def test_build_skips_tables_already_geometric(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """Idempotency: tables whose geometry column is already registered in
    geometry_columns are skipped entirely (no AddGeometryColumn / UPDATE).
    """
    # APT_BASE exists and is already geometric; AWOS exists but isn't.
    fake = _OrchestrationConn(
        existing_user_tables={"APT_BASE", "AWOS"},
        already_geometric={"APT_BASE"},
        existing_indexes=set(),
        update_rowcount=10,
    )
    monkeypatch.setattr(geometry.sqlite3, "connect", lambda _path: fake)
    monkeypatch.setattr(geometry, "_load_mod_spatialite", lambda _conn: None)
    db = tmp_path / "nasr.sqlite"
    db.write_text("")  # exists()

    geometry.build(db_path=db)

    add_geom_calls = [params for sql, params in fake.executed if "AddGeometryColumn" in sql]
    create_idx_calls = [params for sql, params in fake.executed if "CreateSpatialIndex" in sql]
    # Only AWOS (not already geometric) gets AddGeometryColumn.
    assert add_geom_calls == [("AWOS", "geometry")]
    # Both AWOS and APT_BASE get CreateSpatialIndex (since neither has an
    # existing rtree backing table in this scenario).
    indexed_tables = sorted(t for t, _ in create_idx_calls)
    assert indexed_tables == ["APT_BASE", "AWOS"]
    assert fake.committed and fake.closed


def test_build_skips_existing_spatial_indexes(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """If a table's rtree backing table (idx_<t>_<c>) already exists,
    CreateSpatialIndex is not re-run for it."""
    fake = _OrchestrationConn(
        existing_user_tables={"AWOS"},
        already_geometric=set(),
        existing_indexes={"idx_AWOS_geometry"},  # already indexed
        update_rowcount=5,
    )
    monkeypatch.setattr(geometry.sqlite3, "connect", lambda _path: fake)
    monkeypatch.setattr(geometry, "_load_mod_spatialite", lambda _conn: None)
    db = tmp_path / "nasr.sqlite"
    db.write_text("")

    geometry.build(db_path=db)

    create_idx_calls = [params for sql, params in fake.executed if "CreateSpatialIndex" in sql]
    # No CreateSpatialIndex call -- the existing one wins.
    assert create_idx_calls == []


def test_build_runs_full_pipeline_for_fresh_db(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """All tables present, none already geometric, no existing indexes:
    AddGeometryColumn + UPDATE + CreateSpatialIndex run for each."""
    tables_present = {"APT_BASE", "AWOS", "FIX_BASE"}
    fake = _OrchestrationConn(
        existing_user_tables=tables_present,
        already_geometric=set(),
        existing_indexes=set(),
        update_rowcount=100,
    )
    monkeypatch.setattr(geometry.sqlite3, "connect", lambda _path: fake)
    monkeypatch.setattr(geometry, "_load_mod_spatialite", lambda _conn: None)
    db = tmp_path / "nasr.sqlite"
    db.write_text("")

    geometry.build(db_path=db)

    add_geom_tables = sorted(
        t for sql, (t, _c) in [(s, p) for s, p in fake.executed if "AddGeometryColumn" in s]
    )
    update_count = sum(1 for sql, _ in fake.executed if sql.startswith("UPDATE"))
    create_idx_tables = sorted(
        t for sql, (t, _c) in [(s, p) for s, p in fake.executed if "CreateSpatialIndex" in s]
    )

    assert add_geom_tables == sorted(tables_present)
    assert update_count == len(tables_present)
    assert create_idx_tables == sorted(tables_present)


def test_build_initialises_spatial_metadata_when_missing(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """If geometry_columns doesn't exist yet, build() runs InitSpatialMetadata
    once before doing anything else."""

    class _UninitConn(_OrchestrationConn):
        def execute(self, sql: str, params: tuple = ()) -> _OrchestrationCursor:
            self.executed.append((sql, params))
            # Override _spatialite_initialized's check: pretend not initialised.
            if "geometry_columns" in sql and "sqlite_master" in sql:
                return _OrchestrationCursor.empty()
            return (
                super().execute.__wrapped__(self, sql, params)
                if False
                else _OrchestrationCursor.empty()
            )  # type: ignore[no-any-return]

    fake = _UninitConn(
        existing_user_tables=set(),
        already_geometric=set(),
        existing_indexes=set(),
        update_rowcount=0,
    )
    monkeypatch.setattr(geometry.sqlite3, "connect", lambda _path: fake)
    monkeypatch.setattr(geometry, "_load_mod_spatialite", lambda _conn: None)
    db = tmp_path / "nasr.sqlite"
    db.write_text("")

    geometry.build(db_path=db)

    init_calls = [sql for sql, _ in fake.executed if "InitSpatialMetadata" in sql]
    assert init_calls == ["SELECT InitSpatialMetadata(1)"]


def test_build_does_not_re_init_spatial_metadata(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """If geometry_columns already exists (the DB was previously initialised),
    InitSpatialMetadata isn't re-run -- it's an expensive call."""
    fake = _OrchestrationConn(
        existing_user_tables=set(),
        already_geometric=set(),
        existing_indexes=set(),
        update_rowcount=0,
    )
    monkeypatch.setattr(geometry.sqlite3, "connect", lambda _path: fake)
    monkeypatch.setattr(geometry, "_load_mod_spatialite", lambda _conn: None)
    db = tmp_path / "nasr.sqlite"
    db.write_text("")

    geometry.build(db_path=db)

    init_calls = [sql for sql, _ in fake.executed if "InitSpatialMetadata" in sql]
    # Our _OrchestrationConn pretends geometry_columns already exists, so
    # _spatialite_initialized() returns True and the InitSpatialMetadata call
    # is skipped.
    assert init_calls == []
