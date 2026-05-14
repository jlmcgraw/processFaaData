"""Test for airspace._init_spatialite_db -- mocks sqlite3.connect."""

from __future__ import annotations

from typing import Any

import pytest

from faa_nasr import airspace


class _RecordingConn:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.committed = False
        self.closed = False
        self.load_ext_enabled = False

    def enable_load_extension(self, enabled: bool) -> None:
        self.load_ext_enabled = enabled

    def execute(self, sql: str, params: tuple = ()) -> _RecordingConn:
        self.executed.append(sql)
        return self

    def commit(self) -> None:
        self.committed = True

    def close(self) -> None:
        self.closed = True


def test_init_spatialite_db_opens_connection_and_initialises(
    monkeypatch: pytest.MonkeyPatch, tmp_path
):
    fake = _RecordingConn()
    captured_path: list[Any] = []

    def fake_connect(p: Any) -> _RecordingConn:
        captured_path.append(p)
        return fake

    monkeypatch.setattr(airspace.sqlite3, "connect", fake_connect)
    monkeypatch.setattr(airspace, "_load_mod_spatialite", lambda _conn: None)

    dst = tmp_path / "out.sqlite"
    airspace._init_spatialite_db(dst)

    assert captured_path == [dst]
    assert fake.load_ext_enabled is True
    # All four PRAGMAs / init calls fired.
    assert "PRAGMA trusted_schema = ON" in fake.executed
    assert "PRAGMA synchronous = OFF" in fake.executed
    assert "PRAGMA journal_mode = MEMORY" in fake.executed
    assert "SELECT InitSpatialMetadata(1)" in fake.executed
    assert fake.committed is True
    assert fake.closed is True


def test_init_spatialite_db_closes_connection_even_if_init_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path
):
    """The `finally: close()` should fire even when something inside raises."""
    fake = _RecordingConn()
    monkeypatch.setattr(airspace.sqlite3, "connect", lambda _p: fake)

    def boom(_conn: Any) -> None:
        raise RuntimeError("spatialite missing")

    monkeypatch.setattr(airspace, "_load_mod_spatialite", boom)

    with pytest.raises(RuntimeError, match="spatialite missing"):
        airspace._init_spatialite_db(tmp_path / "out.sqlite")

    assert fake.closed is True
