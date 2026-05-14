"""Tests for the airspace helpers that wrap pyogrio.

We mock `pyogrio.raw` to test the wrapping logic (FK row-resolution,
empty-source short-circuits, write argument plumbing) without needing
a real spatialite extension.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pyogrio.errors
import pytest

from faa_nasr import airspace


@pytest.fixture
def captured_writes(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Replace pyogrio.raw.write with a recorder. Returns a list that fills
    with one dict per write call."""
    calls: list[dict[str, Any]] = []

    def fake_write(*args: Any, **kwargs: Any) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(airspace.pyogrio.raw, "write", fake_write)
    return calls


# ---------------------------------------------------------------------------
# _copy_shapefile
# ---------------------------------------------------------------------------


def _stub_pyogrio_read(monkeypatch: pytest.MonkeyPatch, ret: Any) -> None:
    """Make pyogrio.raw.read return a fixed value (or raise the given error)."""

    def fake(*args: Any, **kwargs: Any) -> Any:
        if isinstance(ret, BaseException):
            raise ret
        return ret

    monkeypatch.setattr(airspace.pyogrio.raw, "read", fake)


def test_copy_shapefile_returns_zero_on_data_source_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path, captured_writes
):
    _stub_pyogrio_read(monkeypatch, pyogrio.errors.DataSourceError("missing shapefile"))
    n = airspace._copy_shapefile(src=tmp_path / "x.shp", dst=tmp_path / "out", layer_name="L")
    assert n == 0
    assert captured_writes == []


def test_copy_shapefile_returns_zero_on_empty_geometry(
    monkeypatch: pytest.MonkeyPatch, tmp_path, captured_writes
):
    meta = {"fields": np.array([], dtype=object), "geometry_type": "Polygon", "crs": None}
    _stub_pyogrio_read(monkeypatch, (meta, None, np.array([], dtype=object), []))
    n = airspace._copy_shapefile(src=tmp_path / "x.shp", dst=tmp_path / "out", layer_name="L")
    assert n == 0
    assert captured_writes == []


def test_copy_shapefile_writes_features_and_returns_count(
    monkeypatch: pytest.MonkeyPatch, tmp_path, captured_writes
):
    geom = np.array([b"POLYGON1", b"POLYGON2", b"POLYGON3"], dtype=object)
    field_data = [np.array(["A", "B", "C"], dtype=object)]
    meta = {
        "fields": np.array(["NAME"], dtype=object),
        "geometry_type": "Polygon",
        "crs": "EPSG:4326",
    }
    _stub_pyogrio_read(monkeypatch, (meta, None, geom, field_data))

    n = airspace._copy_shapefile(
        src=tmp_path / "x.shp", dst=tmp_path / "out.sqlite", layer_name="L"
    )

    assert n == 3
    assert len(captured_writes) == 1
    call = captured_writes[0]
    # Geometry promoted from "Polygon" to "MultiPolygon".
    assert call["geometry_type"] == "MultiPolygon"
    assert call["layer"] == "L"
    assert call["dataset_options"] == {"SPATIALITE": "YES"}


# ---------------------------------------------------------------------------
# _read_layer_source
# ---------------------------------------------------------------------------


def test_read_layer_source_returns_none_on_data_source_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path
):
    _stub_pyogrio_read(monkeypatch, pyogrio.errors.DataSourceError("can't read"))
    result = airspace._read_layer_source(xml=tmp_path / "x.xml", source_layer="Foo", fk_lookup={})
    assert result is None


def test_read_layer_source_returns_none_on_index_error(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """AIXM bundles include schema/index XML files with no readable layer --
    pyogrio raises IndexError when there's no default layer."""
    _stub_pyogrio_read(monkeypatch, IndexError("no layers"))
    assert (
        airspace._read_layer_source(xml=tmp_path / "x.xml", source_layer="Foo", fk_lookup={})
        is None
    )


def test_read_layer_source_returns_none_on_empty_source(monkeypatch: pytest.MonkeyPatch, tmp_path):
    meta = {"fields": np.array([], dtype=object), "geometry_type": None, "crs": None}
    _stub_pyogrio_read(monkeypatch, (meta, None, np.array([], dtype=object), []))
    assert (
        airspace._read_layer_source(xml=tmp_path / "x.xml", source_layer="Foo", fk_lookup={})
        is None
    )


def test_read_layer_source_resolves_per_row_fks(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """Each row's gml_id keys into fk_lookup to resolve XLink targets."""
    meta = {
        "fields": np.array(["gml_id", "name"], dtype=object),
        "geometry_type": None,
        "crs": None,
    }
    field_data = [
        np.array(["ATC1", "ATC2"], dtype=object),
        np.array(["Tower A", "Tower B"], dtype=object),
    ]
    _stub_pyogrio_read(monkeypatch, (meta, None, None, field_data))

    fk_lookup = {
        airspace.FeatureRef("ATC", "ATC1"): {"clientAirspace": "uuid-airspace-1"},
        airspace.FeatureRef("ATC", "ATC2"): {"clientAirspace": "uuid-airspace-2"},
    }
    chunk = airspace._read_layer_source(
        xml=tmp_path / "x.xml", source_layer="ATC", fk_lookup=fk_lookup
    )

    assert chunk is not None
    assert chunk.n_rows == 2
    assert chunk.xml_stem == "x"
    assert "clientAirspace" in chunk.fks
    assert list(chunk.fks["clientAirspace"]) == ["uuid-airspace-1", "uuid-airspace-2"]


def test_read_layer_source_handles_missing_fks_for_some_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path
):
    """Rows whose gml_id isn't in fk_lookup get None for that FK column."""
    meta = {
        "fields": np.array(["gml_id"], dtype=object),
        "geometry_type": None,
        "crs": None,
    }
    field_data = [np.array(["ATC1", "ATC2"], dtype=object)]
    _stub_pyogrio_read(monkeypatch, (meta, None, None, field_data))

    # Only ATC1 has a resolved FK.
    fk_lookup = {airspace.FeatureRef("ATC", "ATC1"): {"clientAirspace": "uuid-1"}}
    chunk = airspace._read_layer_source(
        xml=tmp_path / "x.xml", source_layer="ATC", fk_lookup=fk_lookup
    )

    assert chunk is not None
    assert list(chunk.fks["clientAirspace"]) == ["uuid-1", None]


def test_read_layer_source_pads_geometry_with_none_when_absent(
    monkeypatch: pytest.MonkeyPatch, tmp_path
):
    """Layers without geometry (Unit, OrgAuthority, etc.) get a None-padded
    geometry array of the same length as the field data."""
    meta = {
        "fields": np.array(["name"], dtype=object),
        "geometry_type": None,
        "crs": None,
    }
    field_data = [np.array(["a", "b", "c"], dtype=object)]
    _stub_pyogrio_read(monkeypatch, (meta, None, None, field_data))

    chunk = airspace._read_layer_source(xml=tmp_path / "x.xml", source_layer="Unit", fk_lookup={})

    assert chunk is not None
    assert len(chunk.geometry) == 3
    assert all(g is None for g in chunk.geometry)


# ---------------------------------------------------------------------------
# _write_merged_layer (spatial path)
# ---------------------------------------------------------------------------


def test_write_merged_layer_geometry_path_calls_pyogrio_write(captured_writes, tmp_path):
    merged = airspace._MergedLayer(
        geometry=np.array([b"GEOM1", b"GEOM2"], dtype=object),
        fields=["name", "_source_xml"],
        field_data=[
            np.array(["A", "B"], dtype=object),
            np.array(["XML1", "XML2"], dtype=object),
        ],
        geom_type="Polygon",
        crs="EPSG:4326",
        has_geometry=True,
    )

    airspace._write_merged_layer(dst=tmp_path / "out.sqlite", merged=merged, target_layer="MyLayer")

    assert len(captured_writes) == 1
    call = captured_writes[0]
    # Singletons get promoted to multi.
    assert call["geometry_type"] == "MultiPolygon"
    assert call["layer"] == "MyLayer"
    # Spatial index deferred -- built later in pass 3 of _build_sua.
    assert call["layer_options"] == {"SPATIAL_INDEX": "NO", "LAUNDER": "NO"}
    assert call["dataset_options"] == {"SPATIALITE": "YES"}


def test_write_merged_layer_attribute_only_path_uses_sqlite3(tmp_path):
    """When has_geometry=False, _write_merged_layer falls back to
    _write_attribute_only_table (we test that path independently)."""
    import sqlite3

    merged = airspace._MergedLayer(
        geometry=np.array([None, None], dtype=object),
        fields=["name"],
        field_data=[np.array(["A", "B"], dtype=object)],
        geom_type=None,
        crs=None,
        has_geometry=False,
    )

    dst = tmp_path / "out.sqlite"
    airspace._write_merged_layer(dst=dst, merged=merged, target_layer="MyLayer")

    conn = sqlite3.connect(dst)
    try:
        rows = conn.execute("SELECT name FROM MyLayer ORDER BY name").fetchall()
        assert rows == [("A",), ("B",)]
    finally:
        conn.close()
