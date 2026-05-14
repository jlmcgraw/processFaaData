"""Tests for the _build_class_airspace / _build_sua orchestrators.

Both call into pyogrio + spatialite under the hood; we mock those layers
and verify the orchestration logic (file discovery, dispatch, ordering).
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from faa_nasr import airspace

# ---------------------------------------------------------------------------
# _build_class_airspace
# ---------------------------------------------------------------------------


def test_build_class_airspace_processes_each_shp(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """Iterates *.shp files in Additional_Data/Shape_Files/ and copies each."""
    shape_dir = tmp_path / "Additional_Data" / "Shape_Files"
    shape_dir.mkdir(parents=True)
    (shape_dir / "Class_Airspace.shp").touch()
    (shape_dir / "Other_Airspace.shp").touch()

    copied: list[tuple[Path, str]] = []

    def fake_copy(src: Path, dst: Path, layer_name: str) -> int:
        copied.append((src, layer_name))
        return 100

    monkeypatch.setattr(airspace, "_copy_shapefile", fake_copy)

    airspace._build_class_airspace(nasr_dir=tmp_path, dst=tmp_path / "out.sqlite")

    # Both shapefiles processed, with safe-name'd layer names from each stem.
    layer_names = sorted(name for _, name in copied)
    assert layer_names == ["Class_Airspace", "Other_Airspace"]


def test_build_class_airspace_unlinks_existing_dst_first(monkeypatch: pytest.MonkeyPatch, tmp_path):
    shape_dir = tmp_path / "Additional_Data" / "Shape_Files"
    shape_dir.mkdir(parents=True)
    (shape_dir / "Class_Airspace.shp").touch()

    dst = tmp_path / "out.sqlite"
    dst.write_text("stale leftover content")

    saw_dst_at_call_time: list[bool] = []

    def fake_copy(src: Path, dst: Path, layer_name: str) -> int:
        saw_dst_at_call_time.append(dst.exists())
        return 0

    monkeypatch.setattr(airspace, "_copy_shapefile", fake_copy)

    airspace._build_class_airspace(nasr_dir=tmp_path, dst=dst)

    # The dst was deleted before we started copying.
    assert saw_dst_at_call_time == [False]


# ---------------------------------------------------------------------------
# _build_sua
# ---------------------------------------------------------------------------


def _make_minimal_saa_layout(tmp_path: Path) -> Path:
    """Create the directory layout _build_sua expects, with a no-op SAA zip."""
    saa_dir = tmp_path / "Additional_Data" / "AIXM" / "SAA-AIXM_5_Schema"
    saa_dir.mkdir(parents=True)
    saa_zip = saa_dir / "SaaSubscriberFile.zip"
    # Inner file content doesn't matter; we mock everything that reads it.
    with zipfile.ZipFile(saa_zip, "w") as zf:
        zf.writestr("placeholder.xml", b"<root/>")
    return tmp_path


def test_build_sua_short_circuits_when_no_xml_files(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """If the recursive extraction finds no *.xml files (after filtering xsd/),
    we still init the spatialite DB but don't merge anything."""
    nasr_dir = _make_minimal_saa_layout(tmp_path)

    init_called = False

    def fake_init(dst: Path) -> None:
        nonlocal init_called
        init_called = True

    def fake_extract(zip_path: Path, dest: Path) -> None:
        # Don't extract anything, so _build_sua sees zero XML files.
        dest.mkdir(exist_ok=True)

    monkeypatch.setattr(airspace, "_init_spatialite_db", fake_init)
    monkeypatch.setattr(airspace, "_extract_recursive", fake_extract)

    airspace._build_sua(nasr_dir=nasr_dir, dst=tmp_path / "sua.sqlite")

    assert init_called is True


class _FakeConn:
    """Minimal sqlite3.Connection stand-in that records SQL calls."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple]] = []
        self.committed = False
        self.closed = False

    def enable_load_extension(self, _: bool) -> None:
        pass

    def execute(self, sql: str, params: tuple = ()) -> _FakeConn:
        self.executed.append((sql, params))
        return self

    def commit(self) -> None:
        self.committed = True

    def close(self) -> None:
        self.closed = True


def test_build_sua_orchestrates_scan_merge_and_index(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """Full happy path with all pyogrio + spatialite calls mocked. Verifies
    that scan -> merge -> spatial-index runs in the expected order."""
    nasr_dir = _make_minimal_saa_layout(tmp_path)
    dst = tmp_path / "sua.sqlite"

    def fake_extract(zip_path: Path, dest: Path) -> None:
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "AIRSPACE_A.xml").write_text("<root/>")
        (dest / "AIRSPACE_B.xml").write_text("<root/>")

    init_called: list[Path] = []

    def fake_init(d: Path) -> None:
        init_called.append(d)

    list_layers_calls: list[Path] = []

    def fake_list_layers(p: Path) -> list[tuple[str, str]]:
        list_layers_calls.append(p)
        return [("Airspace", "Polygon"), ("Unit", "")]

    def fake_extract_xlinks(p: Path) -> dict:
        return {}

    merge_calls: list[tuple[str, list[airspace.LayerSource]]] = []

    def fake_merge_and_write(
        *, dst: Path, sources: list[airspace.LayerSource], target_layer: str, fk_per_xml: dict
    ) -> tuple[int, bool]:
        merge_calls.append((target_layer, list(sources)))
        # Pretend the Airspace layer has geometry, Unit doesn't.
        return (len(sources), target_layer == "Airspace")

    # Hand the spatial-index pass a fake sqlite3.Connection so we can
    # intercept the CreateSpatialIndex calls without a real extension.
    fake_conn = _FakeConn()
    monkeypatch.setattr(airspace.sqlite3, "connect", lambda _path: fake_conn)
    monkeypatch.setattr(airspace, "_load_mod_spatialite", lambda _conn: None)

    monkeypatch.setattr(airspace, "_extract_recursive", fake_extract)
    monkeypatch.setattr(airspace, "_init_spatialite_db", fake_init)
    monkeypatch.setattr(airspace, "_extract_xlinks", fake_extract_xlinks)
    monkeypatch.setattr(airspace.pyogrio, "list_layers", fake_list_layers)
    monkeypatch.setattr(airspace, "_merge_and_write_layer", fake_merge_and_write)

    airspace._build_sua(nasr_dir=nasr_dir, dst=dst)

    # Init ran, list_layers ran for each XML, merge ran for each unique layer name.
    assert init_called == [dst]
    assert len(list_layers_calls) == 2
    assert sorted(name for name, _ in merge_calls) == ["Airspace", "Unit"]
    # Spatial index built only for the geometry-bearing merged layer.
    spatial_calls = [params for sql, params in fake_conn.executed if "CreateSpatialIndex" in sql]
    assert spatial_calls == [("Airspace", "GEOMETRY")]
    assert fake_conn.committed and fake_conn.closed


def test_build_sua_unlinks_pre_existing_dst(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """If dst already exists, _build_sua deletes it before re-initialising."""
    nasr_dir = _make_minimal_saa_layout(tmp_path)
    dst = tmp_path / "sua.sqlite"
    dst.write_text("stale leftover")

    seen_dst_at_init: list[bool] = []

    def fake_extract(zip_path: Path, dest: Path) -> None:
        dest.mkdir(parents=True, exist_ok=True)

    def fake_init(d: Path) -> None:
        seen_dst_at_init.append(d.exists())

    monkeypatch.setattr(airspace, "_extract_recursive", fake_extract)
    monkeypatch.setattr(airspace, "_init_spatialite_db", fake_init)

    airspace._build_sua(nasr_dir=nasr_dir, dst=dst)

    # When _init_spatialite_db is called, the stale dst is gone.
    assert seen_dst_at_init == [False]


def test_build_sua_skips_xml_when_list_layers_raises(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """A malformed/unreadable XML should not abort the whole build -- just
    skip that file and continue. (Regression for the bare `except Exception`
    in _build_sua's scan loop.)"""
    nasr_dir = _make_minimal_saa_layout(tmp_path)

    def fake_extract(zip_path: Path, dest: Path) -> None:
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "GOOD.xml").write_text("<root/>")
        (dest / "BAD.xml").write_text("<root/>")

    seen: list[Path] = []

    def fake_list_layers(p: Path) -> list[tuple[str, str]]:
        seen.append(p)
        if "BAD" in str(p):
            raise RuntimeError("malformed XML")
        return [("Airspace", "Polygon")]

    merged: list[airspace.LayerSource] = []

    def fake_merge_and_write(*, dst, sources, target_layer, fk_per_xml) -> tuple[int, bool]:
        merged.extend(sources)
        return (len(sources), False)

    monkeypatch.setattr(airspace, "_extract_recursive", fake_extract)
    monkeypatch.setattr(airspace, "_init_spatialite_db", lambda _d: None)
    monkeypatch.setattr(airspace.pyogrio, "list_layers", fake_list_layers)
    monkeypatch.setattr(airspace, "_extract_xlinks", lambda _p: {})
    monkeypatch.setattr(airspace, "_merge_and_write_layer", fake_merge_and_write)

    airspace._build_sua(nasr_dir=nasr_dir, dst=tmp_path / "sua.sqlite")

    # Both files were attempted, but only the good one made it into a bucket.
    assert {p.name for p in seen} == {"GOOD.xml", "BAD.xml"}
    assert len(merged) == 1
    assert merged[0].xml.name == "GOOD.xml"


def test_build_sua_handles_xml_parse_errors_gracefully(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """If _extract_xlinks raises ET.ParseError on a broken XML, we record an
    empty FK lookup for that file and keep going. (Regression for the
    `except ET.ParseError` branch in _build_sua.)"""
    import xml.etree.ElementTree as ET

    nasr_dir = _make_minimal_saa_layout(tmp_path)
    dst = tmp_path / "sua.sqlite"

    def fake_extract(zip_path: Path, dest: Path) -> None:
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "BROKEN.xml").write_text("<not valid xml")

    def fake_init(d: Path) -> None:
        pass

    def fake_list_layers(p: Path) -> list[tuple[str, str]]:
        return [("Airspace", "Polygon")]

    fk_results: dict[Path, dict] = {}

    def fake_extract_xlinks(p: Path) -> dict:
        if "BROKEN" in str(p):
            raise ET.ParseError("malformed XML")
        return {}

    def fake_merge_and_write(*, dst, sources, target_layer, fk_per_xml) -> tuple[int, bool]:
        # Capture what the broken file ended up with in fk_per_xml.
        for src in sources:
            fk_results[src.xml] = fk_per_xml.get(src.xml, "MISSING")  # type: ignore[assignment]
        return (0, False)

    monkeypatch.setattr(airspace, "_extract_recursive", fake_extract)
    monkeypatch.setattr(airspace, "_init_spatialite_db", fake_init)
    monkeypatch.setattr(airspace, "_extract_xlinks", fake_extract_xlinks)
    monkeypatch.setattr(airspace.pyogrio, "list_layers", fake_list_layers)
    monkeypatch.setattr(airspace, "_merge_and_write_layer", fake_merge_and_write)

    # Should not raise.
    airspace._build_sua(nasr_dir=nasr_dir, dst=dst)

    # Broken XML got an empty {} entry rather than crashing the whole build.
    broken_path = next(p for p in fk_results if "BROKEN" in str(p))
    assert fk_results[broken_path] == {}
