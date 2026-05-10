"""Tests for the I/O-touching helpers in faa_nasr.airspace.

These cover the functions that don't need pyogrio or mod_spatialite:
zip extraction, raw sqlite3 attribute-table writing, the XML parsing
orchestrator, and the top-level build dispatcher.
"""

from __future__ import annotations

import sqlite3
import zipfile
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from faa_nasr import airspace


def _make_zip(path: Path, files: dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, "w") as zf:
        for name, data in files.items():
            zf.writestr(name, data)


# ---------------------------------------------------------------------------
# _extract_recursive
# ---------------------------------------------------------------------------


def test_extract_recursive_extracts_top_level_zip(tmp_path):
    src = tmp_path / "outer.zip"
    _make_zip(src, {"a.txt": b"x"})
    dest = tmp_path / "dest"
    dest.mkdir()

    airspace._extract_recursive(src, dest)

    assert (dest / "a.txt").read_bytes() == b"x"


def test_extract_recursive_walks_nested_zips(tmp_path):
    """The SAA bundle ships SaaSubscriberFile.zip containing Saa_Sub_File.zip
    containing the per-airspace XML files. Recurse one level deep."""
    inner = tmp_path / "inner.zip"
    _make_zip(inner, {"AIRSPACE.xml": b"<root/>"})
    outer = tmp_path / "outer.zip"
    with zipfile.ZipFile(outer, "w") as zf:
        zf.write(inner, arcname="Saa_Sub_File.zip")
    dest = tmp_path / "dest"
    dest.mkdir()

    airspace._extract_recursive(outer, dest)

    # Recursion extracted Saa_Sub_File.zip to dest/Saa_Sub_File/, where its
    # contents (AIRSPACE.xml) ended up.
    assert (dest / "Saa_Sub_File" / "AIRSPACE.xml").read_bytes() == b"<root/>"


# ---------------------------------------------------------------------------
# _write_attribute_only_table
# ---------------------------------------------------------------------------


def test_write_attribute_only_table_creates_text_columns(tmp_path):
    db = tmp_path / "out.sqlite"
    fields = ["name", "designator", "_source_xml"]
    field_data = [
        np.array(["Unit A", "Unit B"], dtype=object),
        np.array(["DSG1", "DSG2"], dtype=object),
        np.array(["AIRSPACE_X", "AIRSPACE_Y"], dtype=object),
    ]

    airspace._write_attribute_only_table(db, "Unit", fields, field_data)

    conn = sqlite3.connect(db)
    try:
        rows = conn.execute("SELECT name, designator, _source_xml FROM Unit").fetchall()
        assert rows == [
            ("Unit A", "DSG1", "AIRSPACE_X"),
            ("Unit B", "DSG2", "AIRSPACE_Y"),
        ]
    finally:
        conn.close()


def test_write_attribute_only_table_replaces_existing(tmp_path):
    db = tmp_path / "out.sqlite"
    fields = ["x"]
    airspace._write_attribute_only_table(db, "T", fields, [np.array(["a"], dtype=object)])
    airspace._write_attribute_only_table(db, "T", fields, [np.array(["b", "c"], dtype=object)])

    conn = sqlite3.connect(db)
    try:
        rows = conn.execute("SELECT x FROM T ORDER BY x").fetchall()
        assert rows == [("b",), ("c",)]
    finally:
        conn.close()


def test_write_attribute_only_table_converts_none_to_empty(tmp_path):
    db = tmp_path / "out.sqlite"
    fields = ["a"]
    field_data = [np.array(["x", None, "z"], dtype=object)]

    airspace._write_attribute_only_table(db, "T", fields, field_data)

    conn = sqlite3.connect(db)
    try:
        rows = conn.execute("SELECT a FROM T").fetchall()
        assert rows == [("x",), ("",), ("z",)]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# _extract_xlinks (file orchestrator)
# ---------------------------------------------------------------------------


_AIXM_NS = (
    'xmlns="http://www.aixm.aero/schema/5.0" '
    'xmlns:gml="http://www.opengis.net/gml/3.2" '
    'xmlns:xlink="http://www.w3.org/1999/xlink"'
)


def test_extract_xlinks_parses_file_and_returns_fk_map(tmp_path):
    xml = tmp_path / "test.xml"
    xml.write_text(f"""<?xml version="1.0"?>
<root {_AIXM_NS}>
  <Airspace gml:id="Airspace1"><identifier>uuid-airspace</identifier></Airspace>
  <Unit gml:id="Unit1">
    <identifier>uuid-unit</identifier>
    <ownerOrganisation xlink:href="#NotPresent"/>
  </Unit>
  <AirspaceUsage gml:id="Usage1">
    <identifier>uuid-usage</identifier>
    <restrictedAirspace xlink:href="#Airspace1"/>
  </AirspaceUsage>
</root>
""")

    fks = airspace._extract_xlinks(xml)

    # Resolved reference -> Airspace UUID.
    assert fks[airspace.FeatureRef("AirspaceUsage", "Usage1")] == {
        "restrictedAirspace": "uuid-airspace",
    }
    # Unresolvable href is silently dropped.
    assert airspace.FeatureRef("Unit", "Unit1") not in fks


# ---------------------------------------------------------------------------
# build() top-level orchestrator
# ---------------------------------------------------------------------------


def test_build_calls_class_airspace_and_sua_with_correct_paths(
    monkeypatch: pytest.MonkeyPatch, tmp_path
):
    captured: dict[str, Any] = {}

    def fake_class(nasr_dir: Path, dst: Path) -> None:
        captured["class_nasr_dir"] = nasr_dir
        captured["class_dst"] = dst

    def fake_sua(nasr_dir: Path, dst: Path) -> None:
        captured["sua_nasr_dir"] = nasr_dir
        captured["sua_dst"] = dst

    monkeypatch.setattr(airspace, "_build_class_airspace", fake_class)
    monkeypatch.setattr(airspace, "_build_sua", fake_sua)

    airspace.build(nasr_dir=tmp_path / "nasr", out_dir=tmp_path / "out")

    out = (tmp_path / "out").resolve()
    nasr = (tmp_path / "nasr").resolve()
    assert captured["class_nasr_dir"] == nasr
    assert captured["class_dst"] == out / airspace.CLASS_AIRSPACE_DB
    assert captured["sua_nasr_dir"] == nasr
    assert captured["sua_dst"] == out / airspace.SUA_DB


def test_build_creates_out_dir_if_missing(monkeypatch: pytest.MonkeyPatch, tmp_path):
    monkeypatch.setattr(airspace, "_build_class_airspace", lambda **_: None)
    monkeypatch.setattr(airspace, "_build_sua", lambda **_: None)
    out = tmp_path / "newly_created"

    airspace.build(nasr_dir=tmp_path, out_dir=out)

    assert out.is_dir()


# ---------------------------------------------------------------------------
# _build_class_airspace early-return when shape dir missing
# ---------------------------------------------------------------------------


def test_build_class_airspace_skips_when_no_shapefile_dir(tmp_path):
    """If Shape_Files/ doesn't exist, the function should log + return without
    creating the dst DB."""
    nasr_dir = tmp_path / "nasr"
    nasr_dir.mkdir()
    dst = tmp_path / "out.sqlite"

    airspace._build_class_airspace(nasr_dir=nasr_dir, dst=dst)

    assert not dst.exists()


# ---------------------------------------------------------------------------
# _build_sua early-return when SaaSubscriberFile.zip missing
# ---------------------------------------------------------------------------


def test_build_sua_skips_when_no_saa_zip(tmp_path):
    """Same shape: missing source -> skip without writing anything."""
    nasr_dir = tmp_path / "nasr"
    nasr_dir.mkdir()
    dst = tmp_path / "sua.sqlite"

    airspace._build_sua(nasr_dir=nasr_dir, dst=dst)

    assert not dst.exists()


# ---------------------------------------------------------------------------
# _merge_and_write_layer orchestrator
# ---------------------------------------------------------------------------


def test_merge_and_write_layer_returns_zero_when_no_chunks_read(
    monkeypatch: pytest.MonkeyPatch, tmp_path
):
    """If _read_layer_source returns None for every source (empty / unreadable),
    the orchestrator returns (0, False) without calling _write_merged_layer."""
    write_called = False

    def fake_read(*args: Any, **kwargs: Any) -> None:
        return None

    def fake_write(*args: Any, **kwargs: Any) -> None:
        nonlocal write_called
        write_called = True

    monkeypatch.setattr(airspace, "_read_layer_source", fake_read)
    monkeypatch.setattr(airspace, "_write_merged_layer", fake_write)

    sources = [airspace.LayerSource(xml=tmp_path / "x.xml", source_layer="Foo")]
    n, has_geom = airspace._merge_and_write_layer(
        dst=tmp_path / "out.sqlite",
        sources=sources,
        target_layer="Foo",
        fk_per_xml={},
    )

    assert (n, has_geom) == (0, False)
    assert write_called is False


def test_merge_and_write_layer_calls_write_with_merged_payload(
    monkeypatch: pytest.MonkeyPatch, tmp_path
):
    """When chunks come back from _read_layer_source, the orchestrator merges
    them (via the real _merge_chunks) and writes once."""
    chunk = airspace._SourceChunk(
        xml_stem="X",
        n_rows=2,
        geometry=np.array([b"GEOM", b"GEOM"], dtype=object),
        fields={"name": np.array(["A", "B"], dtype=object)},
        fks={},
        geom_type="Polygon",
        crs=None,
    )

    write_args: dict[str, Any] = {}

    monkeypatch.setattr(airspace, "_read_layer_source", lambda *_a, **_k: chunk)

    def fake_write(*, dst: Path, merged: airspace._MergedLayer, target_layer: str) -> None:
        write_args.update(dst=dst, merged=merged, target_layer=target_layer)

    monkeypatch.setattr(airspace, "_write_merged_layer", fake_write)

    sources = [airspace.LayerSource(xml=tmp_path / "x.xml", source_layer="Foo")]
    n, has_geom = airspace._merge_and_write_layer(
        dst=tmp_path / "out.sqlite",
        sources=sources,
        target_layer="Foo",
        fk_per_xml={},
    )

    assert (n, has_geom) == (2, True)
    assert write_args["dst"] == tmp_path / "out.sqlite"
    assert write_args["target_layer"] == "Foo"
    # Merged payload includes _source_xml column.
    assert "_source_xml" in write_args["merged"].fields
