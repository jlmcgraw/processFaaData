"""Tests for faa_nasr.edai."""

from __future__ import annotations

from pathlib import Path

from faa_nasr import edai


def test_build_calls_copy_shapefile_for_every_shp(monkeypatch, tmp_path):
    extract_dir = tmp_path / "extracted"
    (extract_dir / "Airports").mkdir(parents=True)
    (extract_dir / "Runways").mkdir()
    (extract_dir / "Airports" / "Airports.shp").touch()
    (extract_dir / "Runways" / "Runways.shp").touch()

    init_called: list[Path] = []
    copy_calls: list[tuple[Path, str]] = []

    def fake_init(d: Path) -> None:
        init_called.append(d)

    def fake_copy(src: Path, dst: Path, layer_name: str) -> int:
        copy_calls.append((src, layer_name))
        return 100

    monkeypatch.setattr(edai, "_init_spatialite_db", fake_init)
    monkeypatch.setattr(edai, "_copy_shapefile", fake_copy)

    out_dir = tmp_path / "out"
    edai.build(out_dir=out_dir, extract_dir=extract_dir)

    expected_db = (out_dir / edai.EDAI_OUTPUT_DB).resolve()
    assert init_called == [expected_db]
    assert sorted(name for _, name in copy_calls) == ["Airports", "Runways"]


def test_build_unlinks_existing_db(monkeypatch, tmp_path):
    extract_dir = tmp_path / "extracted"
    extract_dir.mkdir()

    seen_dst_at_init: list[bool] = []

    def fake_init(d: Path) -> None:
        seen_dst_at_init.append(d.exists())

    monkeypatch.setattr(edai, "_init_spatialite_db", fake_init)
    monkeypatch.setattr(edai, "_copy_shapefile", lambda src, dst, layer_name: 0)

    out_dir = tmp_path / "out"
    out_dir.mkdir()
    stale = out_dir / edai.EDAI_OUTPUT_DB
    stale.write_text("stale leftover")

    edai.build(out_dir=out_dir, extract_dir=extract_dir)

    assert seen_dst_at_init == [False]
