"""Tests for the typer CLI dispatch in faa_nasr.cli."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from faa_nasr import _log, airspace, cifp, edai, geometry, mirror, tables, tfr, weather
from faa_nasr.cli import app

runner = CliRunner()


@pytest.fixture(autouse=True)
def reset_quiet() -> None:
    _log.set_quiet(False)


def test_root_help_lists_processing_commands():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for cmd in (
        "build-tables",
        "build-spatial",
        "build-airspace",
        "build-edai",
        "build-cifp",
        "fetch-weather",
        "fetch-tfrs",
        "build",
    ):
        assert cmd in result.stdout
    assert "fetch-cifp" not in result.stdout
    assert "fetch-edai" not in result.stdout


def test_root_no_args_shows_help():
    result = runner.invoke(app, [])
    assert result.exit_code == 2
    assert "Usage" in result.stdout


@pytest.mark.parametrize(
    "cmd",
    [
        "build-tables",
        "build-spatial",
        "build-airspace",
        "build-edai",
        "build-cifp",
        "fetch-weather",
        "fetch-tfrs",
        "build",
    ],
)
def test_each_subcommand_help_works(cmd):
    result = runner.invoke(app, [cmd, "--help"])
    assert result.exit_code == 0
    assert cmd in result.stdout


def test_quiet_flag_sets_log_quiet(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, Any] = {}

    def fake_build(csv_dir: Path, db_path: Path, obstacle_csv: Path | None = None) -> None:
        captured["was_quiet"] = _log.is_quiet()

    monkeypatch.setattr(tables, "build", fake_build)

    result = runner.invoke(app, ["--quiet", "build-tables", "/tmp/csvs"])

    assert result.exit_code == 0
    assert captured["was_quiet"] is True


def test_short_quiet_flag_works(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, Any] = {}

    def fake_build(csv_dir: Path, db_path: Path, obstacle_csv: Path | None = None) -> None:
        captured["was_quiet"] = _log.is_quiet()

    monkeypatch.setattr(tables, "build", fake_build)

    result = runner.invoke(app, ["-q", "build-tables", "/tmp/csvs"])

    assert result.exit_code == 0
    assert captured["was_quiet"] is True


def test_default_is_not_quiet(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, Any] = {}

    def fake_build(csv_dir: Path, db_path: Path, obstacle_csv: Path | None = None) -> None:
        captured["was_quiet"] = _log.is_quiet()

    monkeypatch.setattr(tables, "build", fake_build)

    result = runner.invoke(app, ["build-tables", "/tmp/csvs"])

    assert result.exit_code == 0
    assert captured["was_quiet"] is False


def test_build_tables_passes_args(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, Any] = {}

    def fake_build(csv_dir: Path, db_path: Path, obstacle_csv: Path | None = None) -> None:
        captured.update(csv_dir=csv_dir, db_path=db_path, obstacle_csv=obstacle_csv)

    monkeypatch.setattr(tables, "build", fake_build)

    result = runner.invoke(
        app,
        ["build-tables", "/tmp/csvs", "--db", "/tmp/out.sqlite", "--obstacle-csv", "/tmp/DOF.CSV"],
    )

    assert result.exit_code == 0
    assert captured["csv_dir"] == Path("/tmp/csvs")
    assert captured["db_path"] == Path("/tmp/out.sqlite")
    assert captured["obstacle_csv"] == Path("/tmp/DOF.CSV")


def test_build_spatial_passes_db_path(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, Any] = {}

    def fake_build(db_path: Path) -> None:
        captured["db_path"] = db_path

    monkeypatch.setattr(geometry, "build", fake_build)

    result = runner.invoke(app, ["build-spatial", "/tmp/nasr.sqlite"])

    assert result.exit_code == 0
    assert captured["db_path"] == Path("/tmp/nasr.sqlite")


def test_build_airspace_passes_args(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, Any] = {}

    def fake_build(nasr_dir: Path, out_dir: Path) -> None:
        captured.update(nasr_dir=nasr_dir, out_dir=out_dir)

    monkeypatch.setattr(airspace, "build", fake_build)

    result = runner.invoke(app, ["build-airspace", "/tmp/nasr", "--out", "/tmp/out"])

    assert result.exit_code == 0
    assert captured["nasr_dir"] == Path("/tmp/nasr")
    assert captured["out_dir"] == Path("/tmp/out")


def test_build_edai_uses_explicit_extract_dir(monkeypatch: pytest.MonkeyPatch, tmp_path):
    captured: dict[str, Any] = {}

    def fake_build(out_dir: Path, extract_dir: Path) -> None:
        captured["out_dir"] = out_dir
        captured["extract_dir"] = extract_dir

    monkeypatch.setattr(edai, "build", fake_build)

    result = runner.invoke(
        app,
        ["build-edai", "--out", str(tmp_path / "out"), "--edai-dir", str(tmp_path / "edai")],
    )

    assert result.exit_code == 0
    assert captured["out_dir"] == tmp_path / "out"
    assert captured["extract_dir"] == tmp_path / "edai"


def test_fetch_weather_passes_out_dir(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, Any] = {}

    def fake_fetch(out_dir: Path) -> Path:
        captured["out_dir"] = out_dir
        return out_dir / "weather.sqlite"

    monkeypatch.setattr(weather, "fetch", fake_fetch)

    result = runner.invoke(app, ["fetch-weather", "--out", "/tmp/wx"])

    assert result.exit_code == 0
    assert captured["out_dir"] == Path("/tmp/wx")


def test_fetch_tfrs_passes_out_dir(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, Any] = {}

    def fake_fetch(out_dir: Path) -> Path:
        captured["out_dir"] = out_dir
        return out_dir / "tfrs.sqlite"

    monkeypatch.setattr(tfr, "fetch", fake_fetch)

    result = runner.invoke(app, ["fetch-tfrs", "--out", "/tmp/tfr"])

    assert result.exit_code == 0
    assert captured["out_dir"] == Path("/tmp/tfr")


def test_build_chains_all_stages_from_mirror(monkeypatch: pytest.MonkeyPatch, tmp_path):
    calls: list[str] = []
    mirror_inputs = mirror.MirrorInputs(
        nasr_dir=tmp_path / "nasr",
        csv_dir=tmp_path / "csvs",
        obstacle_csv=tmp_path / "DOF.CSV",
        cifp_file=tmp_path / "FAACIFP18",
        edai_dir=tmp_path / "edai",
    )
    captured: dict[str, Any] = {}

    def fake_resolve_inputs(
        mirror_root: Path,
        manifest_path: Path | None = None,
        *,
        include_cifp: bool = True,
        include_edai: bool = True,
    ) -> mirror.MirrorInputs:
        captured["mirror_root"] = mirror_root
        captured["manifest_path"] = manifest_path
        captured["include_cifp"] = include_cifp
        captured["include_edai"] = include_edai
        return mirror_inputs

    def fake_tables_build(csv_dir: Path, db_path: Path, obstacle_csv: Path | None = None) -> None:
        calls.append("tables")
        captured["tables_csv_dir"] = csv_dir
        captured["tables_obstacle"] = obstacle_csv
        captured["nasr_db"] = db_path

    def fake_geometry_build(db_path: Path) -> None:
        calls.append("geometry")
        captured["geometry_db"] = db_path

    def fake_airspace_build(nasr_dir: Path, out_dir: Path) -> None:
        calls.append("airspace")
        captured["airspace_nasr_dir"] = nasr_dir

    def fake_edai_build(out_dir: Path, extract_dir: Path) -> None:
        calls.append("edai")
        captured["edai_extract_dir"] = extract_dir

    def fake_cifp_build(cifp_path: Path, db_path: Path) -> None:
        calls.append("cifp")
        captured["cifp_path"] = cifp_path
        captured["cifp_db"] = db_path

    monkeypatch.setattr(mirror, "resolve_inputs", fake_resolve_inputs)
    monkeypatch.setattr(tables, "build", fake_tables_build)
    monkeypatch.setattr(geometry, "build", fake_geometry_build)
    monkeypatch.setattr(airspace, "build", fake_airspace_build)
    monkeypatch.setattr(edai, "build", fake_edai_build)
    monkeypatch.setattr(cifp, "build", fake_cifp_build)

    manifest = tmp_path / "aviation_data" / "manifest.json"
    result = runner.invoke(
        app,
        ["build", "--out", str(tmp_path / "out"), "--mirror-manifest", str(manifest)],
    )

    assert result.exit_code == 0, result.output
    assert calls == ["tables", "geometry", "airspace", "edai", "cifp"]
    assert captured["nasr_db"] == captured["geometry_db"] == tmp_path / "out" / "nasr.sqlite"
    assert captured["tables_csv_dir"] == mirror_inputs.csv_dir
    assert captured["tables_obstacle"] == mirror_inputs.obstacle_csv
    assert captured["airspace_nasr_dir"] == mirror_inputs.nasr_dir
    assert captured["edai_extract_dir"] == mirror_inputs.edai_dir
    assert captured["cifp_path"] == mirror_inputs.cifp_file
    assert captured["cifp_db"] == tmp_path / "out" / "cifp.sqlite"
