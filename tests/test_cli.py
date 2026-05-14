"""Tests for the typer CLI dispatch in faa_nasr.cli.

These tests use typer's CliRunner. Each subcommand mocks the underlying
module function so we test argument parsing + wiring, not the real pipeline.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from faa_nasr import _log, airspace, edai, fetch, geometry, tables, tfr, weather
from faa_nasr.cli import app

runner = CliRunner()


@pytest.fixture(autouse=True)
def reset_quiet() -> None:
    """The --quiet flag mutates module-level state in _log; reset it."""
    _log.set_quiet(False)


# ---------------------------------------------------------------------------
# --help
# ---------------------------------------------------------------------------


def test_root_help_lists_all_commands():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for cmd in (
        "fetch",
        "build-tables",
        "build-spatial",
        "build-airspace",
        "fetch-weather",
        "fetch-tfrs",
        "build",
    ):
        assert cmd in result.stdout


def test_root_no_args_shows_help():
    # `no_args_is_help=True` is set on the Typer app -- bare `nasr` should
    # exit 2 with help text rather than running anything.
    result = runner.invoke(app, [])
    assert result.exit_code == 2
    assert "Usage" in result.stdout


@pytest.mark.parametrize(
    "cmd",
    [
        "fetch",
        "build-tables",
        "build-spatial",
        "build-airspace",
        "fetch-weather",
        "fetch-tfrs",
        "build",
    ],
)
def test_each_subcommand_help_works(cmd):
    result = runner.invoke(app, [cmd, "--help"])
    assert result.exit_code == 0
    assert cmd in result.stdout


# ---------------------------------------------------------------------------
# --quiet flag propagation
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Argument plumbing per subcommand
# ---------------------------------------------------------------------------


def test_fetch_passes_args_through(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, Any] = {}

    def fake_fetch(out_dir: Path, edition: str, include_obstacles: bool) -> None:
        captured.update(out_dir=out_dir, edition=edition, include_obstacles=include_obstacles)

    monkeypatch.setattr(fetch, "fetch", fake_fetch)

    result = runner.invoke(
        app,
        ["fetch", "--out", "/tmp/data", "--edition", "next", "--no-obstacles"],
    )

    assert result.exit_code == 0
    assert captured["out_dir"] == Path("/tmp/data")
    assert captured["edition"] == "next"
    assert captured["include_obstacles"] is False


def test_fetch_uses_defaults_when_no_args(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, Any] = {}

    def fake_fetch(out_dir: Path, edition: str, include_obstacles: bool) -> None:
        captured.update(out_dir=out_dir, edition=edition, include_obstacles=include_obstacles)

    monkeypatch.setattr(fetch, "fetch", fake_fetch)

    result = runner.invoke(app, ["fetch"])

    assert result.exit_code == 0
    assert captured["out_dir"] == Path("./local_data")
    assert captured["edition"] == "current"
    assert captured["include_obstacles"] is True


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


def test_build_tables_obstacle_csv_optional(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, Any] = {}

    def fake_build(csv_dir: Path, db_path: Path, obstacle_csv: Path | None = None) -> None:
        captured["obstacle_csv"] = obstacle_csv

    monkeypatch.setattr(tables, "build", fake_build)

    result = runner.invoke(app, ["build-tables", "/tmp/csvs"])

    assert result.exit_code == 0
    assert captured["obstacle_csv"] is None


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


def test_fetch_edai_passes_out_dir(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, Any] = {}

    def fake_fetch(out_dir: Path, include_pending: bool = False) -> edai.EdaiFetchResult:
        captured["out_dir"] = out_dir
        return edai.EdaiFetchResult(
            download_dir=out_dir / "edai_downloads",
            extract_dir=out_dir / "edai_extracted",
        )

    monkeypatch.setattr(edai, "fetch", fake_fetch)

    result = runner.invoke(app, ["fetch-edai", "--out", "/tmp/edai"])

    assert result.exit_code == 0
    assert captured["out_dir"] == Path("/tmp/edai")


def test_build_edai_chains_fetch_then_build(monkeypatch: pytest.MonkeyPatch, tmp_path):
    captured: dict[str, Any] = {}
    fetched = edai.EdaiFetchResult(download_dir=tmp_path / "dl", extract_dir=tmp_path / "ex")

    def fake_fetch(out_dir: Path, include_pending: bool = False) -> edai.EdaiFetchResult:
        captured["fetch_out_dir"] = out_dir
        return fetched

    def fake_build(out_dir: Path, extract_dir: Path) -> None:
        captured["build_out_dir"] = out_dir
        captured["build_extract_dir"] = extract_dir

    monkeypatch.setattr(edai, "fetch", fake_fetch)
    monkeypatch.setattr(edai, "build", fake_build)

    result = runner.invoke(
        app, ["build-edai", "--out", str(tmp_path / "out"), "--work-dir", str(tmp_path / "work")]
    )

    assert result.exit_code == 0
    assert captured["fetch_out_dir"] == tmp_path / "work"
    assert captured["build_out_dir"] == tmp_path / "out"
    # The build step gets the extract dir from the fetch result.
    assert captured["build_extract_dir"] == fetched.extract_dir


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


def test_build_chains_all_stages(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """End-to-end `build` should call fetch -> tables.build -> geometry.build
    -> airspace.build in order, threading the NASR DB path through.
    """
    calls: list[str] = []

    fetched = fetch.FetchResult(
        nasr_dir=tmp_path / "nasr",
        csv_dir=tmp_path / "csvs",
        obstacle_csv=tmp_path / "DOF.CSV",
    )

    def fake_fetch(out_dir: Path, edition: str, include_obstacles: bool) -> fetch.FetchResult:
        calls.append("fetch")
        return fetched

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

    edai_fetched = edai.EdaiFetchResult(
        download_dir=tmp_path / "edai_downloads",
        extract_dir=tmp_path / "edai_extracted",
    )

    def fake_edai_fetch(out_dir: Path, include_pending: bool = False) -> edai.EdaiFetchResult:
        calls.append("edai_fetch")
        captured["edai_include_pending"] = include_pending
        return edai_fetched

    def fake_edai_build(out_dir: Path, extract_dir: Path) -> None:
        calls.append("edai_build")
        captured["edai_extract_dir"] = extract_dir

    captured: dict[str, Any] = {}

    monkeypatch.setattr(fetch, "fetch", fake_fetch)
    monkeypatch.setattr(tables, "build", fake_tables_build)
    monkeypatch.setattr(geometry, "build", fake_geometry_build)
    monkeypatch.setattr(airspace, "build", fake_airspace_build)
    monkeypatch.setattr(edai, "fetch", fake_edai_fetch)
    monkeypatch.setattr(edai, "build", fake_edai_build)

    result = runner.invoke(app, ["build", "--out", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert calls == ["fetch", "tables", "geometry", "airspace", "edai_fetch", "edai_build"]
    # The same nasr.sqlite path is threaded through both build steps.
    assert captured["nasr_db"] == captured["geometry_db"] == tmp_path / "nasr.sqlite"
    assert captured["tables_csv_dir"] == fetched.csv_dir
    assert captured["tables_obstacle"] == fetched.obstacle_csv
    assert captured["airspace_nasr_dir"] == fetched.nasr_dir
    assert captured["edai_extract_dir"] == edai_fetched.extract_dir
    assert captured["edai_include_pending"] is False
