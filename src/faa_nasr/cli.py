from __future__ import annotations

from pathlib import Path

import typer

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Build SQLite/SpatiaLite databases from the FAA NASR CSV subscription.",
)


@app.callback()
def _root(
    quiet: bool = typer.Option(
        False, "--quiet", "-q", help="Suppress progress output on stderr."
    ),
) -> None:
    from faa_nasr import _log

    _log.set_quiet(quiet)


@app.command()
def fetch(
    out_dir: Path = typer.Option(Path("./local_data"), "--out", "-o"),
    edition: str = typer.Option("current", "--edition", help="current or next"),
    include_obstacles: bool = typer.Option(True, "--obstacles/--no-obstacles"),
) -> None:
    """Download the current/next NASR subscription zip and (optionally) the daily DOF."""
    from faa_nasr import fetch as _fetch

    _fetch.fetch(out_dir=out_dir, edition=edition, include_obstacles=include_obstacles)


@app.command("build-tables")
def build_tables_cmd(
    csv_dir: Path = typer.Argument(..., help="Directory containing extracted NASR CSVs."),
    db: Path = typer.Option(Path("nasr.sqlite"), "--db"),
    obstacle_csv: Path | None = typer.Option(None, "--obstacle-csv"),
) -> None:
    """Load all NASR CSVs (and optionally DOF.CSV) into a fresh SQLite database."""
    from faa_nasr import tables

    tables.build(csv_dir=csv_dir, db_path=db, obstacle_csv=obstacle_csv)


@app.command("build-spatial")
def build_spatial_cmd(
    src: Path = typer.Argument(..., help="Source SQLite from build-tables."),
    dst: Path = typer.Option(Path("spatialite_nasr.sqlite"), "--db"),
) -> None:
    """Copy the SQLite database and add SpatiaLite geometry columns + spatial indexes."""
    from faa_nasr import geometry

    geometry.build(src=src, dst=dst)


@app.command("build-airspace")
def build_airspace_cmd(
    nasr_dir: Path = typer.Argument(..., help="Top-level extracted NASR directory."),
    out_dir: Path = typer.Option(Path("."), "--out", "-o"),
) -> None:
    """Convert class-airspace shapefiles and SAA AIXM XML into spatialite databases."""
    from faa_nasr import airspace

    airspace.build(nasr_dir=nasr_dir, out_dir=out_dir)


@app.command()
def build(
    out_dir: Path = typer.Option(Path("."), "--out", "-o"),
    work_dir: Path = typer.Option(Path("./local_data"), "--work-dir"),
    edition: str = typer.Option("current", "--edition"),
) -> None:
    """End-to-end pipeline: fetch -> build-tables -> build-spatial -> build-airspace."""
    from faa_nasr import airspace, fetch as _fetch, geometry, tables

    fetched = _fetch.fetch(out_dir=work_dir, edition=edition, include_obstacles=True)
    tables.build(
        csv_dir=fetched.csv_dir,
        db_path=out_dir / "nasr.sqlite",
        obstacle_csv=fetched.obstacle_csv,
    )
    geometry.build(src=out_dir / "nasr.sqlite", dst=out_dir / "spatialite_nasr.sqlite")
    airspace.build(nasr_dir=fetched.nasr_dir, out_dir=out_dir)


if __name__ == "__main__":
    app()
