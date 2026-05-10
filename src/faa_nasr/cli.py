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
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress progress output on stderr."),
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
    db: Path = typer.Argument(..., help="SQLite DB to add geometry to (modified in-place)."),
) -> None:
    """Add SpatiaLite geometry columns and spatial indexes to an existing NASR DB.

    Operates in place on the SQLite file produced by `build-tables`. Idempotent:
    tables that already have geometry are skipped.
    """
    from faa_nasr import geometry

    geometry.build(db_path=db)


@app.command("build-airspace")
def build_airspace_cmd(
    nasr_dir: Path = typer.Argument(..., help="Top-level extracted NASR directory."),
    out_dir: Path = typer.Option(Path("."), "--out", "-o"),
) -> None:
    """Convert class-airspace shapefiles and SAA AIXM XML into spatialite databases."""
    from faa_nasr import airspace

    airspace.build(nasr_dir=nasr_dir, out_dir=out_dir)


@app.command("fetch-edai")
def fetch_edai_cmd(
    out_dir: Path = typer.Option(Path("./local_data"), "--out", "-o"),
    include_pending: bool = typer.Option(
        False, "--include-pending", help="Also fetch draft 'Pending' datasets."
    ),
) -> None:
    """Download FAA EDAI shapefile datasets from ArcGIS Hub (timestamp-cached).

    Dataset list is fetched dynamically from the FAA's DCAT catalog -- we
    always pick up the current set instead of relying on a hardcoded GUID list.
    """
    from faa_nasr import edai

    edai.fetch(out_dir=out_dir, include_pending=include_pending)


@app.command("build-edai")
def build_edai_cmd(
    out_dir: Path = typer.Option(Path("."), "--out", "-o"),
    work_dir: Path = typer.Option(Path("./local_data"), "--work-dir"),
    include_pending: bool = typer.Option(
        False, "--include-pending", help="Also include draft 'Pending' datasets."
    ),
) -> None:
    """Fetch EDAI shapefile datasets and build edai_spatialite.sqlite."""
    from faa_nasr import edai

    fetched = edai.fetch(out_dir=work_dir, include_pending=include_pending)
    edai.build(out_dir=out_dir, extract_dir=fetched.extract_dir)


@app.command()
def build(
    out_dir: Path = typer.Option(Path("."), "--out", "-o"),
    work_dir: Path = typer.Option(Path("./local_data"), "--work-dir"),
    edition: str = typer.Option("current", "--edition"),
) -> None:
    """End-to-end pipeline: fetch -> build-tables -> build-spatial -> build-airspace."""
    from faa_nasr import airspace, geometry, tables
    from faa_nasr import fetch as _fetch

    fetched = _fetch.fetch(out_dir=work_dir, edition=edition, include_obstacles=True)
    nasr_db = out_dir / "nasr.sqlite"
    tables.build(
        csv_dir=fetched.csv_dir,
        db_path=nasr_db,
        obstacle_csv=fetched.obstacle_csv,
    )
    geometry.build(db_path=nasr_db)
    airspace.build(nasr_dir=fetched.nasr_dir, out_dir=out_dir)


if __name__ == "__main__":  # pragma: no cover
    app()
