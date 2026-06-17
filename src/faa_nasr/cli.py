from __future__ import annotations

from pathlib import Path

import typer

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Build SQLite/SpatiaLite databases from aviation-data-mirror NASR and CIFP data.",
)


@app.callback()
def _root(
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress progress output on stderr."),
) -> None:
    from faa_nasr import _log

    _log.set_quiet(quiet)


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


@app.command("build-edai")
def build_edai_cmd(
    out_dir: Path = typer.Option(Path("."), "--out", "-o"),
    edai_dir: Path | None = typer.Option(
        None,
        "--edai-dir",
        help=(
            "Directory containing extracted EDAI shapefiles. "
            "Defaults to aviation-data-mirror manifest."
        ),
    ),
    mirror_root: Path = typer.Option(Path("./aviation_data"), "--mirror-root"),
    mirror_manifest: Path | None = typer.Option(None, "--mirror-manifest"),
) -> None:
    """Build edai_spatialite.sqlite from aviation-data-mirror EDAI artifacts."""
    from faa_nasr import edai, mirror

    try:
        extract_dir = edai_dir or mirror.resolve_edai_dir(
            manifest=mirror.load_manifest(mirror_root, mirror_manifest),
            mirror_root=mirror_root,
        )
    except FileNotFoundError as exc:
        raise typer.BadParameter(str(exc)) from exc
    if extract_dir is None:
        raise FileNotFoundError("No EDAI artifacts found in the aviation-data-mirror manifest.")
    edai.build(out_dir=out_dir, extract_dir=extract_dir)


@app.command("fetch-weather")
def fetch_weather_cmd(
    out_dir: Path = typer.Option(Path("."), "--out", "-o"),
) -> None:
    """Fetch current aviation weather (METAR/TAF/PIREP/AIRMET/SIGMET) into weather.sqlite.

    Hits aviationweather.gov's GeoJSON API. Each refresh rebuilds the DB --
    this is realtime data, not a publication cycle, so run on whatever
    cadence your map needs (every 5-10 minutes is typical).
    """
    from faa_nasr import weather

    weather.fetch(out_dir=out_dir)


@app.command("fetch-tfrs")
def fetch_tfrs_cmd(
    out_dir: Path = typer.Option(Path("."), "--out", "-o"),
) -> None:
    """Fetch active TFR polygons + metadata into tfrs.sqlite.

    Pulls polygons from tfr.faa.gov's WFS endpoint and joins per-TFR
    metadata from the JSON list API. No-shape TFRs (issued but polygon
    pending) land in a separate attribute-only table.
    """
    from faa_nasr import tfr

    tfr.fetch(out_dir=out_dir)


@app.command("build-cifp")
def build_cifp_cmd(
    cifp_file: Path = typer.Argument(..., help="Path to FAACIFP18 file."),
    db: Path = typer.Option(Path("cifp.sqlite"), "--db"),
) -> None:
    """Parse FAACIFP18 into SQLite (with SpatiaLite geometry for lat/lon fields)."""
    from faa_nasr import cifp

    cifp.build(cifp_path=cifp_file, db_path=db)


@app.command("build-cifp-spatial")
def build_cifp_spatial_cmd(
    db: Path = typer.Argument(..., help="cifp.sqlite produced by build-cifp."),
    design_speed: float = typer.Option(
        150.0,
        "--design-speed",
        help=(
            "Design speed in knots used to compute turn-anticipation arc radius "
            "(r = V / (60·π) nm, standard 3°/s rate). "
            "Use 0 for straight fix-to-fix lines."
        ),
    ),
) -> None:
    """Add LINESTRING procedure-path geometries to an existing cifp.sqlite.

    Creates the ``procedure_paths`` SpatiaLite table with one row per unique
    (airport, procedure, transition) combination, covering IAP/SID/STAR and
    heliport equivalents.  Requires mod_spatialite — run inside the container.
    """
    from faa_nasr import cifp

    cifp.build_spatial(db_path=db, design_speed_kts=design_speed)


@app.command()
def build(
    out_dir: Path = typer.Option(Path("."), "--out", "-o"),
    mirror_root: Path = typer.Option(Path("./aviation_data"), "--mirror-root"),
    mirror_manifest: Path | None = typer.Option(None, "--mirror-manifest"),
    nasr_dir: Path | None = typer.Option(
        None,
        "--nasr-dir",
        help="Top-level extracted NASR directory. Defaults to aviation-data-mirror manifest.",
    ),
    csv_dir: Path | None = typer.Option(
        None,
        "--csv-dir",
        help="Extracted NASR CSV directory. Defaults to aviation-data-mirror manifest.",
    ),
    obstacle_csv: Path | None = typer.Option(
        None,
        "--obstacle-csv",
        help="Optional DOF.CSV path. Defaults to aviation-data-mirror manifest when available.",
    ),
    edai_dir: Path | None = typer.Option(
        None,
        "--edai-dir",
        help="Extracted EDAI shapefile directory. Defaults to aviation-data-mirror manifest.",
    ),
    cifp_file: Path | None = typer.Option(
        None,
        "--cifp-file",
        help="Path to FAACIFP18. Defaults to aviation-data-mirror manifest when --cifp is enabled.",
    ),
    include_cifp: bool = typer.Option(
        True, "--cifp/--no-cifp", help="Build cifp.sqlite from aviation-data-mirror CIFP data."
    ),
    include_edai: bool = typer.Option(
        True,
        "--edai/--no-edai",
        help="Build edai_spatialite.sqlite from aviation-data-mirror EDAI data.",
    ),
) -> None:
    """End-to-end pipeline from aviation-data-mirror data to SQLite/SpatiaLite outputs."""
    from faa_nasr import airspace, cifp, edai, geometry, mirror, tables

    mirror_inputs: mirror.MirrorInputs | None = None
    if (
        nasr_dir is None
        or csv_dir is None
        or (include_cifp and cifp_file is None)
        or (include_edai and edai_dir is None)
    ):
        try:
            mirror_inputs = mirror.resolve_inputs(
                mirror_root=mirror_root,
                manifest_path=mirror_manifest,
                include_cifp=include_cifp,
                include_edai=include_edai,
            )
        except FileNotFoundError as exc:
            raise typer.BadParameter(str(exc)) from exc

    resolved_nasr_dir = nasr_dir or _required_mirror_input(mirror_inputs, "nasr_dir")
    resolved_csv_dir = csv_dir or _required_mirror_input(mirror_inputs, "csv_dir")
    resolved_obstacle_csv = (
        obstacle_csv
        if obstacle_csv is not None
        else _optional_mirror_input(mirror_inputs, "obstacle_csv")
    )
    resolved_cifp_file = (
        cifp_file if cifp_file is not None else _optional_mirror_input(mirror_inputs, "cifp_file")
    )
    resolved_edai_dir = (
        edai_dir if edai_dir is not None else _optional_mirror_input(mirror_inputs, "edai_dir")
    )

    nasr_db = out_dir / "nasr.sqlite"
    tables.build(
        csv_dir=resolved_csv_dir,
        db_path=nasr_db,
        obstacle_csv=resolved_obstacle_csv,
    )
    geometry.build(db_path=nasr_db)
    airspace.build(nasr_dir=resolved_nasr_dir, out_dir=out_dir)
    if include_edai:
        if resolved_edai_dir is None:
            raise FileNotFoundError(
                "No EDAI input found. Pass --edai-dir or refresh aviation_data."
            )
        edai.build(out_dir=out_dir, extract_dir=resolved_edai_dir)
    if include_cifp:
        if resolved_cifp_file is None:
            raise FileNotFoundError(
                "No CIFP input found. Pass --cifp-file or refresh aviation_data."
            )
        cifp.build(cifp_path=resolved_cifp_file, db_path=out_dir / "cifp.sqlite")


def _required_mirror_input(inputs: object | None, attr: str) -> Path:
    value = _optional_mirror_input(inputs, attr)
    if value is None:
        raise FileNotFoundError(f"aviation-data-mirror did not provide required input {attr}.")
    return value


def _optional_mirror_input(inputs: object | None, attr: str) -> Path | None:
    if inputs is None:
        return None
    value = getattr(inputs, attr)
    return value if isinstance(value, Path) else None


if __name__ == "__main__":  # pragma: no cover
    app()
